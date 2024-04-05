// Copyright 2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! TCP Client connector - maintains one connection to the configured upstream host
//!
//! The sink received events from the runtime and writes it to the TCP or TLS stream.
//! Data received from the TCP or TLS connection is forwarded to the source of this connector.
#![allow(clippy::module_name_repetitions)]

use super::TcpReader;
use crate::{
    errors::error_connector_def,
    prelude::*,
    utils::{
        socket::{self, tcp_client, TcpSocketOptions},
        tls::TLSClientConfig,
    },
};
use either::Either;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_rustls::TlsConnector;

const URL_SCHEME: &str = "tremor-tcp-client";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    url: Url<super::TcpDefaults>,
    // IP_TTL for ipv4 and hop limit for ipv6
    //ttl: Option<u32>,
    #[serde(default = "default_buf_size")]
    buf_size: usize,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
    #[serde(default)]
    socket_options: TcpSocketOptions,
}

impl tremor_config::Impl for Config {}

pub(crate) struct TcpClient {
    config: Config,
    tls_connector: Option<TlsConnector>,
    tls_domain: Option<String>,
    source_tx: Sender<SourceReply>,
    source_rx: Option<Receiver<SourceReply>>,
}

/// TCP client builder
#[derive(Debug, Default)]
pub struct Builder {}

impl Builder {
    const MISSING_PORT: &'static str = "Missing port for TCP client";
    const MISSING_HOST: &'static str = "missing host for TCP client";
}
#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "tcp_client".into()
    }
    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        if config.url.port().is_none() {
            return Err(error_connector_def(id, Self::MISSING_PORT).into());
        }
        let host = match config.url.host_str() {
            Some(host) => host.to_string(),
            None => return Err(error_connector_def(id, Self::MISSING_HOST).into()),
        };
        let (tls_connector, tls_domain) = match config.tls.as_ref() {
            Some(Either::Right(true)) => {
                // default config
                (
                    Some(TLSClientConfig::default().to_client_connector()?),
                    Some(host),
                )
            }
            Some(Either::Left(tls_config)) => (
                Some(tls_config.to_client_connector()?),
                tls_config.domain().cloned(),
            ),
            Some(Either::Right(false)) | None => (None, None),
        };
        let (source_tx, source_rx) = bounded(qsize());
        Ok(Box::new(TcpClient {
            config,
            tls_connector,
            tls_domain,
            source_tx,
            source_rx: Some(source_rx),
        }))
    }
}

#[async_trait::async_trait()]
impl Connector for TcpClient {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        if let Some(tls_connector) = self.tls_connector.as_ref() {
            let sink = TcpClientSink::tls(
                tls_connector.clone(),
                self.tls_domain.clone(),
                self.config.clone(),
                self.source_tx.clone(),
            );
            Ok(Some(builder.spawn(sink, ctx)))
        } else {
            let sink = TcpClientSink::plain(self.config.clone(), self.source_tx.clone());
            Ok(Some(builder.spawn(sink, ctx)))
        }
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        // this source is wired up to the ending channel that is forwarding data received from the TCP (or TLS) connection
        let source = ChannelSource::from_channel(
            self.source_tx.clone(),
            self.source_rx
                .take()
                .ok_or(GenericImplementationError::AlreadyConnected)?,
            // we don't need to know if the source is connected. Worst case if nothing is connected is that the receiving task is blocked.
            Arc::new(AtomicBool::new(false)),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

/// TCP/TLS client sink implementation
struct TcpClientSink {
    tls_connector: Option<TlsConnector>,
    tls_domain: Option<String>,
    config: Config,
    wrapped_stream: Option<Box<dyn tokio::io::AsyncWrite + Unpin + Send + Sync>>,
    tcp_stream: Option<TcpStream>,
    source_runtime: ChannelSourceRuntime,
}

impl TcpClientSink {
    fn plain(config: Config, source_tx: Sender<SourceReply>) -> Self {
        let source_runtime = ChannelSourceRuntime::new(source_tx);
        Self {
            tls_connector: None,
            tls_domain: None,
            config,
            wrapped_stream: None,
            tcp_stream: None,
            source_runtime,
        }
    }
    fn tls(
        tls_connector: TlsConnector,
        tls_domain: Option<String>,
        config: Config,
        source_tx: Sender<SourceReply>,
    ) -> Self {
        let source_runtime = ChannelSourceRuntime::new(source_tx);
        Self {
            tls_connector: Some(tls_connector),
            tls_domain,
            config,
            wrapped_stream: None,
            tcp_stream: None,
            source_runtime,
        }
    }

    /// writing to the client socket
    async fn write(&mut self, data: Vec<Vec<u8>>) -> anyhow::Result<()> {
        let stream = self
            .wrapped_stream
            .as_mut()
            .ok_or(socket::Error::NoSocket)?;
        for chunk in data {
            let slice: &[u8] = chunk.as_slice();
            stream.write_all(slice).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait()]
impl Sink for TcpClientSink {
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let buf_size = self.config.buf_size;

        // connect TCP stream
        let stream = tcp_client(&self.config.url, &self.config.socket_options).await?;
        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        // this is known to fail on macOS for IPv6.
        // See: https://github.com/rust-lang/rust/issues/95541
        //if let Some(ttl) = self.config.ttl {
        //    stream.set_ttl(ttl)?;
        //}

        let origin_uri = EventOriginUri {
            scheme: URL_SCHEME.to_string(),
            host: self.config.url.host_or_local().to_string(),
            port: self.config.url.port(),
            path: vec![local_addr.port().to_string()], // local port
        };
        if let Some(tls_connector) = self.tls_connector.as_ref() {
            // TLS
            let tls_stream = tls_connector
                .connect(
                    self.tls_domain
                        .as_ref()
                        .map_or_else(|| self.config.url.host_or_local(), String::as_str)
                        .try_into()?,
                    stream,
                )
                .await?;
            let (read, write) = tokio::io::split(tls_stream);
            let meta = ctx.meta(literal!({
                "tls": true,
                "peer": {
                    "host": peer_addr.ip().to_string(),
                    "port": peer_addr.port()
                }
            }));
            // register writer
            self.wrapped_stream = Some(Box::new(write));
            // register reader
            let tls_reader = TcpReader::tls_client(
                read,
                vec![0; buf_size],
                ctx.alias().clone(),
                origin_uri,
                meta,
            );
            self.source_runtime
                .register_stream_reader(DEFAULT_STREAM_ID, ctx, tls_reader);
        } else {
            // plain TCP
            let meta = ctx.meta(literal!({
                "tls": false,
                // TODO: what to put into meta here?
                "peer": {
                    "host": peer_addr.ip().to_string(),
                    "port": peer_addr.port()
                }
            }));
            // self.tcp_stream = Some(stream.clone());

            let (read_stream, write_stream) = tokio::io::split(stream);
            // register writer
            self.wrapped_stream = Some(Box::new(write_stream));
            //  register reader for receiving from the connection via the source
            let reader = TcpReader::new(
                read_stream,
                vec![0; buf_size],
                ctx.alias().clone(),
                origin_uri,
                meta,
                None, // we don't need to notify any writer, we know if shit goes south from on_event here
            );
            self.source_runtime
                .register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
        }
        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_system::event::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        let ingest_ns = event.ingest_ns;
        for (value, meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, meta, ingest_ns).await?;
            if let Err(e) = self.write(data).await {
                error!("{ctx} Error sending data: {e}. Initiating Reconnect...",);
                // TODO: figure upon which errors to actually reconnect
                self.tcp_stream = None;
                self.wrapped_stream = None;
                ctx.notifier().connection_lost().await?;
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }

    /// when writing is done
    async fn on_stop(&mut self, ctx: &SinkContext) -> anyhow::Result<()> {
        if let Some(stream) = self.tcp_stream.as_mut() {
            if let Err(e) = stream.shutdown().await {
                error!("{ctx} stopping: {e}...",);
            }
        }
        Ok(())
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
