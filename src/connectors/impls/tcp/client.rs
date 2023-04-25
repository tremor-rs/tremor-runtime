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
use crate::connectors::{
    prelude::*,
    utils::{
        socket::{tcp_client_socket, TcpSocketOptions},
        tls::TLSClientConfig,
    },
};
use crate::{connectors::impls::tcp::TcpDefaults, errors::already_created_error};
use dashmap::DashMap;
use either::Either;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

const URL_SCHEME: &str = "tremor-tcp-client";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    default_handle: Option<String>,
    // IP_TTL for ipv4 and hop limit for ipv6
    //ttl: Option<u32>,
    #[serde(default = "default_buf_size")]
    buf_size: usize,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
    #[serde(default)]
    socket_options: TcpSocketOptions,
}

impl ConfigImpl for Config {}

pub(crate) struct TcpClient {
    config: Config,
    source_tx: Sender<SourceReply>,
    source_rx: Option<Receiver<SourceReply>>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "tcp_client".into()
    }
    async fn build_cfg(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;

        let (source_tx, source_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        Ok(Box::new(TcpClient {
            config,
            source_tx,
            source_rx: Some(source_rx),
        }))
    }
}

#[async_trait::async_trait()]
impl Connector for TcpClient {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = TcpClientSink::new(self.config.clone(), self.source_tx.clone());
        Ok(Some(builder.spawn(sink, sink_context)))
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        // this source is wired up to the ending channel that is forwarding data received from the TCP (or TLS) connection
        let source = ChannelSource::from_channel(
            self.source_tx.clone(),
            self.source_rx.take().ok_or_else(already_created_error)?,
            Arc::default(), // we don't need to know if the source is connected. Worst case if nothing is connected is that the receiving task is blocked.
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    fn validate(&self, config: &ConnectorConfig) -> Result<()> {
        for command in &config.initial_commands {
            match structurize::<Command>(command.clone())? {
                Command::SocketClient(_) => {}
                _ => {
                    return Err(ErrorKind::NotImplemented(
                        "Only SocketClient commands are supported for the TCP client connector."
                            .to_string(),
                    )
                    .into())
                }
            }
        }
        Ok(())
    }
}

/// TCP/TLS client sink implementation
#[derive(FileIo, SocketServer, QueueSubscriber, DatabaseWriter)]
struct TcpClientSink {
    config: Config,
    source_runtime: ChannelSourceRuntime,
    connected_clients: Arc<DashMap<String, Box<dyn tokio::io::AsyncWrite + Unpin + Send + Sync>>>,
}

#[async_trait::async_trait]
impl SocketClient for TcpClientSink {
    async fn connect_socket(
        &mut self,
        address: &str,
        handle: &str,
        ctx: &SinkContext,
    ) -> Result<()> {
        let url = Url::<TcpDefaults>::parse(address)?;
        let buf_size = self.config.buf_size;
        let handle = handle.to_string();

        if url.port().is_none() {
            return Err(Error::from(format!("Missing port in url {address}")));
        }
        let host = match url.host_str() {
            Some(host) => host.to_string(),
            None => return Err(Error::from(format!("Missing host in url {address}"))),
        };
        let tls_config = match self.config.tls.as_ref() {
            Some(Either::Right(true)) => {
                // default config
                Some((TLSClientConfig::default().to_client_connector()?, host))
            }
            Some(Either::Left(tls_config)) => Some((
                tls_config.to_client_connector()?,
                tls_config
                    .domain
                    .clone()
                    .unwrap_or_else(|| url.host_or_local().to_string()),
            )),
            Some(Either::Right(false)) | None => None,
        };

        // connect TCP stream
        // TODO: add socket options
        let stream = tcp_client_socket(&url, &self.config.socket_options).await?;
        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        // this is known to fail on macOS for IPv6.
        // See: https://github.com/rust-lang/rust/issues/95541
        //if let Some(ttl) = self.config.ttl {
        //    stream.set_ttl(ttl)?;
        //}

        let origin_uri = EventOriginUri {
            scheme: URL_SCHEME.to_string(),
            host: url.host_or_local().to_string(),
            port: url.port(),
            path: vec![local_addr.port().to_string()], // local port
        };
        if let Some((tls_connector, tls_domain)) = tls_config {
            // TLS
            // let server_name = ServerName::try_from(tls_domain.as_str())?;
            let tls_stream = tls_connector
                .connect(tls_domain.as_str().try_into()?, stream)
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
            self.connected_clients
                .insert(handle.clone(), Box::new(write));
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
            self.connected_clients
                .insert(handle.clone(), Box::new(write_stream));

            // register reader for receiving from the connection via the source
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

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        if let Some(_connected_client) = self.connected_clients.remove(handle) {
            // connected_client.shutdown(std::net::Shutdown::Both)?;
        }

        Ok(())
    }
}

impl TcpClientSink {
    fn new(config: Config, source_tx: Sender<SourceReply>) -> Self {
        let source_runtime = ChannelSourceRuntime::new(source_tx);
        Self {
            config,
            source_runtime,
            connected_clients: Arc::new(DashMap::new()),
        }
    }

    /// writing to the client socket
    async fn write(&mut self, data: Vec<Vec<u8>>, handle: &str) -> Result<()> {
        for data in data {
            self.connected_clients
                .get_mut(handle)
                .ok_or_else(|| Error::from("client not found"))?
                .as_mut()
                .write_all(&data)
                .await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait()]
impl Sink for TcpClientSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let ingest_ns = event.ingest_ns;
        for (value, meta) in event.value_meta_iter() {
            let handle = ctx
                .extract_meta(meta)
                .get_str("handle")
                .map(ToString::to_string)
                .or_else(|| self.config.default_handle.clone())
                .ok_or_else(|| Error::from("no handle found"))?;

            let data = serializer.serialize(value, ingest_ns)?;
            if let Err(e) = self.write(data, &handle).await {
                error!("{ctx} Error sending data: {e}. Initiating Reconnect...",);
                // TODO: figure upon which errors to actually reconnect
                self.connected_clients.remove(&handle);

                ctx.notifier().connection_lost().await?;
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }

    /// when writing is done
    async fn on_stop(&mut self, _ctx: &SinkContext) -> Result<()> {
        self.connected_clients.clear();
        //     if let Err(e) = client.tcp_stream.shutdown(std::net::Shutdown::Write) {
        //         error!("{ctx} stopping: {e}...",);
        //     }
        // }
        Ok(())
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
