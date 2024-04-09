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

//! WS Client connector - maintains a connection to the configured upstream host
#![allow(clippy::module_name_repetitions)]

use super::{WsReader, WsWriter};
use crate::{
    errors::error_connector_def,
    sink::{channel_sink::ChannelSinkRuntime, prelude::*, StreamWriter},
    source::{
        channel_source::{ChannelSource, ChannelSourceRuntime},
        prelude::*,
    },
    utils::{
        socket::{self, tcp_client, TcpSocketOptions},
        tls::TLSClientConfig,
        ConnectionMeta,
    },
};
use either::Either;
use futures::StreamExt;
use rustls::ServerName;
use std::sync::Arc;
use std::{net::SocketAddr, sync::atomic::AtomicBool};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::client_async;
use tremor_common::url::Url;
use tremor_script::EventOriginUri;
use tremor_system::{event::DEFAULT_STREAM_ID, qsize};
use tremor_value::literal;

const URL_SCHEME: &str = "tremor-ws-client";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    url: Url<super::Defaults>,
    #[serde(default)]
    socket_options: TcpSocketOptions,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
}

impl tremor_config::Impl for Config {}

/// The WS Client connector
#[derive(Debug, Default)]
pub struct Builder {}

impl Builder {
    const MISSING_HOST: &'static str = "Invalid `url` - host missing";
    const MISSING_PORT: &'static str = "Not a valid WS type url - port specification missing";
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "ws_client".into()
    }
    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let host = config
            .url
            .host()
            .ok_or_else(|| error_connector_def(id, Self::MISSING_HOST))?
            .to_string();
        // TODO: do we really need to make the port required when we have a default defined on the URL?
        if config.url.port().is_none() {
            return Err(error_connector_def(id, Self::MISSING_PORT).into());
        };

        let (tls_connector, tls_domain) = match config.tls.as_ref() {
            Some(Either::Right(true)) => (
                Some(TLSClientConfig::default().to_client_connector()?),
                host,
            ),
            Some(Either::Left(tls_config)) => (
                Some(tls_config.to_client_connector()?),
                tls_config.domain().cloned().unwrap_or(host),
            ),
            Some(Either::Right(false)) | None => (None, host),
        };
        let (source_tx, source_rx) = channel(qsize());

        Ok(Box::new(WsClient {
            config,
            tls_connector,
            tls_domain,
            source_tx,
            source_rx: Some(source_rx),
        }))
    }
}

pub(crate) struct WsClient {
    config: Config,
    tls_connector: Option<TlsConnector>,
    tls_domain: String,
    source_tx: Sender<SourceReply>,
    source_rx: Option<Receiver<SourceReply>>,
}

impl WsClient {
    fn meta(peer: SocketAddr, has_tls: bool) -> Value<'static> {
        let peer_ip = peer.ip().to_string();
        let peer_port = peer.port();

        literal!({
            "tls": has_tls,
            "peer": {
                "host": peer_ip,
                "port": peer_port
            }
        })
    }
}

struct WsClientSink {
    config: Config,
    tls_connector: Option<TlsConnector>,
    tls_domain: String,
    source_runtime: ChannelSourceRuntime,
    wrapped_stream: Option<Box<dyn StreamWriter>>,
}

impl WsClientSink {
    fn new(
        config: Config,
        tls_connector: Option<TlsConnector>,
        tls_domain: String,
        source_tx: Sender<SourceReply>,
    ) -> Self {
        let source_runtime = ChannelSourceRuntime::new(source_tx);
        Self {
            config,
            tls_connector,
            tls_domain,
            source_runtime,
            wrapped_stream: None,
        }
    }
}

#[async_trait::async_trait]
impl Sink for WsClientSink {
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let tcp_stream = tcp_client(&self.config.url, &self.config.socket_options).await?;
        let (local_addr, peer_addr) = (tcp_stream.local_addr()?, tcp_stream.peer_addr()?);

        if let Some(tls_connector) = self.tls_connector.as_ref() {
            // TLS
            // wrap it into arcmutex, because we need to clone it in order to close it properly
            let server_name = ServerName::try_from(self.tls_domain.as_str())?;
            let tls_stream = tls_connector.connect(server_name, tcp_stream).await?;
            let (ws_stream, _http_response) =
                client_async(self.config.url.as_str(), tls_stream).await?;
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: local_addr.ip().to_string(),
                port: Some(local_addr.port()),
                path: vec![local_addr.port().to_string()], // local port
            };
            let (writer, reader) = ws_stream.split();
            let meta = ctx.meta(WsClient::meta(peer_addr, true));
            let ws_writer = WsWriter::new_tls_client(writer);

            self.wrapped_stream = Some(Box::new(ws_writer));

            let ws_reader = WsReader::new(
                reader,
                None::<ChannelSinkRuntime<ConnectionMeta>>,
                origin_uri,
                meta,
                ctx.clone(),
            );
            self.source_runtime
                .register_stream_reader(DEFAULT_STREAM_ID, ctx, ws_reader);
        } else {
            // No TLS
            let (ws_stream, _http_response) =
                client_async(self.config.url.as_str(), tcp_stream).await?;
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: local_addr.ip().to_string(),
                port: Some(local_addr.port()),
                path: vec![local_addr.port().to_string()], // local port
            };
            let (writer, reader) = ws_stream.split();
            let meta = ctx.meta(WsClient::meta(peer_addr, false));

            let ws_writer = WsWriter::new_tungstenite_client(writer);
            self.wrapped_stream = Some(Box::new(ws_writer));
            let ws_reader = WsReader::new(
                reader,
                None::<ChannelSinkRuntime<ConnectionMeta>>,
                origin_uri,
                meta,
                ctx.clone(),
            );
            self.source_runtime
                .register_stream_reader(DEFAULT_STREAM_ID, ctx, ws_reader);
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
        let writer = self
            .wrapped_stream
            .as_mut()
            .ok_or_else(|| socket::Error::NoSocket)?;
        for (value, meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, meta, ingest_ns).await?;
            if let Err(e) = writer.write(data, Some(meta)).await {
                error!("{ctx} Error sending data: {e}. Initiating Reconnect...",);
                // TODO: figure upon which errors to actually reconnect
                self.wrapped_stream = None;
                ctx.notifier().connection_lost().await?;
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }
    fn auto_ack(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl Connector for WsClient {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        // we don't need to know if the source is connected. Worst case if nothing is connected is that the receiving task is blocked.
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

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = WsClientSink::new(
            self.config.clone(),
            self.tls_connector.clone(),
            self.tls_domain.clone(),
            self.source_tx.clone(),
        );
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}
