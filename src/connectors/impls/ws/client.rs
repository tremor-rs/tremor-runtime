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
    connectors::{
        prelude::*,
        utils::{
            socket::{tcp_client_socket, TcpSocketOptions},
            tls::TLSClientConfig,
        },
    },
    errors::err_connector_def,
};
use either::Either;
use futures::StreamExt;
use rustls::ServerName;
use std::sync::Arc;
use std::{net::SocketAddr, sync::atomic::AtomicBool};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::client_async;

const URL_SCHEME: &str = "tremor-ws-client";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    url: Url<super::WsDefaults>,
    #[serde(default)]
    socket_options: TcpSocketOptions,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

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
        id: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let host = config
            .url
            .host()
            .ok_or_else(|| err_connector_def(id, Self::MISSING_HOST))?
            .to_string();
        // TODO: do we really need to make the port required when we have a default defined on the URL?
        if config.url.port().is_none() {
            return Err(err_connector_def(id, Self::MISSING_PORT));
        };

        let (tls_connector, tls_domain) = match config.tls.as_ref() {
            Some(Either::Right(true)) => (
                Some(TLSClientConfig::default().to_client_connector()?),
                host,
            ),
            Some(Either::Left(tls_config)) => (
                Some(tls_config.to_client_connector()?),
                tls_config.domain.clone().unwrap_or(host),
            ),
            Some(Either::Right(false)) | None => (None, host),
        };

        Ok(Box::new(WsClient {
            config,
            tls_connector,
            tls_domain,
            source_runtime: None,
            sink_runtime: None,
        }))
    }
}

pub(crate) struct WsClient {
    config: Config,
    tls_connector: Option<TlsConnector>,
    tls_domain: String,
    source_runtime: Option<ChannelSourceRuntime>,
    sink_runtime: Option<SingleStreamSinkRuntime>,
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

#[async_trait::async_trait]
impl Connector for WsClient {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        // we don't need to know if the source is connected. Worst case if nothing is connected is that the receiving task is blocked.
        let source = ChannelSource::new(Arc::new(AtomicBool::new(false)));
        self.source_runtime = Some(source.runtime());
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let mut sink = SingleStreamSink::new_with_meta(builder.reply_tx());
        self.sink_runtime = Some(sink.runtime()?);
        Ok(Some(builder.spawn(sink, ctx)))
    }

    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let source_runtime = self
            .source_runtime
            .as_ref()
            .ok_or("Source runtime not initialized")?;
        let mut sink_runtime = self
            .sink_runtime
            .take()
            .ok_or("Sink runtime not initialized")?;

        let tcp_stream = tcp_client_socket(&self.config.url, &self.config.socket_options).await?;
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

            sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, ws_writer)?;

            let ws_reader =
                WsReader::new(reader, Some(sink_runtime), origin_uri, meta, ctx.clone());
            source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, ws_reader);
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
            sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, ws_writer)?;

            let ws_reader =
                WsReader::new(reader, Some(sink_runtime), origin_uri, meta, ctx.clone());
            source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, ws_reader);
        }

        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}
