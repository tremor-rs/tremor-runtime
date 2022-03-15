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
use crate::connectors::prelude::*;
use crate::connectors::utils::tls::{tls_client_connector, TLSClientConfig};
use async_std::net::TcpStream;
use async_tls::TlsConnector;
use async_tungstenite::async_std::connect_async;
use async_tungstenite::client_async;
use async_tungstenite::stream::Stream;
use either::Either;
use futures::StreamExt;
use http_types::Url;
use std::net::SocketAddr;

const URL_SCHEME: &str = "tremor-ws-client";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    // FIXME: (HG) we want to configure both ws client / ws server (or any connector possible)
    //        with the same definiten of endpoint not one with host + port, other other with a rul
    url: String,
    #[serde(default = "default_ttl")]
    ttl: Option<u32>,
    #[serde(default = "default_no_delay")]
    no_delay: bool,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
}

fn default_no_delay() -> bool {
    true
}

fn default_ttl() -> Option<u32> {
    None
}

impl ConfigImpl for Config {}

pub struct WsClient {
    config: Config,
    source_runtime: Option<ChannelSourceRuntime>,
    sink_runtime: Option<SingleStreamSinkRuntime>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

async fn resolve_tls_config(
    config: &Config,
) -> Result<(Option<TlsConnector>, Option<String>, String)> {
    let url = Url::parse(&config.url)?;
    let host = match url.host() {
        Some(host) => host.to_string(),
        None => return Err("Not a valid WS type url - host specification missing".into()),
    };
    let port = match url.port() {
        Some(port) => port.to_string(),
        None => return Err("Not a valid WS type url - port specification missing".into()),
    };

    match config.tls.as_ref() {
        Some(Either::Right(true)) => Ok((
            Some(tls_client_connector(&TLSClientConfig::default()).await?),
            Some(host),
            port,
        )),
        Some(Either::Left(tls_config)) => Ok((
            Some(tls_client_connector(tls_config).await?),
            tls_config.domain.clone(),
            port,
        )),
        Some(Either::Right(false)) | None => Ok((None, None, port)),
    }
}

fn condition_tungstenite_stream(
    config: &Config,
    stream: &Stream<
        async_std::net::TcpStream,
        async_tls::client::TlsStream<async_std::net::TcpStream>,
    >,
) -> Result<(SocketAddr, SocketAddr)> {
    match stream {
        Stream::Plain(stream) => condition_tcp_stream(config, stream),
        Stream::Tls(stream) => condition_tcp_stream(config, stream.get_ref()),
    }
}

fn condition_tcp_stream(config: &Config, stream: &TcpStream) -> Result<(SocketAddr, SocketAddr)> {
    if let Some(ttl) = config.ttl {
        stream.set_ttl(ttl)?;
    }
    stream.set_nodelay(config.no_delay)?;
    Ok((stream.local_addr()?, stream.peer_addr()?))
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "ws_client".into()
    }
    async fn from_config(&self, _id: &str, config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = &config.config {
            let config = Config::new(raw_config)?;
            Ok(Box::new(WsClient {
                config,
                source_runtime: None,
                sink_runtime: None,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("WsClient")).into())
        }
    }
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
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = ChannelSource::new(builder.qsize());
        self.source_runtime = Some(source.runtime());
        let addr = builder.spawn(source, source_context)?;
        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = SingleStreamSink::new_with_meta(builder.qsize(), builder.reply_tx());
        self.sink_runtime = Some(sink.runtime());
        let addr = builder.spawn(sink, sink_context)?;
        Ok(Some(addr))
    }

    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let source_runtime = self
            .source_runtime
            .as_ref()
            .ok_or("Source runtime not initialized")?;
        let sink_runtime = self
            .sink_runtime
            .as_ref()
            .ok_or("Sink runtime not initialized")?;

        match resolve_tls_config(&self.config).await? {
            (None, None, _port) => {
                let (ws_stream, _http_response) = connect_async(self.config.url.as_str()).await?;
                let (local_addr, peer_addr) =
                    condition_tungstenite_stream(&self.config, ws_stream.get_ref())?;
                let origin_uri = EventOriginUri {
                    scheme: URL_SCHEME.to_string(),
                    host: local_addr.ip().to_string(),
                    port: Some(local_addr.port()),
                    path: vec![local_addr.port().to_string()], // local port
                };
                let (writer, reader) = ws_stream.split();
                let meta = ctx.meta(WsClient::meta(peer_addr, false));

                let ws_reader = WsReader::new(reader, origin_uri, meta);
                source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, ws_reader);
                let ws_writer = WsWriter::new_tungstenite_client(writer);
                sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, ws_writer);
            }
            (Some(tls_connector), Some(tls_domain), port) => {
                let tcp_stream =
                    async_std::net::TcpStream::connect(&format!("{}:{}", tls_domain, port)).await?;
                let (local_addr, peer_addr) = condition_tcp_stream(&self.config, &tcp_stream)?;
                let tls_stream = tls_connector.connect(tls_domain, tcp_stream).await?;
                let (ws_stream, _http_response) =
                    client_async(self.config.url.as_str(), tls_stream).await?;
                let origin_uri = EventOriginUri {
                    scheme: URL_SCHEME.to_string(),
                    host: local_addr.ip().to_string(),
                    port: Some(local_addr.port()),
                    path: vec![local_addr.port().to_string()], // local port
                };
                let (writer, reader) = ws_stream.split();
                let meta = ctx.meta(WsClient::meta(peer_addr, false));
                let ws_reader = WsReader::new(reader, origin_uri, meta);
                source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, ws_reader);
                let ws_writer = WsWriter::new_tls_client(writer);
                sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, ws_writer);
            }
            _otherwise => return Err("Expected a TLS stream error".into()),
        };

        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}
