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
use async_tungstenite::async_std::connect_async;
use async_tungstenite::stream::Stream;
use futures::StreamExt;
use std::net::SocketAddr;

const URL_SCHEME: &str = "tremor-ws-client";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    url: String,
    #[serde(default = "default_ttl")]
    ttl: Option<u32>,
    #[serde(default = "default_no_delay")]
    no_delay: bool,
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

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "ws_client".into()
    }
    async fn from_config(
        &self,
        _id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = config {
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
        let source = ChannelSource::new(source_context.clone(), builder.qsize());
        self.source_runtime = Some(source.runtime());
        let addr = builder.spawn(source, source_context)?;
        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = SingleStreamSink::new_no_meta(builder.qsize(), builder.reply_tx());
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
        let (ws_stream, _http_response) = connect_async(self.config.url.as_str()).await?;

        let (local_addr, peer_addr) = match ws_stream.get_ref() {
            Stream::Plain(s) => {
                // Apply TCP options
                if let Some(ttl) = self.config.ttl {
                    s.set_ttl(ttl)?;
                }
                s.set_nodelay(self.config.no_delay)?;
                (s.local_addr()?, s.peer_addr()?)
            }
            Stream::Tls(s) => {
                let tcp = s.get_ref();
                // Apply TCP options
                if let Some(ttl) = self.config.ttl {
                    tcp.set_ttl(ttl)?;
                }
                tcp.set_nodelay(self.config.no_delay)?;
                (tcp.local_addr()?, tcp.peer_addr()?)
            }
        };

        let origin_uri = EventOriginUri {
            scheme: URL_SCHEME.to_string(),
            host: local_addr.ip().to_string(),
            port: Some(local_addr.port()),
            path: vec![local_addr.port().to_string()], // local port
        };

        let (writer, reader) = ws_stream.split();
        let meta = ctx.meta(WsClient::meta(peer_addr, false));

        // register writer
        let ws_reader = WsReader::new(reader, ctx.url.clone(), origin_uri.clone(), meta);

        source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, ws_reader);

        // register writer
        let ws_writer = WsWriter::new_tls_client(writer);
        sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, ws_writer);

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
