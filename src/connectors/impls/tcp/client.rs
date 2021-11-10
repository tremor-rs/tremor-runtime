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

//! TCP Client connector - maintains a connection to the configured upstream host
#![allow(clippy::module_name_repetitions)]

use super::{TcpReader, TcpWriter};
use crate::connectors::prelude::*;
use crate::connectors::utils::tls::{tls_client_connector, TLSClientConfig};
use async_std::net::TcpStream;
use async_tls::TlsConnector;
use either::Either;
use futures::io::AsyncReadExt;

const URL_SCHEME: &str = "tremor-tcp-client";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    host: String,
    port: u16,
    ttl: Option<u32>,
    #[serde(default = "default_no_delay")]
    no_delay: bool,
    #[serde(default = "default_buf_size")]
    buf_size: usize,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
}

fn default_no_delay() -> bool {
    true
}

impl ConfigImpl for Config {}

pub struct TcpClient {
    config: Config,
    tls_connector: Option<TlsConnector>,
    tls_domain: Option<String>,
    source_runtime: Option<ChannelSourceRuntime>,
    sink_runtime: Option<SingleStreamSinkRuntime>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        _id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;
            let (tls_connector, tls_domain) = match config.tls.as_ref() {
                Some(Either::Right(true)) => {
                    // default config
                    (
                        Some(tls_client_connector(&TLSClientConfig::default()).await?),
                        Some(config.host.clone()),
                    )
                }
                Some(Either::Left(tls_config)) => (
                    Some(tls_client_connector(tls_config).await?),
                    tls_config.domain.clone(),
                ),
                Some(Either::Right(false)) | None => (None, None),
            };
            Ok(Box::new(TcpClient {
                config,
                tls_connector,
                tls_domain,
                source_runtime: None,
                sink_runtime: None,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("TcpClient")).into())
        }
    }
}

#[async_trait::async_trait()]
impl Connector for TcpClient {
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

    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let buf_size = self.config.buf_size;
        let source_runtime = self
            .source_runtime
            .as_ref()
            .ok_or("Source runtime not initialized")?;
        let sink_runtime = self
            .sink_runtime
            .as_ref()
            .ok_or("Sink runtime not initialized")?;
        let stream = TcpStream::connect((self.config.host.as_str(), self.config.port)).await?;
        let local_addr = stream.local_addr()?;
        if let Some(ttl) = self.config.ttl {
            stream.set_ttl(ttl)?;
        }
        stream.set_nodelay(self.config.no_delay)?;

        let origin_uri = EventOriginUri {
            scheme: URL_SCHEME.to_string(),
            host: self.config.host.clone(),
            port: Some(self.config.port),
            path: vec![local_addr.port().to_string()], // local port
        };
        if let Some(tls_connector) = self.tls_connector.as_ref() {
            // ~~~ T L S ~~~
            let tls_stream = tls_connector
                .connect(
                    self.tls_domain
                        .as_ref()
                        .map_or_else(|| self.config.host.as_str(), String::as_str),
                    stream.clone(),
                )
                .await?;
            let (read, write) = tls_stream.split();
            let meta = ctx.meta(literal!({
                "tls": true,
                // TODO: what to put into meta here?
                "peer": {
                    "host": self.config.host.clone(),
                    "port": self.config.port
                }
            }));
            // register reader
            let tls_reader = TcpReader::tls_client(
                read,
                stream.clone(),
                vec![0; buf_size],
                ctx.url.clone(),
                origin_uri.clone(),
                meta,
            );
            source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, tls_reader);

            // register writer
            let tls_writer = TcpWriter::tls_client(write, stream);
            sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, tls_writer);
        } else {
            // plain TCP
            let meta = ctx.meta(literal!({
                "tls": false,
                // TODO: what to put into meta here?
                "peer": {
                    "host": self.config.host.clone(),
                    "port": self.config.port
                }
            }));
            // register reader
            let reader = TcpReader::new(
                stream.clone(),
                vec![0; buf_size],
                ctx.url.clone(),
                origin_uri.clone(),
                meta,
            );
            source_runtime.register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
            // register writer
            let writer = TcpWriter::new(stream);
            sink_runtime.register_stream_writer(DEFAULT_STREAM_ID, ctx, writer);
        }
        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
