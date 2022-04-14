// Copyright 2022, The Tremor Team
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

use std::sync::Arc;
use std::time::Duration;

use async_std::channel::{bounded, Receiver, Sender};
use either::Either;
use halfbrown::HashMap;
use http_client::h1::H1Client;
use http_client::HttpClient;
use http_types::Method;

use super::auth::Auth;
use super::meta::HttpRequestBuilder;
use super::utils::RequestId;
use crate::connectors::prelude::*;
use crate::connectors::sink::concurrency_cap::ConcurrencyCap;
use crate::connectors::utils::mime::MimeCodecMap;
use crate::connectors::utils::tls::{tls_client_config, TLSClientConfig};

const CONNECTOR_TYPE: &str = "http_client";
const DEFAULT_CODEC: &str = "json";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Target URL
    #[serde(default = "Default::default")]
    pub(super) url: Url,
    /// Authorization method
    #[serde(default = "default_auth")]
    pub(super) auth: Auth,
    /// Concurrency capacity limits ( in flight requests )
    #[serde(default = "default_concurrency")]
    pub(super) concurrency: usize,
    /// Default HTTP headers
    #[serde(default = "Default::default")]
    pub(super) headers: HashMap<String, Vec<String>>,
    /// Default HTTP method
    #[serde(default = "default_method")]
    pub(super) method: Method,
    /// request timeout in nanoseconds
    #[serde(default = "Default::default")]
    timeout: Option<u64>,
    /// optional tls client config
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
    /// MIME mapping to/from tremor codecs
    #[serde(default)]
    custom_codecs: HashMap<String, String>,
}

const DEFAULT_CONCURRENCY: usize = 4;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_method() -> Method {
    Method::Post
}

fn default_auth() -> Auth {
    Auth::None
}

// for new
impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        CONNECTOR_TYPE.into()
    }

    async fn build(
        &self,
        id: &str,
        connector_config: &ConnectorConfig,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = &connector_config.config {
            let config = Config::new(config)?;

            let tls_client_config = match config.tls.as_ref() {
                Some(Either::Right(true)) => {
                    // default config
                    Some(tls_client_config(&TLSClientConfig::default()).await?)
                }
                Some(Either::Left(tls_config)) => Some(tls_client_config(tls_config).await?),
                Some(Either::Right(false)) | None => None,
            };
            if config.url.scheme() == "https" && tls_client_config.is_none() {
                return Err(ErrorKind::InvalidConfiguration(
                    id.to_string(),
                    format!("missing tls config for {id} with 'https' url. Set 'tls' to 'true' or provide a full config."),
                ).into());
            }
            let (response_tx, response_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
            let mime_codec_map = MimeCodecMap::with_overwrites(&config.custom_codecs)?;

            let configured_codec = connector_config
                .codec
                .as_ref()
                .map_or_else(|| DEFAULT_CODEC.to_string(), |c| c.name.clone());
            Ok(Box::new(Client {
                response_tx,
                response_rx,
                config,
                tls_client_config,
                mime_codec_map,
                configured_codec,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

/// The HTTP client connector - for HTTP-based API interactions
pub struct Client {
    response_tx: Sender<SourceReply>,
    response_rx: Receiver<SourceReply>,
    config: Config,
    tls_client_config: Option<rustls::ClientConfig>,
    mime_codec_map: MimeCodecMap,
    configured_codec: String,
}

#[async_trait::async_trait]
impl Connector for Client {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional(DEFAULT_CODEC)
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = HttpRequestSource {
            rx: self.response_rx.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = HttpRequestSink::new(
            self.response_tx.clone(),
            builder.reply_tx(),
            self.config.clone(),
            self.tls_client_config.clone(),
            self.mime_codec_map.clone(),
            self.configured_codec.clone(),
        );
        builder.spawn(sink, sink_context).map(Some)
    }
}

struct HttpRequestSource {
    rx: Receiver<SourceReply>,
}

#[async_trait::async_trait()]
impl Source for HttpRequestSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        Ok(self.rx.recv().await?)
    }

    fn is_transactional(&self) -> bool {
        false
    }

    /// there is no asynchronous task driving this and is being stopped by the quiescence process
    fn asynchronous(&self) -> bool {
        false
    }
}

struct H1Clients {
    clients: Vec<Arc<H1Client>>,
    idx: usize,
}

impl H1Clients {
    fn new(clients: Vec<Arc<H1Client>>) -> Self {
        Self { clients, idx: 0 }
    }

    /// Returns a freshly clones h1 client inside an Arc
    fn next(&mut self) -> Option<Arc<H1Client>> {
        let len = self.clients.len();
        if len > 0 {
            let idx = self.idx % len;
            self.idx += 1;
            self.clients.get(idx).cloned()
        } else {
            None
        }
    }
}

struct HttpRequestSink {
    request_counter: u64,
    clients: H1Clients,
    response_tx: Sender<SourceReply>,
    config: Config,
    tls_client_config: Option<rustls::ClientConfig>,
    // reply_tx: Sender<AsyncSinkReply>,
    concurrency_cap: ConcurrencyCap,
    origin_uri: EventOriginUri,
    codec_map: MimeCodecMap,
    configured_codec: String,
}

impl HttpRequestSink {
    fn new(
        response_tx: Sender<SourceReply>,
        reply_tx: Sender<AsyncSinkReply>,
        config: Config,
        tls_client_config: Option<rustls::ClientConfig>,
        codec_map: MimeCodecMap,
        configured_codec: String,
    ) -> Self {
        let concurrency_cap = ConcurrencyCap::new(config.concurrency, reply_tx);
        Self {
            // ALLOW: we give it a constant 1
            request_counter: 1,
            clients: H1Clients::new(vec![]),
            response_tx,
            config,
            tls_client_config,
            concurrency_cap,
            origin_uri: EventOriginUri {
                scheme: String::from("http_client"),
                host: String::from("dummy"), // will be replaced in `on_event`
                port: None,
                path: vec![],
            },
            codec_map,
            configured_codec,
        }
    }
}

#[async_trait::async_trait()]
impl Sink for HttpRequestSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let mut clients = Vec::with_capacity(self.config.concurrency);

        let timeout = self.config.timeout.map(Duration::from_nanos);
        let tls_config = self.tls_client_config.as_ref().cloned().map(Arc::new);
        let client_config = http_client::Config::new()
            .set_http_keep_alive(true) // TODO: make configurable, maybe some people don't want that
            .set_tcp_no_delay(true)
            .set_timeout(timeout)
            .set_tls_config(tls_config);

        for _i in 1..self.config.concurrency {
            let client = H1Client::try_from(client_config.clone())
                .map_err(|e| format!("Invalid HTTP Client config: {e}."))?;
            clients.push(Arc::new(client));
        }
        self.clients = H1Clients::new(clients);

        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        // constrain to max concurrency - propagate CB close on hitting limit
        let guard = self.concurrency_cap.inc_for(&event).await?;

        if let Some(client) = self.clients.next() {
            let ctx = ctx.clone();
            let _response_tx = self.response_tx.clone();
            let _origin_uri = self.origin_uri.clone();
            let ingest_ns = event.ingest_ns;

            let event_meta = event.data.suffix().meta();
            let _correlation_meta = event_meta.get("correlation").map(Value::clone_static); // :sob:

            // assign a unique request id to this event
            let request_id = RequestId::new(self.request_counter);
            self.request_counter = self.request_counter.wrapping_add(1).max(1);
            let http_meta = ctx.extract_meta(event_meta);
            let mut builder = ctx.bail_err(
                HttpRequestBuilder::new(
                    request_id,
                    http_meta,
                    &self.codec_map,
                    &self.config,
                    &self.configured_codec,
                ),
                "Error turning event into an HTTP Request",
            )?;

            if let Some(request) = builder.get_chunked_request() {
                // if chunked we can send the response away already - spawn the task
                // FIXME
                // FIXME: extract into a function so we can reuse
                async_std::task::spawn(async move {
                    match client.send(request).await {
                        Ok(_response) => {}
                        Err(_e) => {}
                    }
                    drop(guard);
                });

                for value in event.value_iter() {
                    ctx.bail_err(
                        builder.append(value, ingest_ns, serializer).await,
                        "Error serializing event into request body",
                    )?;
                }
                ctx.bail_err(
                    builder.finalize(serializer).await,
                    "Error serializing final parts of the event into request body",
                )?;
            } else {
                for value in event.value_iter() {
                    ctx.bail_err(
                        builder.append(value, ingest_ns, serializer).await,
                        "Error serializing event into request body",
                    )?;
                }
                let request = ctx.bail_err(
                    builder.finalize(serializer).await,
                    "Error serializing final parts of the event into request body",
                )?;
                // spawn the sending task
                // FIXME
                if let Some(request) = request {
                    async_std::task::spawn(async move {
                        match client.send(request).await {
                            Ok(_response) => {}
                            Err(_e) => {}
                        }

                        drop(guard);
                    });
                }
            }
        } else {
            error!("{ctx} No http client available.");
            return Ok(SinkReply::FAIL);
        }

        Ok(SinkReply::NONE)
    }

    fn asynchronous(&self) -> bool {
        true
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

/* FIXME
#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use http_types::Method;

    #[async_std::test]
    async fn http_client_builder() -> Result<()> {
        let with_processors = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "url": "https://www.google.com"
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &with_processors,
        )?;

        let builder = super::Builder::default();
        builder.build("foo", &config).await?;

        Ok(())
    }

    #[test]
    fn default_http_meta_codec_handling() -> Result<()> {
        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "url": "https://www.google.com"
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "json")?;
        assert_eq!("json", http_meta.codec.name());
        let http_meta = HttpRequestBuilder::from_config(&config, "string")?;
        assert_eq!("string", http_meta.codec.name());
        let http_meta = HttpRequestBuilder::from_config(&config, "snot");
        assert!(http_meta.is_err());

        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "url": "https://www.google.com"
            },
            "codec": "msgpack",
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "snot")?;
        assert_eq!("msgpack", http_meta.codec.name());

        Ok(())
    }

    #[test]
    fn default_http_meta_endpoint_handling() -> Result<()> {
        env_logger::init();
        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "json")?;
        assert_eq!("http://localhost/", http_meta.endpoint.to_string());

        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "url": "https://tremor.rs/"
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "json")?;
        assert_eq!("https://tremor.rs/", http_meta.endpoint.to_string());
        Ok(())
    }

    #[test]
    fn default_http_meta_method_handling() -> Result<()> {
        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "url": "https://tremor.rs/benchmarks/"
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "json")?;
        assert_eq!("json", http_meta.codec.name());
        assert_eq!(
            "https://tremor.rs/benchmarks/",
            http_meta.endpoint.to_string()
        );
        assert_eq!(Method::Post, http_meta.method);

        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "url": "https://tremor.rs/",
                "method": "get"
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "json")?;
        assert_eq!("json", http_meta.codec.name());
        assert_eq!("https://tremor.rs/", http_meta.endpoint.url().to_string());
        assert_eq!(Method::Get, http_meta.method);
        Ok(())
    }

    #[test]
    fn default_http_meta_headers_handling() -> Result<()> {
        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "method": "pUt",
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "json")?;
        assert_eq!("json", http_meta.codec.name());
        assert_eq!("http://localhost/", http_meta.endpoint.url().to_string());
        assert_eq!(Method::Put, http_meta.method);
        assert_eq!(0, http_meta.headers.len());

        let connector_config = literal!({
            "id": "my_rest_client",
            "type": "rest_client",
            "config": {
                "url": "https://tremor.rs/",
                "method": "pAtCH",
                "headers": {
                    "snot": [ "Badger" ],
                }
            },
        });
        let config: ConnectorConfig = crate::config::Connector::from_config(
            ConnectorType("rest_client".into()),
            &connector_config,
        )?;
        let http_meta = HttpRequestBuilder::from_config(&config, "json")?;
        assert_eq!("json", http_meta.codec.name());
        assert_eq!("https://tremor.rs/", http_meta.endpoint.url().to_string());
        assert_eq!(Method::Patch, http_meta.method);
        assert_eq!(1, http_meta.headers.len());
        assert_eq!(
            "Badger",
            http_meta.headers.get("snot").unwrap()[0].to_string()
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use crate::connectors::prelude::ConfigImpl;
    use crate::errors::Result;
    use http_types::Method;
    use tremor_value::literal;

    #[test]
    fn deserialize_method() -> Result<()> {
        let v = literal!({
          "url": "http://localhost:8080/",
          "method": "PATCH"
        });
        let config = Config::new(&v)?;
        assert_eq!(Method::Patch, Method::from_str(&config.method)?);
        Ok(())
    }
}
*/
