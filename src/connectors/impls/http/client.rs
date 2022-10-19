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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use async_std::channel::{bounded, Receiver, Sender};
use either::Either;
use halfbrown::HashMap;
use http_client::h1::H1Client;
use http_client::HttpClient;
use http_types::Method;
use tremor_common::time::nanotime;

use super::auth::Auth;
use super::meta::{extract_request_meta, extract_response_meta, HttpRequestBuilder};
use super::utils::{Header, RequestId};
use crate::connectors::sink::concurrency_cap::ConcurrencyCap;
use crate::connectors::utils::mime::MimeCodecMap;
use crate::connectors::utils::tls::{tls_client_config, TLSClientConfig};
use crate::{connectors::prelude::*, errors::err_connector_def};

const CONNECTOR_TYPE: &str = "http_client";
const DEFAULT_CODEC: &str = "json";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Target URL
    #[serde(default = "Default::default")]
    pub(super) url: Url,
    /// Authorization method
    #[serde(default = "Default::default")]
    pub(super) auth: Auth,
    /// Concurrency capacity limits ( in flight requests )
    #[serde(default = "default_concurrency")]
    pub(super) concurrency: usize,
    /// Default HTTP headers
    #[serde(default = "Default::default")]
    pub(super) headers: HashMap<String, Header>,
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

// for new
impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        CONNECTOR_TYPE.into()
    }

    async fn build_cfg(
        &self,
        id: &Alias,
        connector_config: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
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
            return Err(err_connector_def(
                    id,
                    "missing tls config with 'https' url. Set 'tls' to 'true' or provide a full tls config.",
                ));
        }
        let (response_tx, response_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let mime_codec_map = Arc::new(MimeCodecMap::with_overwrites(&config.custom_codecs));

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
            source_is_connected: Arc::default(),
        }))
    }
}

/// The HTTP client connector - for HTTP-based API interactions
pub(crate) struct Client {
    response_tx: Sender<SourceReply>,
    response_rx: Receiver<SourceReply>,
    config: Config,
    tls_client_config: Option<rustls::ClientConfig>,
    // this is basically an immutable map, we use arc to share it across tasks (e.g. for each request sending)
    mime_codec_map: Arc<MimeCodecMap>,
    configured_codec: String,
    source_is_connected: Arc<AtomicBool>,
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
            is_connected: self.source_is_connected.clone(),
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
            self.source_is_connected.clone(),
        );
        builder.spawn(sink, sink_context).map(Some)
    }
}

struct HttpRequestSource {
    is_connected: Arc<AtomicBool>,
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

    async fn on_cb_open(&mut self, _ctx: &SourceContext) -> Result<()> {
        // we will only know if we are connected to some pipelines if we receive a CBAction::Restore contraflow event
        // we will not send responses to out/err if we are not connected and this is determined by this variable
        self.is_connected.store(true, Ordering::Release);
        Ok(())
    }
}

struct HttpRequestSink {
    request_counter: u64,
    client: Option<Arc<H1Client>>,
    response_tx: Sender<SourceReply>,
    reply_tx: Sender<AsyncSinkReply>,
    config: Config,
    tls_client_config: Option<rustls::ClientConfig>,
    // reply_tx: Sender<AsyncSinkReply>,
    concurrency_cap: ConcurrencyCap,
    origin_uri: EventOriginUri,
    codec_map: Arc<MimeCodecMap>,
    configured_codec: String,
    // we should only send responses down the channel when we know there is a source consuming them
    // otherwise the channel would fill up and we'd be stuck
    // TODO: find/implement a channel that just throws away the oldest message when it is full, like a ring-buffer
    source_is_connected: Arc<AtomicBool>,
}

impl HttpRequestSink {
    fn new(
        response_tx: Sender<SourceReply>,
        reply_tx: Sender<AsyncSinkReply>,
        config: Config,
        tls_client_config: Option<rustls::ClientConfig>,
        codec_map: Arc<MimeCodecMap>,
        configured_codec: String,
        source_is_connected: Arc<AtomicBool>,
    ) -> Self {
        let concurrency_cap = ConcurrencyCap::new(config.concurrency, reply_tx.clone());
        Self {
            request_counter: 1, // always start by 1, 0 is DEFAULT_STREAM_ID and this might interfere with custom codecs
            client: None,
            response_tx,
            reply_tx,
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
            source_is_connected,
        }
    }
}

#[async_trait::async_trait()]
impl Sink for HttpRequestSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let timeout = self.config.timeout.map(Duration::from_nanos);
        let tls_config = self.tls_client_config.as_ref().cloned().map(Arc::new);
        let client_config = http_client::Config::new()
            .set_http_keep_alive(true) // TODO: make configurable, maybe some people don't want that
            .set_tcp_no_delay(true)
            .set_timeout(timeout)
            .set_max_connections_per_host(self.config.concurrency)
            .set_tls_config(tls_config);

        let client = H1Client::try_from(client_config)
            .map_err(|e| format!("Invalid HTTP Client config: {e}."))?;
        self.client = Some(Arc::new(client));

        Ok(true)
    }

    #[allow(clippy::too_many_lines)]
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        // constrain to max concurrency - propagate CB close on hitting limit
        let guard = self.concurrency_cap.inc_for(&event).await?;

        if let Some(client) = self.client.as_ref().cloned() {
            let send_ctx = ctx.clone();
            let response_tx = self
                .source_is_connected
                .load(Ordering::Acquire)
                .then(|| self.response_tx.clone());
            let reply_tx = self.reply_tx.clone();
            let contraflow_data = if event.transactional {
                Some(ContraflowData::from(&event))
            } else {
                None
            };
            let mut origin_uri = self.origin_uri.clone();
            let ingest_ns = event.ingest_ns;

            // take the metadata from the first element of the batch
            let event_meta = event.value_meta_iter().next().map(|t| t.1);
            let correlation_meta = event_meta.get("correlation").map(Value::clone_static); // :sob:

            // assign a unique request id to this event
            let request_id = RequestId::new(self.request_counter);
            self.request_counter = self.request_counter.wrapping_add(1).max(1);

            let http_meta = event_meta.and_then(|meta| ctx.extract_meta(meta));
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
            let configured_codec = self.configured_codec.clone();
            let codec_map = self.codec_map.clone();
            let mut request = builder.get_chunked_request();
            let request_is_chunked = request.is_some();
            if !request_is_chunked {
                // if the request is not chunked
                // we need to populate the request body from the (possibly batched) event payloads first
                for value in event.value_iter() {
                    ctx.bail_err(
                        builder.append(value, ingest_ns, serializer).await,
                        "Error serializing event into request body",
                    )?;
                }
                // the request will only be available after finalizing
                request = ctx.bail_err(
                    builder.finalize(serializer).await,
                    "Error serializing final parts of the event into request body",
                )?;
            }

            if let Some(request) = request {
                // spawn the sending task
                async_std::task::spawn::<_, Result<()>>(async move {
                    // extract request meta for the response metadata from the finally prepared request
                    // the actual sent request might differ from the metadata used to create this request
                    let req_meta = extract_request_meta(&request);
                    if let Some(host) = request.host() {
                        origin_uri.host = host.to_string();
                    }
                    origin_uri.port = request.url().port_or_known_default();
                    origin_uri.path = request
                        .url()
                        .path_segments()
                        .map(|iter| iter.map(ToString::to_string).collect::<Vec<_>>())
                        .unwrap_or_default();
                    match client.send(request).await {
                        Ok(mut response) => {
                            let response_meta = extract_response_meta(&response);
                            let mut meta = send_ctx.meta(literal!({
                                "request": req_meta,
                                "request_id": request_id.get(),
                                "response": response_meta
                            }));

                            if let Some(corr_meta) = correlation_meta {
                                meta.try_insert("correlation", corr_meta);
                            }
                            let data = send_ctx.bail_err(
                                response.body_bytes().await.map_err(Error::from),
                                "Error receiving response body",
                            )?;
                            let codec_name = if let Some(mime) = response.content_type() {
                                codec_map.get_codec_name(mime.essence())
                            } else {
                                None
                            };
                            let codec_overwrite = codec_name
                                .filter(|codec| *codec != &configured_codec)
                                .cloned();
                            let reply = SourceReply::Data {
                                origin_uri,
                                data,
                                meta: Some(meta),
                                stream: None, // a response (as well as a request) is a discrete unit and not part of a stream
                                port: None,
                                codec_overwrite,
                            };
                            if let Some(response_tx) = response_tx {
                                send_ctx.swallow_err(
                                    response_tx.send(reply).await,
                                    "Error sending response to source",
                                );
                            }
                            if let Some(contraflow_data) = contraflow_data {
                                send_ctx.swallow_err(
                                    reply_tx
                                        .send(AsyncSinkReply::Ack(
                                            contraflow_data,
                                            nanotime() - start,
                                        ))
                                        .await,
                                    "Error sending ack contraflow",
                                );
                            }
                        }
                        Err(e) => {
                            error!("{send_ctx} Error sending HTTP request: {e}");
                            if let Some(contraflow_data) = contraflow_data {
                                send_ctx.swallow_err(
                                    reply_tx.send(AsyncSinkReply::Fail(contraflow_data)).await,
                                    "Error sending fail contraflow",
                                );
                            }
                        }
                    }
                    drop(guard);
                    Ok(())
                });
            } else {
                // NOTE: this shouldn't happen
                error!("{ctx} Unable to serialize event into HTTP request.");
                return Err("Unable to serialize event into HTTP request.".into());
            }

            if request_is_chunked {
                // if we have a chunked request we still gotta do some work (sending the chunks)
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

    // we do ack when the response is sent
    fn auto_ack(&self) -> bool {
        false
    }
}
