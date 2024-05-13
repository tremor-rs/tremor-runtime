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

use crate::{
    errors::error_connector_def,
    impls::http::{
        auth::Auth,
        client,
        meta::{extract_request_meta, extract_response_meta},
        utils::{Header, RequestId},
        Error,
    },
    sink::{concurrency_cap::ConcurrencyCap, prelude::*, EventSerializer},
    source::prelude::*,
    utils::{mime::MimeCodecMap, tls::TLSClientConfig},
};
use either::Either;
use futures::Stream;
use halfbrown::HashMap;
use http::{
    header::{self, HeaderName},
    Uri,
};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Bytes, Frame};
use hyper::{Method, Request};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client as HyperClient},
    rt::TokioExecutor,
};
use serde::{Deserialize, Deserializer};
use std::{
    convert::Infallible,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};
use tremor_common::{time::nanotime, url::Url};
use tremor_config::NameWithConfig;
use tremor_system::qsize;
use tremor_value::prelude::*;

use super::meta::{consolidate_mime, content_type, HeaderValueValue};

pub(crate) type StreamingBody = StreamBody<
    Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, Infallible>> + Send + Sync + 'static>>,
>;
type StreamingRequest = Request<StreamingBody>;

//  pipeline -> Sink -> http client
//                          |
//                          v
//                         Sink -> Sink#reply_tx -> Source#rx -> pull_data -> pipline
const CONNECTOR_TYPE: &str = "http_client";
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
    pub(super) method: SerdeishMethod,
    /// request timeout in nanoseconds
    timeout: Option<u64>,
    /// optional tls client config
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
    /// custom codecs mapping from `mime_type` to custom codec name
    /// e.g. for handling `application/json` with the `binary` codec, if desired
    /// the mime type of `*/*` serves as a default / fallback
    mime_mapping: Option<HashMap<String, tremor_config::NameWithConfig>>,
}

/// Just a wrapper
#[derive(Debug, Clone)]
pub(crate) struct SerdeishMethod(pub(crate) Method);

impl<'de> Deserialize<'de> for SerdeishMethod {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let method = Method::from_bytes(s.as_bytes()).map_err(serde::de::Error::custom)?;
        Ok(Self(method))
    }
}

const DEFAULT_CONCURRENCY: usize = 4;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_method() -> SerdeishMethod {
    SerdeishMethod(Method::POST)
}

// for new
impl tremor_config::Impl for Config {}

/// Builder for the HTTP client connector
#[derive(Debug, Default)]
pub struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        CONNECTOR_TYPE.into()
    }

    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _connector_config: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;

        let tls_client_config = match config.tls.as_ref() {
            Some(Either::Right(true)) => {
                // default config
                Some(TLSClientConfig::default().to_client_config()?)
            }
            Some(Either::Left(tls_config)) => Some(tls_config.to_client_config()?),
            Some(Either::Right(false)) | None => None,
        };
        if config.url.scheme() == "https" && tls_client_config.is_none() {
            return Err(error_connector_def(
                    id,
                    "missing tls config with 'https' url. Set 'tls' to 'true' or provide a full tls config.",
                ).into());
        }
        let (response_tx, response_rx) = channel(qsize());
        let mime_codec_map = Arc::new(if let Some(codec_map) = config.mime_mapping.clone() {
            MimeCodecMap::from_custom(codec_map)
        } else {
            MimeCodecMap::new()
        });

        Ok(Box::new(Client {
            response_tx,
            response_rx: Some(response_rx),
            config,
            tls_client_config,
            mime_codec_map,
            source_is_connected: Arc::new(AtomicBool::new(false)),
        }))
    }
}

/// The HTTP client connector - for HTTP-based API interactions
pub(crate) struct Client {
    response_tx: Sender<SourceReply>,
    response_rx: Option<Receiver<SourceReply>>,
    config: Config,
    tls_client_config: Option<rustls::ClientConfig>,
    // this is basically an immutable map, we use arc to share it across tasks (e.g. for each request sending)
    mime_codec_map: Arc<MimeCodecMap>,
    source_is_connected: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Connector for Client {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let source = HttpRequestSource {
            source_is_connected: self.source_is_connected.clone(),
            rx: self
                .response_rx
                .take()
                .ok_or(GenericImplementationError::AlreadyConnected)?,
        };
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = HttpRequestSink::new(
            self.response_tx.clone(),
            builder.reply_tx(),
            self.config.clone(),
            self.tls_client_config.clone(),
            self.mime_codec_map.clone(),
            self.source_is_connected.clone(),
            if self.tls_client_config.is_some() {
                "https"
            } else {
                "http"
            },
        );
        Ok(Some(builder.spawn(sink, ctx)))
    }
}

struct HttpRequestSource {
    source_is_connected: Arc<AtomicBool>,
    rx: Receiver<SourceReply>,
}

#[async_trait::async_trait()]
impl Source for HttpRequestSource {
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        anyhow::Ok(
            self.rx
                .recv()
                .await
                .ok_or(GenericImplementationError::ChannelEmpty)?,
        )
    }

    fn is_transactional(&self) -> bool {
        false
    }

    /// there is no asynchronous task driving this and is being stopped by the quiescence process
    fn asynchronous(&self) -> bool {
        false
    }

    async fn on_cb_restore(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        // we will only know if we are connected to some pipelines if we receive a CBAction::Restore contraflow event
        // we will not send responses to out/err if we are not connected and this is determined by this variable
        self.source_is_connected.store(true, Ordering::Release);
        Ok(())
    }
}

struct HttpRequestSink {
    request_counter: u64,
    client: Option<Arc<HyperClient<HttpsConnector<HttpConnector>, StreamingBody>>>,
    response_tx: Sender<SourceReply>,
    reply_tx: ReplySender,
    config: Config,
    tls_client_config: Option<rustls::ClientConfig>,
    // reply_tx: ReplySender,
    concurrency_cap: ConcurrencyCap,
    origin_uri: EventOriginUri,
    codec_map: Arc<MimeCodecMap>,
    scheme: &'static str,
    // we should only send responses down the channel when we know there is a source consuming them
    // otherwise the channel would fill up and we'd be stuck
    // TODO: find/implement a channel that just throws away the oldest message when it is full, like a ring-buffer
    source_is_connected: Arc<AtomicBool>,
}

impl HttpRequestSink {
    fn new(
        response_tx: Sender<SourceReply>,
        reply_tx: ReplySender,
        config: Config,
        tls_client_config: Option<rustls::ClientConfig>,
        codec_map: Arc<MimeCodecMap>,
        source_is_connected: Arc<AtomicBool>,
        scheme: &'static str,
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
            source_is_connected,
            scheme,
        }
    }
}

#[async_trait::async_trait()]
impl Sink for HttpRequestSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let https = if let Some(tls_config) = self.tls_client_config.clone() {
            HttpsConnectorBuilder::new()
                .with_tls_config(tls_config)
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .build()
        } else {
            HttpsConnectorBuilder::new()
                .with_native_roots()?
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .build()
        };
        let client = HyperClient::builder(TokioExecutor::new()).build(https);

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
    ) -> anyhow::Result<SinkReply> {
        // constrain to max concurrency - propagate CB close on hitting limit
        let guard = self.concurrency_cap.inc_for(&event)?;

        if let Some(client) = self.client.clone() {
            // TODO: think about making ctx an Arc so it doesn't have to be cloned deep
            let task_ctx = ctx.clone();

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
                HttpRequestBuilder::new(request_id, http_meta, &self.codec_map, &self.config),
                "Error turning event into an HTTP Request",
            )?;

            let codec_map = self.codec_map.clone();
            let request = builder.take_request()?;

            let req_meta = extract_request_meta(&request, self.scheme)?;
            let t = self
                .config
                .timeout
                .map_or_else(|| Duration::from_secs(60), Duration::from_nanos);
            // spawn the sending task
            tokio::task::spawn(async move {
                // extract request meta for the response metadata from the finally prepared request
                // the actual sent request might differ from the metadata used to create this request
                if let Some(host) = request.uri().host() {
                    origin_uri.host = host.to_string();
                }
                origin_uri.port = request.uri().port_u16();
                origin_uri.path = request
                    .uri()
                    .path()
                    .split('/')
                    .map(ToString::to_string)
                    .collect();
                let response = client.request(request);
                match timeout(t, response).await {
                    Ok(Ok(response)) => {
                        let response_meta = extract_response_meta(&response)?;

                        let headers = response.headers();

                        if let Some(response_tx) = response_tx {
                            let codec_name = if let Some(mime_header) =
                                headers.get(hyper::header::CONTENT_TYPE)
                            {
                                // https://static.wikia.nocookie.net/disney-fan-fiction/images/9/99/Nemo-Seagulls_.jpg/revision/latest?cb=20130722023815
                                let mime: mime::Mime = mime_header.to_str()?.parse()?;

                                codec_map.get_codec_name(mime.essence_str())
                            } else {
                                None
                            };
                            let codec_overwrite = codec_name.cloned();

                            let body = response.collect().await?;
                            let data = body.to_bytes().to_vec();
                            let content_length = data.len() as u64;

                            let mut meta = task_ctx.meta(literal!({
                                "request": req_meta,
                                "request_id": request_id.get(),
                                "response": response_meta,
                                "content-length": content_length,
                            }));

                            if let Some(corr_meta) = correlation_meta {
                                meta.try_insert("correlation", corr_meta);
                            }

                            let reply = SourceReply::Data {
                                origin_uri,
                                data,
                                meta: Some(meta),
                                stream: None, // a response (as well as a request) is a discrete unit and not part of a stream
                                port: None,
                                codec_overwrite,
                            };
                            task_ctx.swallow_err(
                                response_tx.send(reply).await,
                                "Error sending response to source",
                            );
                        }
                        if let Some(contraflow_data) = contraflow_data {
                            task_ctx.swallow_err(
                                reply_tx
                                    .send(AsyncSinkReply::Ack(contraflow_data, nanotime() - start)),
                                "Error sending ack contraflow",
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        error!("{task_ctx} Error sending HTTP request: {e}");
                        if let Some(contraflow_data) = contraflow_data {
                            task_ctx.swallow_err(
                                reply_tx.send(AsyncSinkReply::Fail(contraflow_data)),
                                "Error sending fail contraflow",
                            );
                        }
                        // We force a reconnect as otherwise the HTTP library can become stale and never progress when sending failed.
                        task_ctx.notifier().connection_lost().await?;
                    }
                    Err(e) => {
                        error!("{task_ctx} Error sending HTTP request: {e}");
                        if let Some(contraflow_data) = contraflow_data {
                            task_ctx.swallow_err(
                                reply_tx.send(AsyncSinkReply::Fail(contraflow_data)),
                                "Error sending fail contraflow",
                            );
                        }
                        // task_ctx.notifier().connection_lost().await?;
                    }
                }
                drop(guard);
                anyhow::Ok(())
            });

            // if we have a chunked request we still gotta do some work (sending the chunks)
            for (value, meta) in event.value_meta_iter() {
                ctx.bail_err(
                    builder.append(value, meta, ingest_ns, serializer).await,
                    "Error serializing event into request body",
                )?;
            }
            ctx.bail_err(
                builder.finalize(serializer).await,
                "Error serializing final parts of the event into request body",
            )?;
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

/// Utility for building an HTTP request from a possibly batched event
/// and some configuration values
pub(crate) struct HttpRequestBuilder {
    request_id: RequestId,
    request: Option<StreamingRequest>,
    chunk_tx: Sender<Vec<u8>>,
    codec_overwrite: Option<NameWithConfig>,
}

// TODO: do some deduplication with SinkResponse
impl HttpRequestBuilder {
    pub(super) fn new(
        request_id: RequestId,
        meta: Option<&Value>,
        codec_map: &MimeCodecMap,
        config: &client::Config,
    ) -> anyhow::Result<Self> {
        let request_meta = meta.get("request");
        let method = if let Some(method_v) = request_meta.get("method") {
            Method::from_bytes(method_v.as_bytes().ok_or(Error::InvalidMethod)?)?
        } else {
            config.method.0.clone()
        };
        let uri: Uri = if let Some(url_v) = request_meta.get("url") {
            url_v.as_str().ok_or(Error::InvalidUrl)?.parse()?
        } else {
            config.url.to_string().parse()?
        };
        let mut builder = Request::builder().method(method).uri(uri);

        // first insert config headers
        for (config_header_name, config_header_values) in &config.headers {
            match &config_header_values.0 {
                Either::Left(config_header_values) => {
                    for header_value in config_header_values {
                        builder =
                            builder.header(config_header_name.as_str(), header_value.as_str());
                    }
                }
                Either::Right(header_value) => {
                    builder = builder.header(config_header_name.as_str(), header_value.as_str());
                }
            }
        }
        let headers = request_meta.get("headers");

        // build headers
        if let Some(headers) = headers.as_object() {
            for (name, values) in headers {
                let name = HeaderName::from_bytes(name.as_bytes())?;
                for value in HeaderValueValue::new(values) {
                    builder = builder.header(&name, value);
                }
            }
        }

        let header_content_type = content_type(builder.headers_ref())?;

        let (codec_overwrite, content_type) =
            consolidate_mime(header_content_type.clone(), codec_map);

        // set the content type if it is not set yet
        if header_content_type.is_none() {
            if let Some(ct) = content_type {
                builder = builder.header(header::CONTENT_TYPE, ct.to_string());
            }
        }
        // handle AUTH
        if let Some(auth_header) = config.auth.as_header_value()? {
            builder = builder.header(hyper::header::AUTHORIZATION, auth_header);
        }

        let (chunk_tx, mut chunk_rx) = channel::<Vec<u8>>(qsize());

        let body: StreamingBody = StreamBody::new(Box::pin(async_stream::stream! {
            while let Some(item) = chunk_rx.recv().await {
                yield Ok::<_, Infallible>(hyper::body::Frame::data(hyper::body::Bytes::from(item)));
            }
        }));

        let request = builder
            .body(body)
            .map_err(|_| Error::RequestAlreadyConsumed)?;

        // extract headers
        // determine content-type, override codec and chunked encoding
        Ok(Self {
            request_id,
            request: Some(request),
            chunk_tx,
            codec_overwrite,
        })
    }

    pub(super) async fn append<'event>(
        &mut self,
        value: &'event Value<'event>,
        meta: &'event Value<'event>,
        ingest_ns: u64,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        let chunks = serializer
            .serialize_for_stream_with_codec(
                value,
                meta,
                ingest_ns,
                self.request_id.get(),
                self.codec_overwrite.as_ref(),
            )
            .await?;
        self.append_data(chunks).await
    }

    async fn append_data(&mut self, chunks: Vec<Vec<u8>>) -> anyhow::Result<()> {
        for chunk in chunks {
            self.chunk_tx.send(chunk).await?;
        }
        Ok(())
    }

    pub(super) fn take_request(&mut self) -> Result<StreamingRequest, Error> {
        self.request.take().ok_or(Error::RequestAlreadyConsumed)
    }

    /// Finalize and send the response.
    /// In the chunked case we have already sent it before.
    ///
    /// After calling this function this instance shouldn't be used anymore
    pub(super) async fn finalize(
        &mut self,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        // finalize the stream
        let rest = serializer.finish_stream(self.request_id.get())?;
        self.append_data(rest).await
    }
}

#[cfg(test)]
mod tests {
    use crate::CodecReq;
    use crate::ConnectorType;
    use tremor_common::alias;
    use tremor_config::Impl;

    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn builder() -> anyhow::Result<()> {
        let request_id = RequestId::new(42);
        let meta = None;
        let codec_map = MimeCodecMap::default();
        let c = literal!({"headers": {
            "cake": ["black forst", "cheese"],
            "pie": "key lime"
        }});
        let mut s = EventSerializer::new(
            None,
            CodecReq::Optional("json"),
            vec![],
            &ConnectorType("http".into()),
            &alias::Connector::new("flow", "http"),
        )?;
        let config = client::Config::new(&c)?;

        let mut b = HttpRequestBuilder::new(request_id, meta, &codec_map, &config)?;

        let r = b.take_request()?;
        b.finalize(&mut s).await?;
        assert_eq!(r.headers().get_all("pie").iter().count(), 1);
        assert_eq!(r.headers().get_all("cake").iter().count(), 2);
        Ok(())
    }
}
