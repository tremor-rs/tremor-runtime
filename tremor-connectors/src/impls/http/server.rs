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
        meta::{consolidate_mime, content_type, extract_request_meta, HeaderValueValue},
        utils::RequestId,
    },
    sink::prelude::*,
    source::prelude::*,
    spawn_task,
    utils::{
        mime::MimeCodecMap,
        socket,
        tls::{self, TLSServerConfig},
    },
};
use dashmap::DashMap;
use halfbrown::{Entry, HashMap};
use http::{header, Response};
use http::{header::HeaderName, StatusCode};
use hyper::{
    body::to_bytes,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request,
};
use std::{convert::Infallible, net::ToSocketAddrs, sync::Arc};
use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};
use tremor_common::{ids::Id, url::Url};
use tremor_config::NameWithConfig;
use tremor_script::ValueAndMeta;
use tremor_system::event::DEFAULT_STREAM_ID;
use tremor_value::prelude::*;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Our target URL
    #[serde(default = "Default::default")]
    url: Url,
    /// TLS configuration, if required
    tls: Option<TLSServerConfig>,
    /// custom codecs mapping from `mime_type` to custom codec name
    /// e.g. for handling `application/json` with the `binary` codec, if desired
    /// the mime type of `*/*` serves as a default / fallback
    mime_mapping: Option<HashMap<String, NameWithConfig>>,
}

impl tremor_config::Impl for Config {}

/// http server connector
#[derive(Debug, Default)]
pub struct Builder {}

impl Builder {
    const HTTPS_REQUIRED: &'static str =
        "Using SSL certificates requires setting up a https endpoint";
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "http_server".into()
    }

    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _raw_config: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let tls_server_config = config.tls.clone();

        if tls_server_config.is_some() && config.url.scheme() != "https" {
            return Err(error_connector_def(id, Self::HTTPS_REQUIRED).into());
        }
        let origin_uri = EventOriginUri {
            scheme: "http-server".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: vec![],
        };
        let inflight = Arc::default();
        let codec_map = if let Some(custom_codecs) = config.mime_mapping.clone() {
            MimeCodecMap::from_custom(custom_codecs)
        } else {
            MimeCodecMap::new()
        };

        Ok(Box::new(HttpServer {
            config,
            origin_uri,
            tls_server_config,
            inflight,
            codec_map,
        }))
    }
}

#[allow(clippy::module_name_repetitions)]
pub(crate) struct HttpServer {
    config: Config,
    origin_uri: EventOriginUri,
    tls_server_config: Option<TLSServerConfig>,
    inflight: Arc<DashMap<RequestId, oneshot::Sender<Response<Body>>>>,
    codec_map: MimeCodecMap,
}

impl HttpServer {
    /// we need to avoid 0 as a request id
    ///
    /// As we misuse the `request_id` as the `stream_id` and if we use 0, we get `DEFAULT_STREAM_ID`
    /// and with that id the custom codec overwrite doesn't work
    const REQUEST_COUNTER_START: u64 = 1;
}

#[async_trait::async_trait()]
impl Connector for HttpServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let (request_tx, request_rx) = channel(qsize());
        let source = HttpServerSource {
            url: self.config.url.clone(),
            inflight: self.inflight.clone(),
            request_counter: Self::REQUEST_COUNTER_START,
            request_tx,
            request_rx,
            origin_uri: self.origin_uri.clone(),
            server_task: None,
            tls_server_config: self.tls_server_config.clone(),

            codec_map: self.codec_map.clone(),
        };
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = HttpServerSink::new(self.inflight.clone(), self.codec_map.clone());
        Ok(Some(builder.spawn(sink, ctx)))
    }
}

struct HttpServerSource {
    url: Url,
    origin_uri: EventOriginUri,
    inflight: Arc<DashMap<RequestId, oneshot::Sender<Response<Body>>>>,
    request_counter: u64,
    request_rx: Receiver<RawRequestData>,
    request_tx: Sender<RawRequestData>,
    server_task: Option<JoinHandle<()>>,
    tls_server_config: Option<TLSServerConfig>,
    codec_map: MimeCodecMap,
}

#[async_trait::async_trait()]
impl Source for HttpServerSource {
    async fn on_stop(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        if let Some(accept_task) = self.server_task.take() {
            // stop acceptin' new connections
            accept_task.abort();
        }
        Ok(())
    }

    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let host = self.url.host_or_local().to_string();
        let port = self.url.port().unwrap_or_else(|| {
            if self.url.scheme() == "https" {
                443
            } else {
                80
            }
        });

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(server_task) = self.server_task.take() {
            server_task.abort();
        }
        // TODO: clear out the inflight map. Q: How to drain the map without losing responses?
        // Answer all pending requests with a 503 status?

        let tx = self.request_tx.clone();

        let ctx = ctx.clone();
        let tls_server_config = self
            .tls_server_config
            .as_ref()
            .map(TLSServerConfig::to_server_config)
            .transpose()?
            .map(Arc::new);

        let server_context = HttpServerState::new(tx, ctx.clone(), "http");
        let addr = (host.as_str(), port)
            .to_socket_addrs()?
            .next()
            .ok_or(socket::Error::InvalidAddress(host, port))?;

        // Server task - this is the main receive loop for http server instances
        self.server_task = Some(spawn_task(ctx.clone(), async move {
            if let Some(server_config) = tls_server_config {
                let make_service = make_service_fn(move |_conn: &tls::Stream| {
                    // We have to clone the context to share it with each invocation of
                    // `make_service`. If your data doesn't implement `Clone` consider using
                    // an `std::sync::Arc`.
                    let server_context = server_context.clone();

                    async move {
                        // Create a `Service` for responding to the request.
                        let service =
                            service_fn(move |req| handle_request(server_context.clone(), req));

                        // Return the service to hyper.
                        anyhow::Ok(service)
                    }
                });
                let incoming = AddrIncoming::bind(&addr)?;
                let listener = hyper::Server::builder(tls::Acceptor::new(server_config, incoming))
                    .serve(make_service);
                listener.await?;
            } else {
                let make_service = make_service_fn(move |_conn: &AddrStream| {
                    // We have to clone the context to share it with each invocation of
                    // `make_service`. If your data doesn't implement `Clone` consider using
                    // an `std::sync::Arc`.
                    let server_context = server_context.clone();

                    // Create a `Service` for responding to the request.
                    let service =
                        service_fn(move |req| handle_request(server_context.clone(), req));

                    // Return the service to hyper.
                    async move { Ok::<_, Infallible>(service) }
                });
                let listener = hyper::Server::bind(&addr).serve(make_service);
                info!(
                    "{ctx} Listening for HTTP requests on {}",
                    listener.local_addr()
                );

                listener.await?;
            };
            Ok(())
        }));

        Ok(true)
    }

    async fn pull_data(
        &mut self,
        pull_id: &mut u64,
        ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        let RawRequestData {
            data,
            request_meta,
            content_type,
            response_channel,
        } = self
            .request_rx
            .recv()
            .await
            .ok_or(GenericImplementationError::ChannelEmpty)?;

        // assign request id, set pull_id
        let request_id = RequestId::new(self.request_counter);
        *pull_id = self.request_counter;
        self.request_counter = self.request_counter.wrapping_add(1);

        // prepare meta
        debug!("{ctx} Received HTTP request with request id {request_id}");
        let meta = ctx.meta(literal!({
            "request": request_meta,
            "request_id": *pull_id
        }));
        // store request context so we can respond to this request
        if self.inflight.insert(request_id, response_channel).is_some() {
            error!("{ctx} Request id collision: {request_id}");
        };
        Ok(if data.is_empty() {
            // NOTE GET, HEAD ...
            SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                payload: EventPayload::from(ValueAndMeta::from_parts(Value::const_null(), meta)),
                stream: DEFAULT_STREAM_ID, // a http request is a discrete unit and not part of any stream
                port: None,
            }
        } else {
            // codec overwrite, depending on requests content-type
            // only set the overwrite if it is different than the configured codec
            let codec_overwrite = content_type
                .and_then(|t| self.codec_map.get_codec_name(t.as_str()))
                .or_else(|| self.codec_map.get_codec_name("*/*"))
                .cloned()
                .unwrap_or_else(|| "binary".into());

            SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data,
                meta: Some(meta),
                stream: None, // a http request is a discrete unit and not part of any stream
                port: None,
                codec_overwrite: Some(codec_overwrite),
            }
        })
    }

    fn is_transactional(&self) -> bool {
        //TODO: add ack/fail handling when not using custom_responses
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}

struct HttpServerSink {
    inflight: Arc<DashMap<RequestId, oneshot::Sender<Response<Body>>>>,
    codec_map: MimeCodecMap,
}

impl HttpServerSink {
    const ERROR_MSG_EXTRACT_VALUE: &'static str = "Error turning Event into HTTP response";
    const ERROR_MSG_APPEND_RESPONSE: &'static str = "Error appending batched data to HTTP response";

    fn new(
        inflight: Arc<DashMap<RequestId, oneshot::Sender<Response<Body>>>>,
        codec_map: MimeCodecMap,
    ) -> Self {
        Self {
            inflight,
            codec_map,
        }
    }
}

#[async_trait::async_trait()]
impl Sink for HttpServerSink {
    #[allow(clippy::too_many_lines)]
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_system::event::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        let ingest_ns = event.ingest_ns;
        let min_pull_id = event
            .id
            .get_min_by_stream(ctx.uid().id(), DEFAULT_STREAM_ID);
        let max_pull_id = event
            .id
            .get_max_by_stream(ctx.uid().id(), DEFAULT_STREAM_ID);

        // batch handling:
        // - extract the request_id for each batch element
        // - get or create a SinkResponse for each request_id (chose content type, chunked etc for each request id separately)
        // - store: request_id -> (SinkResponse, Sender)
        // - update SinkResponse for each element of the batch
        // - send response immediately in case of chunked encoding
        let mut response_map = HashMap::new();
        for (value, meta) in event.value_meta_iter() {
            let http_meta = ctx.extract_meta(meta);

            // first try to extract request_id from event batch element metadata
            if let Some(rid) = http_meta.get_u64("request_id").map(RequestId::new) {
                match response_map.entry(rid) {
                    Entry::Vacant(k) => {
                        if let Some((rid, sender)) = self.inflight.remove(&rid) {
                            debug!("{ctx} Building response for request_id {rid}");
                            let mut response = ctx.bail_err(
                                SinkResponse::build(rid, sender, http_meta, &self.codec_map),
                                Self::ERROR_MSG_EXTRACT_VALUE,
                            )?;

                            ctx.bail_err(
                                response.append(value, meta, ingest_ns, serializer).await,
                                Self::ERROR_MSG_APPEND_RESPONSE,
                            )?;
                            k.insert(response);
                        } else {
                            warn!(
                                "{ctx} No request context found for `request_id`: {rid}. Dropping response."
                            );
                            continue;
                        }
                    }
                    Entry::Occupied(mut o) => {
                        ctx.bail_err(
                            o.get_mut().append(value, meta, ingest_ns, serializer).await,
                            Self::ERROR_MSG_APPEND_RESPONSE,
                        )?;
                    }
                }
            } else {
                // fallback if no request_id is in the metadata, try extract it from tracked pull_id
                match min_pull_id.zip(max_pull_id) {
                    Some((min, max)) if min == max => {
                        // single pull_id
                        let rid = RequestId::new(min);

                        match response_map.entry(rid) {
                            Entry::Vacant(k) => {
                                if let Some((rid, sender)) = self.inflight.remove(&rid) {
                                    debug!("{ctx} Building response for request_id {rid}");
                                    let mut response = ctx.bail_err(
                                        SinkResponse::build(
                                            rid,
                                            sender,
                                            http_meta,
                                            &self.codec_map,
                                        ),
                                        Self::ERROR_MSG_EXTRACT_VALUE,
                                    )?;

                                    ctx.bail_err(
                                        response.append(value, meta, ingest_ns, serializer).await,
                                        Self::ERROR_MSG_APPEND_RESPONSE,
                                    )?;
                                    k.insert(response);
                                } else {
                                    warn!("{ctx} No request context found for Request id: {rid}. Dropping response.");
                                    continue;
                                }
                            }
                            Entry::Occupied(mut o) => {
                                ctx.bail_err(
                                    o.get_mut().append(value, meta, ingest_ns, serializer).await,
                                    Self::ERROR_MSG_APPEND_RESPONSE,
                                )?;
                            }
                        }
                    }
                    Some((min, max)) => {
                        // range of pull_ids, we need to multiplex to multiple requests
                        for rid in (min..=max).map(RequestId::new) {
                            match response_map.entry(rid) {
                                Entry::Vacant(k) => {
                                    if let Some((rid, sender)) = self.inflight.remove(&rid) {
                                        debug!("{ctx} Building response for request_id {rid}");
                                        let mut response = ctx.bail_err(
                                            SinkResponse::build(
                                                rid,
                                                sender,
                                                http_meta,
                                                &self.codec_map,
                                            ),
                                            Self::ERROR_MSG_EXTRACT_VALUE,
                                        )?;

                                        ctx.bail_err(
                                            response
                                                .append(value, meta, ingest_ns, serializer)
                                                .await,
                                            Self::ERROR_MSG_APPEND_RESPONSE,
                                        )?;
                                        k.insert(response);
                                    } else {
                                        warn!("{ctx} No request context found for Request id: {rid}. Dropping response.");
                                        continue;
                                    }
                                }
                                Entry::Occupied(mut o) => {
                                    ctx.bail_err(
                                        o.get_mut()
                                            .append(value, meta, ingest_ns, serializer)
                                            .await,
                                        Self::ERROR_MSG_APPEND_RESPONSE,
                                    )?;
                                }
                            }
                        }
                    }
                    None => {
                        // unroutable
                        warn!("{ctx} Unable to extract request_id from event. Dropping response.");
                    }
                }
            }
        }

        if response_map.is_empty() {
            error!("{ctx} No request context found for event.");
            return Ok(SinkReply::FAIL);
        }
        for (rid, response) in response_map {
            debug!("{ctx} Sending response for request_id {rid}");
            ctx.swallow_err(
                response.finalize(serializer).await,
                &format!("Error sending response for request_id {rid}"),
            );
        }
        Ok(SinkReply::NONE)
    }

    async fn on_signal(
        &mut self,
        _signal: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
    ) -> anyhow::Result<SinkReply> {
        // clean out closed channels
        self.inflight.retain(|_key, sender| !sender.is_closed());
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

struct SinkResponse {
    request_id: RequestId,
    chunk_tx: Sender<Vec<u8>>,
    codec_overwrite: Option<NameWithConfig>,
}

impl SinkResponse {
    fn build(
        request_id: RequestId,
        tx: oneshot::Sender<Response<Body>>,
        http_meta: Option<&Value>,
        codec_map: &MimeCodecMap,
    ) -> anyhow::Result<Self> {
        let mut response = Response::builder();

        // build response headers and status etc.
        let request_meta = http_meta.get("request");
        let response_meta = http_meta.get("response");

        let status = if let Some(response_meta) = response_meta {
            // Use user provided status - or default to 200
            if let Some(status) = response_meta.get_u16("status") {
                StatusCode::try_from(status)?
            } else {
                StatusCode::OK
            }
        } else {
            // Otherwise - Default status based on request method
            request_meta.map_or(StatusCode::OK, |request_meta| {
                let method = request_meta.get_str("method").unwrap_or("error");
                match method.to_lowercase().as_str() {
                    "delete" => StatusCode::NO_CONTENT,
                    "post" => StatusCode::CREATED,
                    _otherwise => StatusCode::OK,
                }
            })
        };
        response = response.status(status);
        let headers = response_meta.get("headers");

        // build headers
        if let Some(headers) = headers.as_object() {
            for (name, values) in headers {
                let name = HeaderName::from_bytes(name.as_bytes())?;
                for value in HeaderValueValue::new(values) {
                    response = response.header(&name, value);
                }
            }
        }

        let header_content_type = content_type(response.headers_ref())?;

        let (codec_overwrite, content_type) =
            consolidate_mime(header_content_type.clone(), codec_map);

        // set content-type if not explicitly set in the response headers meta
        // either from the configured or overwritten codec
        if header_content_type.is_none() {
            if let Some(ct) = content_type {
                response = response.header(header::CONTENT_TYPE, ct.to_string());
            }
        }
        let (chunk_tx, mut chunk_rx) = channel(qsize());

        // we can already send out the response and stream the rest of the chunks upon calling `append`
        let body = Body::wrap_stream(async_stream::stream! {
            while let Some(item) = chunk_rx.recv().await {
                yield Ok::<_, Infallible>(item);
            }
        });
        tx.send(response.body(body)?)
            .map_err(|_| anyhow::anyhow!("failed to send response"))?;
        Ok(Self {
            request_id,
            chunk_tx,
            codec_overwrite,
        })
    }

    async fn append<'event>(
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

    /// Consume self and finalize and send the response.
    /// In the chunked case we have already sent it before.
    async fn finalize(mut self, serializer: &mut EventSerializer) -> anyhow::Result<()> {
        // finalize the stream
        let rest = serializer.finish_stream(self.request_id.get())?;
        if !rest.is_empty() {
            self.append_data(rest).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct HttpServerState {
    tx: Sender<RawRequestData>,
    ctx: SourceContext,
    scheme: &'static str,
}

impl HttpServerState {
    fn new(tx: Sender<RawRequestData>, ctx: SourceContext, scheme: &'static str) -> Self {
        Self { tx, ctx, scheme }
    }
}

#[derive(Debug)]
struct RawRequestData {
    data: Vec<u8>,
    // metadata about the request, not the ready event meta, still needs to be wrapped
    request_meta: Value<'static>,
    content_type: Option<String>,
    response_channel: oneshot::Sender<Response<Body>>,
}

async fn handle_request(
    mut context: HttpServerState,
    req: Request<Body>,
) -> http::Result<Response<Body>> {
    // NOTE We wrap and crap as tide doesn't report donated route handler's errors
    let result = _handle_request(&mut context, req).await;
    match result {
        Ok(response) => Ok(response),
        Err(e) => {
            error!(
                "{ctx} Error handling HTTP server request {e}",
                ctx = &context.ctx
            );
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
        }
    }
}

async fn _handle_request(
    context: &mut HttpServerState,
    req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let request_meta = extract_request_meta(&req, context.scheme)?;
    let content_type =
        content_type(Some(req.headers()))?.map(|mime| mime.essence_str().to_string());

    let body = req.into_body();
    let data: Vec<u8> = to_bytes(body).await?.to_vec(); // Vec::new();

    // Dispatch
    let (response_tx, response_rx) = oneshot::channel();
    context
        .tx
        .send(RawRequestData {
            data,
            request_meta,
            content_type,
            response_channel: response_tx,
        })
        .await?;

    Ok(response_rx.await?)
}
