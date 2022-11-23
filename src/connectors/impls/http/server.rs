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

use crate::connectors::{
    prelude::*,
    utils::{mime::MimeCodecMap, tls::TLSServerConfig},
};
use crate::{connectors::spawn_task, errors::err_connector_def};
use async_std::channel::unbounded;
use async_std::{
    channel::{bounded, Receiver, Sender},
    task::JoinHandle,
};
use dashmap::DashMap;
use halfbrown::{Entry, HashMap};
use http_types::headers::{self, HeaderValue, HeaderValues};
use http_types::{mime::BYTE_STREAM, Mime, StatusCode};
use simd_json::ValueAccess;
use std::{str::FromStr, sync::Arc};
use tide::{
    listener::{Listener, ToListener},
    Response,
};
use tide_rustls::TlsListener;
use tremor_common::ids::Id;

use super::meta::{extract_request_meta, BodyData};
use super::utils::{FixedBodyReader, RequestId, StreamingBodyReader};

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Our target URL
    #[serde(default = "Default::default")]
    url: Url,
    /// TLS configuration, if required
    tls: Option<TLSServerConfig>,
    /// custom codecs mapping from mime_type to custom codec name
    /// e.g. for handling `application/json` with the `binary` codec, if desired
    /// the mime type of `*/*` serves as a default / fallback
    mime_mapping: Option<HashMap<String, String>>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

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
        id: &Alias,
        raw_config: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let tls_server_config = config.tls.clone();

        if tls_server_config.is_some() && config.url.scheme() != "https" {
            return Err(err_connector_def(id, Self::HTTPS_REQUIRED));
        }
        let origin_uri = EventOriginUri {
            scheme: "http-server".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: vec![],
        };
        // extract expected content types from configured codec
        let configured_codec = raw_config
            .codec
            .as_ref()
            .map_or_else(|| HttpServer::DEFAULT_CODEC.to_string(), |c| c.name.clone());
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
            configured_codec,
            codec_map,
        }))
    }
}

#[allow(clippy::module_name_repetitions)]
pub(crate) struct HttpServer {
    config: Config,
    origin_uri: EventOriginUri,
    tls_server_config: Option<TLSServerConfig>,
    inflight: Arc<DashMap<RequestId, Sender<Response>>>,
    configured_codec: String,
    codec_map: MimeCodecMap,
}

impl HttpServer {
    const DEFAULT_CODEC: &'static str = "json";
    /// we need to avoid 0 as a request id
    ///
    /// As we misuse the `request_id` as the `stream_id` and if we use 0, we get `DEFAULT_STREAM_ID`
    /// and with that id the custom codec overwrite doesn't work
    const REQUEST_COUNTER_START: u64 = 1;
}

#[async_trait::async_trait()]
impl Connector for HttpServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional(Self::DEFAULT_CODEC)
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let (request_tx, request_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let source = HttpServerSource {
            url: self.config.url.clone(),
            inflight: self.inflight.clone(),
            request_counter: Self::REQUEST_COUNTER_START,
            request_tx,
            request_rx,
            origin_uri: self.origin_uri.clone(),
            server_task: None,
            tls_server_config: self.tls_server_config.clone(),
            configured_codec: self.configured_codec.clone(),
            codec_map: self.codec_map.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = HttpServerSink::new(
            self.inflight.clone(),
            self.codec_map.clone(),
            self.configured_codec.clone(),
        );
        builder.spawn(sink, sink_context).map(Some)
    }
}

struct HttpServerSource {
    url: Url,
    origin_uri: EventOriginUri,
    inflight: Arc<DashMap<RequestId, Sender<Response>>>,
    request_counter: u64,
    request_rx: Receiver<RawRequestData>,
    request_tx: Sender<RawRequestData>,
    server_task: Option<JoinHandle<()>>,
    tls_server_config: Option<TLSServerConfig>,
    configured_codec: String,
    codec_map: MimeCodecMap,
}

#[async_trait::async_trait()]
impl Source for HttpServerSource {
    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(accept_task) = self.server_task.take() {
            // stop acceptin' new connections
            accept_task.cancel().await;
        }
        Ok(())
    }

    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        let host = self.url.host_or_local();
        let port = self.url.port().unwrap_or_else(|| {
            if self.url.scheme() == "https" {
                443
            } else {
                80
            }
        });
        let hostport = format!("{}:{}", host, port);

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(server_task) = self.server_task.take() {
            server_task.cancel().await;
        }
        // TODO: clear out the inflight map. Q: How to drain the map without losing responses?
        // Answer all pending requests with a 503 status?

        let tx = self.request_tx.clone();

        let ctx = ctx.clone();
        let tls_server_config = self.tls_server_config.clone();

        // Server task - this is the main receive loop for http server instances
        self.server_task = Some(spawn_task(ctx.clone(), async move {
            if let Some(tls_server_config) = tls_server_config {
                let mut endpoint = tide::Server::with_state(HttpServerState::new(tx, ctx.clone()));
                endpoint.at("/").all(handle_request);
                endpoint.at("/*").all(handle_request);

                let mut listener = TlsListener::build()
                    .addrs(&hostport)
                    .cert(&tls_server_config.cert)
                    .key(&tls_server_config.key)
                    .finish()?;
                listener.bind(endpoint).await?;
                if let Some(info) = listener.info().into_iter().next() {
                    info!(
                        "{ctx} Listening for HTTPS requests on {}",
                        info.connection()
                    );
                }
                listener.accept().await?;
            } else {
                let mut endpoint = tide::Server::with_state(HttpServerState::new(tx, ctx.clone()));
                endpoint.at("/").all(handle_request);
                endpoint.at("/*").all(handle_request);
                let mut listener = (&hostport).to_listener()?;
                listener.bind(endpoint).await?;
                if let Some(info) = listener.info().into_iter().next() {
                    info!("{ctx} Listening for HTTP requests on {}", info.connection());
                }
                listener.accept().await?;
            };
            Ok(())
        }));

        Ok(true)
    }

    async fn pull_data(&mut self, pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        let RawRequestData {
            data,
            request_meta,
            content_type,
            response_channel,
        } = self.request_rx.recv().await?;

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
            let codec_overwrite = if let Some(content_type) = content_type {
                let maybe_codec = self.codec_map.get_codec_name(content_type.as_str());
                maybe_codec
                    .filter(|c| *c != &self.configured_codec)
                    .cloned()
            } else {
                None
            };
            SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data,
                meta: Some(meta),
                stream: None, // a http request is a discrete unit and not part of any stream
                port: None,
                codec_overwrite,
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
    inflight: Arc<DashMap<RequestId, Sender<Response>>>,
    codec_map: MimeCodecMap,
    configured_codec: String,
}

impl HttpServerSink {
    const ERROR_MSG_EXTRACT_VALUE: &'static str = "Error turning Event into HTTP response";
    const ERROR_MSG_APPEND_RESPONSE: &'static str = "Error appending batched data to HTTP response";

    fn new(
        inflight: Arc<DashMap<RequestId, Sender<Response>>>,
        codec_map: MimeCodecMap,
        configured_codec: String,
    ) -> Self {
        Self {
            inflight,
            codec_map,
            configured_codec,
        }
    }
}

#[async_trait::async_trait()]
impl Sink for HttpServerSink {
    #[allow(clippy::too_many_lines)]
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let ingest_ns = event.ingest_ns;
        let min_pull_id = event.id.get_min_by_stream(ctx.uid.id(), DEFAULT_STREAM_ID);
        let max_pull_id = event.id.get_max_by_stream(ctx.uid.id(), DEFAULT_STREAM_ID);

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
                                SinkResponse::build(
                                    rid,
                                    sender,
                                    http_meta,
                                    &self.codec_map,
                                    &self.configured_codec,
                                )
                                .await,
                                Self::ERROR_MSG_EXTRACT_VALUE,
                            )?;

                            ctx.bail_err(
                                response.append(value, ingest_ns, serializer).await,
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
                            o.get_mut().append(value, ingest_ns, serializer).await,
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
                                            &self.configured_codec,
                                        )
                                        .await,
                                        Self::ERROR_MSG_EXTRACT_VALUE,
                                    )?;

                                    ctx.bail_err(
                                        response.append(value, ingest_ns, serializer).await,
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
                                    o.get_mut().append(value, ingest_ns, serializer).await,
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
                                                &self.configured_codec,
                                            )
                                            .await,
                                            Self::ERROR_MSG_EXTRACT_VALUE,
                                        )?;

                                        ctx.bail_err(
                                            response.append(value, ingest_ns, serializer).await,
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
                                        o.get_mut().append(value, ingest_ns, serializer).await,
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
    ) -> Result<SinkReply> {
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
    res: Option<Response>,
    body_data: BodyData,
    tx: Sender<Response>,
    codec_overwrite: Option<String>,
}

impl SinkResponse {
    async fn build<'event>(
        request_id: RequestId,
        tx: Sender<Response>,
        http_meta: Option<&Value<'event>>,
        codec_map: &MimeCodecMap,
        configured_codec: &String,
    ) -> Result<Self> {
        let mut res = tide::Response::new(StatusCode::Ok);

        // build response headers and status etc.
        let request_meta = http_meta.get("request");
        let response_meta = http_meta.get("response");

        let status = if let Some(response_meta) = response_meta {
            // Use user provided status - or default to 200
            if let Some(status) = response_meta.get_u16("status") {
                StatusCode::try_from(status)?
            } else {
                StatusCode::Ok
            }
        } else {
            // Otherwise - Default status based on request method
            request_meta.map_or(StatusCode::Ok, |request_meta| {
                let method = request_meta.get_str("method").unwrap_or("error");
                match method {
                    "DELETE" | "delete" => StatusCode::NoContent,
                    "POST" | "post" => StatusCode::Created,
                    _otherwise => StatusCode::Ok,
                }
            })
        };
        res.set_status(status);
        let headers = response_meta.get("headers");

        // build headers
        if let Some(headers) = headers.as_object() {
            for (name, values) in headers {
                if let Some(header_values) = values.as_array() {
                    let mut v = Vec::with_capacity(header_values.len());
                    for value in header_values {
                        if let Some(header_value) = value.as_str() {
                            v.push(HeaderValue::from_str(header_value)?);
                        }
                    }
                    res.append_header(name.as_ref(), v.as_slice());
                } else if let Some(header_value) = values.as_str() {
                    res.append_header(name.as_ref(), header_value);
                }
            }
        }
        let chunked = res
            .header(headers::TRANSFER_ENCODING)
            .map(HeaderValues::last)
            .map_or(false, |te| te.as_str() == "chunked");

        let header_content_type = res.content_type();

        let codec_overwrite = header_content_type
            .as_ref()
            .and_then(|mime| codec_map.get_codec_name(mime.essence()))
            // only overwrite the codec if it is different from the configured one
            .filter(|codec| *codec != configured_codec)
            .cloned();
        let codec_content_type = codec_overwrite
            .as_ref()
            .and_then(|codec| codec_map.get_mime_type(codec.as_str()))
            .or_else(|| codec_map.get_mime_type(configured_codec))
            .and_then(|mime| Mime::from_str(mime).ok());

        // extract content-type and thus possible codec overwrite only from first element
        // precedence:
        //  1. from headers meta
        //  2. from overwritten codec
        //  3. from configured codec
        //  4. fall back to application/octet-stream if codec doesn't provide a mime-type
        let content_type = Some(
            header_content_type
                .or(codec_content_type)
                .unwrap_or(BYTE_STREAM),
        );

        // set content-type if not explicitly set in the response headers meta
        // either from the configured or overwritten codec
        if res.content_type().is_none() {
            if let Some(ct) = content_type {
                res.set_content_type(ct);
            }
        }
        let (body_data, res) = if chunked {
            let (chunk_tx, chunk_rx) = unbounded();
            let streaming_reader = StreamingBodyReader::new(chunk_rx);
            res.set_body(tide::Body::from_reader(streaming_reader, None));
            // chunked encoding and content-length cannot go together
            res.remove_header(headers::CONTENT_LENGTH);
            // we can already send out the response and stream the rest of the chunks upon calling `append`
            tx.send(res).await?;
            (BodyData::Chunked(chunk_tx), None)
        } else {
            (BodyData::Data(Vec::with_capacity(4)), Some(res))
        };
        Ok(Self {
            request_id,
            res,
            body_data,
            tx,
            codec_overwrite,
        })
    }

    async fn append<'event>(
        &mut self,
        value: &'event Value<'event>,
        ingest_ns: u64,
        serializer: &mut EventSerializer,
    ) -> Result<()> {
        let chunks = serializer.serialize_for_stream_with_codec(
            value,
            ingest_ns,
            self.request_id.get(),
            self.codec_overwrite.as_ref(),
        )?;
        self.append_data(chunks).await
    }

    async fn append_data(&mut self, mut chunks: Vec<Vec<u8>>) -> Result<()> {
        match &mut self.body_data {
            BodyData::Chunked(tx) => {
                for chunk in chunks {
                    tx.send(chunk).await?;
                }
            }
            BodyData::Data(data) => data.append(&mut chunks),
        }
        Ok(())
    }

    /// Consume self and finalize and send the response.
    /// In the chunked case we have already sent it before.
    async fn finalize(mut self, serializer: &mut EventSerializer) -> Result<()> {
        // finalize the stream
        let rest = serializer.finish_stream(self.request_id.get())?;
        if !rest.is_empty() {
            self.append_data(rest).await?;
        }
        // send response if necessary
        match self.body_data {
            BodyData::Data(data) => {
                if let Some(mut response) = self.res.take() {
                    // set body
                    let reader = FixedBodyReader::new(data);
                    let len = reader.len();
                    response.set_body(tide::Body::from_reader(reader, Some(len)));
                    // send off the response
                    self.tx.send(response).await?;
                }
            }
            BodyData::Chunked(tx) => {
                // signal EOF to the `StreamingBodyReader`
                tx.close();
                // the response has been sent already, nothing left to do here
            }
        }
        // close the channel. we are done here
        self.tx.close();
        Ok(())
    }
}

#[derive(Clone)]
struct HttpServerState {
    tx: Sender<RawRequestData>,
    ctx: SourceContext,
}

impl HttpServerState {
    fn new(tx: Sender<RawRequestData>, ctx: SourceContext) -> Self {
        Self { tx, ctx }
    }
}

#[derive(Debug)]
struct RawRequestData {
    data: Vec<u8>,
    // metadata about the request, not the ready event meta, still needs to be wrapped
    request_meta: Value<'static>,
    content_type: Option<String>,
    response_channel: Sender<Response>,
}

async fn handle_request(mut req: tide::Request<HttpServerState>) -> tide::Result<tide::Response> {
    // NOTE We wrap and crap as tide doesn't report donated route handler's errors
    let result = _handle_request(&mut req).await;
    if let Err(e) = result {
        error!(
            "{ctx} Error handling HTTP server request {e}",
            ctx = req.state().ctx
        );
        Err(e)
    } else {
        result
    }
}
async fn _handle_request(req: &mut tide::Request<HttpServerState>) -> tide::Result<tide::Response> {
    let request_meta = extract_request_meta(req.as_ref());
    let content_type = req.content_type().map(|mime| mime.essence().to_string());
    let data = req.body_bytes().await?;

    // Dispatch
    let (response_tx, response_rx) = bounded(1);
    req.state()
        .tx
        .send(RawRequestData {
            data,
            request_meta,
            content_type,
            response_channel: response_tx,
        })
        .await?;

    Ok(response_rx.recv().await?)
}
