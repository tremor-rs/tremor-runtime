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

use crate::codec::{self, Codec};
use crate::connectors::spawn_task;
use crate::connectors::{
    prelude::*,
    utils::{mime::MimeCodecMap, tls::TLSServerConfig},
};
use crate::errors::{Kind as ErrorKind, Result};
use async_std::channel::unbounded;
use async_std::{
    channel::{bounded, Receiver, Sender},
    task::JoinHandle,
};
use dashmap::DashMap;
use halfbrown::{Entry, HashMap};
use http_types::headers::{self, HeaderValue};
use http_types::{mime::BYTE_STREAM, Mime, StatusCode};
use simd_json::ValueAccess;
use std::{str::FromStr, sync::Arc};
use tide::{
    listener::{Listener, ToListener},
    Response,
};
use tide_rustls::TlsListener;

use super::utils::{FixedBodyReader, StreamingBodyReader};

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
    #[serde(default)]
    custom_codecs: HashMap<String, String>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "http_server".into()
    }

    async fn config_to_connector(
        &self,
        id: &str,
        raw_config: &ConnectorConfig,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = &raw_config.config {
            let config = Config::new(config)?;
            let tls_server_config = config.tls.clone();

            if tls_server_config.is_some() && config.url.scheme() != "https" {
                return Err(ErrorKind::InvalidConfiguration(
                    id.to_string(),
                    "Using SSL certificates requires setting up a https endpoint".into(),
                )
                .into());
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
            let codec_mime_type = codec::resolve(&configured_codec.as_str().into())?
                .mime_types()
                .into_iter()
                .next()
                .map(ToString::to_string);
            let inflight = Arc::default();
            let codec_map = MimeCodecMap::with_overwrites(&config.custom_codecs)?;

            Ok(Box::new(HttpServer {
                config,
                origin_uri,
                tls_server_config,
                inflight,
                configured_codec,
                codec_mime_type,
                codec_map,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub(crate) struct HttpServer {
    config: Config,
    origin_uri: EventOriginUri,
    tls_server_config: Option<TLSServerConfig>,
    inflight: Arc<DashMap<RequestId, Sender<Response>>>,
    configured_codec: String,
    codec_mime_type: Option<String>,
    codec_map: MimeCodecMap,
}

impl HttpServer {
    const DEFAULT_CODEC: &'static str = "json";
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
            self.codec_mime_type.clone(),
        );
        builder.spawn(sink, sink_context).map(Some)
    }
}

struct HttpServerSource {
    url: Url,
    origin_uri: EventOriginUri,
    inflight: Arc<DashMap<RequestId, Sender<Response>>>,
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

        // assign request id and prepare meta
        let request_id = RequestId(*pull_id);
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
                stream: DEFAULT_STREAM_ID,
                port: None,
            }
        } else {
            // codec overwrite, depending on requests content-type
            // only set the overwrite if it is different than the configured codec
            let codec_overwrite = if let Some(content_type) = content_type {
                let maybe_codec = self.codec_map.get_codec(content_type.as_str());
                maybe_codec
                    .filter(|c| c.name() != self.configured_codec.as_str())
                    .map(codec::Codec::boxed_clone)
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
    codec_mime_type: Option<String>,
}

impl HttpServerSink {
    const ERROR_MSG_EXTRACT_VALUE: &'static str = "Error turning Event into HTTP response";
    const ERROR_MSG_APPEND_RESPONSE: &'static str = "Error appending batched data to HTTP response";

    fn new(
        inflight: Arc<DashMap<RequestId, Sender<Response>>>,
        codec_map: MimeCodecMap,
        configured_codec: String,
        codec_mime_type: Option<String>,
    ) -> Self {
        Self {
            inflight,
            codec_map,
            configured_codec,
            codec_mime_type,
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
        let min_pull_id = event.id.get_min_by_stream(ctx.uid, DEFAULT_STREAM_ID);
        let max_pull_id = event.id.get_max_by_stream(ctx.uid, DEFAULT_STREAM_ID);

        // batch handling:
        // - extract the request_id for each batch element
        // - create a SinkResponse for each request_id
        // - store: request_id -> (SinkResponse, Sender)
        // - update SinkResponse for each element of the batch
        // - send response immediately in case of chunked encoding
        let mut response_map = HashMap::new();
        let mut chunked = None;
        let mut content_type = None;
        let mut codec_overwrite: Option<&dyn Codec> = None;
        // try to extract the request_id from the first batch item,
        // otherwise fall back to min-max from event_id
        for (value, meta) in event.value_meta_iter() {
            // set codec and content-type upon first iteration
            let http_meta = ctx.extract_meta(meta);
            let response_meta = http_meta.get("response");
            let headers_meta = response_meta.get("headers");
            // extract if we use chunked encoding

            let chunked_header = headers_meta.get(headers::TRANSFER_ENCODING.as_str());

            // extract content type and transfer-encoding from first batch element
            if chunked.is_none() {
                chunked = Some(
                    chunked_header
                        .as_array()
                        .and_then(|te| te.last())
                        .and_then(ValueAccess::as_str)
                        .or_else(|| chunked_header.as_str())
                        .map_or(false, |te| te == "chunked"),
                );
            }
            // extract content-type and thus possible codec overwrite only from first element
            // precedence:
            //  1. from headers meta
            //  2. from overwritten codec
            //  3. from configured codec
            //  4. fall back to application/octet-stream if codec doesn't provide a mime-type
            if content_type.is_none() {
                let content_type_header = headers_meta.get(headers::CONTENT_TYPE.as_str());
                let header_content_type = content_type_header
                    .as_array()
                    .and_then(|ct| ct.last())
                    .and_then(ValueAccess::as_str)
                    .or_else(|| content_type_header.as_str())
                    .map(ToString::to_string);

                codec_overwrite = header_content_type
                    .as_ref()
                    .and_then(|mime_str| Mime::from_str(mime_str).ok())
                    .and_then(|codec| self.codec_map.get_codec(codec.essence()))
                    // only overwrite the codec if it is different from the configured one
                    .filter(|codec| codec.name() != self.configured_codec.as_str());
                let codec_content_type = codec_overwrite
                    .and_then(|codec| codec.mime_types().first().map(ToString::to_string))
                    .or_else(|| self.codec_mime_type.clone());
                content_type = Some(
                    header_content_type
                        .or(codec_content_type)
                        .unwrap_or_else(|| BYTE_STREAM.to_string()),
                );
            }

            // first try to extract request_id from event batch element metadata
            if let Some(rid) = http_meta.get_u64("request_id").map(RequestId) {
                match response_map.entry(rid) {
                    Entry::Vacant(k) => {
                        if let Some((rid, sender)) = self.inflight.remove(&rid) {
                            debug!("{ctx} Building response for request_id {rid}");
                            let mut response = ctx.bail_err(
                                SinkResponse::build(
                                    rid,
                                    chunked.unwrap_or_default(),
                                    sender,
                                    http_meta,
                                    content_type.as_ref(),
                                )
                                .await,
                                Self::ERROR_MSG_EXTRACT_VALUE,
                            )?;

                            let data = ctx.bail_err(
                                serializer.serialize_for_stream_with_codec(
                                    value,
                                    ingest_ns,
                                    rid.0,
                                    codec_overwrite,
                                ),
                                Self::ERROR_MSG_EXTRACT_VALUE,
                            )?;
                            ctx.bail_err(
                                response.append(data).await,
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
                        let data = ctx.bail_err(
                            serializer.serialize_for_stream_with_codec(
                                value,
                                ingest_ns,
                                rid.0,
                                codec_overwrite,
                            ),
                            Self::ERROR_MSG_EXTRACT_VALUE,
                        )?;
                        ctx.bail_err(
                            o.get_mut().append(data).await,
                            Self::ERROR_MSG_APPEND_RESPONSE,
                        )?;
                    }
                }
            } else {
                // fallback if no request_id is in the metadata, try extract it from tracked pull_id
                match min_pull_id.zip(max_pull_id) {
                    Some((min, max)) if min == max => {
                        // single pull_id
                        let rid = RequestId(min);

                        match response_map.entry(rid) {
                            Entry::Vacant(k) => {
                                if let Some((rid, sender)) = self.inflight.remove(&rid) {
                                    debug!("{ctx} Building response for request_id {rid}");
                                    let mut response = ctx.bail_err(
                                        SinkResponse::build(
                                            rid,
                                            chunked.unwrap_or_default(),
                                            sender,
                                            http_meta,
                                            content_type.as_ref(),
                                        )
                                        .await,
                                        Self::ERROR_MSG_EXTRACT_VALUE,
                                    )?;

                                    let data = ctx.bail_err(
                                        serializer.serialize_for_stream_with_codec(
                                            value,
                                            ingest_ns,
                                            rid.0,
                                            codec_overwrite,
                                        ),
                                        Self::ERROR_MSG_EXTRACT_VALUE,
                                    )?;
                                    ctx.bail_err(
                                        response.append(data).await,
                                        Self::ERROR_MSG_APPEND_RESPONSE,
                                    )?;
                                    k.insert(response);
                                } else {
                                    warn!("{ctx} No request context found for Request id: {rid}. Dropping response.");
                                    continue;
                                }
                            }
                            Entry::Occupied(mut o) => {
                                let data = ctx.bail_err(
                                    serializer.serialize_for_stream_with_codec(
                                        value,
                                        ingest_ns,
                                        rid.0,
                                        codec_overwrite,
                                    ),
                                    Self::ERROR_MSG_EXTRACT_VALUE,
                                )?;
                                ctx.bail_err(
                                    o.get_mut().append(data).await,
                                    Self::ERROR_MSG_APPEND_RESPONSE,
                                )?;
                            }
                        }
                    }
                    Some((min, max)) => {
                        // range of pull_ids, we need to multiplex to multiple requests
                        for rid in (min..=max).map(RequestId) {
                            match response_map.entry(rid) {
                                Entry::Vacant(k) => {
                                    if let Some((rid, sender)) = self.inflight.remove(&rid) {
                                        debug!("{ctx} Building response for request_id {rid}");
                                        let mut response = ctx.bail_err(
                                            SinkResponse::build(
                                                rid,
                                                chunked.unwrap_or_default(),
                                                sender,
                                                http_meta,
                                                content_type.as_ref(),
                                            )
                                            .await,
                                            Self::ERROR_MSG_EXTRACT_VALUE,
                                        )?;

                                        let data = ctx.bail_err(
                                            serializer.serialize_for_stream_with_codec(
                                                value,
                                                ingest_ns,
                                                rid.0,
                                                codec_overwrite,
                                            ),
                                            Self::ERROR_MSG_EXTRACT_VALUE,
                                        )?;
                                        ctx.bail_err(
                                            response.append(data).await,
                                            Self::ERROR_MSG_APPEND_RESPONSE,
                                        )?;
                                        k.insert(response);
                                    } else {
                                        warn!("{ctx} No request context found for Request id: {rid}. Dropping response.");
                                        continue;
                                    }
                                }
                                Entry::Occupied(mut o) => {
                                    let data = ctx.bail_err(
                                        serializer.serialize_for_stream_with_codec(
                                            value,
                                            ingest_ns,
                                            rid.0,
                                            codec_overwrite,
                                        ),
                                        Self::ERROR_MSG_EXTRACT_VALUE,
                                    )?;
                                    ctx.bail_err(
                                        o.get_mut().append(data).await,
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
}

enum BodyData {
    Data(Vec<Vec<u8>>),
    Chunked(Sender<Vec<u8>>),
}

impl SinkResponse {
    async fn build<'event>(
        request_id: RequestId,
        chunked: bool,
        tx: Sender<Response>,
        http_meta: Option<&Value<'event>>,
        content_type: Option<&String>,
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
        // build headers
        if let Some(headers) = response_meta.get_object("headers") {
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
        // set content-type if not explicitly set in the response headers meta
        // either from the configured or overwritten codec
        if res.content_type().is_none() {
            if let Some(ct) = content_type {
                let mime = Mime::from_str(ct)?;
                res.set_content_type(mime);
            }
        }
        let (body_data, res) = if chunked {
            let (chunk_tx, chunk_rx) = unbounded();
            let chunked_reader = StreamingBodyReader::new(chunk_rx);
            res.set_body(tide::Body::from_reader(chunked_reader, None));
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
        })
    }

    async fn append(&mut self, mut chunks: Vec<Vec<u8>>) -> Result<()> {
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
        let rest = serializer.finish_stream(self.request_id.0)?;
        if !rest.is_empty() {
            self.append(rest).await?;
        }
        // send response if necessary
        if let BodyData::Data(data) = self.body_data {
            if let Some(mut response) = self.res.take() {
                // set body
                let reader = FixedBodyReader::new(data);
                let len = reader.len();
                response.set_body(tide::Body::from_reader(reader, Some(len)));
                // send off the response
                self.tx.send(response).await?;
            }
        } else {
            // TODO: only close the stream if we know we have the last value for this request_id
            // TODO: but how to find out?
            // signal EOF to the `StreamingBodyReader`
            self.tx.close();
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RequestId(u64);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
    let content_type = req.content_type().map(|mime| mime.essence().to_string());
    let headers = req
        .header_names()
        .map(|name| {
            (
                name.to_string(),
                // a header name has the potential to take multiple values:
                // https://tools.ietf.org/html/rfc7230#section-3.2.2
                req.header(name)
                    .iter()
                    .flat_map(|value| {
                        let mut a: Vec<Value> = Vec::new();
                        for v in (*value).iter() {
                            a.push(v.as_str().to_string().into());
                        }
                        a.into_iter()
                    })
                    .collect::<Value>(),
            )
        })
        .collect::<Value>();

    let mut meta = Value::object_with_capacity(3);
    let mut url_meta = Value::object_with_capacity(7);
    let url = req.url();
    url_meta.insert("scheme", url.scheme().to_string())?;
    if !url.username().is_empty() {
        url_meta.insert("username", url.username().to_string())?;
    }
    url.password()
        .and_then(|p| url_meta.insert("password", p.to_string()).ok());
    url.host_str()
        .and_then(|h| url_meta.insert("host", h.to_string()).ok());
    url.port().and_then(|p| url_meta.insert("port", p).ok());
    url_meta.insert("path", url.path().to_string())?;
    url.query()
        .and_then(|q| url_meta.insert("query", q.to_string()).ok());
    url.fragment()
        .and_then(|f| url_meta.insert("fragment", f.to_string()).ok());

    meta.insert("method", req.method().to_string())?;
    meta.insert("headers", headers)?;
    meta.insert("url", url_meta)?;
    let data = req.body_bytes().await?;

    // Dispatch
    let (response_tx, response_rx) = bounded(1);
    req.state()
        .tx
        .send(RawRequestData {
            data,
            request_meta: meta,
            content_type,
            response_channel: response_tx,
        })
        .await?;

    Ok(response_rx.recv().await?)
}
