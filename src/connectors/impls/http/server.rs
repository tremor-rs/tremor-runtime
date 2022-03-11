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

use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use async_std::task::{self, JoinHandle};
use halfbrown::HashMap;
use http_types::headers::HeaderValue;
use http_types::{Mime, Url};
use simd_json::{StaticNode, ValueAccess};
use std::str::FromStr;
use tide_rustls::TlsListener;
use tremor_common::time::nanotime;
use uuid::adapter::Urn;
use uuid::Uuid;

use crate::codec;
use crate::codec::Codec;
use crate::connectors::prelude::*;
use crate::connectors::utils::mime::MimeCodecMap;
use crate::connectors::utils::tls::TLSServerConfig;
use crate::postprocessor::postprocess;
use crate::postprocessor::Postprocessors;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Our target URL
    #[serde(default = "default_url")]
    url: String,
    /// TLS configuration, if required
    tls: Option<TLSServerConfig>,
    /// MIME mapping to/from tremor codecs
    #[serde(default = "default_mime_codec_map")]
    codec_map: MimeCodecMap,
}

fn default_url() -> String {
    "https://localhost:443/".to_string()
}

fn default_mime_codec_map() -> MimeCodecMap {
    MimeCodecMap::with_builtin()
}

impl ConfigImpl for Config {}

#[derive(Debug)]
struct HttpSourceReply(Option<Sender<ValueAndMeta<'static>>>, SourceReply);

impl From<SourceReply> for HttpSourceReply {
    fn from(reply: SourceReply) -> Self {
        Self(None, reply)
    }
}

struct HttpServerState {
    idgen: EventIdGenerator,
    tx: Sender<Rendezvous>,
    uid: u64,
    codec: Box<dyn Codec>,
    codec_map: MimeCodecMap,
}

impl Clone for HttpServerState {
    fn clone(&self) -> Self {
        Self {
            idgen: self.idgen,
            tx: self.tx.clone(),
            uid: self.uid,
            codec: self.codec.boxed_clone(),
            codec_map: self.codec_map.clone(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct HttpServer {
    config: Config,
    origin_uri: EventOriginUri,
    tls_server_config: Option<TLSServerConfig>,
    rendezvous: (Sender<Rendezvous>, Receiver<Rendezvous>),
    codec: Box<dyn Codec>,
    codec_map: MimeCodecMap,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "http_server".into()
    }

    async fn from_config(
        &self,
        _id: &str,
        raw_config: &ConnectorConfig,
    ) -> crate::errors::Result<Box<dyn Connector>> {
        if let Some(config) = &raw_config.config {
            let config = Config::new(config)?;

            let _url = Url::parse(&config.url)?;

            let tls_server_config = config.tls.clone();

            let origin_uri = EventOriginUri {
                scheme: "http_server".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: vec![],
            };

            let codec = if let Some(codec) = &raw_config.codec {
                codec::resolve(codec)?
            } else {
                codec::resolve(&"json".into())?
            };
            let codec_map = config.codec_map.clone();

            let rendezvous = bounded(crate::QSIZE.load(Ordering::Relaxed));

            Ok(Box::new(HttpServer {
                config,
                origin_uri,
                tls_server_config,
                rendezvous,
                codec,
                codec_map,
            }))
        } else {
            Err(crate::errors::ErrorKind::MissingConfiguration(String::from("HttpServer")).into())
        }
    }
}

#[derive(Debug)]
enum Rendezvous {
    Request(Vec<u8>, Value<'static>, u64, Urn, Sender<Rendezvous>),
    Response(ValueAndMeta<'static>),
}

struct HttpServerSource {
    url: String,
    origin_uri: EventOriginUri,
    inflight: HashMap<Urn, Sender<Rendezvous>>,
    inflighttracker_rx: Receiver<Rendezvous>,
    inflighttracker_tx: Sender<Rendezvous>,
    accept_task: Option<JoinHandle<Result<()>>>,
    tls_server_config: Option<TLSServerConfig>,
    codec: Box<dyn Codec>,
    codec_map: MimeCodecMap,
}

struct HttpServerSink {
    #[allow(dead_code)]
    origin_uri: EventOriginUri,
    inflighttracker_tx: Sender<Rendezvous>,
}

#[async_trait::async_trait()]
impl Connector for HttpServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional("json")
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = HttpServerSource {
            url: self.config.url.clone(),
            inflight: HashMap::new(),
            inflighttracker_tx: self.rendezvous.0.clone(),
            inflighttracker_rx: self.rendezvous.1.clone(),
            origin_uri: self.origin_uri.clone(),
            accept_task: None,
            tls_server_config: self.tls_server_config.clone(),
            codec: self.codec.boxed_clone(),
            codec_map: self.codec_map.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = HttpServerSink {
            inflighttracker_tx: self.rendezvous.0.clone(),
            origin_uri: self.origin_uri.clone(),
        };
        builder.spawn(sink, sink_context).map(Some)
    }
}

async fn handle_request(req: tide::Request<HttpServerState>) -> tide::Result<tide::Response> {
    // NOTE We wrap and crap as tide doesn't report donated route handler's errors
    let result = _handle_request(req).await;
    if let Err(e) = result {
        error!("Error handling HTTP server request {}", e);
        Err(e)
    } else {
        result
    }
}
async fn _handle_request(mut req: tide::Request<HttpServerState>) -> tide::Result<tide::Response> {
    let ingest_ns = nanotime();
    let _origin_uri = EventOriginUri {
        scheme: "http_server".to_string(),
        host: req
            .host()
            .unwrap_or("tremor-rest-server-host.remote")
            .to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp)
        path: vec![String::default()],
    };

    let mut headers = req
        .header_names()
        .map(|name| {
            (
                name.to_string(),
                // a header name has the potential to take multiple values:
                // https://tools.ietf.org/html/rfc7230#section-3.2.2
                req.header(name)
                    .iter()
                    .map(|value| {
                        let mut a: Vec<Value> = Vec::new();
                        for v in (*value).iter() {
                            a.push(v.as_str().to_string().into());
                        }
                        Value::from(a)
                    })
                    .collect::<Value>(),
            )
        })
        .collect::<Value>();

    let linking_uuid = Uuid::new_v4().to_urn();
    // FIXME: remove if/when RV issues resolved - useful for debugging for now
    //  - specifically changes to SourceReply::BatchData ( punctuation + metadata ) and
    //  - and resolution of the http cli/srv in the same process ( future ) issue
    //
    headers.insert("x-tremor-http-urn", Value::from(linking_uuid.to_string()))?;

    let ct: Option<Mime> = req.content_type();
    let _codec_override = ct.map(|ct| ct.essence().to_string());

    let mut meta = Value::object_with_capacity(1);
    let mut request_meta = Value::object_with_capacity(3);
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

    request_meta.insert("method", req.method().to_string())?;
    request_meta.insert("headers", headers)?;
    request_meta.insert("url", url_meta)?;
    meta.insert("request", request_meta)?;
    let data = req.body_bytes().await?;

    // Dispatch
    let (call, call_response) = bounded(1);
    req.state()
        .tx
        .send(Rendezvous::Request(
            data,
            meta,
            ingest_ns,
            linking_uuid,
            call,
        ))
        .await?;

    let event = call_response.recv().await?;

    if let Rendezvous::Response(_) = event {
        let codec: &(dyn Codec + 'static) = req.state().codec.as_ref();
        let codec_map = &req.state().codec_map;
        let mut processors = Vec::new();
        if let Ok(response) = make_response(ingest_ns, codec, codec_map, &mut processors, &event) {
            Ok(response)
        } else {
            Ok(tide::Response::builder(503)
                .body(tide::Body::empty())
                .build())
        }
    } else {
        Ok(tide::Response::builder(503)
            .body(tide::Body::empty())
            .build())
    }
}

fn make_response(
    ingest_ns: u64,
    default_codec: &dyn Codec,
    codec_map: &MimeCodecMap,
    postprocessors: &mut Postprocessors,
    event: &Rendezvous,
) -> Result<tide::Response> {
    if let Rendezvous::Response(event) = event {
        _make_response(ingest_ns, default_codec, codec_map, postprocessors, event)
    } else {
        Err(Error::from(
            "Unexpected rendezvous event variant in response builder",
        ))
    }
}

fn _make_response(
    ingest_ns: u64,
    default_codec: &dyn Codec,
    codec_map: &MimeCodecMap,
    post_processors: &mut Postprocessors,
    event: &ValueAndMeta,
) -> Result<tide::Response> {
    let (response_data, meta) = event.parts();

    let request_meta = meta.get("request");
    let response_meta = meta.get("response");

    let status = if let Some(response_meta) = response_meta {
        // Use user provided status - or default to 200
        response_meta.get_u16("status").unwrap_or(200)
    } else {
        // Otherwise - Default status based on request method
        if let Some(ref request_meta) = request_meta {
            let method = request_meta.get_str("method").unwrap_or("error");
            match method {
                "GET" | "PUT" | "PATCH" => 200,
                "DELETE" => 204,
                "POST" => 201,
                _otherwise => 503,
            }
        } else {
            503 // Flag a 503 - malformed pipeline
        }
    };

    let mut builder = tide::Response::builder(status);

    // extract headers
    let mut header_content_type: Option<&str> = None;
    if let Some(headers) = response_meta.get_object("headers") {
        for (name, values) in headers {
            if let Some(header_values) = values.as_array() {
                if name.eq_ignore_ascii_case("content-type") {
                    // pick first value in case of multiple content-type headers
                    header_content_type = header_values.first().and_then(Value::as_str);
                }
                let mut v = Vec::with_capacity(header_values.len());
                for value in header_values {
                    if let Some(header_value) = value.as_str() {
                        v.push(HeaderValue::from_str(header_value)?);
                    }
                }
                builder = builder.header(name.as_ref(), v.as_slice());
            } else if let Some(header_value) = values.as_str() {
                if name.eq_ignore_ascii_case("content-type") {
                    header_content_type = Some(header_value);
                }
                builder = builder.header(name.as_ref(), header_value);
            }
        }
    }

    let maybe_content_type = match header_content_type {
        None => None,
        Some(ct) => Some(Mime::from_str(ct)?),
    };

    if let Some((mime, dynamic_codec)) = maybe_content_type.and_then(|mime| {
        codec_map
            .map
            .get(mime.essence())
            .map(|c| (mime, c.as_ref()))
    }) {
        let processed = postprocess(
            post_processors.as_mut_slice(),
            ingest_ns,
            dynamic_codec.encode(response_data)?,
        )?;

        // TODO: see if we can use a reader instead of creating a new vector
        let v: Vec<u8> = processed.into_iter().flatten().collect();
        let mut body = tide::Body::from_bytes(v);

        body.set_mime(mime);
        builder = builder.body(body);
    } else {
        // fallback to default codec
        let processed = postprocess(
            post_processors.as_mut_slice(),
            ingest_ns,
            default_codec.encode(response_data)?,
        )?;
        // TODO: see if we can use a reader instead of creating a new vector
        let v: Vec<u8> = processed.into_iter().flatten().collect();
        let mut body = tide::Body::from_bytes(v);

        // set mime type for default codec
        // TODO: cache mime type for default codec
        if let Some(mime) = default_codec
            .mime_types()
            .drain(..)
            .find_map(|mstr| Mime::from_str(mstr).ok())
        {
            body.set_mime(mime);
        }
        builder = builder.body(body);
    }
    Ok(builder.build())
}

#[async_trait::async_trait()]
impl Source for HttpServerSource {
    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(accept_task) = self.accept_task.take() {
            // stop acceptin' new connections
            accept_task.cancel().await;
        }
        Ok(())
    }

    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        let url = Url::parse(&self.url)?;

        let hostport = match (url.host(), url.port()) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            _unlikely => "localhost:443".to_string(),
        };

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(accept_task) = self.accept_task.take() {
            accept_task.cancel().await;
        }

        let tx = self.inflighttracker_tx.clone();

        let ctx = ctx.clone();
        let tls_server_config = self.tls_server_config.clone();
        let codec = self.codec.boxed_clone();
        let codec_map = self.codec_map.clone();

        // Accept task - this is the main receive loop for http server instances
        //
        self.accept_task = Some(task::spawn(async move {
            let idgen = EventIdGenerator::new(ctx.uid);
            // NOTE FIXME validate shcme = https for tls server config
            if let Some(tls_server_config) = tls_server_config {
                let mut endpoint = tide::Server::with_state(HttpServerState {
                    idgen,
                    tx,
                    uid: 0, // FIXME
                    codec,
                    codec_map,
                });
                endpoint.at("/").all(handle_request);
                endpoint.at("/*").all(handle_request);
                endpoint
                    .listen(
                        TlsListener::build()
                            .addrs(&hostport)
                            .cert(&tls_server_config.cert)
                            .key(&tls_server_config.key),
                    )
                    .await?;
            } else {
                let mut endpoint = tide::Server::with_state(HttpServerState {
                    idgen,
                    tx,
                    uid: 0, // FIXME
                    codec,
                    codec_map,
                });
                endpoint.at("/").all(handle_request);
                endpoint.at("/*").all(handle_request);
                endpoint.listen(&hostport).await?;
            };

            // notify connector task about disconnect
            // of the listening socket
            ctx.notifier.notify().await?;
            Ok(())
        }));

        Ok(true)
    }

    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if !_ctx.quiescence_beacon().continue_reading().await {
            return Ok(SourceReply::Empty(100));
        }

        match self.inflighttracker_rx.try_recv() {
            Ok(Rendezvous::Request(data, mut meta, _ingest_ns, urn, response_channel)) => {
                // At this point we lose association of urn and call-response tx channel for this event's
                // rpc context. The sink will in fact receive the response ( pipeline originated ) so we need
                // to rendezvous the response somehow - and we use the urn for this

                let reply = if data.is_empty() {
                    // NOTE GET, HEAD ...
                    let value = Value::Static(StaticNode::Null);
                    if self.inflight.insert(urn, response_channel).is_some() {
                        error!("Request tracking urn collision: {}", urn);
                    };
                    SourceReply::Structured {
                        origin_uri: self.origin_uri.clone(),
                        payload: EventPayload::from(ValueAndMeta::from_parts(value, meta)),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    }
                } else {
                    // FIXME
                    // Sketch of alternate to using BatchData vi structured
                    //  - benefit of using batch data is proc+codec is for free
                    //  - but it lacks punctuation so makes batch response handling harder
                    //
                    // let mut chunks =
                    //     preprocess(&mut self.preprocessors, &mut ingest_ns, data, "chunked")?;
                    // let mut batch_data = Vec::with_capacity(chunks.len());

                    if self.inflight.insert(urn, response_channel).is_some() {
                        error!("Request tracking urn collision: {}", urn);
                    };
                    meta.insert("batch-urn", urn.to_string())?;
                    SourceReply::BatchData {
                        origin_uri: self.origin_uri.clone(),
                        batch_data: vec![(data, Some(meta))],
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    }
                };

                Ok(reply)
            }
            Ok(Rendezvous::Response(ref vm)) => {
                if let Some(request) = vm.meta().get("request") {
                    if let Some(headers) = request.get("headers") {
                        if let Some(urn) = headers.get("x-tremor-http-urn") {
                            let urn = Urn::from_uuid(Uuid::from_str(&urn.to_string())?);
                            if let Some(callresponse) = self.inflight.remove(&urn) {
                                callresponse.send(Rendezvous::Response(vm.clone())).await?;
                            }
                        }
                    }
                } else if let Some(urn) = vm.meta().get("batch-urn") {
                    // NOTE Batch - this is limping along *but* technically not correct as tremor does not punctuate BatchData batches so we have no start/end demarcation hints to work with
                    let urn = Urn::from_uuid(Uuid::from_str(&urn.to_string())?);
                    if let Some(callresponse) = self.inflight.remove(&urn) {
                        callresponse
                            .send(Rendezvous::Response(ValueAndMeta::from_parts(
                                Value::Static(StaticNode::Null),
                                literal!({ "response": { "status": 201 }}),
                            )))
                            .await?;
                    }
                }
                Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
            }
            Err(TryRecvError::Closed) => Err(TryRecvError::Closed.into()),
            Err(TryRecvError::Empty) => Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL)),
        }
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        true
    }
}

#[async_trait::async_trait()]
impl Sink for HttpServerSink {
    #[allow(clippy::option_if_let_else)]
    async fn on_event(
        &mut self,
        input: &str,
        event: tremor_pipeline::Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        if "in" == input {
            // Pipeline processing was successful
            for (v, m) in event.value_meta_iter() {
                let vm = ValueAndMeta::from_parts(v.clone_static(), m.clone_static());
                if let Err(_e) = self.inflighttracker_tx.send(Rendezvous::Response(vm)).await {
                    error!("{} Internal server error", &ctx);
                    ctx.notifier().notify().await?;
                }
            }
            // send the event back to
        } else if "err" == input {
            // There was an issue with the pipeline processing
            for (v, reqm) in event.value_meta_iter() {
                let reqm = if let Some(reqm) = reqm.get("request") {
                    reqm.clone_static()
                } else {
                    literal!({})
                };
                let m: Value = literal!({
                    "request": reqm,
                    "response": {
                        "status": 503,
                    }
                });
                let vm = ValueAndMeta::from_parts(v.clone_static(), m.clone_static());
                if let Err(_e) = self.inflighttracker_tx.send(Rendezvous::Response(vm)).await {
                    error!("{} Internal server error", &ctx);
                    ctx.notifier().notify().await?;
                }
            }
        } else {
            for (v, m) in event.value_meta_iter() {
                let vm = ValueAndMeta::from_parts(v.clone_static(), m.clone_static());
                if let Err(_e) = self.inflighttracker_tx.send(Rendezvous::Response(vm)).await {
                    error!("{} Internal server error", &ctx);
                    ctx.notifier().notify().await?;
                }
            }
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}
