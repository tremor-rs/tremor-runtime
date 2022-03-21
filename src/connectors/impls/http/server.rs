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

use crate::codec::Codec;
use crate::connectors::prelude::*;
use crate::connectors::utils::{mime::MimeCodecMap, tls::TLSServerConfig};
use crate::postprocessor::postprocess;
use crate::postprocessor::Postprocessors;
use crate::{codec, connectors::spawn_task};
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use async_std::task::JoinHandle;
use dashmap::DashMap;
use http_types::headers::HeaderValue;
use http_types::Mime;
use simd_json::ValueAccess;
use std::{str::FromStr, sync::Arc};
use tide_rustls::TlsListener;
use tremor_common::time::nanotime;
use uuid::adapter::Urn;
use uuid::Uuid;

const URN_HEADER: &str = "x-tremor-http-urn";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Our target URL
    #[serde(default = "Default::default")]
    url: Url,
    /// TLS configuration, if required
    tls: Option<TLSServerConfig>,
    /// MIME mapping to/from tremor codecs
    #[serde(default = "default_mime_codec_map")]
    codec_map: MimeCodecMap,
}

fn default_mime_codec_map() -> MimeCodecMap {
    MimeCodecMap::with_builtin()
}

impl ConfigImpl for Config {}

struct HttpServerState {
    idgen: EventIdGenerator,
    tx: Sender<RendezvousRequest>,
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
pub(crate) struct HttpServer {
    config: Config,
    origin_uri: EventOriginUri,
    tls_server_config: Option<TLSServerConfig>,
    inflight: Arc<DashMap<String, Sender<RendezvousResponse>>>,
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
            let tls_server_config = config.tls.clone();
            let origin_uri = EventOriginUri {
                scheme: "http-server".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: vec![],
            };
            let codec = raw_config
                .codec
                .as_ref()
                .map_or_else(|| codec::resolve(&"json".into()), codec::resolve)?;
            let codec_map = config.codec_map.clone();
            let inflight = Arc::default();

            Ok(Box::new(HttpServer {
                config,
                origin_uri,
                tls_server_config,
                inflight,
                codec,
                codec_map,
            }))
        } else {
            Err(crate::errors::ErrorKind::MissingConfiguration(String::from("HttpServer")).into())
        }
    }
}

struct RendezvousResponse(ValueAndMeta<'static>);
struct RendezvousRequest {
    data: Vec<u8>,
    meta: Value<'static>,
    urn: Urn,
    response_channel: Sender<RendezvousResponse>,
}

struct HttpServerSource {
    url: Url,
    origin_uri: EventOriginUri,
    inflight: Arc<DashMap<String, Sender<RendezvousResponse>>>,
    request_rx: Receiver<RendezvousRequest>,
    request_tx: Sender<RendezvousRequest>,
    accept_task: Option<JoinHandle<()>>,
    tls_server_config: Option<TLSServerConfig>,
    codec: Box<dyn Codec>,
    codec_map: MimeCodecMap,
}

struct HttpServerSink {
    #[allow(dead_code)]
    origin_uri: EventOriginUri,
    inflight: Arc<DashMap<String, Sender<RendezvousResponse>>>,
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
        let (request_tx, request_rx) = bounded(128);
        let source = HttpServerSource {
            url: self.config.url.clone(),
            inflight: self.inflight.clone(),
            request_tx,
            request_rx,
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
            inflight: self.inflight.clone(),
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

    // This is how we link a request to a response, this might not go away, unless we can do that with event ids somehow?
    headers.insert(URN_HEADER, Value::from(linking_uuid.to_string()))?;

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
    let (response_channel, call_response) = bounded(1);
    req.state()
        .tx
        .send(RendezvousRequest {
            data,
            meta,
            urn: linking_uuid,
            response_channel,
        })
        .await?;

    let event = call_response.recv().await?;

    let codec: &(dyn Codec + 'static) = req.state().codec.as_ref();
    let codec_map = &req.state().codec_map;
    let mut processors = Vec::new();
    if let Ok(response) = make_response(ingest_ns, codec, codec_map, &mut processors, &event.0) {
        Ok(response)
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
        // TODO handle other sockets that are not host/port based
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
        if let Some(accept_task) = self.accept_task.take() {
            accept_task.cancel().await;
        }

        let tx = self.request_tx.clone();
        if self.tls_server_config.is_some() && self.url.scheme() != "https" {
            return Err("Using SSL certificates requires setting up a https endpoint".into());
        }

        let ctx = ctx.clone();
        let tls_server_config = self.tls_server_config.clone();
        let codec = self.codec.boxed_clone();
        let codec_map = self.codec_map.clone();

        // Accept task - this is the main receive loop for http server instances
        self.accept_task = Some(spawn_task(ctx.clone(), async move {
            let idgen = EventIdGenerator::new(ctx.uid);
            if let Some(tls_server_config) = tls_server_config {
                let mut endpoint = tide::Server::with_state(HttpServerState {
                    idgen,
                    tx,
                    uid: ctx.uid,
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
                    uid: ctx.uid,
                    codec,
                    codec_map,
                });
                endpoint.at("/").all(handle_request);
                endpoint.at("/*").all(handle_request);
                endpoint.listen(&hostport).await?;
            };
            Ok(())
        }));

        Ok(true)
    }

    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if !_ctx.quiescence_beacon().continue_reading().await {
            return Ok(SourceReply::Empty(100));
        }

        match self.request_rx.try_recv() {
            Ok(RendezvousRequest {
                data,
                meta,
                urn,
                response_channel,
            }) => {
                // At this point we lose association of urn and call-response tx channel for this event's
                // rpc context. The sink will in fact receive the response ( pipeline originated ) so we need
                // to rendezvous the response somehow - and we use the urn for this

                if self
                    .inflight
                    .insert(urn.to_string(), response_channel)
                    .is_some()
                {
                    error!("Request tracking urn collision: {}", urn);
                };
                Ok(if data.is_empty() {
                    // NOTE GET, HEAD ...
                    SourceReply::Structured {
                        origin_uri: self.origin_uri.clone(),
                        payload: EventPayload::from(ValueAndMeta::from_parts(
                            Value::const_null(),
                            meta,
                        )),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    }
                } else {
                    SourceReply::Data {
                        origin_uri: self.origin_uri.clone(),
                        data,
                        meta: Some(meta),
                        stream: DEFAULT_STREAM_ID,
                        port: None,
                    }
                })
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
        if input == "err" {
            // There was an issue with the pipeline processing
            for (v, m) in event.value_meta_iter() {
                let request = m.get("request");
                let headers = request.get("headers");
                let urn = headers.get_str(URN_HEADER).ok_or("No Urn provided")?;
                let tx = self.inflight.get(urn).ok_or("Unknown Urn")?;

                let reqm = if let Some(reqm) = request {
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
                if let Err(_e) = tx.send(RendezvousResponse(vm)).await {
                    error!("{ctx} Internal server error");
                    ctx.notifier().connection_lost().await?;
                }
            }
        } else {
            for (v, m) in event.value_meta_iter() {
                let request = m.get("request");
                let headers = request.get("headers");
                let urn = headers.get_str(URN_HEADER).ok_or("No Urn provided")?;
                let tx = self.inflight.get(urn).ok_or("Unknown Urn")?;

                let vm = ValueAndMeta::from_parts(v.clone_static(), m.clone_static());
                if let Err(_e) = tx.send(RendezvousResponse(vm)).await {
                    error!("{} Internal server error", &ctx);
                    ctx.notifier().connection_lost().await?;
                }
            }
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}
