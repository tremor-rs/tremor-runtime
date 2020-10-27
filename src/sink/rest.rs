// Copyright 2020, The Tremor Team
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

#![cfg(not(tarpaulin_include))]

use crate::codec::Codec;
use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use async_std::task::JoinHandle;
use halfbrown::HashMap;
use http_types::mime::Mime;
use http_types::{headers::HeaderValue, Method};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::borrow::Borrow;
use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use surf::{Body, Client, Request, Response};
use tremor_pipeline::{Ids, OpMeta};

/// custom Url struct for parsing a url
#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
pub struct Endpoint {
    scheme: Option<String>,
    username: Option<String>,
    password: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    path: Option<String>,
    query: Option<String>,
    fragment: Option<String>,
}

fn none_if_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn err(s: &str) -> Error {
    s.into()
}

impl Endpoint {
    fn as_url(&self) -> Result<url::Url> {
        // scheme is the default, and host will be overwritten anyways, as relative http uris are not supported anyways
        self.as_url_with_base("http://localhost")
    }

    // expensive but seemingly correct way of populating a url from this struct
    fn as_url_with_base(&self, base_url: &str) -> Result<url::Url> {
        let mut res = url::Url::parse(base_url)?; //stupid placeholder, no other way to create a url
        if let Some(scheme) = &self.scheme {
            res.set_scheme(scheme.as_str())
                .map_err(|_| err("Invalid URI scheme"))?;
        };
        if let Some(username) = &self.username {
            res.set_username(username.as_str())
                .map_err(|_| err("Invalid URI username"))?;
        };
        if let Some(password) = &self.password {
            res.set_password(Some(password.as_str()))
                .map_err(|_| err("Invalid URI password"))?;
        };

        if let Some(host) = &self.host {
            res.set_host(Some(host.as_str()))?;
        } else {
            res.set_host(None)?;
        }
        res.set_port(self.port)
            .map_err(|_| err("Invalid URI port"))?;
        if let Some(path) = &self.path {
            res.set_path(path.as_str());
        };
        if let Some(query) = &self.query {
            res.set_query(Some(query.as_str()));
        }
        if let Some(fragment) = &self.fragment {
            res.set_fragment(Some(fragment.as_str()));
        }
        Ok(res)
    }

    /// overwrite fields in self with existing properties in `endpoint_value`
    fn merge(&mut self, endpoint_value: &Value) {
        if let Some(scheme) = endpoint_value.get("scheme").and_then(|s| s.as_str()) {
            self.scheme = Some(scheme.to_string());
        }
        if let Some(host) = endpoint_value.get("host").and_then(|s| s.as_str()) {
            self.host = Some(host.to_string());
        }
        if let Some(port) = endpoint_value.get("port").and_then(|s| s.as_u16()) {
            self.port = Some(port);
        }
        if let Some(username) = endpoint_value.get("username").and_then(|s| s.as_str()) {
            self.username = Some(username.to_string());
        }
        if let Some(password) = endpoint_value.get("password").and_then(|s| s.as_str()) {
            self.password = Some(password.to_string());
        }
        if let Some(path) = endpoint_value.get("path").and_then(|s| s.as_str()) {
            self.path = Some(path.to_string());
        }
        if let Some(query) = endpoint_value.get("query").and_then(|s| s.as_str()) {
            self.query = Some(query.to_string());
        }
        if let Some(fragment) = endpoint_value.get("fragment").and_then(|s| s.as_str()) {
            self.fragment = Some(fragment.to_string());
        }
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let url = url::Url::parse(s)?;
        Ok(Self {
            scheme: none_if_empty(url.scheme()),
            username: none_if_empty(url.username()),
            password: url.password().map(ToString::to_string),
            host: url.host_str().map(ToString::to_string),
            port: url.port(),
            path: none_if_empty(url.path()),
            query: url.query().map(ToString::to_string),
            fragment: url.fragment().map(ToString::to_string),
        })
    }
}

/// serialize with `FromStr` if given a `String`, or with `Deserialize` impl given a map/struct thing.
/// See <https://serde.rs/string-or-struct.html> for reference.
fn string_or_struct<'de, T, D>(deserializer: D) -> core::result::Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = Error>,
    D: Deserializer<'de>,
{
    // This is a Visitor that forwards string types to T's `FromStr` impl and
    // forwards map types to T's `Deserialize` impl. The `PhantomData` is to
    // keep the compiler from complaining about T being an unused generic type
    // parameter. We need T in order to know the Value type for the Visitor
    // impl.
    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where
        T: Deserialize<'de> + FromStr<Err = crate::errors::Error>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> core::result::Result<T, E>
        where
            E: de::Error,
        {
            FromStr::from_str(value).map_err(de::Error::custom)
        }

        fn visit_map<M>(self, map: M) -> core::result::Result<T, M::Error>
        where
            M: MapAccess<'de>,
        {
            // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
            // into a `Deserializer`, allowing it to be used as the input to T's
            // `Deserialize` implementation. T then deserializes itself using
            // the entries from the map visitor.
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrStruct(PhantomData))
}

/// Machinery for deserializing the HTTP method
struct MethodStrVisitor;

impl<'de> Visitor<'de> for MethodStrVisitor {
    type Value = SerdeMethod;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an HTTP method string.")
    }

    fn visit_str<E>(self, v: &str) -> core::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Method::from_str(v)
            .map(SerdeMethod)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SerdeMethod(Method);

impl<'de> Deserialize<'de> for SerdeMethod {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(MethodStrVisitor)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// endpoint url - given as String or struct
    #[serde(deserialize_with = "string_or_struct", default)]
    pub endpoint: Endpoint,

    /// maximum number of parallel in flight batches (default: 4)
    /// this avoids blocking further events from progressing while waiting for upstream responses.
    #[serde(default = "dflt_concurrency")]
    pub concurrency: usize,

    // HTTP method to use (default: POST)
    #[serde(default = "dflt_method")]
    pub method: SerdeMethod,

    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn dflt_concurrency() -> usize {
    4
}

fn dflt_method() -> SerdeMethod {
    SerdeMethod(Method::Post)
}

impl ConfigImpl for Config {}

enum CodecTaskInMsg {
    ToRequest(Event, Sender<SendTaskInMsg>),
    ToEvent {
        id: Ids,
        origin_uri: Box<EventOriginUri>, // box to avoid becoming the struct too big
        op_meta: OpMeta,
        response: Response,
        duration: u64,
    },
    ReportFailure(Ids, OpMeta, EventOriginUri, Error),
}

enum SendTaskInMsg {
    Request(Request),
    Failed,
}

pub struct Rest {
    uid: u64,
    config: Config,
    num_inflight_requests: Arc<AtomicMaxCounter>,
    is_linked: bool,
    reply_channel: Option<Sender<sink::Reply>>,
    codec_task_handle: Option<JoinHandle<Result<()>>>,
    codec_task_tx: Option<Sender<CodecTaskInMsg>>,
    client: Client,
}

impl offramp::Impl for Rest {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let num_inflight_requests = Arc::new(AtomicMaxCounter::new(config.concurrency));
            let client = surf::client();
            Ok(SinkManager::new_box(Self {
                uid: 0,
                config,
                num_inflight_requests,
                is_linked: false,
                reply_channel: None,
                codec_task_handle: None,
                codec_task_tx: None,
                client,
            }))
        } else {
            Err("Rest offramp requires a configuration.".into())
        }
    }
}

#[async_trait::async_trait]
impl Sink for Rest {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        if self.is_linked && event.is_batch {
            return Err("Batched events are not supported for linked rest offramps".into());
        }
        // limit concurrency
        if let Ok(current_inflights) = self.num_inflight_requests.inc() {
            let codec_task_channel = match &self.codec_task_tx {
                Some(codec_task_channel) => codec_task_channel.clone(),
                None => {
                    return Err("Offramp in invalid state: No codec task channel available.".into())
                }
            };
            let (tx, rx) = bounded::<SendTaskInMsg>(1);
            let max_counter = self.num_inflight_requests.clone();
            let sink_uid = self.uid;
            let http_client = self.client.clone(); // should be quite cheap, just some Arcs

            // spawn send task
            task::spawn(async move {
                let id = event.id.clone();
                let op_meta = event.op_meta.clone();

                let start = Instant::now();
                // send command to codec task
                codec_task_channel
                    .send(CodecTaskInMsg::ToRequest(event, tx))
                    .await?;
                // wait for encoded request to come in
                match rx.recv().await? {
                    SendTaskInMsg::Request(request) => {
                        let url = request.url();
                        let event_origin_uri = EventOriginUri {
                            uid: sink_uid,
                            scheme: "tremor-rest".to_string(),
                            host: url.host_str().map_or(String::new(), ToString::to_string),
                            port: url.port(),
                            path: url.path_segments().map_or_else(Vec::new, |segments| {
                                segments.map(String::from).collect()
                            }),
                        };
                        // send request
                        match http_client.send(request).await {
                            Ok(response) => {
                                #[allow(clippy::cast_possible_truncation)]
                                // we dont care about the upper 64 bit
                                let duration = start.elapsed().as_millis() as u64; // measure response duration
                                codec_task_channel
                                    .send(CodecTaskInMsg::ToEvent {
                                        id,
                                        origin_uri: Box::new(event_origin_uri),
                                        op_meta,
                                        response,
                                        duration,
                                    })
                                    .await?
                            }
                            Err(e) => {
                                codec_task_channel
                                    .send(CodecTaskInMsg::ReportFailure(
                                        id,
                                        op_meta,
                                        event_origin_uri,
                                        e.into(),
                                    ))
                                    .await?;
                            }
                        }
                    }
                    SendTaskInMsg::Failed => {} // just stop the task here, error alrady handled and reported in codec_task
                }

                max_counter.dec_from(current_inflights); // be fair to others and free our spot
                Ok::<(), Error>(())
            });

            Ok(None)
        } else {
            error!("Dropped data due to overload");
            Err("Dropped data due to overload".into())
        }
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorURL,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        // clone the hell out of all the shit
        let postprocessors = make_postprocessors(processors.post)?;
        let preprocessors = make_preprocessors(processors.pre)?;
        let my_codec = codec.boxed_clone();
        let my_codec_map = codec_map
            .iter()
            .map(|(k, v)| (k.clone(), v.boxed_clone()))
            .collect::<HashMap<String, Box<dyn Codec>>>();
        let default_method = self.config.method.0;
        let endpoint = self.config.endpoint.clone();
        let config_headers = self.config.headers.clone();
        let sink_url = sink_url.clone();

        // inbound channel towards codec task
        // sending events to be turned into requests
        // and responses to be turned into events
        let (in_tx, in_rx) = bounded::<CodecTaskInMsg>(self.config.concurrency);
        // channel for sending shit back to the connected pipelines
        let reply_tx = reply_channel.clone();
        // start the codec task
        self.codec_task_handle = Some(task::spawn(async move {
            codec_task(
                sink_uid,
                sink_url,
                postprocessors,
                preprocessors,
                my_codec,
                my_codec_map,
                endpoint,
                default_method,
                config_headers,
                reply_tx,
                in_rx,
                is_linked,
            )
            .await
        }));
        self.is_linked = is_linked;
        self.reply_channel = Some(reply_channel);
        self.codec_task_tx = Some(in_tx);
        self.uid = sink_uid;
        Ok(())
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
}

struct ResponseIdGenerator(u64);
impl ResponseIdGenerator {
    fn next(&mut self) -> u64 {
        let res = self.0;
        self.0 += 1;
        res
    }

    fn new() -> Self {
        Self(0)
    }
}

// TODO: use headers from config
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn codec_task(
    sink_uid: u64,
    sink_url: TremorURL,
    mut postprocessors: Vec<Box<dyn Postprocessor>>,
    mut preprocessors: Vec<Box<dyn Preprocessor>>,
    codec: Box<dyn Codec>,
    codec_map: HashMap<String, Box<dyn Codec>>,
    endpoint: Endpoint,
    default_method: Method,
    default_headers: HashMap<String, String>,
    reply_tx: Sender<sink::Reply>,
    in_rx: Receiver<CodecTaskInMsg>,
    is_linked: bool,
) -> Result<()> {
    debug!("REST Sink codec task started.");
    let mut response_ids = ResponseIdGenerator::new();

    while let Ok(msg) = in_rx.recv().await {
        match msg {
            CodecTaskInMsg::ToRequest(mut event, tx) => {
                match build_request(
                    &event,
                    codec.borrow(),
                    &codec_map,
                    postprocessors.as_mut_slice(),
                    default_method,
                    &default_headers,
                    &endpoint,
                ) {
                    Ok(request) => {
                        if let Err(e) = tx.send(SendTaskInMsg::Request(request)).await {
                            error!(
                                "[Sink::{}] Error sending out encoded request {}",
                                &sink_url, e
                            );
                        }
                    }
                    Err(e) => {
                        error!("[Sink::{}] Error encoding the request: {}", &sink_url, &e);
                        // send error result to send task
                        if let Err(e) = tx.send(SendTaskInMsg::Failed).await {
                            error!(
                                "[Sink::{}] Error sending Failed back to send task: {}",
                                &sink_url, e
                            );
                        }
                        // send CB_fail
                        let mut insight = event.insight_fail();
                        insight.ingest_ns = nanotime();
                        if let Err(e) = reply_tx.send(sink::Reply::Insight(insight)).await {
                            error!("[Sink::{}] Error sending CB fail event {}", &sink_url, e);
                        }

                        // send response through error port
                        let error_event = create_error_response(
                            Ids::new(sink_uid, response_ids.next()),
                            &event.id,
                            400,
                            EventOriginUri {
                                uid: sink_uid,
                                scheme: "tremor-rest".to_string(),
                                host: hostname(),
                                ..EventOriginUri::default()
                            },
                            &e,
                        );
                        if let Err(e) = reply_tx.send(sink::Reply::Response(ERR, error_event)).await
                        {
                            error!(
                                "[Sink::{}] Error sending error response event {}",
                                &sink_url, e
                            );
                        }
                    }
                }
            }
            CodecTaskInMsg::ToEvent {
                id,
                origin_uri,
                op_meta,
                mut response,
                duration,
            } => {
                // send CB insight -> handle status >= 400
                let status = response.status();

                let mut meta = simd_json::borrowed::Object::with_capacity(1);
                let mut cb = if status.is_client_error() || status.is_server_error() {
                    // when the offramp is linked to pipeline, we want to send
                    // the response back and not consume it yet (or log about it)
                    if !is_linked {
                        if let Ok(body) = response.body_string().await {
                            error!(
                                "[Sink::{}] HTTP request failed: {} => {}",
                                &sink_url, status, body
                            )
                        } else {
                            error!("[Sink::{}] HTTP request failed: {}", &sink_url, status)
                        }
                    }
                    meta.insert("time".into(), Value::from(duration));
                    CBAction::Fail
                } else {
                    CBAction::Ack
                };

                if is_linked {
                    match build_response_events(
                        &sink_url,
                        &id,
                        origin_uri.as_ref(),
                        response,
                        codec.borrow(),
                        &codec_map,
                        preprocessors.as_mut_slice(),
                    )
                    .await
                    {
                        Ok(response_events) => {
                            for mut response_event in response_events {
                                let my_id = Ids::new(sink_uid, response_ids.next());
                                response_event.id.merge(&my_id);
                                if let Err(e) = reply_tx
                                    .send(sink::Reply::Response(OUT, response_event))
                                    .await
                                {
                                    error!(
                                        "[Sink::{}] Error sending response event: {}",
                                        &sink_url, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            cb = CBAction::Fail;
                            error!("[Sink::{}] Error encoding the request: {}", &sink_url, &e);
                            let error_event = create_error_response(
                                Ids::new(sink_uid, response_ids.next()),
                                &id,
                                500,
                                origin_uri.as_ref().clone(),
                                &e,
                            );

                            if let Err(e) =
                                reply_tx.send(sink::Reply::Response(ERR, error_event)).await
                            {
                                error!(
                                    "[Sink::{}] Error sending error event on response decoding error: {}",
                                    &sink_url,
                                    e
                                );
                            }
                        }
                    }
                }
                // wait with sending CB Ack until response has been handled in linked case, might still fail
                if let Err(e) = reply_tx
                    .send(sink::Reply::Insight(Event {
                        id: id.clone(),
                        op_meta,
                        data: (Value::null(), Value::from(meta)).into(),
                        cb,
                        ingest_ns: nanotime(),
                        ..Event::default()
                    }))
                    .await
                {
                    error!("[Sink::{}] Error sending CB event {}", &sink_url, e);
                };
            }
            CodecTaskInMsg::ReportFailure(id, op_meta, event_origin_uri, e) => {
                // report send error as CB fail and response via ERROR port
                // sending a CB close would mean we need to take measures to reopen - introduce a healthcheck
                let mut insight = Event::cb_fail(nanotime(), id.clone());
                insight.op_meta = op_meta;
                if let Err(send_err) = reply_tx.send(sink::Reply::Insight(insight)).await {
                    error!(
                        "[Sink::{}] Error sending CB trigger event for event {}: {}",
                        &sink_url, id, send_err
                    );
                }
                let error_event = create_error_response(
                    Ids::new(sink_uid, response_ids.next()),
                    &id,
                    503,
                    event_origin_uri,
                    &e,
                );
                if let Err(send_err) = reply_tx.send(sink::Reply::Response(ERR, error_event)).await
                {
                    error!(
                        "[Sink::{}] Error sending error response for failed request send: {}",
                        &sink_url, send_err
                    );
                }
            }
        }
    }
    info!("[Sink::{}] Codec task stopped. Channel closed.", &sink_url);
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_request(
    event: &Event,
    codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    postprocessors: &mut [Box<dyn Postprocessor>],
    default_method: Method,
    default_headers: &HashMap<String, String>,
    config_endpoint: &Endpoint,
) -> Result<surf::Request> {
    let mut body: Vec<u8> = vec![];
    let mut method = None;
    let mut endpoint = None;
    let mut headers: Vec<(&Cow<str>, Vec<HeaderValue>)> = Vec::with_capacity(8);
    let mut codec_in_use = None;
    for (data, meta) in event.value_meta_iter() {
        if let Some(request_meta) = meta.get("request") {
            // use method from first event
            if method.is_none() {
                method = Some(
                    match request_meta
                        .get("method")
                        .and_then(Value::as_str)
                        .map(|m| Method::from_str(&m.trim().to_uppercase()))
                    {
                        Some(Ok(method)) => method,
                        Some(Err(e)) => return Err(e.into()),
                        None => default_method,
                    },
                );
            }
            // use headers from event
            if headers.is_empty() {
                if let Some(map) = request_meta.get("headers").and_then(Value::as_object) {
                    for (header_name, v) in map {
                        // filter out content-length (might be carried over from received request), likely to have changed
                        if "content-length".eq_ignore_ascii_case(header_name) {
                            continue;
                        }
                        if let Some(value) = v.as_str() {
                            let values = vec![HeaderValue::from_str(value)?];
                            headers.push((header_name, values));
                            // TODO: deduplicate
                            // try to determine codec
                            if "content-type".eq_ignore_ascii_case(header_name)
                                && codec_in_use.is_none()
                            {
                                codec_in_use = Some(
                                    Mime::from_str(value)
                                        .ok()
                                        .and_then(|m| codec_map.get(m.essence()))
                                        .map_or(codec, |c| c.borrow()),
                                );
                            }
                        } else if let Some(array) = v.as_array() {
                            let mut values = Vec::with_capacity(array.len());
                            for header in array {
                                if let Some(value) = header.as_str() {
                                    values.push(HeaderValue::from_str(value)?);
                                    // try to determine codec
                                    if "content-type".eq_ignore_ascii_case(header_name)
                                        && codec_in_use.is_none()
                                    {
                                        codec_in_use = Some(
                                            Mime::from_str(value)
                                                .ok()
                                                .and_then(|m| codec_map.get(m.essence()))
                                                .map_or(codec, |c| c.borrow()),
                                        );
                                    }
                                }
                            }
                            headers.push((header_name, values));
                        }
                    }
                }
            }
        }
        // use url from first event
        if endpoint.is_none() {
            endpoint = match meta.get("endpoint") {
                Some(v) if v.is_str() => {
                    // overwrite the configured endpoint with that is provided as String
                    if let Some(s) = v.as_str() {
                        Some(Endpoint::from_str(s)?)
                    } else {
                        None // shouldnt happen
                    }
                }
                Some(v) if v.is_object() => {
                    let mut ep = config_endpoint.clone();
                    ep.merge(v);
                    Some(ep)
                }
                // invalid type
                Some(_) => return Err("Invalid $endpoint type. Use String or Map.".into()),
                None => None, // we use the config endpoint
            }
        }

        // apply the given codec, fall back to the configured codec if none is found
        let codec = codec_in_use.unwrap_or(codec);
        let encoded = codec.encode(data)?;
        let mut processed = postprocess(postprocessors, event.ingest_ns, encoded)?;
        for processed_elem in &mut processed {
            body.append(processed_elem);
        }
    }
    let endpoint = endpoint.map_or_else(|| config_endpoint.as_url(), |ep| ep.as_url())?;
    debug!("endpoint [{}] chosen", &endpoint);
    let host = match (endpoint.host(), endpoint.port()) {
        (Some(host), Some(port)) => Some(format!("{}:{}", host, port)),
        (Some(host), _) => Some(host.to_string()),
        _ => None,
    };

    let mut request_builder = surf::RequestBuilder::new(method.unwrap_or(default_method), endpoint);

    // build headers from config
    for (k, v) in default_headers {
        request_builder = request_builder.header(k.as_str(), v.as_str());
    }

    // build headers from meta - effectivelty overwrite config headers in case of conflict
    for (k, v) in headers {
        if "content-type".eq_ignore_ascii_case(k) {
            'inner: for hv in &v {
                if let Ok(mime) = Mime::from_str(hv.as_str()) {
                    request_builder = request_builder.content_type(mime);
                    break 'inner;
                }
            }
        }
        request_builder = request_builder.header(k.as_ref(), v.as_slice());
    }
    // overwrite Host header
    // otherwise we might run into trouble when overwriting the endpoint.
    if let Some(host) = host {
        request_builder = request_builder.header("Host", host);
    }
    request_builder = request_builder.body(Body::from_bytes(body));
    Ok(request_builder.build())
}

async fn build_response_events(
    sink_url: &TremorURL,
    id: &Ids,
    event_origin_uri: &EventOriginUri,
    mut response: Response,
    codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    preprocessors: &mut [Box<dyn Preprocessor>],
) -> Result<Vec<Event>> {
    let mut meta = Value::object_with_capacity(1);
    let mut response_meta = Value::object_with_capacity(2);

    let numeric_status: u16 = response.status().into();
    response_meta.insert("status", numeric_status)?;

    let mut headers = Value::object_with_capacity(8);
    {
        for (name, values) in response.iter() {
            let header_values: Value = values
                .iter()
                .map(ToString::to_string)
                .map(Value::from)
                .collect();
            headers.insert(name.to_string(), header_values)?;
        }
    }
    response_meta.insert("headers", headers)?;
    meta.insert("response", response_meta)?;
    // chose a codec
    let the_chosen_one = response
        .content_type()
        .and_then(|mime| codec_map.get(mime.essence()))
        .map_or(codec, |c| c.borrow());

    // extract one or multiple events from the body
    let response_bytes = response.body_bytes().await?;
    let mut ingest_ns = nanotime();
    let preprocessed = preprocess(preprocessors, &mut ingest_ns, response_bytes, sink_url)?;

    let mut events = Vec::with_capacity(preprocessed.len());
    for pp in preprocessed {
        events.push(
            LineValue::try_new(vec![pp], |mutd| {
                let mut_data = mutd[0].as_mut_slice();
                let body = the_chosen_one
                    .decode(mut_data, nanotime())?
                    .unwrap_or_else(Value::object);

                Ok(ValueAndMeta::from_parts(body, meta.clone())) // TODO: no need to clone the last element?
            })
            .map_err(|e: rental::RentalError<Error, _>| e.0)
            .map(|data| Event {
                id: id.clone(),
                origin_uri: Some(event_origin_uri.clone()),
                data,
                ..Event::default()
            })?,
        );
    }
    Ok(events)
}

/// build an error event bearing error information and metadata to be handled as http response
fn create_error_response(
    mut error_id: Ids,
    event_id: &Ids,
    status: u16,
    origin_uri: EventOriginUri,
    e: &Error,
) -> Event {
    let mut error_data = simd_json::value::borrowed::Object::with_capacity(2);
    let mut meta = simd_json::value::borrowed::Object::with_capacity(2);
    let mut response_meta = simd_json::value::borrowed::Object::with_capacity(2);
    response_meta.insert_nocheck("status".into(), Value::from(status));
    let mut headers = simd_json::value::borrowed::Object::with_capacity(2);
    headers.insert_nocheck("Content-Type".into(), Value::from("application/json"));
    headers.insert_nocheck("Server".into(), Value::from("Tremor"));
    response_meta.insert_nocheck("headers".into(), Value::from(headers));

    meta.insert_nocheck("response".into(), Value::from(response_meta));
    meta.insert_nocheck("error".into(), Value::from(e.to_string()));

    error_data.insert_nocheck("error".into(), Value::from(e.to_string()));
    error_data.insert_nocheck("event_id".into(), Value::from(event_id.to_string()));
    error_id.merge(event_id); // make sure we carry over the old events ids
    Event {
        id: error_id,
        data: (error_data, meta).into(),
        origin_uri: Some(origin_uri),
        ingest_ns: nanotime(),
        ..Event::default()
    }
}

/// atomically count up from 0 to a given max
/// and fail incrementing further if that max is reached.
struct AtomicMaxCounter {
    counter: AtomicUsize,
    max: usize,
}

impl AtomicMaxCounter {
    fn new(max: usize) -> Self {
        Self {
            counter: AtomicUsize::new(0),
            max,
        }
    }

    fn load(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }

    fn inc_from(&self, cur: usize) -> Result<usize> {
        let mut real_cur = cur;
        if (real_cur + 1) > self.max {
            return Err("max value reached".into());
        }
        while self
            .counter
            .compare_and_swap(real_cur, real_cur + 1, Ordering::AcqRel)
            != real_cur
        {
            real_cur = self.load();
            if (real_cur + 1) > self.max {
                return Err("max value reached".into());
            }
        }
        Ok(real_cur + 1)
    }

    fn inc(&self) -> Result<usize> {
        self.inc_from(self.load())
    }

    fn dec_from(&self, cur: usize) -> usize {
        let mut real_cur = cur;
        if real_cur == 0 {
            // avoid underflow
            return real_cur;
        }
        while self
            .counter
            .compare_and_swap(real_cur, real_cur - 1, Ordering::AcqRel)
            != real_cur
        {
            real_cur = self.load();
            if real_cur == 0 {
                // avoid underflow
                return real_cur;
            }
        }
        real_cur - 1
    }
}

#[cfg(test)]
mod test {

    use http_types::{headers::HeaderValues, StatusCode};

    use super::*;

    #[test]
    fn deserialize_from_string() -> Result<()> {
        let config_s = r#"
            endpoint: "http://localhost:8080/path?query=value#fragment"
            concurrency: 4
            method: GET
            headers:
                Forwarded: "by=tremor(0.9);"
        "#;
        let v: serde_yaml::Value = serde_yaml::from_str(config_s)?;
        let config = Config::new(&v)?;
        let _ = config.endpoint.as_url_with_base("http://localhost");
        assert_eq!(
            config.endpoint,
            Endpoint {
                scheme: Some("http".to_string()),
                host: Some("localhost".to_string()),
                port: Some(8080),
                path: Some("/path".to_string()),
                query: Some("query=value".to_string()),
                fragment: Some("fragment".to_string()),
                ..Endpoint::default()
            }
        );
        assert_eq!(config.method.0, Method::Get);
        Ok(())
    }

    #[test]
    fn deserialize_from_object() -> Result<()> {
        let config_s = r#"
            endpoint: 
                host: example.org
                query: via=tremor
            concurrency: 4
            method: OPTIONS
        "#;
        let v: serde_yaml::Value = serde_yaml::from_str(config_s)?;
        let config = Config::new(&v)?;
        assert_eq!(
            config.endpoint,
            Endpoint {
                host: Some("example.org".to_string()),
                query: Some("via=tremor".to_string()),
                ..Endpoint::default()
            }
        );
        assert_eq!(config.method.0, Method::Options);
        Ok(())
    }

    #[test]
    fn deserialize_method() -> Result<()> {
        let config_s = r#"
          endpoint: http://localhost:8080/
          method: PATCH
        "#;
        let v: serde_yaml::Value = serde_yaml::from_str(config_s)?;
        let config = Config::new(&v)?;
        assert_eq!(config.method.0, Method::Patch);
        Ok(())
    }

    #[test]
    fn endpoint_merge() -> Result<()> {
        let mut ep = Endpoint {
            scheme: Some("http".to_string()),
            host: Some("example.org".to_string()),
            port: Some(80),
            path: Some("/".to_string()),
            ..Endpoint::default()
        };
        let mut value = Value::object_with_capacity(2);
        value.insert("host", "tremor.rs")?;
        value.insert("query", "version=0.9")?;
        ep.merge(&value);
        assert_eq!(
            Endpoint {
                scheme: Some("http".to_string()),
                host: Some("tremor.rs".to_string()),
                port: Some(80),
                path: Some("/".to_string()),
                query: Some("version=0.9".to_string()),
                ..Endpoint::default()
            },
            ep
        );
        Ok(())
    }

    #[test]
    fn endpoint_as_url() -> Result<()> {
        let ep = Endpoint {
            scheme: Some("http".to_string()),
            host: Some("tremor.rs".to_string()),
            path: Some("/getting-started/".to_string()),
            ..Endpoint::default()
        };
        let url = ep.as_url()?;
        assert_eq!(
            url::Url::from_str("http://tremor.rs/getting-started/")?,
            url
        );
        Ok(())
    }

    #[test]
    fn endpoint_as_url_fail() -> Result<()> {
        // endpoint without host
        let ep = Endpoint {
            scheme: Some("http".to_string()),
            path: Some("/getting-started/".to_string()),
            ..Endpoint::default()
        };
        let res = ep.as_url();
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn build_request_multiple_headers() -> Result<()> {
        let mut event = Event::default();
        let mut meta = Value::object_with_capacity(2);
        let mut request_meta = Value::object_with_capacity(2);
        let mut request_headers_meta = Value::object_with_capacity(2);
        request_headers_meta.insert("Content-Type", "text/plain")?;
        request_headers_meta.insert("Content-Length", "3")?;
        request_headers_meta.insert("Overwrite-Me", "indeed!")?;
        let mut multiple = Value::array_with_capacity(2);
        multiple.push("first")?;
        multiple.push("second")?;
        request_headers_meta.insert("Multiple", multiple)?;
        request_meta.insert("headers", request_headers_meta)?;
        meta.insert("request", request_meta)?;

        let data = Value::String("foo".into());
        event.data = (data, meta).into();
        let codec = crate::codec::lookup("string")?;
        let codec_map = crate::codec::builtin_codec_map();
        let mut pp = vec![];
        let mut default_headers = halfbrown::HashMap::with_capacity(2);
        default_headers.insert_nocheck("Server".to_string(), "Tremor".to_string());
        default_headers.insert_nocheck("Overwrite-Me".to_string(), "oh noes!".to_string());
        let endpoint = Endpoint::from_str("http://localhost:65535/path&query=value#fragment")?;
        let request = build_request(
            &event,
            codec.as_ref(),
            &codec_map,
            pp.as_mut_slice(),
            Method::Get,
            &default_headers,
            &endpoint,
        )?;
        let expected = HeaderValues::from(HeaderValue::from_str("indeed!")?);
        assert_eq!(
            Some(expected.to_string()),
            request.header("Overwrite-Me").map(ToString::to_string)
        );
        let mut expected_multiple = HeaderValues::from(HeaderValue::from_str("first")?);
        expected_multiple.append(&mut HeaderValues::from(HeaderValue::from_str("second")?));
        assert_eq!(
            Some(expected_multiple.to_string()),
            request.header("Multiple").map(ToString::to_string)
        );
        assert_eq!(request.url(), &endpoint.as_url()?);
        Ok(())
    }

    #[async_std::test]
    async fn build_response() -> Result<()> {
        use simd_json::json;
        let sink_url = TremorURL::from_offramp_id("rest")?;
        let id = Ids::default();
        let event_origin_uri = EventOriginUri::default();
        let mut body = Body::from_string(r#"{"foo": true}"#.to_string());
        body.set_mime(http_types::mime::JSON);
        let mut response = http_types::Response::new(StatusCode::Ok);
        response.append_header("Server", "Upstream");
        response.append_header("Multiple", "first");
        response.append_header("Multiple", "second");
        response.set_body(body);
        let codec = crate::codec::lookup("string")?;
        let codec_map = crate::codec::builtin_codec_map();
        let mut pp = vec![];

        let res = build_response_events(
            &sink_url,
            &id,
            &event_origin_uri,
            Response::from(response),
            codec.as_ref(),
            &codec_map,
            pp.as_mut_slice(),
        )
        .await?;
        assert_eq!(1, res.len());
        if let Some(event) = res.get(0) {
            let mut expected_data = Value::object_with_capacity(1);
            expected_data.insert("foo", true)?;
            let data_parts = event.data.parts();
            assert_eq!(&mut expected_data, data_parts.0);
            let mut expected_meta = json!({
                "response": {
                    "headers": {
                        "multiple": ["first", "second"],
                        "server": ["Upstream"],
                        "content-type": ["application/json"]
                    }
                }
            });
            if let Some(response) = expected_meta.get_mut("response") {
                response.insert("status", 200_u64)?;
            }
            assert_eq!(expected_meta, data_parts.1.to_owned());
        }
        Ok(())
    }
}
