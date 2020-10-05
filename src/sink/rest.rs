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

use crate::codec::Codec;
use crate::sink::prelude::*;
use crate::url::TremorURL;
use async_channel::{bounded, Receiver, Sender};
use async_std::task::JoinHandle;
use halfbrown::HashMap;
use http_types::mime::Mime;
use http_types::Method;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use surf::{Body, Request, Response};
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

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// endpoint url - given as String or struct
    #[serde(deserialize_with = "string_or_struct", default)]
    pub endpoint: Endpoint,

    /// maximum number of parallel in flight batches (default: 4)
    /// this avoids blocking further events from progressing while waiting for upstream responses.
    #[serde(default = "dflt_concurrency")]
    // TODO adjust for linking
    pub concurrency: usize,
    // TODO add scheme, host, path, query
    // HTTP method to use (default: POST)
    // TODO implement Deserialize for http_types::Method
    // https://docs.rs/http-types/2.4.0/http_types/enum.Method.html
    #[serde(skip_deserializing, default = "dflt_method")]
    pub method: Method,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn dflt_concurrency() -> usize {
    4
}

fn dflt_method() -> Method {
    Method::Get
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
}

impl offramp::Impl for Rest {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let num_inflight_requests = Arc::new(AtomicMaxCounter::new(config.concurrency));
            Ok(SinkManager::new_box(Self {
                uid: 0,
                config,
                num_inflight_requests,
                is_linked: false,
                reply_channel: None,
                codec_task_handle: None,
                codec_task_tx: None,
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

            // TODO: keep track of the join handle - in order to cancel operations
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
                        // TODO reuse client
                        if let Ok(response) = surf::client().send(request).await {
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

    async fn init(
        &mut self,
        sink_uid: u64,
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
        let default_method = self.config.method;
        let endpoint = self.config.endpoint.clone();

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
                postprocessors,
                preprocessors,
                my_codec,
                my_codec_map,
                endpoint,
                default_method,
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

// TODO: use headers from config
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn codec_task(
    sink_uid: u64,
    mut postprocessors: Vec<Box<dyn Postprocessor>>,
    mut preprocessors: Vec<Box<dyn Preprocessor>>,
    codec: Box<dyn Codec>,
    codec_map: HashMap<String, Box<dyn Codec>>,
    endpoint: Endpoint,
    default_method: Method,
    reply_tx: Sender<sink::Reply>,
    in_rx: Receiver<CodecTaskInMsg>,
    is_linked: bool,
) -> Result<()> {
    debug!("REST Sink codec task started.");
    let mut response_id: u64 = 0;

    while let Ok(msg) = in_rx.recv().await {
        match msg {
            CodecTaskInMsg::ToRequest(mut event, tx) => {
                match build_request(
                    &event,
                    codec.borrow(),
                    &codec_map,
                    postprocessors.as_mut_slice(),
                    default_method,
                    &endpoint,
                ) {
                    Ok(request) => {
                        if let Err(e) = tx.send(SendTaskInMsg::Request(request)).await {
                            error!("Error sending out encoded request {}", e);
                        }
                    }
                    Err(e) => {
                        error!(
                            "[Rest Offramp {}] Error encoding the request: {}",
                            sink_uid, &e
                        );
                        // send error result to send task
                        if let Err(e) = tx.send(SendTaskInMsg::Failed).await {
                            error!("Error sending Failed back to send task: {}", e);
                        }
                        // send CB_fail
                        if let Some(insight) = event.insight_fail() {
                            if let Err(e) = reply_tx.send(SinkReply::Insight(insight)).await {
                                error!("Error sending CB fail event {}", e);
                            }
                        }
                        // send response through error port
                        response_id += 1;
                        let error_event = create_error_response(
                            Ids::new(sink_uid, response_id),
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
                        if let Err(e) = reply_tx.send(SinkReply::Response(ERROR, error_event)).await
                        {
                            error!("Error sending error response event {}", e);
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
                            error!("HTTP request failed: {} => {}", status, body)
                        } else {
                            error!("HTTP request failed: {}", status)
                        }
                    }
                    meta.insert("time".into(), Value::from(duration));
                    CBAction::Fail
                } else {
                    CBAction::Ack
                };

                reply_tx
                    .send(sink::Reply::Insight(Event {
                        id: id.clone(),
                        op_meta,
                        data: (Value::null(), Value::from(meta)).into(),
                        cb,
                        ingest_ns: nanotime(),
                        ..Event::default()
                    }))
                    .await?;

                if is_linked {
                    match build_response_events(
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
                            for response_event in response_events {
                                if let Err(e) = reply_tx
                                    .send(sink::Reply::Response(RESPONSE, response_event))
                                    .await
                                {
                                    error!("Error sending response event: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            cb = CBAction::Fail;
                            error!(
                                "[Rest Offramp {}] Error encoding the request: {}",
                                sink_uid, &e
                            );

                            response_id += 1;

                            let error_event = create_error_response(
                                Ids::new(sink_uid, response_id),
                                &id,
                                500,
                                origin_uri.as_ref().clone(),
                                &e,
                            );

                            if let Err(e) =
                                reply_tx.send(SinkReply::Response(ERROR, error_event)).await
                            {
                                error!(
                                    "Error sending error event on response decoding error: {}",
                                    e
                                );
                            }
                        }
                    }
                }
                // wait with sending CB Ack until response has been handled in linked case, might still fail
                if let Err(e) = reply_tx
                    .send(SinkReply::Insight(Event {
                        id: id.clone(),
                        op_meta,
                        data: (Value::null(), Value::from(meta)).into(),
                        cb,
                        ingest_ns: nanotime(),
                        ..Event::default()
                    }))
                    .await
                {
                    error!("Error sending CB event {}", e);
                };
            }
        }
    }
    info!("REST Sink codec task stopped. Channel closed.");
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_request(
    event: &Event,
    codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    postprocessors: &mut [Box<dyn Postprocessor>],
    default_method: Method,
    config_endpoint: &Endpoint,
) -> Result<surf::Request> {
    let mut body: Vec<u8> = vec![];
    let mut method = None;
    let mut endpoint = None;
    let mut headers = Vec::with_capacity(8);
    let mut codec_in_use = None;
    for (data, meta) in event.value_meta_iter() {
        // use method from first event
        if method.is_none() {
            method = Some(
                match meta
                    .get("request_method")
                    .and_then(Value::as_str)
                    .map(|m| Method::from_str(&m.trim().to_uppercase()))
                {
                    Some(Ok(method)) => method,
                    Some(Err(e)) => return Err(e.into()),
                    None => default_method,
                },
            );
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
        // use headers from event
        if headers.is_empty() {
            if let Some(map) = meta.get("request_headers").and_then(Value::as_object) {
                for (header_name, v) in map {
                    if let Some(value) = v.as_str() {
                        headers.push((header_name, value));
                        // TODO: deduplicate
                        // try to determine codec
                        if codec_in_use.is_none()
                            && "content-type".eq_ignore_ascii_case(header_name)
                        {
                            codec_in_use = Some(
                                Mime::from_str(value)
                                    .ok()
                                    .and_then(|m| codec_map.get(m.essence()))
                                    .map_or(codec, |c| c.borrow()),
                            );
                        }
                    }
                    if let Some(array) = v.as_array() {
                        for header in array {
                            if let Some(value) = header.as_str() {
                                headers.push((header_name, value));
                                // try to determine codec
                                if codec_in_use.is_none()
                                    && "content-type".eq_ignore_ascii_case(header_name)
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
                    }
                }
            }
        }
        // apply the given codec, fall back to the configured codec if none is found
        if let Some(codec) = codec_in_use {
            let encoded = codec.encode(data)?;
            let mut processed = postprocess(postprocessors, event.ingest_ns, encoded)?;
            for processed_elem in &mut processed {
                body.append(processed_elem);
            }
        };
    }
    let endpoint = endpoint.map_or_else(|| config_endpoint.as_url(), |ep| ep.as_url())?;
    let host = match (endpoint.host(), endpoint.port()) {
        (Some(host), Some(port)) => Some(format!("{}:{}", host, port)),
        (Some(host), _) => Some(host.to_string()),
        _ => None,
    };

    let mut request_builder = surf::RequestBuilder::new(method.unwrap_or(default_method), endpoint);

    // build headers
    for (k, v) in headers {
        if "content-type".eq_ignore_ascii_case(k) {
            if let Ok(mime) = Mime::from_str(v) {
                request_builder = request_builder.content_type(mime);
            }
        }
        request_builder = request_builder.header(k.to_string().as_str(), v);
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
    id: &Ids,
    event_origin_uri: &EventOriginUri,
    mut response: Response,
    codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    preprocessors: &mut [Box<dyn Preprocessor>],
) -> Result<Vec<Event>> {
    let mut meta = Value::object_with_capacity(8);
    let numeric_status: u16 = response.status().into();
    meta.insert("response_status", numeric_status)?;

    let mut headers = Value::object_with_capacity(8);
    {
        for (name, values) in response.iter() {
            let mut header_value = String::new();
            for value in values {
                header_value.push_str(value.to_string().as_str());
            }
            headers.insert(name.to_string(), header_value)?;
        }
    }
    meta.insert("response_headers", headers)?;
    // chose a codec
    let the_chosen_one = response
        .content_type()
        .and_then(|mime| codec_map.get(mime.essence()))
        .map_or(codec, |c| c.borrow());

    // extract one or multiple events from the body
    let response_bytes = response.body_bytes().await?;
    let mut ingest_ns = nanotime();
    let preprocessed = preprocess(
        preprocessors,
        &mut ingest_ns,
        response_bytes,
        &TremorURL::from_offramp_id("rest")?, // TODO: get proper url from offramp manager
    )?;

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
    error_id: Ids,
    event_id: &Ids,
    status: u16,
    origin_uri: EventOriginUri,
    e: &Error,
) -> Event {
    let mut error_data = simd_json::value::borrowed::Object::with_capacity(1);
    let mut meta = simd_json::value::borrowed::Object::with_capacity(2);
    meta.insert_nocheck("response_status".into(), Value::from(status));
    let err_str = e.to_string();
    let mut headers = simd_json::value::borrowed::Object::with_capacity(2);
    headers.insert_nocheck("Content-Type".into(), Value::from("application/json"));
    headers.insert_nocheck("Server".into(), Value::from("Tremor"));
    meta.insert_nocheck("response_headers".into(), Value::from(headers));

    error_data.insert_nocheck("error".into(), Value::from(err_str));
    error_data.insert_nocheck("event_id".into(), Value::from(event_id.to_string()));
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
}
