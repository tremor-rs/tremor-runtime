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
use std::borrow::Borrow;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use surf::{Body, Request, Response};
use tremor_pipeline::{Ids, OpMeta};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,

    /// maximum number of parallel in flight batches (default: 4)
    /// this avoids blocking further events from progressing while waiting for upstream responses.
    #[serde(default = "concurrency")]
    pub concurrency: usize,
    // TODO add scheme, host, path, query
    // HTTP method to use (default: POST)
    // TODO implement Deserialize for http_types::Method
    // https://docs.rs/http-types/2.4.0/http_types/enum.Method.html
    #[serde(skip_deserializing, default = "dflt_method")]
    pub method: Method,
    #[serde(default = "Default::default")]
    // TODO make header values a vector here?
    pub headers: HashMap<String, String>,
}

fn dflt_method() -> Method {
    Method::Get
}

fn concurrency() -> usize {
    4
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

struct SendTaskInMsg(Request);

pub struct Rest {
    uid: u64,
    config: Config,
    num_inflight_requests: Arc<AtomicMaxCounter>,
    is_linked: bool,
    reply_channel: Option<Sender<SinkReply>>,
    codec_task_handle: Option<JoinHandle<Result<()>>>,
    codec_task_tx: Option<Sender<CodecTaskInMsg>>,
}

#[derive(Debug)]
struct RestRequestMeta {
    // TODO support this layout
    //scheme: String,
    //host: String,
    //path: String,
    //query: Option<String>,
    endpoint: String,
    method: Method,
    headers: Option<HashMap<String, Vec<String>>>,
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

impl Rest {}

#[async_trait::async_trait]
impl Sink for Rest {
    #[allow(clippy::used_underscore_binding)]
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
                let SendTaskInMsg(request) = rx.recv().await?;

                let url = request.url();
                let event_origin_uri = EventOriginUri {
                    uid: sink_uid,
                    scheme: url.scheme().to_string(),
                    host: url.host_str().map_or(String::new(), ToString::to_string),
                    port: url.port(),
                    path: url
                        .path_segments()
                        .map_or_else(Vec::new, |segments| segments.map(String::from).collect()),
                };
                // send request
                // TODO reuse client
                if let Ok(response) = surf::client().send(request).await {
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

    #[allow(clippy::too_many_arguments, clippy::used_underscore_binding)]
    async fn init(
        &mut self,
        sink_uid: u64,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        preprocessors: &[String],
        postprocessors: &[String],
        is_linked: bool,
        reply_channel: Sender<SinkReply>,
    ) -> Result<()> {
        // clone the hell out of all the shit
        let postprocessors = make_postprocessors(postprocessors)?;
        let preprocessors = make_preprocessors(preprocessors)?;
        let my_codec = codec.boxed_clone();
        let my_codec_map = codec_map
            .iter()
            .map(|(k, v)| (k.clone(), v.boxed_clone()))
            .collect::<HashMap<String, Box<dyn Codec>>>();
        let default_method = self.config.method;
        let endpoints = self.config.endpoints.clone();

        // inbound channel towards codec task
        // sending events to be turned into requests
        // and responses to be turned into events
        let (in_tx, in_rx) = bounded::<CodecTaskInMsg>(self.config.concurrency);
        // channel for sending shit back to the connected pipelines
        let reply_tx = reply_channel.clone();
        // start the codec task
        self.codec_task_handle = Some(task::spawn(async move {
            codec_task(
                postprocessors,
                preprocessors,
                my_codec,
                my_codec_map,
                endpoints,
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

    #[allow(clippy::used_underscore_binding)]
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

#[inline]
fn get_endpoint(mut endpoint_idx: usize, endpoints: &[String]) -> Option<&str> {
    endpoint_idx = (endpoint_idx + 1) % endpoints.len();
    endpoints.get(endpoint_idx).map(String::as_str)
}

#[allow(clippy::too_many_arguments)]
async fn codec_task(
    mut postprocessors: Vec<Box<dyn Postprocessor>>,
    mut preprocessors: Vec<Box<dyn Preprocessor>>,
    codec: Box<dyn Codec>,
    codec_map: HashMap<String, Box<dyn Codec>>,
    endpoints: Vec<String>,
    default_method: Method,
    reply_tx: Sender<SinkReply>,
    in_rx: Receiver<CodecTaskInMsg>,
    is_linked: bool,
) -> Result<()> {
    debug!("REST Sink codec task started.");
    let endpoint_idx: usize = 0;

    while let Ok(msg) = in_rx.recv().await {
        match msg {
            CodecTaskInMsg::ToRequest(event, tx) => {
                match build_request(
                    &event,
                    codec.borrow(),
                    &codec_map,
                    postprocessors.as_mut_slice(),
                    default_method,
                    get_endpoint(endpoint_idx, endpoints.as_slice()),
                ) {
                    Ok(request) => {
                        if let Err(e) = tx.send(SendTaskInMsg(request)).await {
                            error!("Error sending out encoded request {}", e);
                        }
                    }
                    Err(_e) => {} // TODO: send CB Fail, error port
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
                let cb = if status.is_client_error() || status.is_server_error() {
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
                    .send(SinkReply::Insight(Event {
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
                        id.clone(),
                        origin_uri,
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
                                    .send(SinkReply::Response(RESPONSE, response_event))
                                    .await
                                {
                                    error!("Error sending response event: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            // TODO log the error here as well?
                            let mut data = simd_json::borrowed::Object::with_capacity(1);
                            // TODO should also include event_id and offramp (as url string) here
                            data.insert_nocheck("error".into(), e.to_string().into());

                            // TODO also pass response meta alongside which can be useful for
                            // errors too [will probably need to return (port, data)
                            // as part of response_events]
                            let error_event = Event {
                                id,
                                origin_uri: None, // TODO create new origin uri for this
                                data: Value::from(data).into(),
                                ..Event::default()
                            };
                            if let Err(e) =
                                reply_tx.send(SinkReply::Response(ERROR, error_event)).await
                            {
                                error!("Error sending error event: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
    info!("REST Sink codec task stopped. Channel closed.");
    Ok(())
}

fn build_request(
    event: &Event,
    codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    postprocessors: &mut [Box<dyn Postprocessor>],
    default_method: Method,
    endpoint: Option<&str>,
) -> Result<surf::Request> {
    let mut body: Vec<u8> = vec![];
    let mut method = None;
    let mut url = None;
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
                    Some(Err(e)) => return Err(e.into()), // method parsing failed
                    None => default_method,
                },
            );
        }
        // use url from first event
        if url.is_none() {
            url = match meta
                .get("endpoint")
                .and_then(Value::as_str)
                .or_else(|| endpoint)
            {
                Some(url_str) => Some(url_str.parse::<surf::Url>()?),
                None => None,
            };
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
        // chose codec based on first event in a batch
        if let Some(codec) = codec_in_use {
            let encoded = codec.encode(data)?;
            let mut processed = postprocess(postprocessors, event.ingest_ns, encoded)?;
            for processed_elem in &mut processed {
                body.append(processed_elem);
            }
        };
    }
    let url =
        url.ok_or_else(|| -> Error { "Unable to determine and endpoint for this event".into() })?;
    let host = match (url.host(), url.port()) {
        (Some(host), Some(port)) => Some(format!("{}:{}", host, port)),
        (Some(host), _) => Some(host.to_string()),
        _ => None,
    };

    let mut request_builder = surf::RequestBuilder::new(method.unwrap_or(default_method), url);

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
    id: Ids,
    event_origin_uri: Box<EventOriginUri>,
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
                origin_uri: Some(event_origin_uri.as_ref().clone()),
                data,
                ..Event::default()
            })?,
        );
    }
    Ok(events)
}

/*
fn create_error_response(id: Ids, op_meta: OpMeta, e: &Error) -> Event {
    let mut error_data = simd_json::value::borrowed::Object::with_capacity(1);
    let mut meta = simd_json::value::borrowed::Object::with_capacity(2);
    meta.insert_nocheck("response_status".into(), Value::from(500));
    let err_str = e.to_string();
    let mut headers = simd_json::value::borrowed::Object::with_capacity(3);
    headers.insert_nocheck("Content-Type".into(), Value::from("application/json"));
    headers.insert_nocheck("Content-Length".into(), Value::from(err_str.len() + 12)); // len of `{"error": err_str}`
    headers.insert_nocheck("Server".into(), Value::from("Tremor"));
    meta.insert_nocheck("response_headers".into(), Value::from(headers));
    error_data.insert_nocheck("error".into(), Value::from(err_str));
    Event {
        id,
        op_meta,
        data: (error_data, meta).into(),
        ..Event::default()
    }
}
*/

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
