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
use async_channel::Sender;
use halfbrown::HashMap;
use http_types::Method;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use surf::Response;
use tremor_pipeline::OpMeta;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// maximum number of parallel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    // TODO adjust for linking
    pub concurrency: usize,
    // TODO add scheme, host, path, query
    // HTTP method to use (default: POST)
    // TODO implement Deserialize for http_types::Method
    // https://docs.rs/http-types/2.4.0/http_types/enum.Method.html
    #[serde(skip_deserializing, default = "dflt_method")]
    pub method: Method,
    #[serde(default = "dflt::d")]
    // TODO make header values a vector here?
    pub headers: HashMap<String, String>,

    // TODO: better name?
    /// mapping from mime-type to codec used to handle requests/responses
    /// with this mime-type
    ///
    /// e.g.:
    ///       codec_map:
    ///         "application/json": "json"
    ///         "text/plain": "string"
    ///
    /// A default builtin codec mapping is defined
    /// for msgpack, json, yaml and plaintext codecs with the common mime-types
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec_map: Option<HashMap<String, String>>,
}

fn dflt_method() -> Method {
    Method::Post
}

impl ConfigImpl for Config {}

pub struct Rest {
    client_idx: usize,
    config: Config,
    num_inflight_requests: AtomicMaxCounter,
    postprocessors: Postprocessors,
    //codec_map: HashMap<String, Box<dyn Codec>>,
    is_linked: bool,
    reply_channel: Option<Sender<SinkReply>>,
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
            let num_inflight_requests = AtomicMaxCounter::new(config.concurrency);
            Ok(SinkManager::new_box(Self {
                client_idx: 0,
                postprocessors: vec![],
                is_linked: false,
                config,
                num_inflight_requests,
                //codec_map: codec::builtin_codec_map(),
                reply_channel: None,
            }))
        } else {
            Err("Rest offramp requires a configuration.".into())
        }
    }
}

impl Rest {
    fn get_endpoint(&mut self) -> Option<&str> {
        self.client_idx = (self.client_idx + 1) % self.config.endpoints.len();
        self.config
            .endpoints
            .get(self.client_idx)
            .map(|s| s.as_str())
    }

    fn build_request(&mut self, event: &Event, codec: &dyn Codec) -> Result<surf::RequestBuilder> {
        let mut body = vec![];
        let mut method = None;
        let mut url = None;
        let mut headers = Vec::with_capacity(8);
        for (data, meta) in event.value_meta_iter() {
            // TODO: enable dynamic codec selection
            let encoded = codec.encode(data)?;
            let mut processed = postprocess(&mut self.postprocessors, event.ingest_ns, encoded)?;
            for processed_elem in processed.iter_mut() {
                body.append(processed_elem);
            }
            // use method from first event
            if method.is_none() {
                method = Some(
                    match meta
                        .get("request_value")
                        .and_then(Value::as_str)
                        .map(|m| Method::from_str(&m.trim().to_uppercase()))
                    {
                        Some(Ok(method)) => method,
                        Some(Err(e)) => return Err(e.into()), // method parsing failed
                        None => self.config.method,
                    },
                );
            }
            // use url from first event
            if url.is_none() {
                url = match meta
                    .get("endpoint")
                    .and_then(Value::as_str)
                    .or_else(|| self.get_endpoint())
                {
                    Some(url_str) => Some(url_str.parse::<surf::url::Url>()?),
                    None => None,
                };
            }
            if headers.is_empty() {
                if let Some(map) = meta.get("request_headers").and_then(Value::as_object) {
                    for (k, v) in map {
                        if let Some(value) = v.as_str() {
                            headers.push((k, value));
                        }
                    }
                }
            }
        }
        let url = url.ok_or::<Error>("Unable to determine and endpoint for this event".into())?;
        let mut request_builder =
            surf::RequestBuilder::new(method.unwrap_or(self.config.method), url);

        // build headers
        for (k, v) in headers {
            request_builder = request_builder.header(k.to_string().as_str(), v);
        }
        Ok(request_builder)
    }

    async fn send_request(&mut self, event: Event, codec: &dyn Codec) -> ResultVec {
        let start = Instant::now();
        let request_builder = self.build_request(&event, codec)?;
        // send request & receive response
        // TODO: keep surf clients around for maintaining persistent connections
        let mut res = request_builder.await?;
        // send ack if status < 400
        // send fail if status >= 400
        // if linked - send response

        let status = res.status();
        let mut meta = Value::object_with_capacity(1);
        let cb = if status.is_client_error() || status.is_server_error() {
            if let Ok(body) = res.body_string().await {
                error!("HTTP request failed: {} => {}", status, body)
            } else {
                error!("HTTP request failed: {}", status)
            }
            let duration = duration_to_millis(start.elapsed());
            meta.insert("time", duration)?;
            CBAction::Fail
        } else {
            CBAction::Ack
        };

        if let Some(reply_channel) = &self.reply_channel {
            let send_res = reply_channel
                .send(SinkReply::Insight(Event {
                    id: event.id.clone(),
                    op_meta: event.op_meta.clone(),
                    data: (Value::null(), meta).into(),
                    cb,
                    ingest_ns: nanotime(),
                    ..Event::default()
                }))
                .await;

            if let Err(e) = send_res {
                error!("Failed to send insight eply: {}", e) // do not fail and trigger cb::fail here
            };

            // send response if we are linked
            if self.is_linked {
                // TODO: how to handle errors creating or sending the response?
                if let Ok(response_event) =
                    Self::make_response_event(event.id.clone(), event.op_meta.clone(), res, codec)
                        .await
                {
                    if let Err(e) = reply_channel
                        .send(SinkReply::Response(response_event))
                        .await
                    {
                        error!("Failed to send response reply: {}", e) // do not fail and trigger cb::fail here
                    }
                }
            }
        }

        Ok(None)
    }

    async fn make_response_event(
        id: Ids,
        op_meta: OpMeta,
        mut response: Response,
        codec: &dyn Codec,
    ) -> Result<Event> {
        let mut meta = Value::object_with_capacity(8);
        let numeric_status: u16 = response.status().into();
        meta.insert("response_status", numeric_status)?;

        let mut headers = Value::object_with_capacity(8);
        {
            let mut iter: http_types::headers::Iter<'_> = response.iter();
            while let Some((name, values)) = iter.next() {
                let mut header_value = String::new();
                for value in values {
                    header_value.push_str(value.to_string().as_str());
                }
                headers.insert(name.to_string(), header_value)?;
            }
        }
        meta.insert("response_headers", headers)?;

        // body
        let response_bytes = response.body_bytes().await?;
        LineValue::try_new(vec![response_bytes], |mutd| {
            let mut_data = mutd[0].as_mut_slice();
            let body = codec
                .decode(mut_data, nanotime())?
                .unwrap_or(Value::object());

            Ok(ValueAndMeta::from_parts(body, meta))
        })
        .map_err(|e| e.0)
        .map(|data| Event {
            id,
            data,
            op_meta,
            origin_uri: None, // TODO
            ..Event::default()
        })
    }
}

#[async_trait::async_trait]
impl Sink for Rest {
    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, codec: &dyn Codec, event: Event) -> ResultVec {
        if self.is_linked && event.is_batch {
            return Err("Batched events are not supported for linked rest offramps".into());
        }
        // limit concurrency
        if let Ok(current_inflights) = self.num_inflight_requests.inc() {
            let result_vec = self.send_request(event, codec).await;
            self.num_inflight_requests.dec_from(current_inflights); // never forget
            result_vec
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
        postprocessors: &[String],
        is_linked: bool,
        reply_channel: Sender<SinkReply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        self.is_linked = is_linked;
        self.reply_channel = Some(reply_channel);
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
        if real_cur <= 0 {
            // avoid underflow
            return real_cur;
        }
        while self
            .counter
            .compare_and_swap(real_cur, real_cur - 1, Ordering::AcqRel)
            != real_cur
        {
            real_cur = self.load();
            if real_cur <= 0 {
                // avoid underflow
                return real_cur;
            }
        }
        real_cur - 1
    }
}
