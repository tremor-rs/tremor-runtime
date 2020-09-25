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

use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use halfbrown::HashMap;
use http_types::Method;
use std::str::FromStr;
use std::time::Instant;
use surf::Response;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// maximum number of parallel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    // TODO adjust for linking
    pub concurrency: usize,
    // TODO add scheme, host, path, query
    /// HTTP method to use (default: POST)
    // TODO implement Deserialize for http_types::Method
    // https://docs.rs/http-types/2.4.0/http_types/enum.Method.html
    #[serde(skip_deserializing, default = "dflt_method")]
    pub method: Method,
    #[serde(default = "dflt::d")]
    // TODO make header values a vector here?
    pub headers: HashMap<String, String>,
    /// whether to enable linked transport (return offramp response to pipeline)
    // TODO remove and auto-infer this based on succesful binding for linked offramps
    pub link: Option<bool>,
}

fn dflt_method() -> Method {
    Method::Post
}

impl ConfigImpl for Config {}

pub struct Rest {
    client_idx: usize,
    config: Config,
    queue: AsyncSink<u64>,
    postprocessors: Postprocessors,
    tx: Sender<SinkReply>,
    rx: Receiver<SinkReply>,
    has_link: bool,
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

            let queue = AsyncSink::new(config.concurrency);
            let (tx, rx) = bounded(crate::QSIZE);
            Ok(SinkManager::new_box(Self {
                client_idx: 0,
                postprocessors: vec![],
                has_link: config.link.clone().unwrap_or(false),
                config,
                queue,
                rx,
                tx,
            }))
        } else {
            Err("Rest offramp requires a configuration.".into())
        }
    }
}

impl Rest {
    async fn request(payload: Vec<u8>, meta: RestRequestMeta) -> Result<(u64, Response)> {
        let start = Instant::now();

        // would be nice if surf had a generic function to handle passed http method
        // also implement connection reuse
        let mut c = match meta.method {
            Method::Post => surf::post(meta.endpoint),
            Method::Put => surf::put(meta.endpoint),
            Method::Get => surf::get(meta.endpoint),
            Method::Connect => surf::connect(meta.endpoint),
            Method::Delete => surf::delete(meta.endpoint),
            Method::Head => surf::head(meta.endpoint),
            Method::Options => surf::options(meta.endpoint),
            Method::Patch => surf::patch(meta.endpoint),
            Method::Trace => surf::trace(meta.endpoint),
        };

        c = c.body(payload);
        if let Some(headers) = meta.headers {
            for (name, values) in headers {
                use http_types::headers::HeaderName;
                match HeaderName::from_bytes(name.as_str().as_bytes().to_vec()) {
                    Ok(h) => {
                        for v in &values {
                            c = c.header(h.clone(), v.as_str());
                        }
                    }
                    Err(e) => error!("Bad header name: {}", e),
                }
            }
        }

        let mut response = c.await?;
        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            if let Ok(body) = response.body_string().await {
                error!("HTTP request failed: {} => {}", status, body)
            } else {
                error!("HTTP request failed: {}", status)
            }
        }

        Ok((duration_to_millis(start.elapsed()), response))
    }

    async fn make_event(id: Ids, mut response: Response) -> Result<Event> {
        let data = response.body_string().await?;
        let headers = response
            .header_names()
            .map(|name| {
                (
                    name.to_string(),
                    // a header name has the potential to take multiple values:
                    // https://tools.ietf.org/html/rfc7230#section-3.2.2
                    // tide does not seem to guarantee the order of values though --
                    // look into it later
                    response
                        .header(name)
                        .iter()
                        .map(|value| value.as_str().to_string())
                        .collect::<Value>(),
                )
            })
            .collect::<Value>();

        let mut meta = Value::object_with_capacity(2);
        meta.insert("response_status", response.status() as u64)?;
        meta.insert("response_headers", headers)?;

        // TODO apply proper codec based on response mime and body bytes
        // also allow pass-through without decoding
        //
        //let body = response.body_bytes().await?;
        //
        //let data = LineValue::new(vec![body], |_| Value::null().into());
        //let response = LineValue::try_new(vec![data], |data| {
        //    Value::from(std::str::from_utf8(data[0].as_slice())?).into()
        //})?;
        //
        // as json
        //let data = LineValue::try_new(vec![body], |data| {
        //    simd_json::to_borrowed_value(&mut data[0])
        //        .map(|v| ValueAndMeta::from_parts(v, event_meta))
        //});
        //
        // as string
        //let data = LineValue::try_new(vec![body], |data| {
        //    std::str::from_utf8(data[0].as_slice())
        //        .map(|v| ValueAndMeta::from_parts(Value::from(v), meta))
        //})
        //.map_err(|e| e.0)?;

        Ok(Event {
            id, // TODO only eid should be preserved for this?
            //data,
            data: (data, meta).into(),
            ingest_ns: nanotime(),
            // TODO implement origin_uri
            origin_uri: None,
            ..Event::default()
        })
    }

    fn get_request_meta(&mut self, meta: &Value) -> RestRequestMeta {
        self.client_idx = (self.client_idx + 1) % self.config.endpoints.len();

        // TODO consolidate logic here
        // also add scheme, host, path, query
        RestRequestMeta {
            endpoint: meta
                .get("endpoint")
                .and_then(Value::as_str)
                .unwrap_or(&self.config.endpoints[self.client_idx])
                .into(),
            method: meta
                .get("request_method")
                // TODO simplify and bubble up error if the conversion here does not work
                .and_then(Value::as_str)
                .map(|v| Method::from_str(&v.to_uppercase()).unwrap_or(dflt_method()))
                .unwrap_or(self.config.method),
            headers: meta
                .get("request_headers")
                .and_then(Value::as_object)
                .map(|headers| {
                    let mut converted_headers: HashMap<String, Vec<String>> = HashMap::new();
                    for (name, values) in headers {
                        if let Some(values) = values.as_array() {
                            converted_headers.insert(
                                name.to_string(),
                                values
                                    .iter()
                                    // TODO bubble up error if the conversion here does not work
                                    .map(|value| value.as_str().unwrap_or("").to_string())
                                    .collect(),
                            );
                        }
                    }
                    converted_headers
                }),
        }
    }

    fn enqueue_send_future(
        &mut self,
        id: Ids,
        op_meta: OpMeta,
        payload: Vec<u8>,
        request_meta: RestRequestMeta,
    ) -> Result<()> {
        let (tx, rx) = bounded(1);
        let reply_tx = self.tx.clone();
        let has_link = self.has_link;

        task::spawn::<_, Result<()>>(async move {
            let r = Self::request(payload, request_meta).await;
            let mut m = Value::object_with_capacity(1);

            let (cb, d) = match r {
                Ok((t, response)) => {
                    m.insert("time", t)?;

                    if has_link {
                        let response_event = Self::make_event(id.clone(), response).await?;
                        if let Err(e) = reply_tx.send(SinkReply::Response(response_event)).await {
                            error!("Failed to send response reply: {}", e)
                        };
                    }

                    (CBAction::Ack, Ok(t))
                }
                Err(e) => {
                    error!("REST offramp error: {:?}", e);
                    (CBAction::Fail, Err(e))
                }
            };

            if let Err(e) = reply_tx
                .send(SinkReply::Insight(Event {
                    id,
                    op_meta,
                    data: (Value::null(), m).into(),
                    cb,
                    ingest_ns: nanotime(),
                    ..Event::default()
                }))
                .await
            {
                error!("Failed to send reply: {}", e)
            };

            if let Err(e) = tx.send(d).await {
                error!("Failed to send reply: {}", e)
            }
            Ok(())
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }
    async fn maybe_enque(
        &mut self,
        id: Ids,
        op_meta: OpMeta,
        payload: Vec<u8>,
        request_meta: RestRequestMeta,
    ) -> Result<()> {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                if let Err(e) = self
                    .tx
                    .send(SinkReply::Insight(Event {
                        id,
                        op_meta,
                        cb: CBAction::Fail,
                        ingest_ns: nanotime(),
                        ..Event::default()
                    }))
                    .await
                {
                    error!("Failed to send reply: {}", e)
                };

                error!("Dropped data due to overload");
                Err("Dropped data due to overload".into())
            }
            _ => {
                if self
                    .enqueue_send_future(id, op_meta, payload, request_meta)
                    .is_err()
                {
                    // TODO: handle reply to the pipeline
                    error!("Failed to enqueue send request");
                    Err("Failed to enqueue send request".into())
                } else {
                    Ok(())
                }
            }
        }
    }
    async fn drain_insights(&mut self) -> ResultVec {
        let mut v = Vec::with_capacity(self.rx.len() + 1);
        while let Ok(e) = self.rx.try_recv() {
            v.push(e)
        }
        Ok(Some(v))
    }
}

#[async_trait::async_trait]
impl Sink for Rest {
    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, codec: &dyn Codec, event: Event) -> ResultVec {
        if self.has_link && event.is_batch {
            return Err("Batched events are not supported for linked rest offramps".into());
        }

        let mut payload = Vec::with_capacity(4096);
        let mut payload_meta = &Value::null();

        // TODO this behaviour should be made configurable
        // payload_meta does not make sense here currently for a batched payload
        for (value, meta) in event.value_meta_iter() {
            let mut raw = codec.encode(value)?;
            payload.append(&mut raw);
            payload.push(b'\n');

            if !event.is_batch {
                payload_meta = meta;
            }
        }
        let request_meta = self.get_request_meta(payload_meta);

        self.maybe_enque(
            // TODO avoid these clones
            event.id.clone(),
            event.op_meta.clone(),
            payload,
            request_meta,
        )
        .await?;
        self.drain_insights().await
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn init(&mut self, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }

    #[allow(clippy::used_underscore_binding)]
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        self.drain_insights().await
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
}
