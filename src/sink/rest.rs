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
use std::str;
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
    #[serde(default = "dflt_method")]
    pub method: String,
    #[serde(default = "dflt::d")]
    // TODO make header values a vector here?
    pub headers: HashMap<String, String>,
    /// whether to enable linked transport (return offramp response to pipeline)
    // TODO remove and auto-infer this based on succesful binding for linked offramps
    pub link: Option<bool>,
}

fn dflt_method() -> String {
    "POST".to_string()
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
    async fn request(
        //endpoint: &str,
        //config: Config,
        payload: Vec<u8>,
        meta: RestRequestMeta,
    ) -> Result<(u64, Response)> {
        let start = Instant::now();

        // TODO would be nice if surf had a generic function to handle a given method
        // also use enums (or static strings) here instead of matching on strings
        let mut c = match meta.method.as_str() {
            "POST" => surf::post(meta.endpoint),
            "PUT" => surf::put(meta.endpoint),
            "GET" => surf::get(meta.endpoint),
            // TODO handle other methods
            _ => surf::post(meta.endpoint),
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

        // TODO add response_headers
        let mut meta = Value::object_with_capacity(2);
        meta.insert("response_status", response.status() as u64)?;

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
            // TODO
            origin_uri: None,
            ..Event::default()
        })
    }

    fn enqueue_send_future(
        &mut self,
        id: Ids,
        op_meta: OpMeta,
        payload: Vec<u8>,
        request_meta: RestRequestMeta,
    ) -> Result<()> {
        let (tx, rx) = bounded(1);
        //let config = self.config.clone();
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

#[derive(Debug)]
struct RestRequestMeta {
    // TODO support this layout
    //scheme: String,
    //host: String,
    //path: String,
    //query: Option<String>,
    endpoint: String,
    // TODO make this enum
    method: String,
    headers: Option<HashMap<String, Vec<String>>>,
}

impl RestRequestMeta {
    fn new(config: Config, event_meta: Value, client_index: usize) -> Self {
        // TODO consolidate logic here
        RestRequestMeta {
            endpoint: event_meta
                .get("endpoint")
                .and_then(Value::as_str)
                .unwrap_or(&config.endpoints[client_index])
                .into(),
            method: event_meta
                .get("request_method")
                .and_then(Value::as_str)
                .unwrap_or(&config.method)
                .into(),
            // TODO
            //query: None,
            headers: None,
        }
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
        let mut payload_meta = Value::null();

        // TODO this behaviour should be made configurable
        // payload_meta does not make sense here currently for a batched payload
        for (value, meta) in event.value_meta_iter() {
            let mut raw = codec.encode(value)?;
            payload.append(&mut raw);
            payload.push(b'\n');

            if !event.is_batch {
                // TODO avoid this clone
                payload_meta = meta.clone();
            }
        }

        // TODO this should also happen inside RestRequestMeta::new
        self.client_idx = (self.client_idx + 1) % self.config.endpoints.len();

        // TODO avoid clone here
        let request_meta = RestRequestMeta::new(self.config.clone(), payload_meta, self.client_idx);
        //let request_meta = RestRequestMeta {
        //    endpoint: payload_meta
        //        .get("endpoint")
        //        .and_then(Value::as_str)
        //        .unwrap_or(&self.config.endpoints[self.client_idx])
        //        .into(),
        //    method: payload_meta
        //        .get("request_method")
        //        .and_then(Value::as_str)
        //        .unwrap_or(&self.config.method)
        //        .into(),
        //    // TODO
        //    //query: None,
        //    headers: None,
        //};

        self.maybe_enque(event.id, event.op_meta, payload, request_meta)
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
