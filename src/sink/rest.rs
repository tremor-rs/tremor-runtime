// Copyright 2018-2020, Wayfair GmbH
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
use tremor_pipeline::{CBAction, OpMeta};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    pub concurrency: usize,
    /// If put should be used instead of post.
    #[serde(default = "dflt::d")]
    pub put: bool,
    #[serde(default = "dflt::d")]
    pub headers: HashMap<String, String>,
}

impl ConfigImpl for Config {}

pub struct Rest {
    client_idx: usize,
    config: Config,
    queue: AsyncSink<u64>,
    postprocessors: Postprocessors,
    tx: Sender<Event>,
    rx: Receiver<Event>,
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
    async fn flush(endpoint: &str, config: Config, payload: Vec<u8>) -> Result<u64> {
        let start = Instant::now();
        let mut c = if config.put {
            surf::put(endpoint)
        } else {
            surf::post(endpoint)
        };
        c = c.body(payload);
        for (k, v) in config.headers {
            use http_types::headers::HeaderName;
            match HeaderName::from_bytes(k.as_str().as_bytes().to_vec()) {
                Ok(h) => {
                    c = c.header(h, v.as_str());
                }
                Err(e) => error!("Bad header name: {}", e),
            }
        }

        let mut reply = c.await?;
        let status = reply.status();
        if status.is_client_error() || status.is_server_error() {
            if let Ok(body) = reply.body_string().await {
                error!("HTTP request failed: {} => {}", status, body)
            } else {
                error!("HTTP request failed: {}", status)
            }
        }

        let d = duration_to_millis(start.elapsed());
        Ok(d)
    }

    fn enqueue_send_future(&mut self, id: Ids, op_meta: OpMeta, payload: Vec<u8>) -> Result<()> {
        self.client_idx = (self.client_idx + 1) % self.config.endpoints.len();
        let destination = self.config.endpoints[self.client_idx].clone();
        let (tx, rx) = bounded(1);
        let config = self.config.clone();
        let insight_tx = self.tx.clone();

        task::spawn::<_, Result<()>>(async move {
            let r = Self::flush(&destination, config, payload).await;
            let mut m = Value::object_with_capacity(1);

            let cb = if let Ok(t) = r {
                m.insert("time", t)?;
                Some(CBAction::Ack)
            } else {
                error!("REST offramp error: {:?}", r);
                Some(CBAction::Fail)
            };

            if let Err(e) = insight_tx
                .send(Event {
                    id,
                    op_meta,
                    data: (Value::null(), m).into(),
                    cb,
                    ingest_ns: nanotime(),
                    ..Event::default()
                })
                .await
            {
                error!("Failed to send reply: {}", e)
            };

            if let Err(e) = tx.send(r).await {
                error!("Failed to send reply: {}", e)
            }
            Ok(())
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }
    async fn maybe_enque(&mut self, id: Ids, op_meta: OpMeta, payload: Vec<u8>) -> Result<()> {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                if let Err(e) = self
                    .tx
                    .send(Event {
                        id,
                        op_meta,
                        cb: Some(CBAction::Fail),
                        ingest_ns: nanotime(),
                        ..Event::default()
                    })
                    .await
                {
                    error!("Failed to send reply: {}", e)
                };

                error!("Dropped data due to overload");
                Err("Dropped data due to overload".into())
            }
            _ => {
                if self.enqueue_send_future(id, op_meta, payload).is_err() {
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
        let mut v = Vec::with_capacity(self.tx.len() + 1);
        while let Ok(e) = self.rx.try_recv() {
            v.push(e)
        }
        Ok(Some(v))
    }
}

#[async_trait::async_trait]
impl Sink for Rest {
    #[allow(unused_variables)]
    async fn on_event(&mut self, input: &str, codec: &dyn Codec, event: Event) -> ResultVec {
        let mut payload = Vec::with_capacity(4096);
        for value in event.value_iter() {
            let mut raw = codec.encode(value)?;
            payload.append(&mut raw);
            payload.push(b'\n');
        }
        self.maybe_enque(event.id, event.op_meta, payload).await?;
        self.drain_insights().await
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn init(&mut self, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
    #[allow(unused_variables)]
    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        self.drain_insights().await
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
}
