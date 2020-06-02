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

use crate::offramp::prelude::*;
use crossbeam_channel::bounded;
use halfbrown::HashMap;
use simd_json::borrowed::Object;
use std::str;
use std::time::Instant;
use tremor_script::prelude::*;

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
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
}

impl offramp::Impl for Rest {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            let queue = AsyncSink::new(config.concurrency);
            Ok(Box::new(Self {
                client_idx: 0,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                config,
                queue,
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
        c = c.body_bytes(&payload);
        for (k, v) in config.headers {
            use http_types::headers::HeaderName;
            match HeaderName::from_bytes(k.as_str().as_bytes().to_vec()) {
                Ok(h) => {
                    c = c.set_header(h, v.as_str());
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

    fn enqueue_send_future(&mut self, payload: Vec<u8>) -> Result<()> {
        self.client_idx = (self.client_idx + 1) % self.config.endpoints.len();
        let destination = self.config.endpoints[self.client_idx].clone();
        let (tx, rx) = bounded(1);
        let config = self.config.clone();
        let pipelines: Vec<(TremorURL, pipeline::Addr)> = self
            .pipelines
            .iter()
            .map(|(i, p)| (i.clone(), p.clone()))
            .collect();
        task::spawn(async move {
            let r = Self::flush(&destination, config, payload).await;
            let mut m = Object::new();
            if let Ok(t) = r {
                m.insert("time".into(), t.into());
            } else {
                error!("REST offramp error: {:?}", r);
                m.insert("error".into(), "Failed to send".into());
            };
            let insight = Event {
                is_batch: false,
                id: 0,
                data: (Value::null(), m).into(),
                ingest_ns: nanotime(),
                origin_uri: None,
                kind: None,
            };

            for (pid, p) in pipelines {
                if p.addr
                    .send(pipeline::Msg::Insight(insight.clone()))
                    .is_err()
                {
                    error!("Failed to send contraflow to pipeline {}", pid)
                };
            }

            if let Err(e) = tx.send(r) {
                error!("Failed to send reply: {}", e)
            }
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }
    fn maybe_enque(&mut self, payload: Vec<u8>) -> Result<()> {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                let mut m = Object::new();
                m.insert(
                    "error".into(),
                    "Dropped data due to REST endpoint overload".into(),
                );

                let insight = Event {
                    is_batch: false,
                    id: 0,
                    data: (Value::null(), m).into(),
                    ingest_ns: nanotime(),
                    origin_uri: None,
                    kind: None,
                };

                let pipelines: Vec<(TremorURL, pipeline::Addr)> = self
                    .pipelines
                    .iter()
                    .map(|(i, p)| (i.clone(), p.clone()))
                    .collect();
                for (pid, p) in pipelines {
                    if p.addr
                        .send(pipeline::Msg::Insight(insight.clone()))
                        .is_err()
                    {
                        error!("Failed to send contraflow to pipeline {}", pid)
                    };
                }
                error!("Dropped data due to overload");
                Err("Dropped data due to overload".into())
            }
            _ => {
                if self.enqueue_send_future(payload).is_err() {
                    // TODO: handle reply to the pipeline
                    error!("Failed to enqueue send request");
                    Err("Failed to enqueue send request".into())
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl Offramp for Rest {
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) -> Result<()> {
        let mut payload = Vec::with_capacity(4096);
        for value in event.value_iter() {
            let mut raw = codec.encode(value)?;
            payload.append(&mut raw);
            payload.push(b'\n');
        }
        self.maybe_enque(payload)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
}
