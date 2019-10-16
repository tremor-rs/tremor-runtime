// Copyright 2018-2019, Wayfair GmbH
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
use crate::rest::HttpC;
use halfbrown::HashMap;
use serde_yaml;
use std::convert::From;
use std::str;
use std::sync::mpsc::channel;
use std::time::Instant;
use threadpool::ThreadPool;
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

pub struct Rest {
    client_idx: usize,
    clients: Vec<HttpC>,
    config: Config,
    pool: ThreadPool,
    queue: AsyncSink<u64>,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
}

impl offramp::Impl for Rest {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            let clients = config
                .endpoints
                .iter()
                .map(|endpoint| HttpC::new(endpoint.clone()))
                .collect();

            let pool = ThreadPool::new(config.concurrency);
            let queue = AsyncSink::new(config.concurrency);
            Ok(Box::new(Rest {
                client_idx: 0,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                config,
                pool,
                clients,
                queue,
            }))
        } else {
            Err("Rest offramp requires a configuration.".into())
        }
    }
}

impl Rest {
    fn flush(client: &HttpC, config: Config, payload: &str) -> Result<u64> {
        let start = Instant::now();
        let c = if config.put {
            client.put("".to_string())?
        } else {
            client.post("".to_string())?
        };
        let c = config
            .headers
            .into_iter()
            .fold(c, |c, (k, v)| c.header(k.as_str(), v.as_str()));
        c.body(payload.to_owned()).send()?;
        let d = duration_to_millis(start.elapsed());
        Ok(d)
    }

    fn enqueue_send_future(&mut self, payload: String) -> Result<()> {
        self.client_idx = (self.client_idx + 1) % self.clients.len();
        let destination = self.clients[self.client_idx].clone();
        let (tx, rx) = channel();
        let pipelines: Vec<(TremorURL, PipelineAddr)> = self
            .pipelines
            .iter()
            .map(|(i, p)| (i.clone(), p.clone()))
            .collect();
        let config = self.config.clone();
        self.pool.execute(move || {
            let r = Self::flush(&destination, config, payload.as_str());
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

                data: (Value::Null, m).into(),
                ingest_ns: nanotime(),
                kind: None,
            };

            for (pid, p) in pipelines {
                if p.addr.send(PipelineMsg::Insight(insight.clone())).is_err() {
                    error!("Failed to send contraflow to pipeline {}", pid)
                };
            }

            // TODO: Handle contraflow for notification
            let _ = tx.send(r);
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }
    fn maybe_enque(&mut self, payload: String) -> Result<()> {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                //TODO: how do we handle this?
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
    fn on_event(&mut self, codec: &Box<dyn Codec>, _input: String, event: Event) {
        let mut payload = String::from("");
        for value in event.value_iter() {
            match codec.encode(value) {
                Ok(raw) => {
                    if let Ok(s) = str::from_utf8(&raw) {
                        payload.push_str(s);
                        payload.push('\n');
                    } else {
                        error!("Contant is not valid utf8")
                    }
                }
                Err(e) => error!("Event data needs to be raw: {}", e),
            }
        }
        let _ = self.maybe_enque(payload);
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
}
