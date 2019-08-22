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

use super::{Offramp, OfframpImpl};
use crate::async_sink::{AsyncSink, SinkDequeueError};
use crate::codec::Codec;
use crate::dflt;
use crate::errors::*;
use crate::offramp::prelude::make_postprocessors;
use crate::postprocessor::Postprocessors;
use crate::rest::HttpC;
use crate::system::{PipelineAddr, PipelineMsg};
use crate::url::TremorURL;
use crate::utils::{duration_to_millis, nanotime};
use crate::{Event, OpConfig};
use halfbrown::HashMap;
use serde_yaml;
use simd_json::json;
use std::convert::From;
use std::str;
use std::sync::mpsc::channel;
use std::time::Instant;
use threadpool::ThreadPool;
use tremor_pipeline::MetaMap;
use tremor_script::{LineValue, Value};

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

impl OfframpImpl for Rest {
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
            let mut m = MetaMap::new();
            if let Ok(t) = r {
                m.insert("time".into(), json!(t));
            } else {
                error!("Elastic search error: {:?}", r);
                m.insert("error".into(), json!("Failed to send to ES"));
            };
            let insight = Event {
                is_batch: false,
                id: 0,
                meta: m,
                value: LineValue::new(Box::new(vec![]), |_| Value::Null),
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
                error!("Dropped data due to es overload");
                Err("Dropped data due to es overload".into())
            }
            _ => {
                if self.enqueue_send_future(payload).is_err() {
                    // TODO: handle reply to the pipeline
                    error!("Failed to enqueue send request to elastic");
                    Err("Failed to enqueue send request to elastic".into())
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
        for event in event.into_iter() {
            match codec.encode(event.value) {
                Ok(raw) => {
                    if let Ok(s) = str::from_utf8(&raw) {
                        payload.push_str(s);
                        payload.push('\n');
                    } else {
                        error!("Contant is not valid utf8")
                    }
                }
                _ => error!("Event data needs to be raw"),
            }
        }
        let _ = self.maybe_enque(payload);
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) {
        self.postprocessors = make_postprocessors(postprocessors)
            .expect("failed to setup post processors for stdout");
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
}
