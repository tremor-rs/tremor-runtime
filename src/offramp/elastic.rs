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

//! # Elastic Search Offramp
//!
//! The Elastic Search Offramp uses batch writes to send data to elastic search
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Input Variables
//!   * `index` - index to write to (required)
//!   * `doc-type` - document type for the event (required)
//!   * `pipeline` - pipeline to use
//!
//! ## Outputs
//!
//! The 1st additional output is used to send divert messages that can not be
//! enqueued due to overload

use super::{Offramp, OfframpImpl};
use crate::async_sink::{AsyncSink, SinkDequeueError};
use crate::codec::Codec;
use crate::dflt;
use crate::errors::*;
use crate::system::{PipelineAddr, PipelineMsg};
use crate::url::TremorURL;
use crate::utils::{duration_to_millis, nanotime, ConfigImpl};
use crate::{Event, OpConfig};
use elastic::client::prelude::BulkErrorsResponse;
use elastic::client::requests::BulkRequest;
use elastic::client::{Client, SyncSender};
use elastic::prelude::SyncClientBuilder;
use halfbrown::HashMap;
// use hostname::get_hostname;
use crate::offramp::prelude::make_postprocessors;
use crate::postprocessor::Postprocessors;
use simd_json::{json, OwnedValue};
use std::convert::From;
use std::str;
use std::sync::mpsc::channel;
use std::time::Instant;
use threadpool::ThreadPool;
use tremor_pipeline::MetaMap;
use tremor_script::{LineValue, Value};

#[derive(Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    pub concurrency: usize,
}

impl ConfigImpl for Config {}

#[derive(Clone)]
struct Destination {
    client: Client<SyncSender>,
    url: String,
}

pub struct Elastic {
    client_idx: usize,
    clients: Vec<Destination>,
    // config: Config,
    pool: ThreadPool,
    queue: AsyncSink<u64>,
    // hostname: String,
    pipelines: HashMap<TremorURL, PipelineAddr>,
    postprocessors: Postprocessors,
}

impl OfframpImpl for Elastic {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let clients: Result<Vec<Destination>> = config
                .endpoints
                .iter()
                .map(|s| {
                    Ok(Destination {
                        client: SyncClientBuilder::new().base_url(s.clone()).build()?,
                        url: s.clone(),
                    })
                })
                .collect();
            let clients = clients?;

            let pool = ThreadPool::new(config.concurrency);
            let queue = AsyncSink::new(config.concurrency);
            //let hostname = match get_hostname() {
            //    Some(h) => h,
            //    None => "tremor-host.local".to_string(),
            //};

            Ok(Box::new(Elastic {
                client_idx: 0,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                // config,
                pool,
                clients,
                queue,
                // hostname,
            }))
        } else {
            Err("Elastic offramp requires a configuration.".into())
        }
    }
}

impl Elastic {
    fn flush(client: &Client<SyncSender>, payload: &str) -> Result<u64> {
        let start = Instant::now();
        let req = BulkRequest::new(payload.to_owned());
        let res = client.request(req).send()?;
        for item in res.into_response::<BulkErrorsResponse>()? {
            error!("Elastic Search item error: {:?}", item);
        }
        let d = start.elapsed();
        let d = duration_to_millis(d);
        Ok(d)
    }

    fn enqueue_send_future(
        &mut self,
        pipelines: Vec<(TremorURL, PipelineAddr)>,
        payload: String,
        mut meta: MetaMap,
    ) -> Result<()> {
        self.client_idx = (self.client_idx + 1) % self.clients.len();
        let destination = self.clients[self.client_idx].clone();
        let (tx, rx) = channel();
        self.pool.execute(move || {
            let r = Self::flush(&destination.client, payload.as_str());
            if let Ok(t) = r {
                meta.insert("time".into(), json!(t));
            } else {
                error!("Elastic search error: {:?}", r);
                meta.insert("error".into(), json!("Failed to send to ES"));
            };
            let insight = Event {
                is_batch: false,
                id: 0,
                meta,
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
    fn maybe_enque(&mut self, payload: String, mut meta: MetaMap) -> Result<()> {
        let pipelines: Vec<(TremorURL, PipelineAddr)> = self
            .pipelines
            .iter()
            .map(|(i, p)| (i.clone(), p.clone()))
            .collect();
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                meta.insert("error".into(), json!("Offramp overloaded"));
                let insight = Event {
                    is_batch: false,
                    id: 0,
                    meta,
                    value: LineValue::new(Box::new(vec![]), |_| Value::Null),
                    ingest_ns: nanotime(),
                    kind: None,
                };

                for (pid, p) in pipelines {
                    if p.addr.send(PipelineMsg::Insight(insight.clone())).is_err() {
                        error!("Failed to send contraflow to pipeline {}", pid)
                    };
                }
                error!("Dropped data due to es overload");
                Err("Dropped data due to es overload".into())
            }
            _ => {
                if self.enqueue_send_future(pipelines, payload, meta).is_err() {
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

impl Offramp for Elastic {
    // We enforce json here!
    fn on_event(&mut self, _codec: &Box<dyn Codec>, _input: String, event: Event) {
        let mut payload = String::from("");
        let mut meta = MetaMap::new();
        for event in event.into_iter() {
            let index = if let Some(OwnedValue::String(index)) = event.meta.get("index") {
                index
            } else {
                error!("'index' not set for elastic offramp!");
                return;
            };
            let doc_type = if let Some(OwnedValue::String(doc_type)) = event.meta.get("doc_type") {
                doc_type
            } else {
                error!("'doc_type' not set for elastic offramp!");
                return;
            };
            let pipeline = if let Some(OwnedValue::String(pipeline)) = event.meta.get("pipeline") {
                Some(pipeline)
            } else {
                None
            };
            match pipeline {
                None => {
                    if let Ok(s) = serde_json::to_string(&json!({
                    "index":
                    {
                        "_index": index,
                        "_type": doc_type
                    }})) {
                        payload.push_str(s.as_str())
                    }
                }
                Some(ref pipeline) => payload.push_str(
                    json!({
                    "index":
                    {
                        "_index": index,
                        "_type": doc_type,
                        "pipeline": pipeline
                    }})
                    .to_string()
                    .as_str(),
                ),
            };
            meta = event.meta;
            payload.push('\n');
            match serde_json::to_string(&event.value) {
                Ok(s) => {
                    payload.push_str(s.as_str());
                    payload.push('\n');
                }
                Err(e) => error!("Failed to encode json {}", e),
            }
        }
        let _ = self.maybe_enque(payload, meta);
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: PipelineAddr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    fn start(&mut self, _codec: &Box<dyn Codec>, postprocessors: &[String]) {
        self.postprocessors = make_postprocessors(postprocessors)
            .expect("failed to setup post processors for stdout");
    }
}
