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

use crate::offramp::prelude::make_postprocessors;
use crate::offramp::prelude::*;
use crate::postprocessor::Postprocessors;
use async_channel::bounded;
use elastic::prelude::*;
use halfbrown::HashMap;
use simd_json::borrowed::Object;
use simd_json::json;
use std::str;
use std::time::Instant;
use tremor_pipeline::OpMeta;
use tremor_script::prelude::*;

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
    client: SyncClient,
    url: String,
}

pub struct Elastic {
    client_idx: usize,
    clients: Vec<Destination>,
    // config: Config,
    queue: AsyncSink<u64>,
    // hostname: String,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    postprocessors: Postprocessors,
}

impl offramp::Impl for Elastic {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let clients: Result<Vec<Destination>> = config
                .endpoints
                .iter()
                .map(|s| {
                    Ok(Destination {
                        client: SyncClientBuilder::new().static_node(s.clone()).build()?,
                        url: s.clone(),
                    })
                })
                .collect();
            let clients = clients?;

            let queue = AsyncSink::new(config.concurrency);
            //let hostname = match hostname::get() {
            //    Some(h) => h,
            //    None => "tremor-host.local".to_string(),
            //};

            Ok(Box::new(Self {
                client_idx: 0,
                pipelines: HashMap::new(),
                postprocessors: vec![],
                // config,
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
    fn flush(client: &SyncClient, payload: Vec<u8>) -> Result<u64> {
        let start = Instant::now();
        let res = client.request(BulkRequest::new(payload)).send()?;
        for item in res.into_response::<BulkErrorsResponse>()? {
            // TODO update error metric here?
            error!("Elastic Search item error: {:?}", item);
        }
        let d = start.elapsed();
        let d = duration_to_millis(d);
        Ok(d)
    }

    fn enqueue_send_future(&mut self, op_meta: OpMeta, payload: Vec<u8>) -> Result<()> {
        self.client_idx = (self.client_idx + 1) % self.clients.len();
        let destination = self.clients[self.client_idx].clone();
        let (tx, rx) = bounded(1);
        let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = self
            .pipelines
            .iter()
            .map(|(i, p)| (i.clone(), p.clone()))
            .collect();
        task::spawn(async move {
            let r = Self::flush(&destination.client, payload);
            let mut m = Value::object_with_capacity(2);

            if let Ok(t) = r {
                // FIXME println!("Elastic search ok: {:?}", t);
                if m.insert("time", t).is_err() {
                    // ALLOW: this is OK
                    unreachable!()
                };
            } else {
                // TODO update error metric here?
                error!("Elastic search error: {:?}", r);
                // FIXME println!("Elastic search error: {:?}", r);
                if m.insert("error", "Failed to send to ES").is_err() {
                    // ALLOW: this is OK
                    unreachable!()
                };
            };
            let insight = Event {
                data: (Value::null(), m).into(),
                ingest_ns: nanotime(),
                op_meta,
                ..Event::default()
            };

            for (pid, p) in &mut pipelines {
                if p.send_insight(insight.clone()).await.is_err() {
                    error!("Failed to send contraflow to pipeline {}", pid)
                };
            }

            // TODO: Handle contraflow for notification
            if let Err(e) = tx.send(r).await {
                error!("Failed to send reply: {}", e)
            }
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }

    fn maybe_enque(&mut self, op_meta: OpMeta, payload: Vec<u8>) -> Result<()> {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                let mut m = Object::new();
                m.insert("error".into(), "Dropped data due to es overload".into());

                let insight = Event {
                    data: (Value::null(), m).into(),
                    ingest_ns: nanotime(),
                    ..Event::default()
                };

                let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = self
                    .pipelines
                    .iter()
                    .map(|(i, p)| (i.clone(), p.clone()))
                    .collect();
                task::block_on(async {
                    for (pid, p) in &mut pipelines {
                        if p.send_insight(insight.clone()).await.is_err() {
                            error!("Failed to send contraflow to pipeline {}", pid)
                        };
                    }
                });

                error!("Dropped data due to es overload");
                Err("Dropped data due to es overload".into())
            }
            _ => {
                if self.enqueue_send_future(op_meta, payload).is_err() {
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
#[async_trait::async_trait]
impl Offramp for Elastic {
    // We enforce json here!
    async fn on_event(&mut self, _codec: &dyn Codec, _input: &str, event: Event) -> Result<()> {
        // We estimate a single message is 512 byte on everage, might be off but it's
        // a guess
        let mut payload = Vec::with_capacity(4096);
        let mut output = None;
        let op_meta = event.op_meta.clone();

        for (value, meta) in event.value_meta_iter() {
            if output.is_none() {
                output = meta.get("backpressure-output").map(Value::clone_static);
            }
            let index = meta
                .get("index")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::from("'index' not set for elastic offramp!"))?;
            let doc_type = meta
                .get("doc_type")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::from("'doc-type' not set for elastic offramp!"))?;
            match meta.get("pipeline").and_then(Value::as_str) {
                None => json!({
                "index":
                {
                    "_index": index,
                    "_type": doc_type
                }})
                .write(&mut payload)?,

                Some(pipeline) => json!({
                "index":
                {
                    "_index": index,
                    "_type": doc_type,
                    "pipeline": pipeline
                }})
                .write(&mut payload)?,
            };
            payload.push(b'\n');
            value.write(&mut payload)?;
            payload.push(b'\n');
        }
        self.maybe_enque(op_meta, payload)
    }

    async fn on_signal(&mut self, _event: Event) -> Option<Event> {
        None
    }

    fn default_codec(&self) -> &str {
        "json"
    }
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }
    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }
    async fn start(&mut self, _codec: &dyn Codec, postprocessors: &[String]) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }
}
