// Copyright 2018, Wayfair GmbH
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

use crate::async_sink::{AsyncSink, SinkDequeueError};
use crate::dflt;
use crate::errors::*;
use crate::metrics;
use crate::pipeline::pool::WorkerPoolStep;
use crate::pipeline::prelude::*;
use crate::utils::{duration_to_millis, nanos_to_millis, nanotime};
use elastic::client::prelude::BulkErrorsResponse;
use elastic::client::requests::BulkRequest;
use elastic::client::{Client, SyncSender};
use elastic::prelude::SyncClientBuilder;
use hostname::get_hostname;
use prometheus::{exponential_buckets, HistogramVec}; // w/ instance
use serde_yaml;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::From;
use std::sync::mpsc::channel;
use std::time::Instant;
use std::{f64, fmt, str};
use threadpool::ThreadPool;

lazy_static! {
    // Histogram of the duration it takes between getting a message and
    // sending (or dropping) it.
    static ref SEND_HISTOGRAM: HistogramVec = {
        let opts = histogram_opts!(
            "latency",
            "Latency for Elastic Search offramp.",
            exponential_buckets(0.0005, 1.1, 20).unwrap()
        ).namespace("tremor").subsystem("es").const_labels(hashmap!{"instance".into() => instance!()});
        register_histogram_vec!(opts, &["dest"]).unwrap()
    };

}

#[derive(Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// number of events in each batch
    pub batch_size: usize,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    pub concurrency: usize,
    /// Timeout before a batch is always send
    #[serde(default = "dflt::d_0")]
    pub timeout: u64,
}

#[derive(Clone)]
struct Destination {
    client: Client<SyncSender>,
    url: String,
}

impl fmt::Debug for Destination {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.url)
    }
}

#[derive(Debug)]
pub struct Offramp {
    client_idx: usize,
    clients: Vec<Destination>,
    config: Config,
    payload: VecDeque<EventData>,
    pool: ThreadPool,
    queue: AsyncSink<Option<f64>>,
    hostname: String,
}

impl Offramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let clients: Vec<Destination> = config
            .endpoints
            .iter()
            .map(|s| Destination {
                client: SyncClientBuilder::new()
                    .base_url(s.clone())
                    .build()
                    .unwrap(),
                url: s.clone(),
            })
            .collect();

        let pool = ThreadPool::new(config.concurrency);
        let queue = AsyncSink::new(config.concurrency);
        let hostname = match get_hostname() {
            Some(h) => h,
            None => "tremor-host.local".to_string(),
        };

        Ok(Offramp {
            client_idx: 0,
            payload: VecDeque::with_capacity(config.batch_size),
            config,
            pool,
            clients,
            queue,
            hostname,
        })
    }
    fn render_payload(&mut self) -> (String, HashMap<ReturnDest, Vec<u64>>) {
        let mut payload = String::from("");
        let mut returns = HashMap::new();
        while let Some(event) = self.payload.pop_front() {
            let index = if let Some(serde_json::Value::String(index)) = event.var_clone(&"index") {
                index
            } else {
                unreachable!();
            };
            let doc_type =
                if let Some(serde_json::Value::String(doc_type)) = event.var_clone(&"doc-type") {
                    doc_type
                } else {
                    unreachable!();
                };
            let pipeline =
                if let Some(serde_json::Value::String(pipeline)) = event.var_clone(&"pipeline") {
                    Some(pipeline)
                } else {
                    None
                };
            match pipeline {
                None => payload.push_str(
                    json!({
                    "index":
                    {
                        "_index": index,
                        "_type": doc_type
                    }})
                    .to_string()
                    .as_str(),
                ),
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
            payload.push('\n');
            let hostname = &self.hostname;
            let ingest_ns = event.ingest_ns;
            let class = event.var_clone(&"classification");
            let res = event.maybe_extract(|val| {
                if let EventValue::Raw(raw) = val {
                    let raw = String::from_utf8(raw.to_vec())?;
                    Ok((raw, Ok(None)))
                } else {
                    unreachable!()
                }
            });
            match res {
                Ok((raw, ret)) => {
                    let mut ids = ret.ids;
                    payload.push_str(
                        self.update_send_time(raw, hostname, ingest_ns, class)
                            .as_str(),
                    );
                    payload.push('\n');
                    match returns.entry(ReturnDest {
                        source: ret.source,
                        chain: ret.chain,
                    }) {
                        Vacant(entry) => {
                            entry.insert(ids);
                        }
                        Occupied(entry) => {
                            entry.into_mut().append(&mut ids);
                        }
                    }
                }
                Err(e) => error!("{:?}", e),
            };

            //self.payload.push_str(update_send_time(event).as_str());
        }
        (payload, returns)
    }

    fn update_send_time(
        &self,
        raw: String,
        hostname: &str,
        ingest_ns: u64,
        class: Option<MetaValue>,
    ) -> String {
        let start_time_ms = nanos_to_millis(nanotime());
        let mut map_vec = vec![
            (
                String::from("egress_time"),
                serde_json::Value::Number(serde_json::Number::from(start_time_ms)),
            ),
            (
                String::from("ingest_time"),
                serde_json::Value::Number(serde_json::Number::from(nanos_to_millis(ingest_ns))),
            ),
            (
                String::from("hostname"),
                serde_json::Value::String(hostname.to_string()),
            ),
        ];
        if let Some(serde_json::Value::String(class)) = class {
            map_vec.push((
                String::from("classification"),
                serde_json::Value::String(class),
            ));
        }
        let tremor_map: serde_json::Map<String, serde_json::Value> =
            map_vec.iter().cloned().collect();
        let tmap = serde_json::to_string(&serde_json::Value::Object(tremor_map));

        add_json_kv(raw, "tremor", tmap.unwrap().as_str())
    }
}

impl WorkerPoolStep for Offramp {
    fn dequeue(&mut self) -> std::result::Result<Result<Option<f64>>, SinkDequeueError> {
        self.queue.dequeue()
    }
    fn has_capacity(&self) -> bool {
        self.queue.has_capacity()
    }
    fn pop_payload(&mut self) -> Option<EventData> {
        self.payload.pop_front()
    }
    fn push_payload(&mut self, event: EventData) {
        self.payload.push_back(event)
    }
    fn batch_size(&self) -> usize {
        self.config.batch_size
    }
    fn payload_len(&self) -> usize {
        self.payload.len()
    }
    fn empty(&mut self) {
        self.queue.empty()
    }
    fn timeout(&self) -> u64 {
        self.config.timeout
    }
    fn enqueue_send_future(&mut self) -> Result<()> {
        self.client_idx = (self.client_idx + 1) % self.clients.len();
        let (payload, returns) = self.render_payload();
        let destination = self.clients[self.client_idx].clone();
        let (tx, rx) = channel();
        self.pool.execute(move || {
            let dst = destination.url.as_str();
            let r = flush(&destination.client, dst, payload.as_str());
            Self::handle_return(&r, returns);
            let _ = tx.send(r);
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }

    fn event_errors(&self, event: &EventData) -> Option<String> {
        match event.vars.get("index") {
            Some(serde_json::Value::String(_)) => (),
            Some(_) => return Some("Variable `index` not set but required".into()),
            _ => (),
        };
        match event.vars.get("doc-type") {
            Some(serde_json::Value::String(_)) => (),
            Some(_) => return Some("Variable `index` not set but required".into()),
            _ => (),
        };
        None
    }
}

fn flush(client: &Client<SyncSender>, url: &str, payload: &str) -> EventReturn {
    let start = Instant::now();
    let timer = SEND_HISTOGRAM.with_label_values(&[url]).start_timer();
    let req = BulkRequest::new(payload.to_owned());
    let res = client.request(req).send()?;
    for item in res.into_response::<BulkErrorsResponse>()? {
        error!("Elastic Search item error: {:?}", item);
    }
    timer.observe_duration();
    let d = start.elapsed();
    let d = duration_to_millis(d) as f64;
    Ok(Some(d))
}

impl Opable for Offramp {
    opable_types!(ValueType::Raw, ValueType::Raw);

    fn input_vars(&self) -> HashSet<String> {
        let mut h = HashSet::new();
        h.insert("index".to_string());
        h.insert("doc-type".to_string());
        h.insert("pipeline".to_string());
        h
    }

    fn on_event(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "offramp::elastic", ValueType::Raw);
        self.handle_event(event)
    }

    fn on_timeout(&mut self) -> EventResult {
        self.maybe_enque()
    }

    fn shutdown(&mut self) {
        <Offramp as WorkerPoolStep>::shutdown(self)
    }
}
fn add_json_kv(json: String, key: &str, val: &str) -> String {
    let mut s = json;
    s.pop();
    s.push_str(",\"");
    s.push_str(key);
    s.push_str("\":");
    s.push_str(val);
    s.push('}');
    s
}

#[test]
fn test_add_json_kv() {
    let json = "{\"k1\": 1}".to_string();
    let res = add_json_kv(json, "k2", "2");
    assert_eq!(res, "{\"k1\": 1,\"k2\":2}");
}
