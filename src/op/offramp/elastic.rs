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
use crate::error::TSError;
use crate::errors::*;
use crate::metrics;
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
use std::sync::mpsc::{channel, Receiver};
use std::time::Instant;
use std::{f64, fmt, str};
use threadpool::ThreadPool;

//#[cfg(test)]
//use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

lazy_static! {
    // Histogram of the duration it takes between getting a message and
    // sending (or dropping) it.
    static ref SEND_HISTOGRAM: HistogramVec = {
        let opts = histogram_opts!(
            "latency",
            "Latency for Elastic Search offramp.",
            exponential_buckets(0.0005, 1.1, 20).unwrap()
        ).namespace("tremor").subsystem("es").const_labels(hashmap!{"instance".into() => unsafe{ metrics::INSTANCE.to_string() }});
        register_histogram_vec!(opts, &["dest"]).unwrap()
    };

}

//[endpoints, index, batch_size, batch_timeout]
#[derive(Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// number of events in each batch
    pub batch_size: usize,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    pub concurrency: usize,
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
            let index = if let Some(MetaValue::String(index)) = event.var_clone(&"index") {
                index
            } else {
                unreachable!();
            };
            let doc_type = if let Some(MetaValue::String(doc_type)) = event.var_clone(&"doc-type") {
                doc_type
            } else {
                unreachable!();
            };
            let pipeline = if let Some(MetaValue::String(pipeline)) = event.var_clone(&"pipeline") {
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
                    if let Ok(raw) = String::from_utf8(raw.to_vec()) {
                        Ok((raw, Ok(None)))
                    } else {
                        Err(TSError::new(&"Bad UTF8"))
                    }
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

    fn send_future(&mut self) -> Receiver<EventReturn> {
        self.client_idx = (self.client_idx + 1) % self.clients.len();
        let (payload, returns) = self.render_payload();
        let destination = self.clients[self.client_idx].clone();
        let (tx, rx) = channel();
        self.pool.execute(move || {
            let dst = destination.url.as_str();
            let r = flush(&destination.client, dst, payload.as_str());
            match r.clone() {
                Ok(r) => {
                    for (dst, ids) in returns.iter() {
                        let ret = Return {
                            source: dst.source.to_owned(),
                            chain: dst.chain.to_owned(),
                            ids: ids.to_owned(),
                            v: Ok(r),
                        };
                        ret.send();
                    }
                }
                Err(e) => {
                    for (dst, ids) in returns.iter() {
                        let ret = Return {
                            source: dst.source.to_owned(),
                            chain: dst.chain.to_owned(),
                            ids: ids.to_owned(),
                            v: Err(e.clone()),
                        };
                        ret.send();
                    }
                }
            };
            let _ = tx.send(r);
        });
        rx
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
        if let Some(MetaValue::String(class)) = class {
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

fn flush(client: &Client<SyncSender>, url: &str, payload: &str) -> EventReturn {
    let start = Instant::now();
    let timer = SEND_HISTOGRAM.with_label_values(&[url]).start_timer();
    let req = BulkRequest::new(payload.to_owned());
    let res = client.request(req).send();
    if let Err(e) = res {
        error!("Elastic Search request error: {:?}", e);
        return Err(TSError::from(e));
    }
    let response = res.unwrap().into_response::<BulkErrorsResponse>();
    if let Err(e) = response {
        error!("Elastic Search response error: {:?}", e);
        return Err(TSError::from(e));
    }
    for item in response.unwrap() {
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

    fn exec(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "offramp::elastic", ValueType::Raw);

        //EventResult::Next(event)
        // We only add the message if it is not already dropped and
        // we are not in backoff time.
        if !event.var_is_type(&"index", &MetaValueType::String) {
            return EventResult::Error(
                event,
                Some(TSError::new(&"Variable `index` not set but required")),
            );
        };
        if !event.var_is_type(&"doc-type", &MetaValueType::String) {
            return EventResult::Error(
                event,
                Some(TSError::new(&"Variable `doc-type` not set but required")),
            );
        };

        self.payload.push_back(event);
        if self.config.batch_size > self.payload.len() {
            EventResult::Done
        } else {
            match self.queue.dequeue() {
                Err(SinkDequeueError::NotReady) => {
                    if self.queue.has_capacity() {
                        let rx = self.send_future();
                        if self.queue.enqueue(rx).is_ok() {
                            EventResult::Done
                        } else {
                            EventResult::StepError(TSError::new(&"Could not enqueue event"))
                        }
                    } else if let Some(e_out) = self.payload.pop_front() {
                        EventResult::NextID(3, e_out)
                    } else {
                        EventResult::StepError(TSError::new(&"No capacity in queue"))
                    }
                }
                _ => {
                    let rx = self.send_future();
                    if self.queue.enqueue(rx).is_ok() {
                        EventResult::Done
                    } else {
                        EventResult::StepError(TSError::new(&"Could not enqueue event"))
                    }
                }
            }
        }
    }
    fn shutdown(&mut self) {
        self.queue.empty();
        let rx = self.send_future();
        let _ = rx.recv();
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

/*
TODO: We have to decide on how indexes are handled
#[test]
fn index_test() {
    let s = Event::new("{\"key\":\"value\"}", None, nanotime());
    let mut p = ::parser::new("json", "");
    let o = Offramp::new("{\"endpoints\":[\"http://elastic:9200\"], \"suffix\":\"demo\",\"batch_size\":100,\"batch_timeout\":500}");

    let r = p.apply(s).expect("couldn't parse data");
    let idx = o.index(&r);
    assert_eq!(idx, "demo");
}

#[test]
fn index_prefix_test() {
    let mut e = Event::new("{\"key\":\"value\"}", None, nanotime());
    e.index = Some(String::from("value"));
    let o = Offramp::new("{\"endpoints\":[\"http://elastic:9200\"], \"suffix\":\"_demo\",\"batch_size\":100,\"batch_timeout\":500}");

    let idx = o.index(&e);
    assert_eq!(idx, "value_demo");
}

#[test]
fn index_suffix_test() {
    println!("This test could be a false positive if it ran exactly at midnight, but that's OK.");
    let e = Event::new("{\"key\":\"value\"}", None, nanotime());
    let o = Offramp::new("{\"endpoints\":[\"http://elastic:9200\"], \"suffix\":\"demo\",\"batch_size\":100,\"batch_timeout\":500, \"append_date\": true}");

    let idx = o.index(&e);
    let utc: DateTime<Utc> = Utc::now();
    assert_eq!(
        idx,
        format!("demo-{}", utc.format("%Y.%m.%d").to_string().as_str())
    );
}

#[test]
fn index_prefix_suffix_test() {
    println!("This test could be a false positive if it ran exactly at midnight, but that's OK.");
    let mut e = Event::new("{\"key\":\"value\"}", None, nanotime());
    e.index = Some(String::from("value"));
    let o = Offramp::new("{\"endpoints\":[\"http://elastic:9200\"], \"suffix\":\"_demo\",\"batch_size\":100,\"batch_timeout\":500, \"append_date\": true}");

    let idx = o.index(&e);
    let utc: DateTime<Utc> = Utc::now();
    assert_eq!(
        idx,
        format!("value_demo-{}", utc.format("%Y.%m.%d").to_string().as_str())
    );
}

#[test]
fn test_add_json_kv() {
    let json = "{\"k1\": 1}";
    let res = add_json_kv(json, "k2", "2");
    assert_eq!(res, "{\"k1\": 1,\"k2\":2}");
}

 */
