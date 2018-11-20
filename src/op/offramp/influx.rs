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

//! # InfluxDB Offramp
//!
//! The InfluxDB Offramp uses batch writes to send data to InfluxDB search
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//! ## Outputs
//!
//! The 1st additional output is used to send divert messages that can not be
//! enqueued due to overload

use async_sink::{AsyncSink, SinkDequeueError};
use dflt;
use error::TSError;
use errors::*;
use metrics;
use pipeline::prelude::*;
use prometheus::{exponential_buckets, HistogramVec}; // w/ instance
use reqwest;
use serde_yaml;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::f64;
use std::fmt;
use std::sync::mpsc::{channel, Receiver};
use std::time::Instant;
use threadpool::ThreadPool;
use utils::duration_to_millis;

lazy_static! {
    // Histogram of the duration it takes between getting a message and
    // sending (or dropping) it.
    static ref SEND_HISTOGRAM: HistogramVec = {
        let opts = histogram_opts!(
            "latency",
            "Latency for influx offramp.",
            exponential_buckets(0.0005, 1.1, 20).unwrap()
        ).namespace("tremor").subsystem("influx").const_labels(hashmap!{"instance".into() => unsafe{ metrics::INSTANCE.to_string() }});
        register_histogram_vec!(opts, &["dest"]).unwrap()
    };
}

#[derive(Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// number of events in each batch
    pub batch_size: usize,
    /// database to write to
    pub database: String,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    pub concurrency: usize,
}

#[derive(Clone)]
struct Destination {
    client: reqwest::Client,
    url: String,
}

impl fmt::Debug for Destination {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.url)
    }
}

// Influx offramp connector
#[derive(Debug)]
pub struct Offramp {
    client_idx: usize,
    clients: Vec<Destination>,
    config: Config,
    payload: VecDeque<EventData>,
    pool: ThreadPool,
    queue: AsyncSink<Option<f64>>,
}

impl Offramp {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let clients = config
            .endpoints
            .iter()
            .map(|client| Destination {
                client: reqwest::Client::new(),
                url: format!("{}/write?db={}", client, config.database),
            }).collect();

        let pool = ThreadPool::new(config.concurrency);
        let queue = AsyncSink::new(config.concurrency);
        Ok(Offramp {
            client_idx: 0,
            payload: VecDeque::with_capacity(config.batch_size),
            clients,
            config,
            pool,
            queue,
        })
    }

    fn render_payload(&mut self) -> (String, HashMap<ReturnDest, Vec<u64>>) {
        let mut payload = String::from("");
        let mut returns = HashMap::new();
        while let Some(event) = self.payload.pop_front() {
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
            //let (ret, raw) = event.to_return_and_value(Ok(None));
            match res {
                Ok((raw, ret)) => {
                    let mut ids = ret.ids;
                    payload.push_str(raw.as_str());
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
                Ok(r) => for (dst, ids) in returns.iter() {
                    let ret = Return {
                        source: dst.source.to_owned(),
                        chain: dst.chain.to_owned(),
                        ids: ids.to_owned(),
                        v: Ok(r),
                    };
                    ret.send();
                },
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
}

fn flush(client: &reqwest::Client, url: &str, payload: &str) -> EventReturn {
    let start = Instant::now();
    let timer = SEND_HISTOGRAM.with_label_values(&[url]).start_timer();
    client.post(url).body(payload.to_owned()).send()?;
    timer.observe_duration();
    let d = start.elapsed();
    let d = duration_to_millis(d) as f64;
    Ok(Some(d))
}

impl Opable for Offramp {
    opable_types!(ValueType::Raw, ValueType::Raw);

    fn exec(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "offramp::influx", ValueType::Raw);
        //EventResult::Next(event)
        // We only add the message if it is not already dropped and
        // we are not in backoff time.

        self.payload.push_back(event);

        if self.config.batch_size > self.payload.len() {
            EventResult::Done
        } else {
            match self.queue.dequeue() {
                Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                    if let Some(e_out) = self.payload.pop_front() {
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
