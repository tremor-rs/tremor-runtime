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

use crate::async_sink::{AsyncSink, SinkDequeueError};
use crate::dflt;
use crate::errors::*;
use crate::metrics;
use crate::pipeline::pool::WorkerPoolStep;
use crate::pipeline::prelude::*;
use crate::utils::duration_to_millis;
use prometheus::{exponential_buckets, HistogramVec}; // w/ instance
use reqwest;
use serde_yaml;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::f64;
use std::fmt;
use std::sync::mpsc::channel;
use std::time::Instant;
use threadpool::ThreadPool;

lazy_static! {
    // Histogram of the duration it takes between getting a message and
    // sending (or dropping) it.
    static ref SEND_HISTOGRAM: HistogramVec = {
        let opts = histogram_opts!(
            "latency",
            "Latency for influx offramp.",
            exponential_buckets(0.0005, 1.1, 20).unwrap()
        ).namespace("tremor").subsystem("influx").const_labels(hashmap!{"instance".into() => instance!()});
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
    /// Timeout before a batch is always send
    #[serde(default = "dflt::d_0")]
    pub timeout: u64,
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
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let clients = config
            .endpoints
            .iter()
            .map(|client| Destination {
                client: reqwest::Client::new(),
                url: format!("{}/write?db={}", client, config.database),
            })
            .collect();

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
                    let raw = String::from_utf8(raw.to_vec())?;
                    Ok((raw, Ok(None)))
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

    fn on_event(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "offramp::influx", ValueType::Raw);
        self.handle_event(event)
    }

    fn on_timeout(&mut self) -> EventResult {
        self.maybe_enque()
    }

    fn shutdown(&mut self) {
        <Offramp as WorkerPoolStep>::shutdown(self)
    }
}
