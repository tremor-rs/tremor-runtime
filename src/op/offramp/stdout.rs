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

//! # StdOut Offramp
//!
//! The `stdout` offramp writes events to the standard output (conse).
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Input Variables
//!
//! * `prefix` - sets the prefix (overrides the configuration) for raw and is ignored for JSON

use crate::async_sink::{AsyncSink, SinkDequeueError};
use crate::dflt;
use crate::errors::*;
use crate::pipeline::pool::WorkerPoolStep;
use crate::pipeline::prelude::*;
use crate::utils::duration_to_millis;
use serde_yaml;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::str;
use std::sync::mpsc::channel;
use std::time::Instant;
use threadpool::ThreadPool;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    /// string a prepend to a message (default: '')
    #[serde(default = "dflt::d_empty")]
    pub prefix: String,
    /// number of events in each batch
    #[serde(default = "dflt::d_1")]
    pub batch_size: usize,
    /// Timeout before a batch is always send
    #[serde(default = "dflt::d_0")]
    pub timeout: u64,
    /// Maximum number of parallel in flight batches ( default: 1)
    #[serde(default = "dflt::d_1")]
    pub concurrency: usize,
}

/// An offramp that writes to stdout
#[derive(Debug)]
pub struct Offramp {
    config: Config,
    payload: VecDeque<EventData>,
    pool: ThreadPool,
    queue: AsyncSink<Option<f64>>,
}

impl Offramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        if let ConfValue::Null = opts {
            let config = Config {
                prefix: "".into(),
                batch_size: 1,
                timeout: 0,
                concurrency: 1,
            };
            let queue = AsyncSink::new(config.concurrency);
            let pool = ThreadPool::new(config.concurrency);
            let batch_size = config.batch_size;
            Ok(Offramp {
                config,
                payload: VecDeque::with_capacity(batch_size),
                queue,
                pool,
            })
        } else {
            let config: Config = serde_yaml::from_value(opts.clone())?;
            let queue = AsyncSink::new(config.concurrency);
            let pool = ThreadPool::new(config.concurrency);
            let batch_size = config.batch_size;
            Ok(Offramp {
                config,
                payload: VecDeque::with_capacity(batch_size),
                queue,
                pool,
            })
        }
    }

    fn render_payload(&mut self) -> (String, HashMap<ReturnDest, Vec<u64>>) {
        let mut payload = String::from("");
        let mut returns = HashMap::new();
        while let Some(event) = self.payload.pop_front() {
            let res = event.maybe_extract(|val| {
                if let EventValue::Raw(raw) = val {
                    let raw = String::from_utf8(raw.to_vec())?;
                    Ok((raw, Ok(None)))
                } else if let EventValue::JSON(json) = val {
                    Ok((json.to_string(), Ok(None)))
                } else {
                    unreachable!()
                }
            });
            match res {
                Ok((x, ret)) => {
                    let mut ids = ret.ids;
                    payload.push_str(x.as_str());
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
        let (tx, rx) = channel();
        let (payload, returns) = self.render_payload();
        let data = payload.clone();
        let prefix = self.config.prefix.clone();
        self.pool.execute(move || {
            let r = flush(&prefix.clone(), &data);
            Self::handle_return(&r, returns);
            let _ = tx.send(r);
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }

    fn event_errors(&self, _event: &EventData) -> Option<String> {
        None
    }
}

fn flush(prefix: &str, payload: &str) -> EventReturn {
    let start = Instant::now();
    println!("{}{}", prefix, payload);
    let d = start.elapsed();
    let d = duration_to_millis(d) as f64;
    Ok(Some(d))
}

impl Opable for Offramp {
    fn on_event(&mut self, event: EventData) -> EventResult {
        let _pfx = if let Some(serde_json::Value::String(ref pfx)) = event.var(&"prefix") {
            pfx.clone()
        } else {
            self.config.prefix.clone().to_string()
        };

        self.handle_event(event)
    }

    fn on_timeout(&mut self) -> EventResult {
        self.maybe_enque()
    }

    fn shutdown(&mut self) {
        <Offramp as WorkerPoolStep>::shutdown(self)
    }

    opable_types!(ValueType::Any, ValueType::Any);
}
