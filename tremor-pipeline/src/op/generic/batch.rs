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

use crate::config::dflt;
use crate::errors::*;
use crate::{Event, MetaMap, Operator};
use serde_yaml;
use simd_json::OwnedValue;
use tremor_script::{LineValue, Value};
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub count: usize,
    /// The amount time between messags to flush
    #[serde(default = "dflt")]
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone)]
struct Batch {
    pub config: Config,
    data: Vec<OwnedValue>,
    max_delay_ns: Option<u64>,
    first_ns: u64,
    id: String,
    event_id: u64,
}

op!(BatchFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = serde_yaml::from_value(map.clone())?;
        let max_delay_ns = if let Some(max_delay_ms) = config.timeout {
            Some(max_delay_ms * 1_000_000)
        } else {
            None
        };
        Ok(Box::new(Batch {
            event_id: 0,
            data: Vec::with_capacity(config.count),
            config,
            max_delay_ns,
            first_ns: 0,
            id: node.id.clone(),
        }))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone()).into())

    }});

impl Operator for Batch {
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        let ingest_ns = event.ingest_ns;
        // TODO: This is ugly
        self.data.push(simd_json::serde::to_owned_value(event)?);
        let l = self.data.len();
        if l == 1 {
            self.first_ns = ingest_ns;
        };
        let flush = match self.max_delay_ns {
            Some(t) if ingest_ns - self.first_ns > t => true,
            _ => l == self.config.count,
        };
        if flush {
            //TODO: This is ugly
            let out: Vec<Value> = self.data.drain(0..).map(std::convert::Into::into).collect();
            let value = LineValue::new(Box::new(vec![]), |_| Value::Array(out));
            let event = Event {
                id: self.event_id,
                meta: MetaMap::new(),
                value,
                ingest_ns: self.first_ns,
                kind: None,
                is_batch: true,
            };
            self.event_id += 1;
            Ok(vec![("out".to_string(), event)])
        } else {
            Ok(vec![])
        }
    }

    fn handles_signal(&self) -> bool {
        true
    }

    fn on_signal(&mut self, signal: &mut Event) -> Result<Vec<(String, Event)>> {
        if let Some(delay_ns) = self.max_delay_ns {
            if signal.ingest_ns - self.first_ns > delay_ns {
                // We don't want to modify the original signal we clone it to
                // create a new event.
                let out: Vec<Value> = self.data.drain(0..).map(std::convert::Into::into).collect();
                let value = LineValue::new(Box::new(vec![]), |_| Value::Array(out));
                let event = Event {
                    id: self.event_id,
                    meta: MetaMap::new(),
                    value,
                    ingest_ns: self.first_ns,
                    kind: None,
                    is_batch: true,
                };
                self.event_id += 1;
                Ok(vec![("out".to_string(), event)])
            } else {
                Ok(vec![])
            }
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::MetaMap;
    use tremor_script::Value;

    #[test]
    fn size() {
        let mut op = Batch {
            config: Config {
                count: 2,
                timeout: None,
            },
            event_id: 0,
            first_ns: 0,
            max_delay_ns: None,
            data: Vec::with_capacity(2),
            id: "badger".to_string(),
        };
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op
            .on_event("in", event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event2 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(Value::from("badger")),
            kind: None,
        };

        let mut r = op
            .on_event("in", event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        let events: Vec<Event> = event.into_iter().collect();
        assert_eq!(events, vec![event1, event2]);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op.on_event("in", event).expect("could not run pipeline");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn time() {
        let mut op = Batch {
            config: Config {
                count: 100,
                timeout: Some(1),
            },
            event_id: 0,
            first_ns: 0,
            max_delay_ns: Some(1_000_000),
            data: Vec::with_capacity(2),
            id: "badger".to_string(),
        };
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op
            .on_event("in", event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event2 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2_000_000,
            meta: MetaMap::new(),
            value: sjv!(Value::from("badger")),
            kind: None,
        };

        let mut r = op
            .on_event("in", event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty results");
        assert_eq!("out", out);

        let events: Vec<Event> = event.into_iter().collect();
        assert_eq!(events, vec![event1, event2]);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op.on_event("in", event).expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op.on_event("in", event).expect("could not run pipeline");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn signal() {
        let mut op = Batch {
            config: Config {
                count: 100,
                timeout: Some(1),
            },
            event_id: 0,
            first_ns: 0,
            max_delay_ns: Some(1_000_000),
            data: Vec::with_capacity(2),
            id: "badger".to_string(),
        };
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op
            .on_event("in", event1.clone())
            .expect("failed to run peipeline");
        assert_eq!(r.len(), 0);

        let mut signal = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2_000_000,
            meta: MetaMap::new(),
            value: OwnedValue::Null.into(),
            kind: None,
        };

        let mut r = op.on_signal(&mut signal).expect("failed to run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty resultset");
        assert_eq!("out", out);

        let events: Vec<Event> = event.into_iter().collect();
        assert_eq!(events, vec![event1]);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op.on_event("in", event).expect("failed to run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            meta: MetaMap::new(),
            value: sjv!(Value::from("snot")),
            kind: None,
        };

        let r = op.on_event("in", event).expect("failed to run piepeline");
        assert_eq!(r.len(), 0);
    }
}
