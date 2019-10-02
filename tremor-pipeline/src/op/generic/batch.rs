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
use crate::{Event, Operator};
use serde_yaml;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub count: usize,
    /// The amount time between messags to flush
    #[serde(default = "dflt")]
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Batch {
    pub config: Config,
    pub data: LineValue,
    pub len: usize,
    pub max_delay_ns: Option<u64>,
    pub first_ns: u64,
    pub id: String,
    pub event_id: u64,
}

pub fn empty() -> LineValue {
    LineValue::new(vec![], |_| ValueAndMeta {
        value: Value::Array(vec![]),
        meta: Value::Object(Map::default()),
    })
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
            data: empty(),
            len: 0,
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
        // TODO: This is ugly
        let Event {
            id,
            data,
            ingest_ns,
            is_batch,
            ..
        } = event;
        self.data.consume(
            data,
            move |this: &mut ValueAndMeta<'static>, other: ValueAndMeta<'static>| -> Result<()> {
                if let Some(ref mut a) = this.value.as_array_mut() {
                    let mut e = Map::with_capacity(7);
                    // {"id":1,
                    e.insert("id".into(), id.into());
                    //  "data": {
                    //      "value": "snot", "meta":{}
                    //  },
                    let mut data = Map::with_capacity(2);
                    data.insert("value".into(), other.value);
                    data.insert("meta".into(), other.meta);
                    e.insert("data".into(), Value::Object(data));
                    //  "ingest_ns":1,
                    e.insert("ingest_ns".into(), ingest_ns.into());
                    //  "kind":null,
                    // kind is always null on events
                    e.insert("kind".into(), Value::Null);
                    //  "is_batch":false
                    e.insert("is_batch".into(), is_batch.into());
                    // }
                    a.push(Value::Object(e))
                };
                Ok(())
            },
        )?;
        self.len += 1;
        if self.len == 1 {
            self.first_ns = ingest_ns;
        };
        let flush = match self.max_delay_ns {
            Some(t) if ingest_ns - self.first_ns > t => true,
            _ => self.len == self.config.count,
        };
        if flush {
            //TODO: This is ugly
            let mut data = empty();
            std::mem::swap(&mut data, &mut self.data);
            self.len = 0;
            let event = Event {
                id: self.event_id,
                data,
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

                let mut data = empty();
                std::mem::swap(&mut data, &mut self.data);
                self.len = 0;
                let event = Event {
                    id: self.event_id,
                    data,
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
            data: empty(),
            len: 0,
            id: "badger".to_string(),
        };
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
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
            data: Value::from("badger").into(),
            kind: None,
        };

        let mut r = op
            .on_event("in", event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        dbg!(&event);
        assert_eq!("out", out);
        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![&event1.data.suffix().value, &event2.data.suffix().value]
        );

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
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
            data: empty(),
            len: 0,
            id: "badger".to_string(),
        };
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            kind: None,
        };

        println!(
            "{}",
            simd_json::serde::to_owned_value(event1.clone())
                .expect("")
                .to_string()
        );

        let r = op
            .on_event("in", event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event2 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2_000_000,
            data: Value::from("badger").into(),
            kind: None,
        };

        let mut r = op
            .on_event("in", event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty results");
        assert_eq!("out", out);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![&event1.data.suffix().value, &event2.data.suffix().value]
        );

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            kind: None,
        };

        let r = op.on_event("in", event).expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            data: Value::from("snot").into(),
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
            data: empty(),
            len: 0,
            id: "badger".to_string(),
        };
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
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
            data: Value::Null.into(),
            kind: None,
        };

        let mut r = op.on_signal(&mut signal).expect("failed to run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty resultset");
        assert_eq!("out", out);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(events, vec![&event1.data.suffix().value]);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            kind: None,
        };

        let r = op.on_event("in", event).expect("failed to run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 2,
            data: Value::from("snot").into(),
            kind: None,
        };

        let r = op.on_event("in", event).expect("failed to run piepeline");
        assert_eq!(r.len(), 0);
    }
}
