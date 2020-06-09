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

use crate::config::dflt;
use crate::op::prelude::*;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub count: usize,
    /// The amount time between messags to flush
    #[serde(default = "dflt")]
    pub timeout: Option<u64>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
pub struct Batch {
    pub config: Config,
    pub data: LineValue,
    pub len: usize,
    pub max_delay_ns: Option<u64>,
    pub first_ns: u64,
    pub id: Cow<'static, str>,
    pub event_id: u64,
}

pub fn empty() -> LineValue {
    LineValue::new(vec![], |_| ValueAndMeta::from(Value::array()))
}

op!(BatchFactory(node) {
if let Some(map) = &node.config {
    let config: Config = Config::new(map)?;
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
    Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())

}});

impl Operator for Batch {
    fn on_event(
        &mut self,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<Vec<(Cow<'static, str>, Event)>> {
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
                if let Some(ref mut a) = this.value_mut().as_array_mut() {
                    let mut e = Object::with_capacity(7);
                    // {"id":1,
                    e.insert_nocheck("id".into(), id.into());
                    //  "data": {
                    //      "value": "snot", "meta":{}
                    //  },
                    let mut data = Object::with_capacity(2);
                    let (value, meta) = other.into_parts();
                    data.insert_nocheck("value".into(), value);
                    data.insert_nocheck("meta".into(), meta);
                    e.insert_nocheck("data".into(), Value::from(data));
                    //  "ingest_ns":1,
                    e.insert_nocheck("ingest_ns".into(), ingest_ns.into());
                    //  "kind":null,
                    // kind is always null on events
                    e.insert_nocheck("kind".into(), Value::null());
                    //  "is_batch":false
                    e.insert_nocheck("is_batch".into(), is_batch.into());
                    // }
                    a.push(Value::from(e))
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
                is_batch: true,
                ..std::default::Default::default()
            };
            self.event_id += 1;
            Ok(vec![("out".into(), event)])
        } else {
            Ok(vec![])
        }
    }

    fn handles_signal(&self) -> bool {
        true
    }

    fn on_signal(&mut self, signal: &mut Event) -> Result<SignalResponse> {
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
                    is_batch: true,
                    ..std::default::Default::default()
                };
                self.event_id += 1;
                Ok((vec![("out".into(), event)], None))
            } else {
                Ok((vec![], None))
            }
        } else {
            Ok((vec![], None))
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
            id: "badger".into(),
        };
        let event1 = Event {
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        let mut state = Value::null();

        let r = op
            .on_event("in", &mut state, event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event2 = Event {
            id: 1,
            ingest_ns: 1,
            data: Value::from("badger").into(),
            ..std::default::Default::default()
        };

        let mut r = op
            .on_event("in", &mut state, event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![event1.data.suffix().value(), event2.data.suffix().value()]
        );

        let event = Event {
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        let r = op
            .on_event("in", &mut state, event)
            .expect("could not run pipeline");
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
            id: "badger".into(),
        };
        let event1 = Event {
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        println!(
            "{}",
            simd_json::serde::to_owned_value(event1.clone())
                .expect("")
                .encode()
        );

        let mut state = Value::null();

        let r = op
            .on_event("in", &mut state, event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event2 = Event {
            id: 1,
            ingest_ns: 2_000_000,
            data: Value::from("badger").into(),
            ..std::default::Default::default()
        };

        let mut r = op
            .on_event("in", &mut state, event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty results");
        assert_eq!("out", out);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![event1.data.suffix().value(), event2.data.suffix().value()]
        );

        let event = Event {
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        let r = op
            .on_event("in", &mut state, event)
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            id: 1,
            ingest_ns: 2,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        let r = op
            .on_event("in", &mut state, event)
            .expect("could not run pipeline");
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
            id: "badger".into(),
        };
        let event1 = Event {
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        let mut state = Value::null();

        let r = op
            .on_event("in", &mut state, event1.clone())
            .expect("failed to run peipeline");
        assert_eq!(r.len(), 0);

        let mut signal = Event {
            id: 1,
            ingest_ns: 2_000_000,
            data: Value::null().into(),
            ..std::default::Default::default()
        };

        let (mut r, _) = op.on_signal(&mut signal).expect("failed to run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty resultset");
        assert_eq!("out", out);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(events, vec![event1.data.suffix().value()]);

        let event = Event {
            id: 1,
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        let r = op
            .on_event("in", &mut state, event)
            .expect("failed to run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            id: 1,
            ingest_ns: 2,
            data: Value::from("snot").into(),
            ..std::default::Default::default()
        };

        let r = op
            .on_event("in", &mut state, event)
            .expect("failed to run piepeline");
        assert_eq!(r.len(), 0);
    }
}
