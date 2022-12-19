// Copyright 2020-2021, The Tremor Team
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

//! The batch operator is used to batch multiple events and send them in a bulk fashion. It also allows to set a timeout of how long the operator should wait for a batch to be filled.
//!
//! This operator batches both the event payload and event metadata into a single bulk event. Downstream pipeline nodes or offramps will receive 1 such bulk event but will treat its context as multiple events and might act different e.g. when it comes to building a request payload in the offramp context or other use cases. Empty bulk events are usually considered as `no` event.
//!
//! Supported configuration options are:
//!
//! - `count` - Elements per batch
//! - `timeout` - Maximum delay between the first element of a batch and the last element of a batch, in nanoseconds.
//!
//! **Outputs**:
//!
//! - `out`
//!
//! **Example**:
//!
//! The following operator will batch up to `300` events into one, if they arrive within 1 second. Otherwise, after 1 second, it will emit the amount of events it batched up within the past second.
//!
//! ```tremor
//! use std::nanos;
//!
//! define operator batch from generic::batch
//! with
//!   count = 300,
//!   timeout = nanos::from_seconds(1)
//! end;
//! ```

use crate::{op::prelude::*, EventId, EventIdGenerator};
use std::mem::swap;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub count: usize,
    /// The amount of time between messages to flush in nanoseconds
    #[serde(default = "Default::default")]
    pub timeout: Option<u64>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
struct Batch {
    config: Config,
    data: EventPayload,
    len: usize,
    max_delay_ns: Option<u64>,
    first_ns: u64,
    /// event id for the resulting batched event
    /// the resulting id will be a new distinct id and will be tracking
    /// all event ids (min and max) in the batched event
    batch_event_id: EventId,
    is_transactional: bool,
    event_id_gen: EventIdGenerator,
}

fn empty_payload() -> EventPayload {
    EventPayload::new(vec![], |_| ValueAndMeta::from(Value::array()))
}

op!(BatchFactory(uid, node) {
if let Some(map) = &node.config {
    let config: Config = Config::new(map)?;
    let max_delay_ns = config.timeout;
    let mut idgen = EventIdGenerator::for_operator(uid);
    Ok(Box::new(Batch {
        data: empty_payload(),
        len: 0,
        config,
        max_delay_ns,
        first_ns: 0,
        batch_event_id: idgen.next_id(),
        is_transactional: false,
        event_id_gen: idgen,
    }))
} else {
    Err(ErrorKind::MissingOpConfig(node.id.clone()).into())

}});

impl Operator for Batch {
    /// emit a new event once the batch is flushed
    /// with a new event id tracking all events within that batch
    fn on_event(
        &mut self,
        _uid: OperatorId,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        // TODO: This is ugly
        let Event {
            id,
            data,
            ingest_ns,
            is_batch,
            transactional,
            ..
        } = event;
        self.batch_event_id.track(&id);
        self.is_transactional = self.is_transactional || transactional;
        self.data.consume(
            data,
            move |this: &mut ValueAndMeta, other: ValueAndMeta| -> Result<()> {
                if let Some(ref mut a) = this.value_mut().as_array_mut() {
                    let (value, meta) = other.into_parts();
                    let e = literal!({
                        "data": {
                            "value": value,
                            "meta": meta,
                            "ingest_ns": ingest_ns,
                            "kind": Value::null(),
                            "is_batch": is_batch
                        }
                    });
                    a.push(e);
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
            let mut data = empty_payload();
            swap(&mut data, &mut self.data);
            self.len = 0;

            let mut event = Event {
                id: self.event_id_gen.next_id(),
                data,
                ingest_ns: self.first_ns,
                is_batch: true,
                transactional: self.is_transactional,
                ..Event::default()
            };
            self.is_transactional = false;
            swap(&mut self.batch_event_id, &mut event.id);
            Ok(event.into())
        } else {
            Ok(EventAndInsights::default())
        }
    }

    fn handles_signal(&self) -> bool {
        true
    }

    fn on_signal(
        &mut self,
        _uid: OperatorId,
        _state: &mut Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        Ok(self
            .max_delay_ns
            .map_or_else(EventAndInsights::default, |delay_ns| {
                if (signal.ingest_ns - self.first_ns) > delay_ns {
                    // We don't want to modify the original signal we clone it to
                    // create a new event.

                    if self.len > 0 {
                        let mut data = empty_payload();
                        swap(&mut data, &mut self.data);

                        self.len = 0; // reset len
                        let mut event = Event {
                            id: self.event_id_gen.next_id(),
                            data,
                            ingest_ns: self.first_ns,
                            is_batch: true,
                            transactional: self.is_transactional,
                            ..Event::default()
                        };
                        self.is_transactional = false;
                        swap(&mut self.batch_event_id, &mut event.id);
                        EventAndInsights::from(event)
                    } else {
                        EventAndInsights::default()
                    }
                } else {
                    EventAndInsights::default()
                }
            }))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json_derive::Serialize;
    use tremor_common::ids::Id;
    use tremor_value::Value;

    #[test]
    fn size() {
        let operator_id = OperatorId::new(0);
        let mut idgen = EventIdGenerator::for_operator(operator_id);
        let mut op = Batch {
            config: Config {
                count: 2,
                timeout: None,
            },
            first_ns: 0,
            max_delay_ns: None,
            data: empty_payload(),
            len: 0,
            batch_event_id: idgen.next_id(),
            is_transactional: false,
            event_id_gen: idgen,
        };
        let event_1 = Event {
            id: EventId::from_id(0, 0, 1),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let mut state = Value::null();

        let r = op
            .on_event(operator_id, "in", &mut state, event_1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event_2 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("badger").into(),
            ..Event::default()
        };

        let mut r = op
            .on_event(operator_id, "in", &mut state, event_2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.events.pop().expect("no results");
        assert_eq!("out", out);
        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![event_1.data.suffix().value(), event_2.data.suffix().value()]
        );

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(operator_id, "in", &mut state, event)
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn time() -> Result<()> {
        let operator_id = OperatorId::new(42);
        let node_config = NodeConfig::from_config(
            &"badger",
            Some(literal!({
                "count": 100,
                "timeout": 1,
            })),
        );
        let mut op = BatchFactory::new().node_to_operator(operator_id, &node_config)?;

        let event_1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        println!("{}", event_1.json_string().expect(""));

        let mut state = Value::null();

        let r = op
            .on_event(operator_id, "in", &mut state, event_1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event_2 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2_000_000,
            data: Value::from("badger").into(),
            transactional: true,
            ..Event::default()
        };

        let mut r = op
            .on_event(operator_id, "in", &mut state, event_2.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty results");
        assert_eq!("out", out);
        assert!(event.transactional);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![event_1.data.suffix().value(), event_2.data.suffix().value()]
        );

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(operator_id, "in", &mut state, event)
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(operator_id, "in", &mut state, event)
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);
        Ok(())
    }

    #[test]
    fn signal() {
        let operator_id = OperatorId::new(0);
        let mut idgen = EventIdGenerator::for_operator(operator_id);
        let mut op = Batch {
            config: Config {
                count: 100,
                timeout: Some(1),
            },
            first_ns: 0,
            max_delay_ns: Some(1_000_000),
            data: empty_payload(),
            len: 0,
            batch_event_id: idgen.next_id(),
            is_transactional: false,
            event_id_gen: idgen,
        };
        let event_1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            transactional: true,
            ..Event::default()
        };

        let mut state = Value::null();

        let r = op
            .on_event(operator_id, "in", &mut state, event_1.clone())
            .expect("failed to run peipeline");
        assert_eq!(r.len(), 0);

        let mut signal = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2_000_000,
            data: Value::null().into(),
            ..Event::default()
        };

        let mut r = op
            .on_signal(operator_id, &mut state, &mut signal)
            .expect("failed to run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty resultset");
        assert_eq!("out", out);
        assert!(event.transactional);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(events, vec![event_1.data.suffix().value()]);

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(operator_id, "in", &mut state, event)
            .expect("failed to run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(operator_id, "in", &mut state, event)
            .expect("failed to run piepeline");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn forbid_empty_batches() -> Result<()> {
        let operator_id = OperatorId::new(0);
        let mut idgen = EventIdGenerator::for_operator(operator_id);
        let mut op = Batch {
            config: Config {
                count: 2,
                timeout: Some(1),
            },
            first_ns: 0,
            max_delay_ns: Some(100_000),
            data: empty_payload(),
            len: 0,
            batch_event_id: idgen.next_id(),
            is_transactional: false,
            event_id_gen: idgen,
        };

        let mut state = Value::null();
        let mut signal = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            data: Value::null().into(),
            ..Event::default()
        };

        let r = op.on_signal(operator_id, &mut state, &mut signal)?.events;
        assert_eq!(r.len(), 0);

        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2_000_000,
            data: Value::from("snot").into(),
            ..Event::default()
        };
        let r = op.on_event(operator_id, "in", &mut state, event1)?;
        assert_eq!(r.len(), 0);

        signal.ingest_ns = 3_000_000;
        signal.id = (1, 1, 2).into();
        let r = op.on_signal(operator_id, &mut state, &mut signal)?.events;
        assert_eq!(r.len(), 1);

        signal.ingest_ns = 4_000_000;
        signal.id = (1, 1, 3).into();
        let r = op.on_signal(operator_id, &mut state, &mut signal)?.events;
        assert_eq!(r.len(), 0);

        Ok(())
    }
}
