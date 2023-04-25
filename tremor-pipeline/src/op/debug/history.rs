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
// li

#![allow(clippy::doc_markdown)] // we need thise for :::::note

//! :::note
//!     This operator is for debugging purposes only, and should not be used in production deployments.
//! :::
//!
//! This operator generates a history entry in the event metadata underneath the field provided in the `name` config value. Data is pushed to the array as a Striong in the form: `"event: <op>(<event_id>)"`.
//!
//! This can be used as a tracepoint of events in a complex pipeline setup.
//!
//! This operator manipulates a section of the event metadata.
//!
//! **Configuration options**:
//!
//! - `op` - The operation name of this operator
//! - `name` - The field to store the history on
//!
//! **Outputs**:
//!
//! - `out`
//!
//! **Example**:
//!
//! ```tremor
//! define operator history from debug::history
//! with
//!   op = "my-checkpoint",
//!   name = "event_history"
//! end;
//! ```

use crate::op::prelude::*;
use crate::ConfigImpl;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub op: String,
    /// Name of the field to store data in
    pub name: String,
}

impl ConfigImpl for Config {}

op!(EventHistoryFactory(_uid, node) {
if let Some(map) = &node.config {
    let config: Config = Config::new(map)?;
    Ok(Box::new(History {
        config,
    }))
} else {
    Err(ErrorKind::MissingOpConfig(node.id.clone()).into())

}});

#[derive(Debug, Clone)]
struct History {
    config: Config,
}

impl Operator for History {
    fn on_event(
        &mut self,
        _node_id: u64,
        _uid: OperatorUId,
        _port: &Port<'static>,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let Event {
            ref mut data,
            ref id,
            ..
        } = event;
        data.rent_mut(|data| {
            let (_, meta) = data.parts_mut();

            match meta
                .get_mut(self.config.name.as_str())
                .and_then(Value::as_array_mut)
            {
                Some(ref mut history) => {
                    history.push(Value::from(format!("evt: {}({id})", self.config.op)));
                }
                None => {
                    if let Some(ref mut obj) = meta.as_object_mut() {
                        obj.insert(
                            self.config.name.clone().into(),
                            Value::from(vec![Value::from(format!(
                                "evt: {}({id})",
                                self.config.op
                            ))]),
                        );
                    }
                }
            };
        });

        Ok(event.into())
    }

    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(
        &mut self,
        _node_id: u64,
        _uid: OperatorUId,
        _state: &mut Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        let Event {
            ref id,
            ref mut data,
            ..
        } = signal;
        data.rent_mut(|data| {
            let (_, m) = data.parts_mut();

            match m
                .get_mut(self.config.name.as_str())
                .and_then(Value::as_array_mut)
            {
                Some(ref mut history) => {
                    history.push(Value::from(format!("sig: {}({id})", self.config.op)));
                }
                None => {
                    if let Some(ref mut obj) = m.as_object_mut() {
                        obj.insert(
                            self.config.name.clone().into(),
                            Value::from(vec![Value::from(format!(
                                "sig: {}({id})",
                                self.config.op
                            ))]),
                        );
                    }
                }
            };
        });
        Ok(EventAndInsights::default())
    }
}

#[cfg(test)]
mod test {
    use tremor_common::uids::UId;

    use super::*;
    use crate::EventId;

    #[test]
    fn history_op_test() -> Result<()> {
        let mut op = History {
            config: Config {
                op: "green".to_string(),
                name: "snot".to_string(),
            },
        };
        let operator_id = OperatorUId::new(0);
        let event = Event {
            id: EventId::from_id(0, 0, 1),
            ingest_ns: 1,
            data: (Value::from("snot"), Value::object()).into(),
            ..Event::default()
        };

        let mut state = Value::null();

        let (out, event) = op
            .on_event(0, operator_id, &Port::In, &mut state, event)
            .expect("Failed to run pipeline")
            .events
            .pop()
            .expect("Empty results");
        assert_eq!(out, "out");

        let (out, _event) = op
            .on_event(0, operator_id, &Port::In, &mut state, event)
            .expect("Failed to run pipeline")
            .events
            .pop()
            .expect("Empty results");
        assert_eq!(out, "out");

        let mut event = Event {
            id: EventId::from_id(0, 0, 1),
            ingest_ns: 1,
            data: (Value::from("snot"), Value::object()).into(),
            ..Event::default()
        };
        let mut state = Value::null();

        op.on_signal(0, operator_id, &mut state, &mut event)?;
        op.on_signal(0, operator_id, &mut state, &mut event)?;

        let history = event.data.suffix().meta().get(op.config.name.as_str());

        assert_eq!(history.as_array().map(Vec::len), Some(2));
        Ok(())
    }
}
