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

use crate::errors::*;
use crate::{Event, Operator};
use serde_yaml;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub op: String,
    /// Name of the fiuled to store data in
    pub name: String,
}

op!(EventHistoryFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = serde_yaml::from_value(map.clone())?;
        Ok(Box::new(EventHistory {
            config,
            id: node.id.clone(),
        }))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone()).into())

    }});

#[derive(Debug, Clone)]
pub struct EventHistory {
    pub config: Config,
    pub id: String,
}

impl Operator for EventHistory {
    fn on_event(&mut self, _port: &str, mut event: Event) -> Result<Vec<(String, Event)>> {
        let id = event.id;
        event.data.rent_mut(|data| {
            let meta = &mut data.meta;
            match meta.get_mut(&self.config.name) {
                Some(Value::Array(ref mut history)) => {
                    history.push(Value::from(format!("evt: {}({})", self.config.op, id)));
                }
                None => {
                    if let Some(ref mut obj) = meta.as_object_mut() {
                        obj.insert(
                            self.config.name.clone().into(),
                            Value::Array(vec![Value::from(format!(
                                "evt: {}({})",
                                self.config.op, id
                            ))]),
                        );
                    }
                }
                _ => (),
            };
        });
        Ok(vec![("out".to_string(), event)])
    }

    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(&mut self, signal: &mut Event) -> Result<Vec<(String, Event)>> {
        let id = signal.id;
        signal.data.rent_mut(|data| {
            let meta = &mut data.meta;
            match meta.get_mut(&self.config.name) {
                Some(Value::Array(ref mut history)) => {
                    history.push(Value::from(format!("sig: {}({})", self.config.op, id)));
                }
                None => {
                    if let Some(ref mut obj) = meta.as_object_mut() {
                        obj.insert(
                            self.config.name.clone().into(),
                            Value::Array(vec![Value::from(format!(
                                "sig: {}({})",
                                self.config.op, id
                            ))]),
                        );
                    }
                }
                _ => (),
            };
        });
        Ok(vec![])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn history_op_test() {
        let mut op = EventHistory {
            config: Config {
                op: "green".to_string(),
                name: "snot".to_string(),
            },
            id: "badger".to_string(),
        };
        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("badger").into(),
            kind: None,
        };

        let (out, mut event) = op
            .on_event("in", event)
            .expect("failed to run pipeline")
            .pop()
            .expect("empty results");
        assert_eq!("out", out);
        let _ = op.on_signal(&mut event);

        let history = event.data.suffix().meta.get(&op.config.name);

        match history.and_then(Value::as_array) {
            Some(history) => {
                assert_eq!(2, history.len());
            }
            _ => unreachable!(),
        }
    }
}
