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
use simd_json::OwnedValue;
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
struct EventHistory {
    pub config: Config,
    id: String,
}

impl Operator for EventHistory {
    fn on_event(&mut self, _port: &str, mut event: Event) -> Result<Vec<(String, Event)>> {
        match event.meta.get_mut(&self.config.name) {
            Some(OwnedValue::Array(ref mut history)) => {
                history.push(OwnedValue::from(format!(
                    "evt: {}({})",
                    self.config.op, event.id
                )));
            }
            None => {
                event.meta.insert(
                    self.config.name.clone(),
                    OwnedValue::Array(vec![OwnedValue::from(format!(
                        "evt: {}({})",
                        self.config.op, event.id
                    ))]),
                );
            }
            _ => (),
        };
        Ok(vec![("out".to_string(), event)])
    }

    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(&mut self, signal: &mut Event) -> Result<Vec<(String, Event)>> {
        match signal.meta.get_mut(&self.config.name) {
            Some(OwnedValue::Array(ref mut history)) => {
                history.push(OwnedValue::from(format!(
                    "sig: {}({})",
                    self.config.op, signal.id
                )));
            }
            None => {
                signal.meta.insert(
                    self.config.name.clone(),
                    OwnedValue::Array(vec![OwnedValue::from(format!(
                        "sig: {}({})",
                        self.config.op, signal.id
                    ))]),
                );
            }
            _ => (),
        };
        Ok(vec![])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::MetaMap;
    use simd_json::OwnedValue;
    use tremor_script::Value;

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
            meta: MetaMap::new(),
            value: sjv!(Value::from("badger")),
            kind: None,
        };

        let (out, mut event) = op
            .on_event("in", event)
            .expect("failed to run pipeline")
            .pop()
            .expect("empty results");
        assert_eq!("out", out);
        let _ = op.on_signal(&mut event);

        let history = event.meta.get(&op.config.name);

        match history {
            Some(OwnedValue::Array(ref history)) => {
                assert_eq!(2, history.len());
            }
            _ => assert!(false),
        }
    }
}
