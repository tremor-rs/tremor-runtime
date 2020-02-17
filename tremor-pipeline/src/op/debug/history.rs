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

use crate::op::prelude::*;
use crate::ConfigImpl;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub op: String,
    /// Name of the fiuled to store data in
    pub name: String,
}

impl ConfigImpl for Config {}

op!(EventHistoryFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;
        Ok(Box::new(EventHistory {
            config,
            id: node.id.clone(),
        }))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())

    }});

#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct EventHistory {
    pub config: Config,
    pub id: Cow<'static, str>,
}

impl Operator for EventHistory {
    fn on_event(&mut self, _port: &str, _state: &mut StateObject, event: Event) -> Result<Vec<(Cow<'static, str>, Event)>> {
        let id = event.id;
        let (_, meta) = event.data.parts();
        match meta
            .get_mut(self.config.name.as_str())
            .and_then(Value::as_array_mut)
        {
            Some(ref mut history) => {
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
        };
        Ok(vec![("out".into(), event)])
    }

    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(&mut self, signal: &mut Event) -> Result<Vec<(Cow<'static, str>, Event)>> {
        let id = signal.id;
        let (_, meta) = signal.data.parts();

        match meta
            .get_mut(self.config.name.as_str())
            .and_then(Value::as_array_mut)
        {
            Some(ref mut history) => {
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
        };
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
            id: "badger".into(),
        };
        let event = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from("badger").into(),
            kind: None,
        };

        let mut state = Value::null();

        let (out, mut event) = op
            .on_event("in", &mut state, event)
            .expect("Failed to run pipeline")
            .pop()
            .expect("Empty results");
        assert_eq!("out", out);
        let _ = op.on_signal(&mut event);

        let history = event.data.suffix().meta.get(op.config.name.as_str());

        match history.and_then(Value::as_array) {
            Some(history) => {
                assert_eq!(2, history.len());
            }
            _ => unreachable!(),
        }
    }
}
