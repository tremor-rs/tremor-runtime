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
use crate::FN_REGISTRY;
use crate::{Event, EventValue, Operator, ValueType};
use tremor_script::{self, Script};

#[derive(Debug, Clone)]
pub struct Context {
    ingest_ns: u64,
}
impl tremor_script::Context for Context {}

op!(TremorFactory(node) {
        if let Some(map) = &node.config {
            let config: Config = serde_yaml::from_value(map.clone())?;
            let runtime = Script::parse(&config.script, &FN_REGISTRY.lock().unwrap())?;
            Ok(Box::new(Tremor {
                runtime,
                config,
                id: node.id.clone(),
            }))
        } else {
            Err(ErrorKind::MissingOpConfig(node.id.clone()).into())
    }
});

#[derive(Debug, Clone, Deserialize)]
struct Config {
    script: String,
}

#[derive(Debug)]
pub struct Tremor {
    config: Config,
    runtime: Script<Context>,
    id: String,
}

impl Operator for Tremor {
    fn on_event(&mut self, _port: &str, mut event: Event) -> Result<Vec<(String, Event)>> {
        let context = Context {
            ingest_ns: event.ingest_ns,
        };
        match (&mut event.value, &mut event.meta) {
            (EventValue::JSON(ref mut json), ref mut meta) => {
                self.runtime.run(&context, json, meta)?
            }
            (v, _) => return type_error!(self.id.clone(), v.t(), ValueType::JSON),
        };
        Ok(vec![("out".to_string(), event)])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::EventValue;
    use crate::FN_REGISTRY;
    use hashbrown::HashMap;
    use serde_json::json;
    use tremor_script::Script;

    #[test]
    fn mutate() {
        let config = Config {
            script: r#"a=1 {snot := "badger";}"#.to_string(),
        };
        let runtime = Script::parse(&config.script, &FN_REGISTRY.lock().unwrap()).unwrap();
        let mut op = Tremor {
            config,
            runtime,
            id: "badger".to_string(),
        };
        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: HashMap::new(),
            value: EventValue::JSON(json!({"a": 1})),
            kind: None,
        };

        let (out, event) = op.on_event("in", event).unwrap().pop().unwrap();
        assert_eq!("out", out);

        assert_eq!(event.value.t(), ValueType::JSON);
        if let EventValue::JSON(j) = event.value {
            assert_eq!(j, json!({"snot": "badger", "a": 1}));
        }
    }
}
