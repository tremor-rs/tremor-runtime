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
use crate::{Event, Operator};

use simd_json::borrowed::Value;
use tremor_script::{self, Return, Script};

#[derive(Debug, Clone)]
pub struct TremorContext {
    pub ingest_ns: u64,
}
impl tremor_script::Context for TremorContext {}

op!(TremorFactory(node) {
        if let Some(map) = &node.config {
            let config: Config = serde_yaml::from_value(map.clone())?;

        let runtime = Script::parse(&config.script, FN_REGISTRY.lock()?.clone())?;
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
    runtime: Script<TremorContext>,
    id: String,
}

impl Operator for Tremor {
    fn on_event(&mut self, _port: &str, mut event: Event) -> Result<Vec<(String, Event)>> {
        let context = TremorContext {
            ingest_ns: event.ingest_ns,
        };
        let stack = tremor_script::ValueStack::default();
        #[allow(clippy::transmute_ptr_to_ptr)]
        #[allow(mutable_transmutes)]
        let mut unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(event.value.suffix()) };
        let mut event_meta: simd_json::borrowed::Value =
            simd_json::owned::Value::Object(event.meta).into();
        // unwind_event => the event
        // event_meta => mneta
        let value = self.runtime.run(
            &context,
            &mut unwind_event, // event
            &mut event_meta,   // $
            &stack,
        );
        match value {
            Ok(Return::Emit(data)) => {
                let event_meta: simd_json::owned::Value = event_meta.into();
                if let simd_json::owned::Value::Object(map) = event_meta {
                    event.meta = map;
                    *unwind_event = data;
                    Ok(vec![("out".to_string(), event)])
                } else {
                    unreachable!();
                }
            }
            Ok(Return::Drop(data)) => {
                let event_meta: simd_json::owned::Value = event_meta.into();
                if let simd_json::owned::Value::Object(map) = event_meta {
                    event.meta = map;
                    *unwind_event = data;
                    Ok(vec![("drop".to_string(), event)])
                } else {
                    unreachable!();
                }
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{MetaMap, FN_REGISTRY};
    use simd_json::{json, OwnedValue};

    #[test]
    fn mutate() {
        let config = Config {
            script: r#"match event.a of case 1 => let event.snot = "badger" end; event;"#
                .to_string(),
        };
        let runtime = Script::parse(
            &config.script,
            FN_REGISTRY.lock().expect("could not claim lock").clone(),
        )
        .expect("failed to parse script");
        let mut op = Tremor {
            config,
            runtime,
            id: "badger".to_string(),
        };
        let event = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: MetaMap::new(),
            value: sjv!(json!({"a": 1}).into()),
            kind: None,
        };

        let (out, event) = op
            .on_event("in", event)
            .expect("failed to run pipeline")
            .pop()
            .expect("no event returned");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(j, json!({"snot": "badger", "a": 1}))
    }
}
