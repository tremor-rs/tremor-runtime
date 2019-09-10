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
use halfbrown::hashmap;
use simd_json::borrowed::Value;
use simd_json::value::ValueTrait;
use tremor_script::highlighter::DumbHighlighter;
use tremor_script::{self, Return, Script};
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TremorContext {
    pub ingest_ns: u64,
}
impl tremor_script::Context for TremorContext {
    fn ingest_ns(&self) -> u64 {
        self.ingest_ns
    }
}

op!(TremorFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = serde_yaml::from_value(map.clone())?;

        match tremor_script::Script::parse(&config.script, &*FN_REGISTRY.lock()?) {
            Ok(runtime) =>
                Ok(Box::new(Tremor {
                    runtime,
                    config,
                    id: node.id.clone(),
                })),
            Err(e) => {
                let mut h = DumbHighlighter::new();
                if let Err(e) = tremor_script::Script::<()>::format_error_from_script(&config.script, &mut h, &e) {
                    error!("{}", e.to_string());
                } else {
                    error!("{}", h.to_string());
                };
                Err(e.into())
            }
        }
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
        );
        match value {
            Ok(Return::EmitEvent { port }) => {
                let event_meta: simd_json::owned::Value = event_meta.into();
                if let simd_json::owned::Value::Object(map) = event_meta {
                    event.meta = map;
                    Ok(vec![(port.unwrap_or_else(|| "out".to_string()), event)])
                } else {
                    unreachable!();
                }
            }

            Ok(Return::Emit { value, port }) => {
                let event_meta: simd_json::owned::Value = event_meta.into();
                if let simd_json::owned::Value::Object(map) = event_meta {
                    event.meta = map;
                    *unwind_event = value;
                    Ok(vec![(port.unwrap_or_else(|| "out".to_string()), event)])
                } else {
                    unreachable!();
                }
            }
            Ok(Return::Drop) => Ok(vec![]),
            Err(e) => {
                /*
                *unwind_event = Value::Object(hashmap! {
                    "error".into() => Value::String(self.runtime.format_error(e).into()),
                    "event".into() => *unwind_event
                });
                 */
                let event_meta: simd_json::owned::Value = event_meta.into();
                if let simd_json::owned::Value::Object(map) = event_meta {
                    event.meta = map;
                    let mut o = Value::Object(hashmap! {
                        "error".into() => Value::String(self.runtime.format_error(e).into()),
                    });
                    std::mem::swap(&mut o, unwind_event);
                    if let Some(error) = unwind_event.as_object_mut() {
                        error.insert("event".into(), o);
                    };
                    //*unwind_event = data;
                    Ok(vec![("error".to_string(), event)])
                } else {
                    unreachable!();
                }
            }
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
            &*FN_REGISTRY.lock().expect("could not claim lock"),
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

    #[test]
    pub fn test_how_it_handles_errors() {
        let config = Config {
            script: r#"match this is invalid code so no match case"#.to_string(),
        };
        let runtime = Script::parse(
            &config.script,
            &*FN_REGISTRY.lock().expect("could not claim lock"),
        );

        dbg!(&runtime);
    }
}
