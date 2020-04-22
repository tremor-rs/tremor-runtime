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
use crate::FN_REGISTRY;
use simd_json::borrowed::Value;
use tremor_script::highlighter::Dumb as DumbHighlighter;
use tremor_script::path::load_module_path;
use tremor_script::prelude::*;
use tremor_script::{self, AggrType, EventContext, Return, Script};

op!(TremorFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;

        match tremor_script::Script::parse(
               &load_module_path(),
               config.script.clone(), &*FN_REGISTRY.lock()?) {
            Ok(runtime) =>
                Ok(Box::new(Tremor {
                    runtime,
                    config,
                    id: node.id.clone().to_string(),
                })),
            Err(e) => {
                let mut h = DumbHighlighter::new();
                if let Err(e) = tremor_script::Script::format_error_from_script(&config.script, &mut h, &e) {
                    error!("{}", e.to_string());
                } else {
                    error!("{}", h.to_string());
                };
                Err(e.into())
            }
        }
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone().to_string()).into())
    }
});

#[derive(Debug, Clone, Deserialize)]
struct Config {
    script: String,
}
impl ConfigImpl for Config {}

#[derive(Debug)]
pub struct Tremor {
    config: Config,
    runtime: Script,
    id: String,
}

impl Operator for Tremor {
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn on_event(
        &mut self,
        _port: &str,
        state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<Vec<(Cow<'static, str>, Event)>> {
        let context = EventContext::new(event.ingest_ns, event.origin_uri);
        let data = event.data.suffix();
        let mut unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(&data.value) };
        let mut event_meta: &mut Value<'_> = unsafe { std::mem::transmute(&data.meta) };
        // unwind_event => the event
        // event_meta => meta
        let value = self.runtime.run(
            &context,
            AggrType::Emit,
            &mut unwind_event, // event
            state,             // state
            &mut event_meta,   // $
        );
        // move origin_uri back to event again
        event.origin_uri = context.origin_uri;
        match value {
            Ok(Return::EmitEvent { port }) => {
                Ok(vec![(port.map_or_else(|| "out".into(), Cow::Owned), event)])
            }
            Ok(Return::Emit { value, port }) => {
                *unwind_event = value;
                Ok(vec![(port.map_or_else(|| "out".into(), Cow::Owned), event)])
            }
            Ok(Return::Drop) => Ok(vec![]),
            Err(e) => {
                let mut o = Value::from(hashmap! {
                    "error".into() => Value::String(self.runtime.format_error(&e).into()),
                });
                std::mem::swap(&mut o, unwind_event);
                if let Some(error) = unwind_event.as_object_mut() {
                    error.insert("event".into(), o);
                };
                Ok(vec![("error".into(), event)])
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::FN_REGISTRY;
    use simd_json::json;
    use tremor_script::path::ModulePath;

    #[test]
    fn mutate() {
        let config = Config {
            script: r#"match event.a of case 1 => let event.snot = "badger" end; event;"#
                .to_string(),
        };
        let runtime = Script::parse(
            &ModulePath { mounts: vec![] }, // FIXME config cpp
            config.script.clone(),
            &*FN_REGISTRY.lock().expect("could not claim lock"),
        )
        .expect("failed to parse script");
        let mut op = Tremor {
            config,
            runtime,
            id: "badger".into(),
        };
        let event = Event {
            origin_uri: None,
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            data: Value::from(json!({"a": 1})).into(),
            kind: None,
        };
        let mut state = Value::null();

        let (out, event) = op
            .on_event("in", &mut state, event)
            .expect("failed to run pipeline")
            .pop()
            .expect("no event returned");
        assert_eq!("out", out);

        assert_eq!(
            event.data.suffix().value,
            Value::from(json!({"snot": "badger", "a": 1}))
        )
    }

    #[test]
    pub fn test_how_it_handles_errors() {
        let config = Config {
            script: r#"match this is invalid code so no match case"#.to_string(),
        };
        let _runtime = Script::parse(
            &ModulePath { mounts: vec![] }, // FIXME config cpp
            config.script,
            &*FN_REGISTRY.lock().expect("could not claim lock"),
        );
    }
}
