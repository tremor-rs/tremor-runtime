// Copyright 2020, The Tremor Team
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
use tremor_script::path::load as load_module_path;
use tremor_script::prelude::*;
use tremor_script::{self, AggrType, EventContext, Return, Script};

op!(TremorFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;

        match tremor_script::Script::parse(
               &load_module_path(),
               "<operator>",
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
                Err(e.error().into())
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
    fn on_event(
        &mut self,
        _uid: u64,
        _in_port: &str,
        state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let out_port = {
            let context = EventContext::new(event.ingest_ns, event.origin_uri);
            // This lifetimes will be `&'run mut Value<'event>` as that is the
            // requirement of the `self.runtime.run` we can not declare them
            // as the trait function for the operator doesn't allow that
            let (unwind_event, event_meta) = event.data.parts();
            // unwind_event => the event
            // event_meta => meta
            let value = self.runtime.run(
                &context,
                AggrType::Emit,
                unwind_event, // event
                state,        // state
                event_meta,   // $
            );
            // move origin_uri back to event again
            event.origin_uri = context.origin_uri;
            match value {
                Ok(Return::EmitEvent { port }) => port.map_or(OUT, Cow::Owned),
                Ok(Return::Emit { value, port }) => {
                    *unwind_event = value;
                    port.map_or(OUT, Cow::Owned)
                }
                Ok(Return::Drop) => return Ok(EventAndInsights::default()),
                Err(ref e) => {
                    let mut o = Value::from(hashmap! {
                        "error".into() => Value::from(self.runtime.format_error(&e)),
                    });
                    std::mem::swap(&mut o, unwind_event);
                    if let Some(error) = unwind_event.as_object_mut() {
                        error.insert("event".into(), o);
                    } else {
                        // ALLOW: we know this never happens since we swap the event three lines above
                        unreachable!();
                    };
                    ERR
                }
            }
        };
        Ok(vec![(out_port, event)].into())
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
            &ModulePath { mounts: vec![] }, // TODO config cpp
            "<test>",
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
            id: 1.into(),
            ingest_ns: 1,
            data: Value::from(json!({"a": 1})).into(),
            ..Event::default()
        };
        let mut state = Value::null();

        let (out, event) = op
            .on_event(0, "in", &mut state, event)
            .expect("failed to run pipeline")
            .events
            .pop()
            .expect("no event returned");
        assert_eq!("out", out);

        assert_eq!(
            *event.data.suffix().value(),
            Value::from(json!({"snot": "badger", "a": 1}))
        )
    }

    #[test]
    pub fn test_how_it_handles_errors() {
        let config = Config {
            script: r#"match this is invalid code so no match case"#.to_string(),
        };
        let _runtime = Script::parse(
            &ModulePath { mounts: vec![] }, // TODO config cpp
            "<test>",
            config.script,
            &*FN_REGISTRY.lock().expect("could not claim lock"),
        );
    }
}
