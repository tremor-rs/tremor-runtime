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
use halfbrown::hashmap;
use std::sync::Arc;
use tremor_script::prelude::*;

rental! {
    pub mod rentals {
        use tremor_script::query::rentals::Stmt;
        use std::sync::Arc;
        use halfbrown::HashMap;
        use serde::Serialize;
        use super::*;

        #[rental(covariant,debug)]
        pub struct Script {
            stmt: Arc<Stmt>,
            script: tremor_script::ast::ScriptDecl<'stmt>,
        }
    }
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TrickleScript {
    pub id: String,
    pub defn: Arc<tremor_script::query::rentals::Stmt>,
    pub node: Arc<tremor_script::query::rentals::Stmt>,
    script: rentals::Script,
}

impl TrickleScript {
    pub fn with_stmt(
        id: String,
        defn_rentwrapped: tremor_script::query::StmtRentalWrapper,
        node_rentwrapped: tremor_script::query::StmtRentalWrapper,
    ) -> Result<TrickleScript> {
        let script = match node_rentwrapped.stmt.suffix() {
            tremor_script::ast::Stmt::ScriptDecl(ref script) => script.clone(),
            _ => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non script into a script operator".into(),
                )
                .into())
            }
        };

        Ok(TrickleScript {
            id,
            defn: defn_rentwrapped.stmt.clone(),
            node: node_rentwrapped.stmt.clone(),
            script: rentals::Script::new(defn_rentwrapped.stmt.clone(), move |_| unsafe {
                std::mem::transmute(script)
            }),
        })
    }
}

impl Operator for TrickleScript {
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        let context = EventContext::from_ingest_ns(event.ingest_ns);

        let data = event.data.suffix();
        #[allow(clippy::transmute_ptr_to_ptr)]
        #[allow(mutable_transmutes)]
        let mut unwind_event: &mut tremor_script::Value<'_> =
            unsafe { std::mem::transmute(&data.value) };
        #[allow(clippy::transmute_ptr_to_ptr)]
        #[allow(mutable_transmutes)]
        let mut event_meta: &mut tremor_script::Value<'_> =
            unsafe { std::mem::transmute(&data.meta) };

        // PERF This should be a compile time AST transform const'ing params and/or introducing arguments keyword [ DISCUSS ]
        // For now we disabuse $metadata but undesired side-effect is it pollutes the metadata value-space along causal flow lines
        if let tremor_script::Value::Object(o) = event_meta {
            if let Some(p) = &self.script.suffix().params {
                // Set params from decl as meta vars
                for (name, value) in p {
                    o.insert(name.into(), value.clone());
                }
                // Set params from instance as meta vars ( eg: upsert ~= override + add )
                if let tremor_script::ast::query::Stmt::Script(instance) = &*self.defn.suffix() {
                    if let Some(map) = &instance.params {
                        for (name, value) in map {
                            o.insert(name.into(), value.clone());
                        }
                    }
                } else {
                    dbg!("stmt not set");
                }
            }
        }

        let value = self.script.suffix().script.run(
            &context,
            AggrType::Emit,
            &mut unwind_event, // event
            &mut event_meta,   // $
        );
        match value {
            Ok(Return::EmitEvent { port }) => {
                Ok(vec![(port.unwrap_or_else(|| "out".to_string()), event)])
            }

            Ok(Return::Emit { value, port }) => {
                *unwind_event = value;
                Ok(vec![(port.unwrap_or_else(|| "out".to_string()), event)])
            }
            Ok(Return::Drop) => Ok(vec![]),
            Err(e) => {
                let mut o = Value::Object(hashmap! {
                    "error".into() => Value::String(self.node.head().format_error(e).into()),
                });
                std::mem::swap(&mut o, unwind_event);
                if let Some(error) = unwind_event.as_object_mut() {
                    error.insert("event".into(), o);
                };
                //*unwind_event = data;
                Ok(vec![("error".to_string(), event)])
            }
        }
    }
}
