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
use halfbrown::{hashmap, HashMap};
use std::mem;
use std::sync::Arc;
use tremor_script::ast::ARGS_CONST_ID;
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
    ) -> Result<Self> {
        use std::borrow::Cow;

        // We require Value to be static here to enforce the constraint that
        // arguments name/value pairs live at least as long as the operator nodes that have
        // dependencies on them.
        //
        // Note also that definitional parameters and instance parameters have slightly
        // different costs. The definitional paraemeters ( if not overriden ) never change
        // but instance parameters that do override must be guaranteed as static to ensure
        // their lifetimes don't go out of scope. We avoid this with definitional arguments
        // as they are always available once specified.
        //
        // The key to why this is the case is the binding lifetime as it is associated with
        // the definition ( from which all instances are incarnated ) not the 'create' instances.
        // The binding association chooses the definition simply as it hosts the parsed script.
        //
        let args: Value;

        let mut params = HashMap::new();
        if let tremor_script::ast::query::Stmt::ScriptDecl(ref defn) =
            defn_rentwrapped.stmt.suffix()
        {
            if let Some(p) = &defn.params {
                // Set params from decl as meta vars
                for (name, value) in p {
                    // We could clone here since we bind Script to defn_rentwrapped.stmt's lifetime
                    params.insert(Cow::Owned(name.clone()), value.clone());
                }
                // Set params from instance as meta vars ( eg: upsert ~= override + add )
                if let tremor_script::ast::query::Stmt::Script(instance) =
                    node_rentwrapped.stmt.suffix()
                {
                    if let Some(map) = &instance.params {
                        for (name, value) in map {
                            // We can not clone here since we do not bind Script to node_rentwrapped's lifetime
                            params.insert(Cow::Owned(name.clone()), value.clone_static());
                        }
                    }
                } else {
                    return Err(ErrorKind::PipelineError(
                        "Trying to turn something into script create that isn't a script create"
                            .into(),
                    )
                    .into());
                }
            }
            args = tremor_script::Value::Object(params);
        } else {
            return Err(ErrorKind::PipelineError(
                "Trying to turn something into script define that isn't a script define".into(),
            )
            .into());
        };
        let script = match defn_rentwrapped.stmt.suffix() {
            tremor_script::ast::Stmt::ScriptDecl(ref script) => script.clone(),
            _other => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non script into a script operator".into(),
                )
                .into())
            }
        };

        let script = rentals::Script::new(defn_rentwrapped.stmt.clone(), move |_| unsafe {
            std::mem::transmute(script)
        });

        #[allow(clippy::transmute_ptr_to_ptr)]
        #[allow(mutable_transmutes)]
        let script_ref: &mut tremor_script::ast::ScriptDecl =
            unsafe { mem::transmute(script.suffix()) };
        script_ref.script.consts = vec![Value::Null, Value::Null, Value::Null];
        script_ref.script.consts[ARGS_CONST_ID] = args;

        Ok(Self {
            id,
            defn: defn_rentwrapped.stmt,
            node: node_rentwrapped.stmt,
            script,
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
