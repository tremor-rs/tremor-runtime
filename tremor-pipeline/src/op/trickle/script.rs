// Copyright 2020-2021, The Tremor Team
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
use std::mem;
use std::sync::Arc;
use tremor_script::prelude::*;
use tremor_script::ARGS_CONST_ID;

rental! {
    pub mod rentals {
        use tremor_script::query::StmtRental;
        use std::sync::Arc;
        use halfbrown::HashMap;
        use serde::Serialize;
        use super::*;

        #[rental(covariant,debug)]
        pub struct Script {
            stmt: Arc<StmtRental>,
            script: tremor_script::ast::ScriptDecl<'stmt>,
        }
    }
}

#[derive(Debug)]
pub struct Trickle {
    pub id: String,
    pub defn: Arc<tremor_script::query::StmtRental>,
    pub node: Arc<tremor_script::query::StmtRental>,
    script: rentals::Script,
}

impl Trickle {
    pub fn with_stmt(
        id: String,
        defn_rentwrapped: tremor_script::query::StmtRentalWrapper,
        node_rentwrapped: tremor_script::query::StmtRentalWrapper,
    ) -> Result<Self> {
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
        if let tremor_script::ast::query::Stmt::ScriptDecl(ref defn) = defn_rentwrapped.suffix() {
            if let Some(p) = &defn.params {
                // Set params from decl as meta vars
                for (name, value) in p {
                    // We could clone here since we bind Script to defn_rentwrapped.stmt's lifetime
                    params.insert(Cow::from(name.clone()), value.clone());
                }
                // Set params from instance as meta vars ( eg: upsert ~= override + add )
                if let tremor_script::ast::query::Stmt::Script(instance) = node_rentwrapped.suffix()
                {
                    if let Some(map) = &instance.params {
                        for (name, value) in map {
                            // We can not clone here since we do not bind Script to node_rentwrapped's lifetime
                            params.insert(Cow::from(name.clone()), value.clone_static());
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
            args = tremor_script::Value::from(params);
        } else {
            return Err(ErrorKind::PipelineError(
                "Trying to turn something into script define that isn't a script define".into(),
            )
            .into());
        };
        let script = match defn_rentwrapped.suffix() {
            tremor_script::ast::Stmt::ScriptDecl(ref script) => script.clone(),
            _other => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non script into a script operator".into(),
                )
                .into())
            }
        };

        let script = rentals::Script::try_new(defn_rentwrapped.stmt.clone(), move |_| unsafe {
            use tremor_script::ast::ScriptDecl;
            // This is sound since defn_rentwrapped.stmt is an arc by cloning
            // it we ensure that the referenced data remains available until
            // the rental is dropped.
            let mut decl = mem::transmute::<ScriptDecl<'_>, ScriptDecl<'static>>(*script);
            let args: Value<'static> = mem::transmute(args);

            decl.script.consts = vec![Value::null(), Value::null(), Value::null()];
            *decl
                .script
                .consts
                .get_mut(ARGS_CONST_ID)
                .ok_or_else(|| Error::from("Can't access ARGS_CONST_ID!"))? = args;
            Ok(decl)
        })
        .map_err(|e: rental::RentalError<Error, _>| e.0)?;

        Ok(Self {
            id,
            defn: defn_rentwrapped.stmt,
            node: node_rentwrapped.stmt,
            script,
        })
    }
}

impl Operator for Trickle {
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let context = EventContext::new(event.ingest_ns, event.origin_uri);

        let data = event.data.suffix();
        // This lifetimes will be `&'run mut Value<'event>` as that is the
        // requirement of the `self.runtime.run` we can not declare them
        // as the trait function for the operator doesn't allow that

        let unwind_event: &'_ mut tremor_script::Value<'_> =
            unsafe { mem::transmute(data.value()) };
        let event_meta: &'_ mut tremor_script::Value<'_> = unsafe { mem::transmute(data.meta()) };

        let value = self.script.suffix().script.run(
            &context,
            AggrType::Emit,
            unwind_event, // event
            state,        // state
            event_meta,   // $
        );

        // move origin_uri back to event again
        event.origin_uri = context.origin_uri;

        match value {
            Ok(Return::EmitEvent { port }) => Ok(vec![(port.map_or(OUT, Cow::from), event)].into()),

            Ok(Return::Emit { value, port }) => {
                *unwind_event = value;
                Ok(vec![(port.map_or(OUT, Cow::from), event)].into())
            }
            Ok(Return::Drop) => Ok(EventAndInsights::default()),
            Err(e) => {
                let mut o = Value::from(hashmap! {
                    "error".into() => Value::from(self.node.head().format_error(&e)),
                });
                mem::swap(&mut o, unwind_event);
                if let Some(error) = unwind_event.as_object_mut() {
                    error.insert("event".into(), o);
                };
                Ok(vec![(ERR, event)].into())
            }
        }
    }
}
