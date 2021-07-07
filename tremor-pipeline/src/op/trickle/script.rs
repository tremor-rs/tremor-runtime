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
use crate::srs;
use std::mem;
use tremor_script::{ast::query, highlighter, prelude::*, query::SRSStmt, Query};

#[derive(Debug)]
pub struct Script {
    pub id: String,
    script: srs::Script,
}

impl Script {
    pub fn with_stmt(id: String, decl: &SRSStmt, node_rentwrapped: &SRSStmt) -> Result<Self> {
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

        let mut script = srs::Script::try_new_from_srs(decl, |decl| {
            let args: Value;

            let mut params = HashMap::new();

            let mut script = match decl {
                query::Stmt::ScriptDecl(ref script) => *script.clone(),
                _other => return Err("Trying to turn a non script into a script operator"),
            };
            if let Some(p) = &script.params {
                // Set params from decl as meta vars
                for (name, value) in p {
                    // We could clone here since we bind Script to defn_rentwrapped.stmt's lifetime
                    params.insert(Cow::from(name.clone()), value.clone());
                }
            }
            args = tremor_script::Value::from(params);

            script.script.consts.args = args;
            Ok(script)
        })?;

        script.apply(node_rentwrapped, |this, other| {
            if let query::Stmt::Script(instance) = other {
                if let Some(map) = &instance.params {
                    for (name, value) in map {
                        // We can not clone here since we do not bind Script to node_rentwrapped's lifetime
                        this.script
                            .consts
                            .args
                            .try_insert(Cow::from(name.clone()), value.clone_static());
                    }
                }
            } else {
                return Err(
                    "Trying to turn something into script create that isn't a script create",
                );
            }
            Ok(())
        })?;

        Ok(Self { id, script })
    }
}

impl Operator for Script {
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let context = EventContext::new(event.ingest_ns, event.origin_uri);

        let port = event.data.apply(&self.script, |data, decl| {
            let (unwind_event, event_meta) = data.parts_mut();

            let value = decl.script.run(
                &context,
                AggrType::Emit,
                unwind_event, // event
                state,        // state
                event_meta,   // $
            );

            match value {
                Ok(Return::EmitEvent { port }) => Some(port.map_or(OUT, Cow::from)),

                Ok(Return::Emit { value, port }) => {
                    *unwind_event = value;
                    Some(port.map_or(OUT, Cow::from))
                }
                Ok(Return::Drop) => None,
                Err(e) => {
                    let s = self
                        .script
                        .raw()
                        .get(0)
                        .and_then(|v| {
                            let s: &[u8] = &v;
                            let s = std::str::from_utf8(s).ok()?;
                            let mut h = highlighter::Dumb::default();
                            Query::format_error_from_script(s, &mut h, &e).ok()?;
                            Some(h.to_string())
                        })
                        .unwrap_or_default();

                    let mut o = Value::from(hashmap! {

                        "error".into() => Value::from(s),
                    });
                    mem::swap(&mut o, unwind_event);
                    if let Some(error) = unwind_event.as_object_mut() {
                        error.insert("event".into(), o);
                    };
                    Some(ERR)
                }
            }
        });

        // move origin_uri back to event again
        event.origin_uri = context.origin_uri;

        Ok(port.map_or_else(EventAndInsights::default, |port| vec![(port, event)].into()))
    }
}
