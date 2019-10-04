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
pub struct TrickleScript {
    pub id: String,
    pub stmt: Arc<tremor_script::query::rentals::Stmt>,
    script: rentals::Script,
}

impl TrickleScript {
    pub fn with_stmt(
        id: String,
        stmt_rentwrapped: tremor_script::query::StmtRentalWrapper,
    ) -> TrickleScript {
        let s = match stmt_rentwrapped.stmt.suffix() {
            tremor_script::ast::Stmt::ScriptDecl(ref script) => Some(script.clone()),
            _ => None,
        };

        if let Some(script) = s {
            TrickleScript {
                id,
                stmt: stmt_rentwrapped.stmt.clone(),
                script: rentals::Script::new(stmt_rentwrapped.stmt.clone(), |_| unsafe {
                    std::mem::transmute(script.clone())
                }),
            }
        } else {
            unreachable!("bad script");
        }
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
                    "error".into() => Value::String(self.stmt.head().format_error(e).into()),
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
