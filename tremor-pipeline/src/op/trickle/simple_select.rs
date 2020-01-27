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

// [x] PERF0001: handle select without grouping or windows easier.

use crate::errors::*;
use crate::{Event, Operator};
use std::borrow::Cow;
use tremor_script::interpreter::Env;
use tremor_script::{
    self,
    ast::{InvokeAggrFn, SelectStmt},
    prelude::*,
    query::rentals::Stmt as StmtRental,
};

rental! {
    pub mod rentals {
        use std::sync::Arc;
        use halfbrown::HashMap;
        use super::*;

        #[rental(covariant,debug)]
        pub struct Select {
            stmt: Arc<StmtRental>,
            select: tremor_script::ast::SelectStmt<'stmt>,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct TrickleSimpleSelect {
    pub id: String,
    pub select: rentals::Select,
}

const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl TrickleSimpleSelect {
    pub fn with_stmt(
        id: String,
        stmt_rentwrapped: &tremor_script::query::StmtRentalWrapper,
    ) -> Result<Self> {
        let select = match stmt_rentwrapped.stmt.suffix() {
            tremor_script::ast::Stmt::Select(ref select) => select.clone(),
            _ => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non select into a select operator".into(),
                )
                .into())
            }
        };

        Ok(Self {
            id,
            select: rentals::Select::new(stmt_rentwrapped.stmt.clone(), move |_| unsafe {
                std::mem::transmute(select)
            }),
        })
    }
    fn opts() -> ExecOpts {
        ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        }
    }
}

impl Operator for TrickleSimpleSelect {
    #[allow(
        mutable_transmutes,
        clippy::transmute_ptr_to_ptr,
        clippy::too_many_lines
    )]
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(Cow<'static, str>, Event)>> {
        let opts = Self::opts();
        // We guarantee at compile time that select in itself can't have locals, so this is safe

        // NOTE We are unwrapping our rental wrapped stmt
        let SelectStmt {
            stmt,
            locals,
            consts,
            node_meta,
            ..
        }: &mut SelectStmt = unsafe { std::mem::transmute(self.select.suffix()) };
        let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);
        // TODO avoid origin_uri clone here
        let ctx = EventContext::new(event.ingest_ns, event.origin_uri.clone());

        //
        // Before any select processing, we filter by where clause
        //
        // FIXME: ?
        if let Some(guard) = &stmt.maybe_where {
            let (unwind_event, event_meta) = event.data.parts();
            let env = Env {
                context: &ctx,
                consts: &consts,
                aggrs: &NO_AGGRS,
                meta: &node_meta,
            };
            let test = guard.run(opts, &env, unwind_event, event_meta, &local_stack)?;
            if let Some(test) = test.as_bool() {
                if !test {
                    return Ok(vec![]);
                };
            } else {
                return tremor_script::errors::query_guard_not_bool(
                    &stmt, guard, &test, &node_meta,
                )?;
            };
        }

        if let Some(guard) = &stmt.maybe_having {
            let (unwind_event, event_meta) = event.data.parts();
            let env = Env {
                context: &ctx,
                consts: &consts,
                aggrs: &NO_AGGRS,
                meta: &node_meta,
            };
            let test = guard.run(opts, &env, unwind_event, event_meta, &local_stack)?;
            if let Some(test) = test.as_bool() {
                if !test {
                    return Ok(vec![]);
                };
            } else {
                return tremor_script::errors::query_guard_not_bool(
                    &stmt, guard, &test, &node_meta,
                )?;
            };
        }

        Ok(vec![("out".into(), event)])
    }
}

#[cfg(test)]
mod test {
    // FIXME: TODO
}
