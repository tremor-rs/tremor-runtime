// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a cstd::result::Result::Err(*right_val)::Result::Err(*right_val)License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Nopt yet implemented - this is behind a feature flag
#![cfg_attr(tarpaulin, skip)]

use super::*;
use crate::ast::Exprs;
use crate::EventContext;
use simd_json::prelude::*;
use simd_json::BorrowedValue as Value;
use std::borrow::Cow;
use std::mem;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct CustomFn<'script> {
    //module: Arc<Script>,
    pub name: Cow<'script, str>,
    pub body: Exprs<'script>,
    pub args: Vec<String>,
    pub locals: usize,
    pub is_const: bool,
}

impl<'script> CustomFn<'script> {
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    pub(crate) fn invoke<'event>(
        &self,
        ctx: &EventContext,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        use crate::ast::InvokeAggrFn;
        use crate::interpreter::{AggrType, Cont, Env, ExecOpts, LocalStack};
        const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];
        const NO_CONSTS: [Value<'static>; 0] = [];
        let mut this_local = LocalStack::with_size(self.locals);
        for (i, arg) in args.iter().enumerate() {
            this_local.values[i] = Some((*arg).clone());
        }
        let opts = ExecOpts {
            result_needed: false,
            aggr: AggrType::Tick,
        };
        let mut exprs = self.body.iter().peekable();
        // FIXME .unwrap()
        let meta = NodeMetas::default();
        let env = Env {
            context: ctx,
            consts: &NO_CONSTS,
            aggrs: &NO_AGGRS,
            meta: &meta,
        };
        let mut no_event = Value::null();
        let mut no_meta = Value::null();
        let mut state = Value::null().into_static();
        unsafe {
            while let Some(expr) = exprs.next() {
                if exprs.peek().is_none() {
                    return if let Cont::Cont(v) = expr.run(
                        opts.with_result(),
                        &env,
                        mem::transmute(&mut no_event),
                        &mut state,
                        mem::transmute(&mut no_meta),
                        &mut this_local,
                    )? {
                        Ok(mem::transmute(v.into_owned()))
                    } else {
                        Err(FunctionError::Error("can't emit here".into()))
                    };
                } else {
                    expr.run(
                        opts,
                        &env,
                        mem::transmute(&mut no_event),
                        &mut state,
                        mem::transmute(&mut no_meta),
                        &mut this_local,
                    )?;
                }
            }
        }

        Ok(Value::null())
    }
    pub(crate) fn is_const(&self) -> bool {
        self.is_const
    }
}
