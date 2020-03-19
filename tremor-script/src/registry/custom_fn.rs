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
use crate::ast::{Expr, Exprs, ImutExpr, ImutExprInt, ImutExprs};
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
    pub open: bool,
    pub locals: usize,
    pub is_const: bool,
    pub inline: bool,
}

impl<'script> CustomFn<'script> {
    pub(crate) fn is_const(&self) -> bool {
        self.is_const
    }
    pub(crate) fn can_inline(&self) -> bool {
        if self.body.len() != 1 {
            return false;
        }
        if self.inline {
            return true;
        }
        let i = match self.body.get(0) {
            Some(Expr::Imut(ImutExprInt::Invoke1(i))) => i,
            Some(Expr::Imut(ImutExprInt::Invoke2(i))) => i,
            Some(Expr::Imut(ImutExprInt::Invoke3(i))) => i,
            Some(Expr::Imut(ImutExprInt::Invoke(i))) => i,
            _ => return false,
        };
        let mut a_idx = 0;
        for a in &i.args {
            if let ImutExpr(ImutExprInt::Local { idx, .. }) = a {
                if *idx != a_idx {
                    return false;
                }
                a_idx += 1;
            } else {
                return false;
            }
        }

        true
    }
    pub(crate) fn inline(&self, args: ImutExprs<'script>) -> Result<ImutExprInt<'script>> {
        if self.body.len() != 1 {
            return Err(format!("can't inline {}: too large body", self.name).into());
        }

        let i = match self.body.get(0) {
            Some(Expr::Imut(ImutExprInt::Invoke1(i))) => i,
            Some(Expr::Imut(ImutExprInt::Invoke2(i))) => i,
            Some(Expr::Imut(ImutExprInt::Invoke3(i))) => i,
            Some(Expr::Imut(ImutExprInt::Invoke(i))) => i,
            Some(e) => {
                return Err(format!("can't inline {}: bad expression: {:?}", self.name, e).into())
            }
            None => return Err(format!("can't inline {}: no body", self.name).into()),
        };

        if i.args.len() != self.args.len() {
            return Err(format!("can't inline {}: different argc", self.name).into());
        }

        let mut i = i.clone();
        i.args = args;

        Ok(match i.args.len() {
            1 => ImutExprInt::Invoke1(i),
            2 => ImutExprInt::Invoke2(i),
            3 => ImutExprInt::Invoke3(i),
            _ => ImutExprInt::Invoke(i),
        })
    }
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    pub(crate) fn invoke<'event>(
        &self,
        ctx: &EventContext,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        use crate::ast::InvokeAggrFn;
        use crate::interpreter::{AggrType, Cont, Env, ExecOpts, LocalStack};
        const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

        let consts = if self.open {
            vec![
                Value::null(),
                Value::null(),
                Value::from(
                    args.iter()
                        .skip(self.locals)
                        .map(|v| v.clone_static())
                        .collect::<Vec<Value<'static>>>(),
                ),
            ]
        } else {
            vec![Value::null(), Value::null(), Value::null()]
        };
        let mut this_local = LocalStack::with_size(self.locals);
        for (i, arg) in args.iter().enumerate() {
            if i == self.locals {
                break;
            }
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
            consts: &consts,
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
}
