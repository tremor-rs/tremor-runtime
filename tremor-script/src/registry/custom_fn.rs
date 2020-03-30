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
use crate::ast::query::ARGS_CONST_ID;
use crate::ast::{Expr, Exprs, ImutExpr, ImutExprInt, ImutExprs, InvokeAggrFn};
use crate::interpreter::{AggrType, Cont, Env, ExecOpts, LocalStack};
use simd_json::prelude::*;
use simd_json::BorrowedValue as Value;
use std::borrow::Cow;
//use std::mem;

pub(crate) const RECUR: Value<'static> = Value::String(Cow::Borrowed("recur"));
const RECURSION_LIMIT: u32 = 1024;

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

    #[allow(mutable_transmutes)]
    pub(crate) fn invoke<'event, 'run>(
        &'script self,
        env: &'run Env<'run, 'event, 'script>,
        args: &'run [&'run Value<'event>],
    ) -> FResult<Value<'event>>
    where
        'script: 'event,
        'event: 'run,
    {
        use std::mem;
        const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

        let mut args_const = Value::from(
            args.iter()
                .skip(self.locals)
                .map(|v| v.clone_static())
                .collect::<Vec<Value<'static>>>(),
        );

        let consts: &'run mut [Value<'event>] = unsafe { mem::transmute(env.consts) };

        mem::swap(&mut consts[ARGS_CONST_ID], &mut args_const);

        let mut this_local = LocalStack::with_size(self.locals);
        for (i, arg) in args.iter().enumerate() {
            if i == self.locals {
                break;
            }
            this_local.values[i] = Some((*arg).clone_static());
        }
        let opts = ExecOpts {
            result_needed: false,
            aggr: AggrType::Tick,
        };
        // FIXME .unwrap()
        let meta = NodeMetas::default();
        let env = Env {
            context: env.context,
            consts: env.consts,
            aggrs: &NO_AGGRS,
            meta: &meta,
        };
        let mut recursion_count = 0;
        'recur: loop {
            let mut exprs = self.body.iter().peekable();
            let mut no_event = Value::null();
            let mut no_meta = Value::null();
            let mut state = Value::null().into_static();
            while let Some(expr) = exprs.next() {
                if exprs.peek().is_none() {
                    let r = expr.run(
                        opts.with_result(),
                        &env,
                        &mut no_event,
                        &mut state,
                        &mut no_meta,
                        &mut this_local,
                    );
                    if r.is_err() {
                        mem::swap(&mut consts[ARGS_CONST_ID], &mut args_const);
                    };
                    match r? {
                        Cont::Cont(v) => {
                            return Ok(v.into_owned());
                        }
                        Cont::Drop => {
                            recursion_count += 1;
                            if recursion_count == RECURSION_LIMIT {
                                mem::swap(&mut consts[ARGS_CONST_ID], &mut args_const);
                                return Err(FunctionError::Error("recursion limit reached".into()));
                            }
                            // clear the local variables (that are not the
                            // arguments)
                            for local in this_local.values.iter_mut().skip(args.len()) {
                                *local = None;
                            }
                            //We are abusing this as a recursion hint
                            continue 'recur;
                        }
                        _ => {
                            mem::swap(&mut consts[ARGS_CONST_ID], &mut args_const);
                            return Err(FunctionError::Error("can't emit here".into()));
                        }
                    };
                } else {
                    let r = expr.run(
                        opts,
                        &env,
                        &mut no_event,
                        &mut state,
                        &mut no_meta,
                        &mut this_local,
                    );
                    if r.is_err() {
                        mem::swap(&mut consts[ARGS_CONST_ID], &mut args_const);
                    };
                    r?;
                }
            }
        }
    }
}
