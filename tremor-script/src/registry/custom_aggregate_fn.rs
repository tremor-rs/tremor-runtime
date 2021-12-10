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

use crate::ast::Exprs;
use crate::interpreter::{Cont, Env, LocalStack};
use crate::prelude::{Builder, ExecOpts};
use crate::registry::{FResult, FunctionError};
use crate::{AggrType, Value};
use beef::Cow;

/// public because lalrpop
#[derive(Clone)]
pub struct CustomAggregateFn<'script> {
    /// public because lalrpop
    pub name: Cow<'script, str>,
    /// public because lalrpop
    pub init_body: Exprs<'script>,
    /// public because lalrpop
    pub aggregate_args: Vec<String>,
    /// public because lalrpop
    pub aggregate_body: Exprs<'script>,
    /// public because lalrpop
    pub mergein_args: Vec<String>,
    /// public because lalrpop
    pub mergein_body: Exprs<'script>,
    /// public because lalrpop
    pub emit_args: Vec<String>,
    /// public because lalrpop
    pub emit_body: Exprs<'script>,
    /// public because lalrpop
    pub state: Value<'script>,
}

impl<'script> CustomAggregateFn<'script> {
    /// Initialize the instance of aggregate function
    /// # Errors
    /// Returns an error if the initialization fails, or the init function does not return a value
    pub fn init<'event>(&mut self, env: &Env<'_, 'event>) -> FResult<()>
    where
        'script: 'event,
    {
        let mut local_stack = LocalStack::with_size(128);

        let mut no_meta = Value::null();
        let mut state = Value::null().into_static();
        let mut no_event = Value::null();

        let mut body_iter = self.init_body.iter().peekable();
        while let Some(expr) = body_iter.next() {
            let env_local = Env {
                context: env.context,
                consts: env.consts,
                aggrs: &[],
                meta: env.meta,
                recursion_limit: env.recursion_limit,
            };

            let cont = expr.run(
                ExecOpts {
                    result_needed: true,
                    aggr: AggrType::Tick,
                },
                &env_local,
                &mut no_event,
                &mut state,
                &mut no_meta,
                &mut local_stack,
            )?;

            if body_iter.peek().is_none() {
                if let Cont::Cont(value) = cont {
                    self.state = value.into_owned().clone_static();
                } else {
                    return Err(FunctionError::NoAggregateValueInInit);
                }
            }
        }

        Ok(())
    }

    /// Aggregate a value
    /// # Errors
    /// Will return an error if the aggregate function does not return a value, of it errors
    pub fn aggregate<'event>(&mut self, args: &[&Value], env: &Env<'_, 'event>) -> FResult<()>
    where
        'script: 'event,
    {
        let mut body_iter = self.aggregate_body.iter().peekable();
        let args = Value::Array(args.iter().map(|v| v.clone_static()).collect());
        let mut no_meta = Value::null();
        let mut state = Value::null().into_static();
        let mut no_event = Value::null();
        let mut local_stack = LocalStack::with_size(2);
        local_stack.values.insert(0, Some(self.state.clone()));
        local_stack.values.insert(1, Some(args[0].clone()));

        let env = Env {
            context: env.context,
            consts: env.consts,
            aggrs: &[],
            meta: env.meta,
            recursion_limit: env.recursion_limit,
        };

        while let Some(expr) = body_iter.next() {
            let cont = expr
                .run(
                    ExecOpts {
                        result_needed: true,
                        aggr: AggrType::Tick,
                    },
                    &env,
                    &mut no_event,
                    &mut state,
                    &mut no_meta,
                    &mut local_stack,
                )
                .expect("FIXME");

            if body_iter.peek().is_none() {
                if let Cont::Cont(value) = cont {
                    self.state = value.into_owned().clone_static();
                } else {
                    return Err(FunctionError::NoAggregateValueInAggregate);
                }
            }
        }

        Ok(())
    }

    /// Merge with another instance
    /// # Errors
    /// Return an error if the execution of the script fails or if there's no value returned
    pub fn merge<'event>(&mut self, other: &CustomAggregateFn, env: &Env<'_, 'event>) -> FResult<()>
    where
        'script: 'event,
    {
        dbg!(&self.state, &other.state);
        let mut body_iter = self.mergein_body.iter().peekable();
        let mut no_meta = Value::null();
        let mut state = Value::null().into_static();
        let mut no_event = Value::null();
        let mut local_stack = LocalStack::with_size(2);
        local_stack.values.insert(0, Some(self.state.clone()));
        local_stack
            .values
            .insert(1, Some(other.state.clone_static()));

        let env = Env {
            context: env.context,
            consts: env.consts,
            aggrs: &[],
            meta: env.meta,
            recursion_limit: env.recursion_limit,
        };

        while let Some(expr) = body_iter.next() {
            let cont = expr
                .run(
                    ExecOpts {
                        result_needed: true,
                        aggr: AggrType::Tick,
                    },
                    &env,
                    &mut no_event,
                    &mut state,
                    &mut no_meta,
                    &mut local_stack,
                )
                .expect("FIXME");

            if body_iter.peek().is_none() {
                if let Cont::Cont(value) = cont {
                    self.state = value.into_owned().clone_static();
                } else {
                    return Err(FunctionError::NoAggregateValueInAggregate);
                }
            }
        }

        Ok(())
    }

    /// Emit the state
    pub(crate) fn emit<'event>(&mut self, env: &Env<'_, 'event>) -> FResult<Value<'event>>
    where
        'script: 'event,
    {
        let mut body_iter = self.emit_body.iter().peekable();
        let mut local_stack = LocalStack::with_size(1);

        let mut no_meta = Value::null();
        let mut state = Value::null().into_static();
        let mut no_event = Value::null();

        local_stack.values.insert(0, Some(self.state.clone()));

        let env = Env {
            context: env.context,
            consts: env.consts,
            aggrs: &[],
            meta: env.meta,
            recursion_limit: env.recursion_limit,
        };

        while let Some(expr) = body_iter.next() {
            let cont = expr.run(
                ExecOpts {
                    result_needed: true,
                    aggr: AggrType::Tick,
                },
                &env,
                &mut no_event,
                &mut state,
                &mut no_meta,
                &mut local_stack,
            )?;

            if body_iter.peek().is_none() {
                if let Cont::Cont(value) = cont {
                    return Ok(value.into_owned());
                }
            }
        }

        Err(FunctionError::RecursionLimit) // todo return a correct error
    }
}
