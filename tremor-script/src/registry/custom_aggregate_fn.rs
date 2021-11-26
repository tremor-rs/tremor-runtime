use crate::ast::{Exprs, LocalPath, Path, RunConsts};
use crate::{AggrType, Value};
use beef::Cow;
use halfbrown::HashMap;
use tremor_value::Value::Object;
use crate::interpreter::{Cont, Env, LocalStack};
use crate::prelude::{Builder, ExecOpts};
use crate::registry::{FResult, FunctionError};

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
    pub state: Value<'script>
}

impl<'script> CustomAggregateFn<'script> {
    /// Initialize the instance of aggregate function
    pub fn init<'event>(&mut self, env: &Env<'_, 'event>,) -> FResult<()> where 'script : 'event {
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
                recursion_limit: env.recursion_limit
            };

            let cont = expr.run(
                ExecOpts { result_needed: true, aggr: AggrType::Tick },
                &env_local,
                &mut no_event,
                &mut state,
                &mut no_meta,
                &mut local_stack
            )?;

            if body_iter.peek().is_none() {
                if let Cont::Cont(value) = cont
                {
                    self.state = value.into_owned().clone_static();
                } else {
                    todo!("No state returned in init! Return a proper error here.");
                }
            }
        }

        Ok(())
    }

    /// Aggregate a value
    pub fn aggregate<'event>(&mut self, args: &[&Value], env: &Env<'_, 'event>) where 'script : 'event {
        let mut body_iter = self.aggregate_body.iter();
        let args = Value::Array(args.iter().map(|v| v.clone_static()).collect());
        while let Some(expr) = body_iter.next() {
            let _cont = expr.run(
                ExecOpts { result_needed: false, aggr: AggrType::Tick },
                &Env {
                    context: env.context,
                    consts: env.consts.with_new_args(&args), // fixme .with_new_args
                    aggrs: &[],
                    meta: env.meta,
                    recursion_limit: env.recursion_limit
                },
                &mut Default::default(),
                &mut Default::default(),
                &mut Default::default(),
                &mut Default::default()
            );
        }
    }

    /// Merge with another instance
    pub fn merge(&mut self, _other: &CustomAggregateFn) {
        todo!("Implement merge")
    }

    /// Emit the state
    pub(crate) fn emit<'event>(&mut self, env: &Env<'_, 'event>) -> FResult<Value<'event>> where 'script : 'event {
        let mut body_iter = self.emit_body.iter();
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
            recursion_limit: env.recursion_limit
        };

        while let Some(expr) = body_iter.next() {
            let cont = expr.run(
                ExecOpts { result_needed: true, aggr: AggrType::Tick },
                &env,
                &mut no_event,
                &mut state,
                &mut no_meta,
                &mut local_stack
            )?;

            if let Cont::Cont(value) = cont
            {
                return Ok(value.into_owned());
            }
        }

        Err(FunctionError::RecursionLimit) // todo return a correct error
    }
}
