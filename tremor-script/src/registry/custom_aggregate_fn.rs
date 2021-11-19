use crate::ast::Exprs;
use crate::Value;
use beef::Cow;
use crate::registry::FResult;

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
}

impl<'script> CustomAggregateFn<'script> {
    /// Initialize the instance of aggregate function
    pub fn init(&mut self) {
        todo!("Implement init");
    }

    /// Aggregate a value
    pub fn aggregate(&mut self, _args: &[&Value]) {
        todo!("Implement aggregate");
    }

    /// Merge with another instance
    pub fn merge(&mut self, _other: &CustomAggregateFn) {
        todo!("Implement merge")
    }

    /// Emit the state
    pub(crate) fn emit(&self) -> FResult<Value> {
        todo!("Implement emit")
    }
}
