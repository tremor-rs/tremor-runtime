use crate::ast::raw::{ExprsRaw, IdentRaw};

pub enum AggregateFnDecl<'input> {
    Normal{name:IdentRaw<'input>, body:AggregateFnBody<'input>}
}

pub enum AggregateFnBody<'input> {
    Normal{
        init_body:ExprsRaw<'input>,
        aggregate_args:Vec<IdentRaw<'input>>,
        aggregate_body:ExprsRaw<'input>,
        merge_args:Vec<IdentRaw<'input>>,
        merge_body:ExprsRaw<'input>,
        emit_args:Vec<IdentRaw<'input>>,
        emit_body:ExprsRaw<'input>,
    }
}