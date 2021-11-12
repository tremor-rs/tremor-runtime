use crate::ast::raw::{ExprsRaw, IdentRaw};
use serde::{Serialize, Deserialize};
use crate::impl_expr;
use crate::lexer::{Location, Range};
use super::{BaseExpr, NodeMetas};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum AggregateFnDecl<'input> {
    Normal{start: Location, end: Location, name:IdentRaw<'input>, body:AggregateFnBody<'input>}
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum AggregateFnBody<'input> {
    Normal{
        start: Location,
        end: Location,
        init_body:ExprsRaw<'input>,
        aggregate_args:Vec<IdentRaw<'input>>,
        aggregate_body:ExprsRaw<'input>,
        merge_args:Vec<IdentRaw<'input>>,
        merge_body:ExprsRaw<'input>,
        emit_args:Vec<IdentRaw<'input>>,
        emit_body:ExprsRaw<'input>,
    }
}

impl BaseExpr for AggregateFnDecl<'_> {
    fn mid(&self) -> usize {
        0
    }

    fn s(&self, _meta: &NodeMetas) -> Location {
        match self {
            AggregateFnDecl::Normal { start, .. } => *start
        }
    }

    fn e(&self, _meta: &NodeMetas) -> Location {
        match self {
            AggregateFnDecl::Normal { end, .. } => *end
        }
    }
}

impl BaseExpr for AggregateFnBody<'_> {
    fn mid(&self) -> usize {
        0
    }

    fn s(&self, _meta: &NodeMetas) -> Location {
        match self {
            AggregateFnBody::Normal { start, .. } => *start
        }
    }

    fn e(&self, _meta: &NodeMetas) -> Location {
        match self {
            AggregateFnBody::Normal { end, .. } => *end
        }
    }
}