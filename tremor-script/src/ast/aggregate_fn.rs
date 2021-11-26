use super::{BaseExpr, NodeMetas};
use crate::ast::raw::{ExprsRaw, IdentRaw};
use crate::ast::upable::Upable;
use crate::ast::{Exprs, Helper, Ident};
use crate::lexer::Location;
use halfbrown::HashMap;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RawAggregateFnDecl<'input> {
    /// public because lalrpop
    pub start: Location,
    /// public because lalrpop
    pub end: Location,
    /// public because lalrpop
    pub name: IdentRaw<'input>,
    /// public because lalrpop
    pub body: RawAggregateFnBody<'input>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RawAggregateFnBody<'input> {
    /// public because lalrpop
    pub start: Location,
    /// public because lalrpop
    pub end: Location,
    /// public because lalrpop
    pub init_body: ExprsRaw<'input>,
    /// public because lalrpop
    pub aggregate_args: Vec<IdentRaw<'input>>,
    /// public because lalrpop
    pub aggregate_body: ExprsRaw<'input>,
    /// public because lalrpop
    pub merge_args: Vec<IdentRaw<'input>>,
    /// public because lalrpop
    pub merge_body: ExprsRaw<'input>,
    /// public because lalrpop
    pub emit_args: Vec<IdentRaw<'input>>,
    /// public because lalrpop
    pub emit_body: ExprsRaw<'input>,
}

pub struct AggregateFnDecl<'script> {
    /// public because lalrpop
    pub name: Ident<'script>,
    /// public because lalrpop
    pub init_body: Exprs<'script>,
    /// public because lalrpop
    pub aggregate_args: Vec<Ident<'script>>,
    /// public because lalrpop
    pub aggregate_body: Exprs<'script>,
    /// public because lalrpop
    pub merge_args: Vec<Ident<'script>>,
    /// public because lalrpop
    pub merge_body: Exprs<'script>,
    /// public because lalrpop
    pub emit_args: Vec<Ident<'script>>,
    /// public because lalrpop
    pub emit_body: Exprs<'script>,
}

impl<'script> Upable<'script> for RawAggregateFnDecl<'script> {
    type Target = AggregateFnDecl<'script>;

    fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> crate::ast::visitors::prelude::Result<Self::Target> {
        let mut locals = HashMap::new();
        locals.insert("st".to_string(), 0usize);
        locals.insert("x".to_string(), 1usize);
        helper.swap(&mut Vec::new(), &mut locals);
        Ok(Self::Target {
            name: self.name.up(helper)?,
            init_body: self.body.init_body.up(helper)?,
            aggregate_args: self.body.aggregate_args.up(helper)?,
            aggregate_body: self.body.aggregate_body.up(helper)?,
            merge_args: self.body.merge_args.up(helper)?,
            merge_body: self.body.merge_body.up(helper)?,
            emit_args: self.body.emit_args.up(helper)?,
            emit_body: self.body.emit_body.up(helper)?,
        })
    }
}

impl BaseExpr for RawAggregateFnDecl<'_> {
    fn mid(&self) -> usize {
        self.end.absolute() - self.start.absolute()
    }
}

impl BaseExpr for RawAggregateFnBody<'_> {
    fn mid(&self) -> usize {
        self.end.absolute() - self.start.absolute()
    }
}
