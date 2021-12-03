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

use super::BaseExpr;
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
    pub init: RawInitDecl<'input>,
    /// public because lalrpop
    pub aggregate: RawAggregateDecl<'input>,
    /// public because lalrpop
    pub merge: RawMergeInDecl<'input>,
    /// public because lalrpop
    pub emit: RawEmitDecl<'input>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RawInitDecl<'input>(pub ExprsRaw<'input>);
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RawAggregateDecl<'input>(pub Vec<IdentRaw<'input>>, pub ExprsRaw<'input>);
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RawMergeInDecl<'input>(pub Vec<IdentRaw<'input>>, pub ExprsRaw<'input>);
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RawEmitDecl<'input>(pub Vec<IdentRaw<'input>>, pub ExprsRaw<'input>);

pub struct FnDecl<'script> {
    /// public because lalrpop
    pub name: Ident<'script>,
    /// public because lalrpop
    pub init: InitDecl<'script>,
    /// public because lalrpop
    pub aggregate: AggregateDecl<'script>,
    /// public because lalrpop
    pub merge: MergeInDecl<'script>,
    /// public because lalrpop
    pub emit: EmitDecl<'script>,
}

pub struct InitDecl<'script>(pub Exprs<'script>);
pub struct AggregateDecl<'script>(pub Vec<Ident<'script>>, pub Exprs<'script>);
pub struct MergeInDecl<'script>(pub Vec<Ident<'script>>, pub Exprs<'script>);
pub struct EmitDecl<'script>(pub Vec<Ident<'script>>, pub Exprs<'script>);

impl<'script> Upable<'script> for RawInitDecl<'script> {
    type Target = InitDecl<'script>;

    fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> crate::ast::visitors::prelude::Result<Self::Target> {
        Ok(InitDecl(self.0.up(helper)?))
    }
}

impl<'script> Upable<'script> for RawAggregateDecl<'script> {
    type Target = AggregateDecl<'script>;

    fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> crate::ast::visitors::prelude::Result<Self::Target> {
        let RawAggregateDecl(args, body) = self;
        let args: Vec<Ident> = args
            .into_iter()
            .map(|x| x.up(helper).expect("booo booo"))
            .collect();

        let mut locals = HashMap::new();
        for (i, arg_name) in args.iter().enumerate() {
            locals.insert(arg_name.to_string(), i);
        }

        Ok(AggregateDecl(args, body.up(helper)?))
    }
}

impl<'script> Upable<'script> for RawMergeInDecl<'script> {
    type Target = MergeInDecl<'script>;

    fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> crate::ast::visitors::prelude::Result<Self::Target> {
        let RawMergeInDecl(args, body) = self;
        let args: Vec<Ident> = args
            .into_iter()
            .map(|x| x.up(helper).expect("booo booo"))
            .collect();

        let mut locals = HashMap::new();
        for (i, arg_name) in args.iter().enumerate() {
            locals.insert(arg_name.to_string(), i);
        }

        Ok(MergeInDecl(args, body.up(helper)?))
    }
}

impl<'script> Upable<'script> for RawEmitDecl<'script> {
    type Target = EmitDecl<'script>;

    fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> crate::ast::visitors::prelude::Result<Self::Target> {
        let RawEmitDecl(args, body) = self;
        let args: Vec<Ident> = args
            .into_iter()
            .map(|x| x.up(helper).expect("booo booo"))
            .collect();

        let mut locals = HashMap::new();
        for (i, arg_name) in args.iter().enumerate() {
            locals.insert(arg_name.to_string(), i);
        }

        Ok(EmitDecl(args, body.up(helper)?))
    }
}

impl<'script> Upable<'script> for RawAggregateFnDecl<'script> {
    type Target = FnDecl<'script>;

    fn up<'registry>(
        self,
        helper: &mut Helper<'script, 'registry>,
    ) -> crate::ast::visitors::prelude::Result<Self::Target> {
        Ok(Self::Target {
            name: self.name.up(helper)?,
            init: self.body.init.up(helper)?,
            aggregate: self.body.aggregate.up(helper)?,
            merge: self.body.merge.up(helper)?,
            emit: self.body.emit.up(helper)?,
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
