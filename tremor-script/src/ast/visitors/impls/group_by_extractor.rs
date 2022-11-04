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

use super::super::prelude::*;

/// extracts all expressions used within a tremor-query `group by` clause into field `expressions`
pub(crate) struct GroupByExprExtractor<'script> {
    pub(crate) expressions: Vec<ImutExpr<'script>>,
}

impl<'script> GroupByExprExtractor<'script> {
    pub(crate) fn new() -> Self {
        Self {
            expressions: vec![],
        }
    }

    pub(crate) fn extract_expressions(&mut self, group_by: &GroupBy<'script>) {
        self.walk_group_by(group_by);
    }
}

impl<'script> GroupByVisitor<'script> for GroupByExprExtractor<'script> {
    fn visit_expr(&mut self, expr: &ImutExpr<'script>) {
        self.expressions.push(expr.clone()); // take this, lifetimes (yes, i am stupid)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        ast::{GroupBy, List},
        NodeMeta,
    };
    use tremor_value::Value;

    use super::*;
    #[test]
    fn test_group_expr_extractor() {
        let mut visitor = GroupByExprExtractor::new();
        let lit_42 = ImutExpr::literal(NodeMeta::dummy(), Value::from(42));
        let false_array = ImutExpr::List(List {
            mid: NodeMeta::dummy(),
            exprs: vec![ImutExpr::literal(NodeMeta::dummy(), Value::from(false))],
        });
        let group_by = GroupBy::Set {
            mid: NodeMeta::dummy(),
            items: vec![
                GroupBy::Expr {
                    mid: NodeMeta::dummy(),
                    expr: lit_42.clone(),
                },
                GroupBy::Each {
                    mid: NodeMeta::dummy(),
                    expr: false_array.clone(),
                },
            ],
        };
        visitor.extract_expressions(&group_by);
        assert_eq!(2, visitor.expressions.len());
        assert_eq!(&[lit_42, false_array], visitor.expressions.as_slice());
    }
}
