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

use super::{
    ArrayPattern, ArrayPredicatePattern, AssignPattern, Expression, ImutExprInt, Pattern,
    PredicateClause, PredicatePattern, RecordPattern, TestExpr, TuplePattern,
};

struct Cost {}
impl Cost {
    /// we consider this free
    const FREE: u64 = 0;
    /// Cheap operations such as constant or variable lookups, simple comparisons, singe key lookups
    /// and so on
    const CONST: u64 = 10;

    /// Cost for a average calculation, multiple lookups and comparisons, somewhat nested structures
    const AVERAGE: u64 = 100;
}

pub(crate) trait Costly {
    fn cost(&self) -> u64;
}

impl Costly for TestExpr {
    fn cost(&self) -> u64 {
        self.extractor.cost()
    }
}

impl<'script, Ex: Expression + 'script> Costly for PredicateClause<'script, Ex> {
    fn cost(&self) -> u64 {
        let g = if self.guard.is_some() {
            Cost::CONST
        } else {
            Cost::FREE
        };
        g + self.pattern.cost()
    }
}

impl<'script> Costly for Pattern<'script> {
    fn cost(&self) -> u64 {
        match self {
            Pattern::DoNotCare | Pattern::Default => Cost::FREE,
            Pattern::Expr(_) => Cost::CONST,
            Pattern::Record(r) => r.cost(),
            Pattern::Array(a) => a.cost(),
            Pattern::Assign(a) => a.cost(),
            Pattern::Tuple(t) => t.cost(),
            Pattern::Extract(e) => e.cost(),
        }
    }
}

impl<'script> Costly for PredicatePattern<'script> {
    fn cost(&self) -> u64 {
        match self {
            PredicatePattern::FieldPresent { .. } | PredicatePattern::FieldAbsent { .. } => {
                Cost::CONST
            }
            PredicatePattern::Bin {
                rhs: ImutExprInt::Literal(_),
                ..
            } => Cost::CONST * 2,
            PredicatePattern::Bin { .. } => Cost::AVERAGE,
            PredicatePattern::TildeEq { test, .. } => test.cost(),
            PredicatePattern::RecordPatternEq { pattern, .. } => pattern.cost(),
            PredicatePattern::ArrayPatternEq { pattern, .. } => pattern.cost(),
        }
    }
}

impl<'script> Costly for RecordPattern<'script> {
    fn cost(&self) -> u64 {
        self.fields.iter().map(|f| f.cost() + Cost::AVERAGE).sum()
    }
}

impl<'script> Costly for ArrayPredicatePattern<'script> {
    fn cost(&self) -> u64 {
        match self {
            ArrayPredicatePattern::Ignore => 1,
            ArrayPredicatePattern::Expr(ImutExprInt::Literal(_))
            | ArrayPredicatePattern::Expr(_) => Cost::CONST,
            ArrayPredicatePattern::Tilde(t) => t.cost(),
            ArrayPredicatePattern::Record(r) => r.cost(),
        }
    }
}

// For each pattern in the array we iterate over the entire target array, so 3 patterns on a
// 10 element array iterate over 30 elements.
// We don't know how many elements will be there in the final array but a conservative approximation
//  is that if there aer `n` different patterns the array it is looking at will have at least `n`
// elements.
impl<'script> Costly for ArrayPattern<'script> {
    fn cost(&self) -> u64 {
        let s: u64 = self.exprs.iter().map(Costly::cost).sum();
        s * (self.exprs.len() as u64)
    }
}

impl<'script> Costly for AssignPattern<'script> {
    fn cost(&self) -> u64 {
        self.pattern.cost()
    }
}
impl<'script> Costly for TuplePattern<'script> {
    fn cost(&self) -> u64 {
        self.exprs.iter().map(Costly::cost).sum()
    }
}
