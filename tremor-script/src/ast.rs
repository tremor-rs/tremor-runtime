// Copyright 2018-2019, Wayfair GmbH
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

use crate::pos::Location;
use crate::tilde::Extractor;
use std::fmt;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Script {
    pub exprs: Exprs,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Ident {
    pub id: String,
}

pub trait BaseExpr {
    fn s(&self) -> Location;
    fn e(&self) -> Location;
    fn extent(&self) -> (Location, Location) {
        (self.s(), self.e())
    }
}

macro_rules! impl_expr {
    ($name:ident) => {
        impl BaseExpr for $name {
            fn s(&self) -> Location {
                self.start
            }

            fn e(&self) -> Location {
                self.end
            }
        }
    };
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TodoExpr {
    pub start: Location,
    pub end: Location,
}
impl_expr!(TodoExpr);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub start: Location,
    pub end: Location,
    pub name: String,
    pub value: Box<Expr>,
}
impl_expr!(Field);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub start: Location,
    pub end: Location,
    pub fields: Fields,
}
impl_expr!(Record);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Literal {
    pub start: Location,
    pub end: Location,
    pub value: LiteralValue,
}

impl_expr!(Literal);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LiteralValue {
    Native(simd_json::OwnedValue),
    List(Exprs),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    #[allow(unused)]
    FieldExpr(Box<Field>),
    RecordExpr(Record),
    MatchExpr(Box<Match>),
    PatchExpr(Box<Patch>),
    MergeExpr(Box<Merge>),
    Path(Path),
    Literal(Literal),
    Binary(BinExpr),
    Unary(UnaryExpr),
    Invoke(Invoke),
    Let(Let),
    Assign(Assign),
    Comprehension(Comprehension),
    Drop(DropExpr),
    Emit(EmitExpr),
    Test(TestExpr),
    //    PatternExpr(Box<Pattern>),
}

impl Expr {
    pub fn dummy(start: Location, end: Location) -> Self {
        Expr::Literal(Literal {
            value: LiteralValue::Native("dummy".into()),
            start,
            end,
        })
    }
    pub fn dummy_from<T: BaseExpr>(from: &T) -> Self {
        Expr::dummy(from.s(), from.e())
    }
}
impl BaseExpr for Expr {
    fn s(&self) -> Location {
        match self {
            Expr::FieldExpr(e) => e.s(),
            Expr::RecordExpr(e) => e.s(),
            Expr::MatchExpr(e) => e.s(),
            Expr::PatchExpr(e) => e.s(),
            Expr::MergeExpr(e) => e.s(),
            Expr::Path(e) => e.s(),
            Expr::Literal(e) => e.s(),
            Expr::Binary(e) => e.s(),
            Expr::Unary(e) => e.s(),
            Expr::Invoke(e) => e.s(),
            Expr::Let(e) => e.s(),
            Expr::Assign(e) => e.s(),
            Expr::Comprehension(e) => e.s(),
            Expr::Drop(e) => e.s(),
            Expr::Emit(e) => e.s(),
            Expr::Test(e) => e.s(),
            //            Expr::PatternExpr(e) => Location::default(), // FIXME: e.s()
        }
    }
    fn e(&self) -> Location {
        match self {
            Expr::FieldExpr(e) => e.e(),
            Expr::RecordExpr(e) => e.e(),
            Expr::MatchExpr(e) => e.e(),
            Expr::PatchExpr(e) => e.e(),
            Expr::MergeExpr(e) => e.e(),
            Expr::Path(e) => e.e(),
            Expr::Literal(e) => e.e(),
            Expr::Binary(e) => e.e(),
            Expr::Unary(e) => e.e(),
            Expr::Invoke(e) => e.e(),
            Expr::Let(e) => e.e(),
            Expr::Assign(e) => e.e(),
            Expr::Comprehension(e) => e.e(),
            Expr::Drop(e) => e.e(),
            Expr::Emit(e) => e.e(),
            Expr::Test(e) => e.e(),
            //            Expr::PatternExpr(e) => Location::default(), //FIXME: e.e(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DropExpr {
    pub start: Location,
    pub end: Location,
    pub expr: Box<Expr>,
}
impl_expr!(DropExpr);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EmitExpr {
    pub start: Location,
    pub end: Location,
    pub expr: Box<Expr>,
}
impl_expr!(EmitExpr);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Let {
    pub start: Location,
    pub end: Location,
    pub exprs: Exprs,
}
impl_expr!(Let);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Assign {
    pub start: Location,
    pub end: Location,
    pub path: Path,
    pub expr: Box<Expr>,
}
impl_expr!(Assign);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Invoke {
    pub start: Location,
    pub end: Location,
    pub module: String,
    pub fun: String,
    pub args: Exprs,
}
impl_expr!(Invoke);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestExpr {
    pub start: Location,
    pub end: Location,
    pub id: String,
    pub test: String,
    pub extractor: Extractor,
}
impl_expr!(TestExpr);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Match {
    pub start: Location,
    pub end: Location,
    pub target: Expr,
    pub patterns: Predicates,
}
impl_expr!(Match);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredicateClause {
    pub pattern: Pattern,
    pub guard: Option<Box<Expr>>,
    pub exprs: Exprs,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Patch {
    pub start: Location,
    pub end: Location,
    pub target: Expr,
    pub operations: PatchOperations,
}
impl_expr!(Patch);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PatchOperation {
    Insert { ident: String, expr: Expr },
    Upsert { ident: String, expr: Expr },
    Update { ident: String, expr: Expr },
    Erase { ident: String },
    Merge { ident: String, expr: Expr },
    TupleMerge { expr: Expr },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Merge {
    pub start: Location,
    pub end: Location,
    pub target: Expr,
    pub expr: Expr,
}
impl_expr!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Comprehension {
    pub start: Location,
    pub end: Location,
    pub target: Box<Expr>,
    pub cases: ComprehensionCases,
}
impl_expr!(Comprehension);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComprehensionCase {
    pub start: Location,
    pub end: Location,
    pub key_name: String,
    pub value_name: String,
    pub guard: Option<Box<Expr>>,
    pub expr: Box<Expr>,
}
impl_expr!(ComprehensionCase);

// NOTE: We do not want the pain of having boxes here
// this is the script it does only rarely get allocated
// so the memory isn't that much of an issue.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Pattern {
    Predicate(PredicatePattern),
    Record(RecordPattern),
    Array(ArrayPattern),
    Expr(Expr),
    Assign(AssignPattern),
    Default,
}

pub trait BasePattern {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PredicatePattern {
    TildeEq {
        assign: String,
        lhs: String,
        test: TestExpr,
    },
    Eq {
        lhs: String,
        rhs: Expr,
        not: bool,
    },
    RecordPatternEq {
        lhs: String,
        pattern: RecordPattern,
    },
    ArrayPatternEq {
        lhs: String,
        pattern: ArrayPattern,
    },
}

impl PredicatePattern {
    pub fn lhs(&self) -> &str {
        use PredicatePattern::*;
        match self {
            TildeEq { lhs, .. } => &lhs,
            Eq { lhs, .. } => &lhs,
            RecordPatternEq { lhs, .. } => &lhs,
            ArrayPatternEq { lhs, .. } => &lhs,
        }
    }
}

/*
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredicatePattern {
    pub kind: PredicatePatternKind,
    pub lhs: String,
    pub rhs: Expr,
    pub rp: Option<RecordPattern>,
    pub ap: Option<ArrayPattern>,
    pub assign: Option<Path>,
}
*/

impl BasePattern for PredicatePattern {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordPattern {
    pub start: Location,
    pub end: Location,
    pub fields: PatternFields,
}
impl_expr!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PatternField {
    pub pattern: Box<PredicatePattern>,
}
impl BasePattern for RecordPattern {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ArrayPredicatePattern {
    Expr(Expr),
    Tilde(TestExpr),
    Record(RecordPattern),
    Array(ArrayPattern),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArrayPattern {
    pub start: Location,
    pub end: Location,
    pub exprs: ArrayPredicatePatterns,
}
impl BasePattern for ArrayPattern {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssignPattern {
    pub id: Path,
    pub pattern: Box<Pattern>,
}
impl BasePattern for AssignPattern {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Path {
    Local(LocalPath),
    Event(EventPath),
    Meta(MetadataPath),
}

impl BaseExpr for Path {
    fn s(&self) -> Location {
        match self {
            Path::Local(e) => e.s(),
            Path::Meta(e) => e.s(),
            Path::Event(e) => e.s(),
        }
    }
    fn e(&self) -> Location {
        match self {
            Path::Local(e) => e.e(),
            Path::Meta(e) => e.e(),
            Path::Event(e) => e.e(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Segment {
    ElementSelector {
        expr: Expr,
        start: Location,
        end: Location,
    },
    RangeSelector {
        start_lower: Location,
        range_start: Expr,
        end_lower: Location,
        start_upper: Location,
        range_end: Expr,
        end_upper: Location,
    },
}

impl Segment {
    pub fn from_id(id: Ident, start: Location, end: Location) -> Self {
        Segment::ElementSelector {
            start,
            end,
            expr: Expr::Literal(Literal {
                start,
                end,
                value: LiteralValue::Native(id.id.into()),
            }),
        }
    }
}

impl BaseExpr for Segment {
    fn s(&self) -> Location {
        match self {
            Segment::ElementSelector { start, .. } => *start,
            Segment::RangeSelector { start_lower, .. } => *start_lower,
        }
    }
    fn e(&self) -> Location {
        match self {
            Segment::ElementSelector { end, .. } => *end,
            Segment::RangeSelector { end_upper, .. } => *end_upper,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocalPath {
    pub start: Location,
    pub end: Location,
    pub segments: Segments,
}
impl_expr!(LocalPath);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataPath {
    pub start: Location,
    pub end: Location,
    pub segments: Segments,
}
impl_expr!(MetadataPath);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventPath {
    pub start: Location,
    pub end: Location,
    pub segments: Segments,
}
impl_expr!(EventPath);

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BinOpKind {
    Or,
    And,
    Eq,
    NotEq,

    Gte,
    Gt,
    Lte,
    Lt,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl fmt::Display for BinOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinOpKind::Or => write!(f, "or"),
            BinOpKind::And => write!(f, "and"),

            BinOpKind::Eq => write!(f, "=="),
            BinOpKind::NotEq => write!(f, "!="),
            BinOpKind::Gte => write!(f, ">="),
            BinOpKind::Gt => write!(f, ">"),
            BinOpKind::Lte => write!(f, "<="),
            BinOpKind::Lt => write!(f, "<"),

            BinOpKind::Add => write!(f, "+"),
            BinOpKind::Sub => write!(f, "-"),
            BinOpKind::Mul => write!(f, "*"),
            BinOpKind::Div => write!(f, "/"),
            BinOpKind::Mod => write!(f, "%"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinExpr {
    pub start: Location,
    pub end: Location,
    pub kind: BinOpKind,
    pub lhs: Box<Expr>,
    pub rhs: Box<Expr>,
}
impl_expr!(BinExpr);

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UnaryOpKind {
    Plus,
    Minus,
    Not,
}
impl fmt::Display for UnaryOpKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UnaryOpKind::Plus => write!(f, "+"),
            UnaryOpKind::Minus => write!(f, "-"),
            UnaryOpKind::Not => write!(f, "not"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryExpr {
    pub start: Location,
    pub end: Location,
    pub kind: UnaryOpKind,
    pub expr: Box<Expr>,
}
impl_expr!(UnaryExpr);

pub type Exprs = Vec<Expr>;
pub type Fields = Vec<Box<Field>>;
pub type Segments = Vec<Segment>;
pub type PatternFields = Vec<PatternField>;
pub type Predicates = Vec<PredicateClause>;
pub type PatchOperations = Vec<PatchOperation>;
pub type ComprehensionCases = Vec<ComprehensionCase>;
pub type ArrayPredicatePatterns = Vec<ArrayPredicatePattern>;
