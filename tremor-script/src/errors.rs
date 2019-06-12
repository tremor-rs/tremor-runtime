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

// NOTE: We node this because of error_chain
#![allow(clippy::large_enum_variant)]
#![allow(deprecated)]

use crate::ast::{self, BaseExpr, Expr};
use crate::lexer;
use crate::pos;
use crate::pos::Location;
use base64;
use error_chain::error_chain;
use glob;
use lalrpop_util;
use regex;
use serde_json;

pub use simd_json::ValueType;
use simd_json::{BorrowedValue as Value, ValueTrait};
use std::num;
use std::ops::Range;

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Error) -> bool {
        // This might be Ok since we try to compare Result in tets
        false
    }
}

impl From<lexer::LexerError> for Error {
    fn from(error: lexer::LexerError) -> Self {
        ErrorKind::LexerError(error).into()
    }
}

type ParserError<'screw_lalrpop> =
    lalrpop_util::ParseError<pos::Location, lexer::Token<'screw_lalrpop>, lexer::LexerError>;

impl<'screw_lalrpop> From<ParserError<'screw_lalrpop>> for Error {
    fn from(error: ParserError<'screw_lalrpop>) -> Self {
        ErrorKind::ParserError(format!("{:?}", error)).into()
    }
}

// We need this since we call objects records
fn t2s(t: ValueType) -> &'static str {
    match t {
        ValueType::Null => "null",
        ValueType::Bool => "boolean",
        ValueType::String => "string",
        ValueType::I64 => "integer",
        ValueType::F64 => "float",
        ValueType::Array => "array",
        ValueType::Object => "record",
    }
}

pub type ErrorLocation = (Option<(Location, Location)>, Option<(Location, Location)>);
// FIXME: Option<(Location, Location, Option<(Location, Location)>)>
impl ErrorKind {
    pub fn expr(&self) -> ErrorLocation {
        use ErrorKind::*;
        match self {
            BadAccessInEvent(expr, inner, _) => (Some(expr.extent()), Some(inner.extent())),
            BadAccessInLocal(expr, inner, _) => (Some(expr.extent()), Some(inner.extent())),
            BadAccessInGlobal(expr, inner, _) => (Some(expr.extent()), Some(inner.extent())),
            InvalidBinary(expr, inner, _, _, _) => (Some(expr.extent()), Some(inner.extent())),
            InvalidDrop(expr, inner) => (Some(expr.extent()), Some(inner.extent())),
            InvalidEmit(expr, inner) => (Some(expr.extent()), Some(inner.extent())),
            InvalidUnary(expr, inner, _, _) => (Some(expr.extent()), Some(inner.extent())),
            NoClauseHit(expr) => (Some(expr.extent()), None),
            Oops(expr) => (Some(expr.extent()), None),
            PredicateInMatch(expr) => (Some(expr.extent()), None),
            TypeConflict(expr, inner, _, _) => (Some(expr.extent()), Some(inner.extent())),
            _ => (None, None),
        }
    }
    pub fn hint(&self) -> Option<String> {
        use ErrorKind::*;
        match self {
            BadAccessInLocal(_, _, key) if distance::damerau_levenshtein(key, "event") <= 2 => {
                Some("Did you mean to use event?".to_owned())
            }
            TypeConflict(_, _, ValueType::F64, expected) => match expected.as_slice() {
                [ValueType::I64] => Some(
                    "You can use math::trunc() and related functions to ensure numbers are integers."
                        .to_owned(),
                ),
                _ => None
            },
            NoClauseHit(_) => Some("Consider adding a `default => null` clause at the end of your match or validate full coverage beforehand.".into()),
            Oops(_) => Some("Please take the error output script and idealy data and open a ticket, this should not happen.".into()),
            _ => None,
        }
    }
}

impl Error {
    pub fn context(&self) -> ErrorLocation {
        self.0.expr()
    }
    pub fn hint(&self) -> Option<String> {
        self.0.hint()
    }
}

error_chain! {
    foreign_links {
        RegexError(regex::Error);
        SerdeError(serde_json::Error);
        ParseIntError(num::ParseIntError);
        SIMDError(simd_json::Error);
        GlobError(glob::PatternError);
        Base64Error(base64::DecodeError);
    }
    errors {
        /*
         * Generic
         */
        EmptyScript {
            description("No expressions were found in the script")
                display("No expressions were found in the script")
        }
        TypeConflict(expr: Expr, inner: Expr, got: ValueType, expected: Vec<ValueType>) {
            description("Conflicting types")
                display("Conflicting types, got {} but expected {}", t2s(*got), if expected.len() == 1 {
                    t2s(expected[0]).to_string()
                } else {
                    let expected:  Vec<&str> = expected.iter().map(|t| t2s(*t)).collect();
                    format!("one of {:?}", expected)
                })
        }
        Oops(expr: Expr) {
            description("Something went wrong and we're not sure what it was")
                display("Something went wrong and we're not sure what it was")
        }
        /*
         * Functions
         */
        BadArrity(m: String, f: String, a: usize) {
            description("Bad arrity for function")
                display("Bad arrity for function {}::{}/{}", m, f, a)
        }
        MissingModule(m: String) {
            description("Call to undefined module")
                display("Call to undefined module {}", m)
        }
        MissingFunction(m: String, f: String) {
            description("Call to undefined function")
                display("Call to undefined function {}::{}", m, f)
        }
        BadType(m: String, f: String, a: usize) {
            description("Bad type passed to function")
                display("Bad type passed to function {}::{}/{}", m, f, a)
        }
        RuntimeError(m: String, f: String,  a: usize, c: String) {
            description("Runtime error in function")
                display("Runtime error in function {}::{}/{}: {}", m, f, a, c)
        }
        /*
         * Lexer and Parser
         */
        LexerError(e: crate::lexer::LexerError) {
            description("Lexical error tokenizing tremor-script")
                display("Lexical error tokenizing treor-script {}", e)
        }
        ParserError(pos: String) {
            description("Parser error")
                display("Parser error at {}", pos)
        }
        /*
         * Resolve / Assign path walking
         */

        BadAccessInLocal(expr: Expr, inner: Expr, key: String ) {
            description("Trying to access a non existing local key")
                display("Trying to access a non existing local key `{}`", key)
        }
        BadAccessInGlobal(expr: Expr, inner: Expr, key: String) {
            description("Trying to access a non existing global key")
                display("Trying to access a non existing global key `{}`", key)
        }
        BadAccessInEvent(expr: Expr, inner: Expr, key: String) {
            description("Trying to access a non existing event key")
                display("Trying to access a non existing event key `{}`", key)
        }
        ArrayOutOfRange(expr: Expr, inner: Expr, r: Range<usize>) {
            description("Trying to access an index that is out of range")
                display("Trying to access an index that is out of range {}", if r.start == r.end {
                    format!("{}", r.start)
                } else {
                    format!("{}..{}", r.start, r.end)
                })
        }
        AssignIntoArray(expor: Expr, inner: Expr) {
            description("Can not assign tinto a array")
                display("It is not supported to assign value into an array")
        }
        /*
         * Emit & Drop
         */
        InvalidEmit(expr: Expr, inner: Expr) {
            description("Can not emit from this location")
                display("Can not emit from this location")

        }
        InvalidDrop(expr: Expr, inner: Expr) {
            description("Can not drop from this location")
                display("Can not drop from this location")

        }
        /*
         * Operators
         */
        InvalidUnary(expr: Expr, inner: Expr, op: ast::UnaryOpKind, val: ValueType) {
            description("Invalid ynary operation")
                display("The unary operation operation `{}` is not defined for the type `{}`", op, t2s(*val))
        }
        InvalidBinary(expr: Expr, inner: Expr, op: ast::BinOpKind, left: ValueType, right: ValueType) {
            description("Invalid ynary operation")
                display("The binary operation operation `{}` is not defined for the type `{}` and `{}`", op, t2s(*left), t2s(*right))
        }
        /*
         * match
         */
        InvalidExtractor(expr: Expr){
            description("Not a valid tilde predicate pattern")
                display("Not a valid tilde predicate pattern")
        }
        PredicateInMatch(expr: Expr){
            description("Not supported in match case clauses")
                display("Not supported in match case clauses")
        }
        NoClauseHit(expr: Expr){
            description("A match expression executed bu no clause matched")
                display("A match expression executed bu no clause matched")
        }
        /*
         * Patch
         */
        InsertKeyExists(expr: Expr, inner: Expr, key:String) {
            description("The key that is suposed to be inserted already exists")
                display("The key that is suposed to be inserted already exists: {}", key)
        }
        UpdateKeyMissing(expr: Expr, inner: Expr, key:String) {
            description("The key that is suposed to be updated does not exists")
                display("The key that is suposed to be updated does not exists: {}", key)
        }

        MergeTypeConflict(expr: Expr, inner: Expr, key:String, val: ValueType) {
            description("Merge can only be performed on keys that either do not exist or are records")
                display("Merge can only be performed on keys that either do not exist or are records but the key '{}' has the type {}", key, t2s(*val))
        }

        OverwritingLocal(expr: Expr, inner: Expr, val: String) {
            description("Trying to overwrite a local variable in a comprehension case")
                display("Trying to overwrite the local variable `{}` in a comprehension case", val)
        }
    }
}

impl Expr {
    pub fn error_array_out_of_bound<T>(
        &self,
        inner: &Expr,
        path: &ast::Path,
        r: Range<usize>,
    ) -> Result<T> {
        let expr: ast::Expr = self.clone();
        Err(match path {
            ast::Path::Local(_path) => ErrorKind::ArrayOutOfRange(expr, inner.clone(), r).into(),
            ast::Path::Meta(_path) => ErrorKind::ArrayOutOfRange(expr, inner.clone(), r).into(),
            ast::Path::Event(_path) => ErrorKind::ArrayOutOfRange(expr, inner.clone(), r).into(),
        })
    }

    pub fn error_bad_key<T>(&self, inner: &Expr, path: &ast::Path, key: String) -> Result<T> {
        let expr: ast::Expr = self.clone();
        Err(match path {
            ast::Path::Local(_p) => ErrorKind::BadAccessInLocal(expr, inner.clone(), key).into(),
            ast::Path::Meta(_p) => ErrorKind::BadAccessInGlobal(expr, inner.clone(), key).into(),
            ast::Path::Event(_p) => ErrorKind::BadAccessInEvent(expr, inner.clone(), key).into(),
        })
    }
    pub fn error_type_conflict_mult<T>(
        &self,
        inner: &ast::Expr,
        got: ValueType,
        expected: Vec<ValueType>,
    ) -> Result<T> {
        Err(ErrorKind::TypeConflict(self.clone(), inner.clone(), got, expected).into())
    }

    pub fn error_type_conflict<T>(
        &self,
        inner: &ast::Expr,
        got: ValueType,
        expected: ValueType,
    ) -> Result<T> {
        self.error_type_conflict_mult(inner, got, vec![expected])
    }

    pub fn error_guard_not_bool<T>(&self, inner: &ast::Expr, got: &Value) -> Result<T> {
        self.error_type_conflict(inner, got.kind(), ValueType::Bool)
    }

    pub fn error_invalid_unary<T>(
        &self,
        inner: &ast::Expr,
        op: ast::UnaryOpKind,
        val: &Value,
    ) -> Result<T> {
        Err(ErrorKind::InvalidUnary(self.clone(), inner.clone(), op, val.kind()).into())
    }
    pub fn error_invalid_binary<T>(
        &self,
        inner: &ast::Expr,
        op: ast::BinOpKind,
        left: &Value,
        right: &Value,
    ) -> Result<T> {
        Err(
            ErrorKind::InvalidBinary(self.clone(), inner.clone(), op, left.kind(), right.kind())
                .into(),
        )
    }
    pub fn error_predicate_in_match<T>(&self) -> Result<T> {
        Err(ErrorKind::PredicateInMatch(self.clone()).into())
    }
    pub fn error_no_clause_hit<T>(&self) -> Result<T> {
        Err(ErrorKind::NoClauseHit(self.clone()).into())
    }

    pub fn error_oops<T>(&self) -> Result<T> {
        Err(ErrorKind::Oops(self.clone()).into())
    }

    pub fn error_patch_insert_key_exists<T>(&self, inner: &ast::Expr, key: String) -> Result<T> {
        Err(ErrorKind::InsertKeyExists(self.clone(), inner.clone(), key).into())
    }

    pub fn error_patch_update_key_missing<T>(&self, inner: &ast::Expr, key: String) -> Result<T> {
        Err(ErrorKind::UpdateKeyMissing(self.clone(), inner.clone(), key).into())
    }
    pub fn error_overwriting_local_in_comprehension<T>(
        &self,
        inner: &ast::Expr,
        key: String,
    ) -> Result<T> {
        Err(ErrorKind::OverwritingLocal(self.clone(), inner.clone(), key).into())
    }

    pub fn error_patch_merge_type_conflict<T>(
        &self,
        inner: &ast::Expr,
        key: String,
        val: &Value,
    ) -> Result<T> {
        Err(ErrorKind::MergeTypeConflict(self.clone(), inner.clone(), key, val.kind()).into())
    }

    pub fn error_assign_array<T>(&self, inner: &ast::Expr) -> Result<T> {
        Err(ErrorKind::AssignIntoArray(self.clone(), inner.clone()).into())
    }
}
