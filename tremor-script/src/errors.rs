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
#![allow(unused_imports)]

use crate::ast::{self, BaseExpr, Expr};
use crate::errors;
use crate::lexer;
use crate::pos;
use crate::pos::{Location, Range};
use crate::registry::Context;
use base64;
use dissect;
use error_chain::error_chain;
use glob;
use grok;
use lalrpop_util;
use lalrpop_util::ParseError as LalrpopError;
use regex;
use serde_json;
pub use simd_json::ValueType;
use simd_json::{BorrowedValue as Value, ValueTrait};
use std::num;
use std::ops::Range as IRange;

#[macro_export]
macro_rules! stry {
    ($e:expr) => {
        match $e {
            ::std::result::Result::Ok(val) => val,
            ::std::result::Result::Err(err) => return ::std::result::Result::Err(err),
        }
    };
}

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Error) -> bool {
        // This might be Ok since we try to compare Result in tets
        false
    }
}

type ParserError<'screw_lalrpop> =
    lalrpop_util::ParseError<pos::Location, lexer::Token<'screw_lalrpop>, errors::Error>;

impl<'screw_lalrpop> From<ParserError<'screw_lalrpop>> for Error {
    fn from(error: ParserError<'screw_lalrpop>) -> Self {
        match error {
            LalrpopError::UnrecognizedToken {
                token: (start, token, end),
                expected,
            } => ErrorKind::UnrecognizedToken(
                (start.move_up_lines(2), end.move_down_lines(2)).into(),
                (start, end).into(),
                token.to_string(),
                expected.iter().map(|s| s.replace('"', "`")).collect(),
            )
            .into(),
            LalrpopError::ExtraToken {
                token: (start, token, end),
            } => ErrorKind::ExtraToken(
                (start.move_up_lines(2), end.move_down_lines(2)).into(),
                (start, end).into(),
                token.to_string(),
            )
            .into(),
            LalrpopError::InvalidToken { location: start } => {
                let mut end = start;
                end.column.0 += 1;
                ErrorKind::InvalidToken(
                    (start.move_up_lines(2), end.move_down_lines(2)).into(),
                    (start, end).into(),
                )
                .into()
            }
            _ => ErrorKind::ParserError(format!("{:?}", error)).into(),
        }
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

pub fn best_hint(given: &str, options: &[String], max_dist: usize) -> Option<(usize, String)> {
    options
        .iter()
        .filter_map(|option| {
            let d = distance::damerau_levenshtein(given, &option);
            if d <= max_dist {
                Some((d, option))
            } else {
                None
            }
        })
        .min()
        .map(|(d, s)| (d, s.clone()))
}
pub type ErrorLocation = (Option<Range>, Option<Range>);
// FIXME: Option<(Location, Location, Option<(Location, Location)>)>
impl ErrorKind {
    pub fn expr(&self) -> ErrorLocation {
        use ErrorKind::*;
        match self {
            ArrayOutOfRange(outer, inner, _) => (Some(*outer), Some(*inner)),
            AssignIntoArray(outer, inner) => (Some(*outer), Some(*inner)),
            BadAccessInEvent(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            BadAccessInGlobal(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            BadAccessInLocal(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            BadArity(outer, inner, _, _, _, _) => (Some(*outer), Some(*inner)),
            BadType(outer, inner, _, _, _) => (Some(*outer), Some(*inner)),
            BinaryDrop(outer, inner) => (Some(*outer), Some(*inner)),
            BinaryEmit(outer, inner) => (Some(*outer), Some(*inner)),
            ExtraToken(outer, inner, _) => (Some(*outer), Some(*inner)),
            PatchKeyExists(outer, inner, _) => (Some(*outer), Some(*inner)),
            InvalidAssign(outer, inner) => (Some(*outer), Some(*inner)),
            InvalidBinary(outer, inner, _, _, _) => (Some(*outer), Some(*inner)),
            InvalidDrop(outer, inner) => (Some(*outer), Some(*inner)),
            InvalidEmit(outer, inner) => (Some(*outer), Some(*inner)),
            InvalidConst(outer, inner) => (Some(*outer), Some(*inner)),
            AssignToConst(outer, inner, _) => (Some(*outer), Some(*inner)),
            DoubleConst(outer, inner, _) => (Some(*outer), Some(*inner)),
            InvalidExtractor(outer, inner, _, _, _) => (Some(*outer), Some(*inner)),
            InvalidFloatLiteral(outer, inner) => (Some(*outer), Some(*inner)),
            InvalidHexLiteral(outer, inner) => (Some(*outer), Some(*inner)),
            InvalidIntLiteral(outer, inner) => (Some(*outer), Some(*inner)),
            InvalidToken(outer, inner) => (Some(*outer), Some(*inner)),
            InvalidUnary(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            MergeTypeConflict(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            MissingEffectors(outer, inner) => (Some(*outer), Some(*inner)),
            MissingFunction(outer, inner, _, _, _) => (Some(*outer), Some(*inner)),
            MissingModule(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            NoClauseHit(outer) => (Some(outer.expand_lines(2)), Some(*outer)),
            Oops(outer) => (Some(outer.expand_lines(2)), Some(*outer)),
            RuntimeError(outer, inner, _, _, _, _) => (Some(*outer), Some(*inner)),
            TypeConflict(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            UnexpectedCharacter(outer, inner, _) => (Some(*outer), Some(*inner)),
            UnexpectedEscapeCode(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            UnrecognizedToken(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            UnterminatedExtractor(outer, inner, _) => (Some(*outer), Some(*inner)),
            UnterminatedIdentLiteral(outer, inner, _) => (Some(*outer), Some(*inner)),
            UnterminatedStringLiteral(outer, inner, _) => (Some(*outer), Some(*inner)),
            UpdateKeyMissing(outer, inner, _) => (Some(*outer), Some(*inner)),
            UnterminatedHereDoc(outer, inner, _) => (Some(*outer), Some(*inner)),
            TailingHereDoc(outer, inner, _, _) => (Some(*outer), Some(*inner)),
            // Special cases
            EmptyScript
            | Grok(_)
            | InvalidInfluxData(_)
            | Io(_)
            | Msg(_)
            | ParseIntError(_)
            | ParserError(_)
            | UnexpectedEndOfStream
            | Utf8Error(_) => (Some(Range::default()), None),
            ErrorKind::__Nonexhaustive { .. } => (Some(Range::default()), None),
        }
    }
    pub fn token(&self) -> Option<String> {
        use ErrorKind::*;
        match self {
            UnterminatedExtractor(_, _, token) => Some(token.to_string()),
            UnterminatedStringLiteral(_, _, token) => Some(token.to_string()),
            UnterminatedIdentLiteral(_, _, token) => Some(token.to_string()),
            UnexpectedEscapeCode(_, _, s, _) => Some(s.to_string()),
            _ => None,
        }
    }
    pub fn hint(&self) -> Option<String> {
        use ErrorKind::*;
        match self {
            UnrecognizedToken(outer, inner, t, _) if t == "" && inner.0.absolute == outer.1.absolute => Some("It looks like a `;` is missing at the end of the script".into()),
            UnrecognizedToken(_, _, t, _) if t == "default" || t == "case" => Some("You might have a trailing `,` in the prior statement".into()),
            BadAccessInLocal(_, _, key, _) if key == "nil" => {
                Some("Did you mean null?".to_owned())
            }
            BadAccessInLocal(_, _, key, options) => {
                let mut options = options.clone();
                options.push("event".to_owned());
                options.push("true".to_owned());
                options.push("false".to_owned());
                options.push("null".to_owned());
                match best_hint(&key, &options, 2) {
                    Some((_d, o)) => Some(format!("Did you mean to use `{}`?", o)),
                    _ => None
                }
            }

            BadAccessInEvent(_, _, key, options) => {
                match best_hint(&key, &options, 2) {
                    Some((_d, o)) => Some(format!("Did you mean to use `{}`?", o)),
                    _ => None
                }
            }
            BadAccessInGlobal(_, _, key, options) => {
                match best_hint(&key, &options, 2) {
                    Some((_d, o)) => Some(format!("Did you mean to use `{}`?", o)),
                    _ => None
                }
            }
            TypeConflict(_, _, ValueType::F64, expected) => match expected.as_slice() {
                [ValueType::I64] => Some(
                    "You can use math::trunc() and related functions to ensure numbers are integers."
                        .to_owned(),
                ),
                _ => None
            },
            MissingModule(_, _, m, _) if m == "object" => Some("Did you mean to use the `record` module".into()),
            MissingModule(_, _, _, Some((_, suggestion))) => Some(format!("Did you mean `{}`", suggestion)),
            MissingFunction(_, _, _, _, Some((_, suggestion))) => Some(format!("Did you mean `{}`", suggestion)),
            UnrecognizedToken(_, _, t, l) if t == "event" && l.contains(&("`<ident>`".to_string())) => Some("It looks like you tried to use the key 'event' as part of a path expression, consider quoting it as `event` to make it an identifier or use array like access such as [\"event\"].".into()),
            UnrecognizedToken(_, _, t, l) if t == "-" && l.contains(&("`(`".to_string())) => Some("Try wrapping this expression in parentheses `(` ... `)`".into()),
            NoClauseHit(_) => Some("Consider adding a `default => null` clause at the end of your match or validate full coverage beforehand.".into()),
            Oops(_) => Some("Please take the error output script and test data and open a ticket, this should not happen.".into()),
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
    pub fn token(&self) -> Option<String> {
        self.0.token()
    }
}

fn choices<T>(choices: &[T]) -> String
where
    T: ToString,
{
    if choices.len() == 1 {
        choices[0].to_string()
    } else {
        format!(
            "one of {}",
            choices
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

error_chain! {
    foreign_links {
        Io(std::io::Error);
        Grok(grok::Error);
        Utf8Error(std::str::Utf8Error);
        ParseIntError(num::ParseIntError);
    }
    errors {
        /*
         * ParserError
         */
        UnrecognizedToken(range: Range, loc: Range, token: String, expected: Vec<String>) {
            description("Unrecognized token")
                display("Found the token `{}` but expected {}", token, choices(expected))
        }
        ExtraToken(range: Range, loc: Range, token: String) {
            description("Extra token")
                display("Found an extra token: `{}` that does not belong there", token)
        }
        InvalidToken(range: Range, loc: Range) {
            description("Invalid token")
                display("Invalid token")
        }
        /*
         * Generic
         */
        EmptyScript {
            description("No expressions were found in the script")
                display("No expressions were found in the script")
        }
        TypeConflict(expr: Range, inner: Range, got: ValueType, expected: Vec<ValueType>) {
            description("Conflicting types")
                display("Conflicting types, got {} but expected {}", t2s(*got), choices(&expected.iter().map(|v| t2s(*v).to_string()).collect::<Vec<String>>()))
        }
        Oops(expr: Range) {
            description("Something went wrong and we're not sure what it was")
                display("Something went wrong and we're not sure what it was")
        }
        /*
         * Functions
         */
        BadArity(expr: Range, inner: Range, m: String, f: String, a: usize, calling_a: usize) {
            description("Bad arity for function")
                display("Bad arity for function {}::{}/{} but was called with {} arguments", m, f, a, calling_a)
        }
        MissingModule(expr: Range, inner: Range, m: String, suggestion: Option<(usize, String)>) {
            description("Call to undefined module")
                display("Call to undefined module {}", m)
        }
        MissingFunction(expr: Range, inner: Range, m: String, f: String, suggestion: Option<(usize, String)>) {
            description("Call to undefined function")
                display("Call to undefined function {}::{}", m, f)
        }
        BadType(expr: Range, inner: Range, m: String, f: String, a: usize) {
            description("Bad type passed to function")
                display("Bad type passed to function {}::{}/{}", m, f, a)
        }
        RuntimeError(expr: Range, inner: Range, m: String, f: String,  a: usize, c: String) {
            description("Runtime error in function")
                display("Runtime error in function {}::{}/{}: {}", m, f, a, c)
        }
        /*
         * Lexer and Parser
         */
        UnterminatedExtractor(expr: Range, inner: Range, extractor: String) {
            description("Unterminated extractor")
                display("It looks like you forgot to terminate an extractor with a closing '|'")
        }

        UnterminatedStringLiteral(expr: Range, inner: Range, string: String) {
            description("Unterminated string")
                display("It looks like you forgot to terminate a string with a closing '\"'")
        }

        UnterminatedHereDoc(expr: Range, inner: Range, string: String) {
            description("Unterminated heredoc")
                display("It looks like you forgot to terminate a here doc with with a closing '\"\"\"'")
        }
        TailingHereDoc(expr: Range, inner: Range, hd: String, ch: char) {
            description("Tailing Characters after opening a here doc")
                display("It looks like you have characters tailing the here doc opening, it needs to be followed by a newline")
        }

        UnterminatedIdentLiteral(expr: Range, inner: Range, extractor: String)
        {
            description("Unterminated ident")
                display("It looks like you forgot to terminate an ident with a closing '`'")

        }

        UnexpectedCharacter(expr: Range, inner: Range, found: char){
            description("An unexpected character was found")
                display("An unexpected character '{}' was found", found)
        }


        UnexpectedEscapeCode(expr: Range, inner: Range, token: String, found: char){
            description("An unexpected escape code was found")
                display("An unexpected escape code '{}' was found", found)

        }

        InvalidHexLiteral(expr: Range, inner: Range){
            description("An invalid hexadecimal")
                display("An invalid hexadecimal")

        }

        InvalidIntLiteral(expr: Range, inner: Range) {
            description("An invalid integer literal")
                display("An invalid integer literal")

        }
        InvalidFloatLiteral(expr: Range, inner: Range) {
            description("An invalid float literal")
                display("An invalid float literal")

        }

        UnexpectedEndOfStream {
            description("An unexpected end of stream was found")
                display("An unexpected end of stream was found")

        }

        /*
         * Parser
         */
        ParserError(pos: String) {
            description("Parser user error")
                display("Parser user error: {}", pos)
        }
        /*
         * Resolve / Assign path walking
         */

        BadAccessInLocal(expr: Range, inner: Range, key: String, options: Vec<String>) {
            description("Trying to access a non existing local key")
                display("Trying to access a non existing local key `{}`", key)
        }
        BadAccessInGlobal(expr: Range, inner: Range, key: String, options: Vec<String>) {
            description("Trying to access a non existing global key")
                display("Trying to access a non existing global key `{}`", key)
        }
        BadAccessInEvent(expr: Range, inner: Range, key: String, options: Vec<String>) {
            description("Trying to access a non existing event key")
                display("Trying to access a non existing event key `{}`", key)
        }
        ArrayOutOfRange(expr: Range, inner: Range, r: IRange<usize>) {
            description("Trying to access an index that is out of range")
                display("Trying to access an index that is out of range {}", if r.start == r.end {
                    format!("{}", r.start)
                } else {
                    format!("{}..{}", r.start, r.end)
                })
        }
        AssignIntoArray(expor: Range, inner: Range) {
            description("Can not assign tinto a array")
                display("It is not supported to assign value into an array")
        }
        InvalidAssign(expor: Range, inner: Range) {
            description("You can not assign that")
                display("You are trying to assing to a value that isn't valid")
        }
        InvalidConst(expr: Range, inner: Range) {
            description("Can't declare a const here location")
                display("Can't declare a const here location")
        }
        DoubleConst(expr: Range, inner: Range, name: String) {
            description("Can't declare a constant twice")
                display("Can't declare the constant `{}` twice", name)
        }
        AssignToConst(expr: Range, inner: Range, name: String) {
            description("Can't assign to a constant")
                display("Can't assign to the `{}` constant", name)
        }
        /*
         * Emit & Drop
         */
        InvalidEmit(expr: Range, inner: Range) {
            description("Can not emit from this location")
                display("Can not emit from this location")

        }
        InvalidDrop(expr: Range, inner: Range) {
            description("Can not drop from this location")
                display("Can not drop from this location")

        }
        BinaryEmit(expr: Range, inner: Range) {
            description("Please enclose the value you want to emit")
                display("The expression can be read as a binary expression, please put the value you wan to emit in parentheses.")
        }
        BinaryDrop(expr: Range, inner: Range) {
            description("Please enclose the value you want to rop")
                display("The expression can be read as a binary expression, please put the value you wan to drop in parentheses.")
        }
        /*
         * Operators
         */
        InvalidUnary(expr: Range, inner: Range, op: ast::UnaryOpKind, val: ValueType) {
            description("Invalid ynary operation")
                display("The unary operation operation `{}` is not defined for the type `{}`", op, t2s(*val))
        }
        InvalidBinary(expr: Range, inner: Range, op: ast::BinOpKind, left: ValueType, right: ValueType) {
            description("Invalid ynary operation")
                display("The binary operation operation `{}` is not defined for the type `{}` and `{}`", op, t2s(*left), t2s(*right))
        }

        /*
         * match
         */
        InvalidExtractor(expr: Range, inner: Range, name: String, pattern: String, error: String){
            description("Invalid tilde predicate pattern")
                display("Invalid tilde predicate pattern: {}", error)
        }
        NoClauseHit(expr: Range){
            description("A match expression executed but no clause matched")
                display("A match expression executed but no clause matched")
        }
        MissingEffectors(expr: Range, inner: Range) {
            description("The clause has no effectors")
                display("The clause is missing a body")
        }
        /*
         * Patch
         */
        PatchKeyExists(expr: Range, inner: Range, key: String) {
            description("The key that is supposed to be written to already exists")
                display("The key that is supposed to be written to already exists: {}", key)
        }
        UpdateKeyMissing(expr: Range, inner: Range, key:String) {
            description("The key that is supposed to be updated does not exists")
                display("The key that is supposed to be updated does not exists: {}", key)
        }

        MergeTypeConflict(expr: Range, inner: Range, key:String, val: ValueType) {
            description("Merge can only be performed on keys that either do not exist or are records")
                display("Merge can only be performed on keys that either do not exist or are records but the key '{}' has the type {}", key, t2s(*val))
        }

        InvalidInfluxData(s: String) {
            description("Invalid Influx Line Protocol data")
                display("Invalid Influx Line Protocol data: {}", s)
        }

    }
}

pub fn error_type_conflict_mult<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: Vec<ValueType>,
) -> Result<T> {
    Err(ErrorKind::TypeConflict(outer.extent(), inner.extent(), got, expected).into())
}

pub fn error_type_conflict<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: ValueType,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![expected])
}

pub fn error_guard_not_bool<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: &Value,
) -> Result<T> {
    error_type_conflict(outer, inner, got.kind(), ValueType::Bool)
}

pub fn error_invalid_unary<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    op: ast::UnaryOpKind,
    val: &Value,
) -> Result<T> {
    Err(ErrorKind::InvalidUnary(outer.extent(), inner.extent(), op, val.kind()).into())
}

pub fn error_invalid_binary<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
    left: &Value,
    right: &Value,
) -> Result<T> {
    Err(ErrorKind::InvalidBinary(
        outer.extent(),
        inner.extent(),
        op,
        left.kind(),
        right.kind(),
    )
    .into())
}
pub fn error_no_clause_hit<T, O: BaseExpr>(outer: &O) -> Result<T> {
    Err(ErrorKind::NoClauseHit(outer.extent()).into())
}

pub fn error_oops<T, O: BaseExpr>(outer: &O) -> Result<T> {
    Err(ErrorKind::Oops(outer.extent()).into())
}

pub fn error_patch_key_exists<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
) -> Result<T> {
    Err(ErrorKind::PatchKeyExists(outer.extent(), inner.extent(), key).into())
}

pub fn error_patch_update_key_missing<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
) -> Result<T> {
    Err(ErrorKind::UpdateKeyMissing(outer.extent(), inner.extent(), key).into())
}
pub fn error_missing_effector<T, O: BaseExpr, I: BaseExpr>(outer: &O, inner: &I) -> Result<T> {
    Err(ErrorKind::MissingEffectors(outer.extent(), inner.extent()).into())
}

pub fn error_patch_merge_type_conflict<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
    val: &Value,
) -> Result<T> {
    Err(ErrorKind::MergeTypeConflict(outer.extent(), inner.extent(), key, val.kind()).into())
}

pub fn error_assign_array<T, O: BaseExpr, I: BaseExpr>(outer: &O, inner: &I) -> Result<T> {
    Err(ErrorKind::AssignIntoArray(outer.extent(), inner.extent()).into())
}
pub fn error_invalid_assign_target<T, O: BaseExpr>(outer: &O) -> Result<T> {
    let inner: Range = outer.extent();

    Err(ErrorKind::InvalidAssign(inner.expand_lines(2), inner).into())
}
pub fn error_assign_to_const<T, O: BaseExpr>(outer: &O, name: String) -> Result<T> {
    let inner: Range = outer.extent();

    Err(ErrorKind::AssignToConst(inner.expand_lines(2), inner, name).into())
}
pub fn error_array_out_of_bound<'script, T, O: BaseExpr, I: BaseExpr, Ctx: Context>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script, Ctx>,
    r: IRange<usize>,
) -> Result<T> {
    let expr: Range = outer.extent();
    Err(match path {
        ast::Path::Local(_path) | ast::Path::Const(_path) => {
            ErrorKind::ArrayOutOfRange(expr, inner.extent(), r).into()
        }
        ast::Path::Meta(_path) => ErrorKind::ArrayOutOfRange(expr, inner.extent(), r).into(),
        ast::Path::Event(_path) => ErrorKind::ArrayOutOfRange(expr, inner.extent(), r).into(),
    })
}

pub fn error_bad_key<'script, T, O: BaseExpr, I: BaseExpr, Ctx: Context>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script, Ctx>,
    key: String,
    options: Vec<String>,
) -> Result<T> {
    let expr: Range = outer.extent();
    Err(match path {
        ast::Path::Local(_p) | ast::Path::Const(_p) => {
            ErrorKind::BadAccessInLocal(expr, inner.extent(), key, options).into()
        }
        ast::Path::Meta(_p) => {
            ErrorKind::BadAccessInGlobal(expr, inner.extent(), key, options).into()
        }
        ast::Path::Event(_p) => {
            ErrorKind::BadAccessInEvent(expr, inner.extent(), key, options).into()
        }
    })
}
