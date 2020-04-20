// Copyright 2018-2020, Wayfair GmbH
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
#![allow(missing_docs)]

use crate::ast::{self, BaseExpr, Expr, Ident, NodeMetas};
use crate::errors;
use crate::lexer;
use crate::pos;
use crate::pos::{Location, Range};
use base64;
use dissect;
use error_chain::error_chain;
use glob;
use grok;
use lalrpop_util;
use lalrpop_util::ParseError as LalrpopError;
use regex;
use serde::{Deserialize, Serialize};
use serde_json;
pub use simd_json::ValueType;
use simd_json::{prelude::*, BorrowedValue as Value};
use std::num;
use std::ops::{Range as RangeExclusive, RangeInclusive};
use url;

#[doc(hidden)]
/// Optimised try
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
        // This might be Ok since we try to compare Result in tests
        false
    }
}

type ParserError<'screw_lalrpop> =
    lalrpop_util::ParseError<pos::Location, lexer::Token<'screw_lalrpop>, errors::Error>;

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Self::from(format!("Url Parse Error: {}", e))
    }
}

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
                end.column += 1;
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
        ValueType::I64 | ValueType::U64 => "integer",
        ValueType::F64 => "float",
        ValueType::Array => "array",
        ValueType::Object => "record",
    }
}

pub(crate) fn best_hint(
    given: &str,
    options: &[String],
    max_dist: usize,
) -> Option<(usize, String)> {
    options
        .iter()
        .map(|option| (distance::damerau_levenshtein(given, &option), option))
        .filter(|(distance, _)| *distance <= max_dist)
        .min()
        .map(|(d, s)| (d, s.clone()))
}
pub(crate) type ErrorLocation = (Option<Range>, Option<Range>);
// FIXME: Option<(Location, Location, Option<(Location, Location)>)>
impl ErrorKind {
    pub(crate) fn expr(&self) -> ErrorLocation {
        use ErrorKind::*;
        match self {
            NoClauseHit(outer) | Oops(outer, _) => (Some(outer.expand_lines(2)), Some(*outer)),
            QueryStreamNotDefined(outer, inner, _)
            | ArrayOutOfRange(outer, inner, _, _)
            | AssignIntoArray(outer, inner)
            | BadAccessInEvent(outer, inner, _, _)
            | BadAccessInState(outer, inner, _, _)
            | BadAccessInGlobal(outer, inner, _, _)
            | BadAccessInLocal(outer, inner, _, _)
            | BadArity(outer, inner, _, _, _, _)
            | BadArrayIndex(outer, inner, _, _)
            | BadType(outer, inner, _, _, _)
            | BinaryDrop(outer, inner)
            | BinaryEmit(outer, inner)
            | DecreasingRange(outer, inner, _, _)
            | ExtraToken(outer, inner, _)
            | PatchKeyExists(outer, inner, _)
            | InvalidAssign(outer, inner)
            | InvalidBinary(outer, inner, _, _, _)
            | InvalidBitshift(outer, inner)
            | InvalidDrop(outer, inner)
            | InvalidEmit(outer, inner)
            | InvalidConst(outer, inner)
            | InvalidFn(outer, inner)
            | AssignToConst(outer, inner, _)
            | DoubleConst(outer, inner, _)
            | InvalidExtractor(outer, inner, _, _, _)
            | InvalidFloatLiteral(outer, inner)
            | InvalidHexLiteral(outer, inner)
            | InvalidIntLiteral(outer, inner)
            | InvalidToken(outer, inner)
            | InvalidUnary(outer, inner, _, _)
            | MergeTypeConflict(outer, inner, _, _)
            | MissingEffectors(outer, inner)
            | MissingFunction(outer, inner, _, _, _)
            | MissingModule(outer, inner, _, _)
            | NoLocalsAllowed(outer, inner)
            | NoConstsAllowed(outer, inner)
            | RuntimeError(outer, inner, _, _, _, _)
            | TypeConflict(outer, inner, _, _)
            | UnexpectedCharacter(outer, inner, _)
            | UnexpectedEscapeCode(outer, inner, _, _)
            | UnrecognizedToken(outer, inner, _, _)
            | UnterminatedExtractor(outer, inner, _)
            | UnterminatedIdentLiteral(outer, inner, _)
            | UnterminatedStringLiteral(outer, inner, _)
            | UpdateKeyMissing(outer, inner, _)
            | UnterminatedHereDoc(outer, inner, _)
            | TailingHereDoc(outer, inner, _, _)
            | AggrInAggr(outer, inner)
            | NotConstant(outer, inner) => (Some(*outer), Some(*inner)),
            // Special cases
            EmptyScript
            | NoObjectError(_)
            | NotFound
            | Grok(_)
            | InvalidInfluxData(_, _)
            | Io(_)
            | JSONError(_)
            | Msg(_)
            | ParseIntError(_)
            | ParserError(_)
            | SerdeJSONError(_)
            | UnexpectedEndOfStream
            | Utf8Error(_)
            | Self::__Nonexhaustive { .. } => (Some(Range::default()), None),
        }
    }
    pub(crate) fn token(&self) -> Option<String> {
        use ErrorKind::*;
        match self {
            UnterminatedExtractor(_, _, token)
            | UnterminatedStringLiteral(_, _, token)
            | UnterminatedIdentLiteral(_, _, token)
            | UnexpectedEscapeCode(_, _, token, _) => Some(token.to_string()),
            _ => None,
        }
    }
    pub(crate) fn hint(&self) -> Option<String> {
        use ErrorKind::*;
        match self {
            UnrecognizedToken(outer, inner, t, _) if t == "" && inner.0.absolute == outer.1.absolute => Some("It looks like a `;` is missing at the end of the script".into()),
            UnrecognizedToken(_, _, t, _) if t == "default" || t == "case" => Some("You might have a trailing `,` in the prior statement".into()),
            UnrecognizedToken(_, _, t, l) if t == "event" && l.contains(&("`<ident>`".to_string())) => Some("It looks like you tried to use the key 'event' as part of a path expression, consider quoting it as `event` to make it an identifier or use array like access such as [\"event\"].".into()),
            UnrecognizedToken(_, _, t, l) if t == "-" && l.contains(&("`(`".to_string())) => Some("Try wrapping this expression in parentheses `(` ... `)`".into()),
            UnrecognizedToken(_, _, key, options) => {
                match best_hint(&key, &options, 3) {
                    Some((_d, o)) => Some(format!("Did you mean to use {}?", o)),
                    _ => None
                }
            }
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

            BadAccessInEvent(_, _, key, options) |BadAccessInGlobal(_, _, key, options) => {
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
            MissingModule(_, _, _, Some((_, suggestion))) | MissingFunction(_, _, _, _, Some((_, suggestion))) => Some(format!("Did you mean `{}`?", suggestion)),

            NoClauseHit(_) => Some("Consider adding a `default => null` clause at the end of your match or validate full coverage beforehand.".into()),
            Oops(_, _) => Some("Please take the error output script and test data and open a ticket, this should not happen.".into()),
            _ => None,
        }
    }
}

impl Error {
    pub(crate) fn context(&self) -> ErrorLocation {
        self.0.expr()
    }
    pub(crate) fn hint(&self) -> Option<String> {
        self.0.hint()
    }
    pub(crate) fn token(&self) -> Option<String> {
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
        Grok(grok::Error);
        Io(std::io::Error);
        JSONError(simd_json::Error);
        ParseIntError(num::ParseIntError);
        SerdeJSONError(serde_json::Error);
        Utf8Error(std::str::Utf8Error);
        NoObjectError(simd_json::KnownKeyError);
    }
    errors {
        /*
         * ParserError
         */
         /// A unrecognized token
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
        NotConstant(expr: Range, inner: Range) {
            description("The expression isn't constant and can't be evaluated at compile time")
                display("The expression isn't constant and can't be evaluated at compile time")
        }
        TypeConflict(expr: Range, inner: Range, got: ValueType, expected: Vec<ValueType>) {
            description("Conflicting types")
                display("Conflicting types, got {} but expected {}", t2s(*got), choices(&expected.iter().map(|v| t2s(*v).to_string()).collect::<Vec<String>>()))
        }
        Oops(expr: Range, msg: String) {
            description("Something went wrong and we're not sure what it was")
                display("Something went wrong and we're not sure what it was: {}", msg)
        }
        NotFound {
            description("Something wasn't found, aka NoneError.")
                display("Something wasn't found, aka NoneError.")
        }
        /*
         * Functions
         */
        BadArity(expr: Range, inner: Range, m: String, f: String, a: RangeInclusive<usize>, calling_a: usize) {
            description("Bad arity for function")
                display("Bad arity for function {}::{}/{:?} but was called with {} arguments", m, f, a, calling_a)
        }
        MissingModule(expr: Range, inner: Range, m: String, suggestion: Option<(usize, String)>) {
            description("Call to undefined module")
                display("Call to undefined module {}", m)
        }
        MissingFunction(expr: Range, inner: Range, m: String, f: String, suggestion: Option<(usize, String)>) {
            description("Call to undefined function")
                display("Call to undefined function {}::{}", m, f)
        }
        AggrInAggr(expr: Range, inner: Range) {
            description("Aggregates can not be called inside of aggregates")
                display("Aggregates can not be called inside of aggregates")
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
        BadAccessInState(expr: Range, inner: Range, key: String, options: Vec<String>) {
            description("Trying to access a non existing state key")
                display("Trying to access a non existing state key `{}`", key)
        }
        BadArrayIndex(expr: Range, inner: Range, idx: Value<'static>, len: usize) {
            description("Trying to index into an array with invalid index")
                display("Bad array index, got `{}` but expected an index in the range 0:{}",
                        idx, len)
        }
        DecreasingRange(expr: Range, inner: Range, start_idx: usize, end_idx: usize) {
            description("A range's end cannot be smaller than its start")
                display("A range's end cannot be smaller than its start, {}:{} is invalid",
                        start_idx, end_idx)
        }
        ArrayOutOfRange(expr: Range, inner: Range, r: RangeExclusive<usize>, len: usize) {
            description("Array index out of bounds")
                display("Array index out of bounds, got {} but expected {}",
                        if r.start == r.end {
                            format!("index {}", r.start)
                        } else {
                            format!("index range {}:{}", r.start, r.end)
                        },
                        if r.start == r.end {
                            format!("an index in the range 0:{}", len)
                        } else {
                            format!("a subrange of 0:{}", len)
                        })
        }
        AssignIntoArray(expor: Range, inner: Range) {
            description("Can not assign into an array")
                display("It is not supported to assign value into an array")
        }
        InvalidAssign(expor: Range, inner: Range) {
            description("You can not assign that")
                display("You are trying to assign to a value that isn't valid")
        }
        InvalidConst(expr: Range, inner: Range) {
            description("Can't declare a const here")
                display("Can't declare a const here")
        }

        InvalidFn(expr: Range, inner: Range) {
            description("Can't declare a function here")
                display("Can't declare a function here")
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
            description("Please enclose the value you want to drop")
                display("The expression can be read as a binary expression, please put the value you wan to drop in parentheses.")
        }
        /*
         * Operators
         */
        InvalidUnary(expr: Range, inner: Range, op: ast::UnaryOpKind, val: ValueType) {
            description("Invalid unary operation")
                display("The unary operation `{}` is not defined for the type `{}`", op, t2s(*val))
        }
        InvalidBinary(expr: Range, inner: Range, op: ast::BinOpKind, left: ValueType, right: ValueType) {
            description("Invalid binary operation")
                display("The binary operation `{}` is not defined for the type `{}` and `{}`", op, t2s(*left), t2s(*right))
        }
        InvalidBitshift(expr: Range, inner: Range) {
            description("Invalid value for bitshift")
                display("RHS value is larger than or equal to the number of bits in LHS value")
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

        InvalidInfluxData(s: String, e: tremor_influx::DecoderError) {
            description("Invalid Influx Line Protocol data")
                display("Invalid Influx Line Protocol data: {}\n{}", e, s)
        }

        /*
         * Query stream declarations
         */
        QueryStreamNotDefined(stmt: Range, inner: Range, name: String) {
            description("Stream is not defined")
                display("Stream is not defined: {}", name)
        }
        NoLocalsAllowed(stmt: Range, inner: Range) {
            description("Local variables are not allowed here")
                display("Local variables are not allowed here")
        }
        NoConstsAllowed(stmt: Range, inner: Range) {
            description("Local variables are not allowed here")
                display("Local variables are not allowed here")
        }
    }
}

/// Creates an stream not defined error
#[allow(clippy::borrowed_box)]
pub fn query_stream_not_defined<T, S: BaseExpr, I: BaseExpr>(
    stmt: &Box<S>,
    inner: &I,
    name: String,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::QueryStreamNotDefined(stmt.extent(meta), inner.extent(meta), name).into())
}

/// Creates a guard not bool error
#[allow(clippy::borrowed_box)]
pub fn query_guard_not_bool<T, O: BaseExpr, I: BaseExpr>(
    stmt: &Box<O>,
    inner: &I,
    _got: &Value,
    meta: &NodeMetas,
) -> Result<T> {
    // FIXME Should actually say expected/actualf or type ( error_type_conflict )
    Err(ErrorKind::QueryStreamNotDefined(
        stmt.extent(meta),
        inner.extent(meta),
        "snot at error type conflict".to_string(),
    )
    .into())
}

pub(crate) fn error_type_conflict_mult<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: Vec<ValueType>,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::TypeConflict(outer.extent(meta), inner.extent(meta), got, expected).into())
}

pub(crate) fn error_no_locals<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::NoLocalsAllowed(outer.extent(meta), inner.extent(meta)).into())
}

pub(crate) fn error_no_consts<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::NoConstsAllowed(outer.extent(meta), inner.extent(meta)).into())
}

pub(crate) fn error_need_obj<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    meta: &NodeMetas,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::Object], meta)
}
pub(crate) fn error_need_arr<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    meta: &NodeMetas,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::Array], meta)
}

pub(crate) fn error_need_str<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    meta: &NodeMetas,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::String], meta)
}

pub(crate) fn error_need_int<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    meta: &NodeMetas,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::I64], meta)
}

pub(crate) fn error_type_conflict<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: ValueType,
    meta: &NodeMetas,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![expected], meta)
}

pub(crate) fn error_guard_not_bool<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: &Value,
    meta: &NodeMetas,
) -> Result<T> {
    error_type_conflict(outer, inner, got.value_type(), ValueType::Bool, meta)
}

pub(crate) fn error_invalid_unary<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    op: ast::UnaryOpKind,
    val: &Value,
    meta: &NodeMetas,
) -> Result<T> {
    Err(
        ErrorKind::InvalidUnary(outer.extent(meta), inner.extent(meta), op, val.value_type())
            .into(),
    )
}

pub(crate) fn error_invalid_binary<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
    left: &Value,
    right: &Value,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::InvalidBinary(
        outer.extent(meta),
        inner.extent(meta),
        op,
        left.value_type(),
        right.value_type(),
    )
    .into())
}

pub(crate) fn error_invalid_bitshift<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::InvalidBitshift(outer.extent(meta), inner.extent(meta)).into())
}

pub(crate) fn error_no_clause_hit<T, O: BaseExpr>(outer: &O, meta: &NodeMetas) -> Result<T> {
    Err(ErrorKind::NoClauseHit(outer.extent(meta)).into())
}

pub(crate) fn error_oops<T, O: BaseExpr, S: ToString + ?Sized>(
    outer: &O,
    msg: &S,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::Oops(outer.extent(meta), msg.to_string()).into())
}

pub(crate) fn error_patch_key_exists<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::PatchKeyExists(outer.extent(meta), inner.extent(meta), key).into())
}

pub(crate) fn error_patch_update_key_missing<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::UpdateKeyMissing(outer.extent(meta), inner.extent(meta), key).into())
}
pub(crate) fn error_missing_effector<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::MissingEffectors(outer.extent(meta), inner.extent(meta)).into())
}

pub(crate) fn error_patch_merge_type_conflict<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
    val: &Value,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::MergeTypeConflict(
        outer.extent(meta),
        inner.extent(meta),
        key,
        val.value_type(),
    )
    .into())
}

pub(crate) fn error_assign_array<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    meta: &NodeMetas,
) -> Result<T> {
    Err(ErrorKind::AssignIntoArray(outer.extent(meta), inner.extent(meta)).into())
}
pub(crate) fn error_invalid_assign_target<T, O: BaseExpr>(
    outer: &O,
    meta: &NodeMetas,
) -> Result<T> {
    let inner: Range = outer.extent(meta);

    Err(ErrorKind::InvalidAssign(inner.expand_lines(2), inner).into())
}
pub(crate) fn error_assign_to_const<T, O: BaseExpr>(
    outer: &O,
    name: String,
    meta: &NodeMetas,
) -> Result<T> {
    let inner: Range = outer.extent(meta);

    Err(ErrorKind::AssignToConst(inner.expand_lines(2), inner, name).into())
}
pub(crate) fn error_array_out_of_bound<'script, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
    r: RangeExclusive<usize>,
    len: usize,
    meta: &NodeMetas,
) -> Result<T> {
    let expr: Range = outer.extent(meta);
    // FIXME: Why match on `path` when all arms do the same?!
    // -> ideally: put the `path` into the `ErrorKind::ArrayOutOfRange`, handle in display
    //        but: not trivial: `Path` is parametric in non-'static lifetime 'script
    Err(match path {
        ast::Path::Local(_path) | ast::Path::Const(_path) => {
            ErrorKind::ArrayOutOfRange(expr, inner.extent(meta), r, len).into()
        }
        ast::Path::Meta(_path) => {
            ErrorKind::ArrayOutOfRange(expr, inner.extent(meta), r, len).into()
        }
        ast::Path::Event(_path) => {
            ErrorKind::ArrayOutOfRange(expr, inner.extent(meta), r, len).into()
        }
        ast::Path::State(_path) => {
            ErrorKind::ArrayOutOfRange(expr, inner.extent(meta), r, len).into()
        }
    })
}

pub(crate) fn error_bad_array_index<'script, 'idx, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
    idx: &Value<'idx>,
    len: usize,
    meta: &NodeMetas,
) -> Result<T> {
    let expr: Range = outer.extent(meta);
    let idx = idx.clone_static();
    Err(match path {
        ast::Path::Local(_path) | ast::Path::Const(_path) => {
            ErrorKind::BadArrayIndex(expr, inner.extent(meta), idx, len).into()
        }
        ast::Path::Meta(_path) => {
            ErrorKind::BadArrayIndex(expr, inner.extent(meta), idx, len).into()
        }
        ast::Path::Event(_path) => {
            ErrorKind::BadArrayIndex(expr, inner.extent(meta), idx, len).into()
        }
        ast::Path::State(_path) => {
            ErrorKind::BadArrayIndex(expr, inner.extent(meta), idx, len).into()
        }
    })
}
pub(crate) fn error_decreasing_range<'script, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
    start_idx: usize,
    end_idx: usize,
    meta: &NodeMetas,
) -> Result<T> {
    let expr: Range = outer.extent(meta);
    Err(match path {
        ast::Path::Local(_path) | ast::Path::Const(_path) => {
            ErrorKind::DecreasingRange(expr, inner.extent(meta), start_idx, end_idx).into()
        }
        ast::Path::Meta(_path) => {
            ErrorKind::DecreasingRange(expr, inner.extent(meta), start_idx, end_idx).into()
        }
        ast::Path::Event(_path) => {
            ErrorKind::DecreasingRange(expr, inner.extent(meta), start_idx, end_idx).into()
        }
        ast::Path::State(_path) => {
            ErrorKind::DecreasingRange(expr, inner.extent(meta), start_idx, end_idx).into()
        }
    })
}

pub(crate) fn error_bad_key<'script, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
    key: String,
    options: Vec<String>,
    meta: &NodeMetas,
) -> Result<T> {
    let expr: Range = outer.extent(meta);
    Err(match path {
        ast::Path::Local(_p) | ast::Path::Const(_p) => {
            ErrorKind::BadAccessInLocal(expr, inner.extent(meta), key, options).into()
        }
        ast::Path::Meta(_p) => {
            ErrorKind::BadAccessInGlobal(expr, inner.extent(meta), key, options).into()
        }
        ast::Path::Event(_p) => {
            ErrorKind::BadAccessInEvent(expr, inner.extent(meta), key, options).into()
        }
        ast::Path::State(_p) => {
            ErrorKind::BadAccessInState(expr, inner.extent(meta), key, options).into()
        }
    })
}
