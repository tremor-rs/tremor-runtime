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

pub use crate::prelude::ValueType;
use crate::{
    arena,
    ast::{self, BooleanBinOpKind},
    errors, lexer,
    pos::{self, Span},
    prelude::*,
};
use lalrpop_util::ParseError as LalrpopError;
use std::fmt::Write;
use std::ops::{Range as RangeExclusive, RangeInclusive};
use std::{fmt::Display, num};

/// A Result for Tremor script Errors
pub type Result<T> = ::std::result::Result<T, Error>;

/// Tremor script Error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Url Error
    #[error("Url Parse Error: {0}")]
    Url(#[from] url::ParseError),
    /// Poison Error
    #[error("Poison Error: {0}")]
    Poison(String),
    // foreign errors
    /// Grok
    #[error(transparent)]
    Grok(#[from] grok::Error),
    /// IO
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// JSON
    #[error(transparent)]
    JsonError(#[from] simd_json::Error),
    /// Value
    #[error(transparent)]
    ValueError(#[from] tremor_value::Error),
    /// when parsing an int fails
    #[error(transparent)]
    ParseIntError(#[from] num::ParseIntError),
    /// UTF-8
    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),
    /// UTF-8
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    /// not an object
    #[error(transparent)]
    NoObjectError(#[from] tremor_value::KnownKeyError),
    /// can't access value
    #[error(transparent)]
    AccessError(#[from] value_trait::AccessError),
    /// error in Tremor codec
    #[error(transparent)]
    CodecError(#[from] tremor_codec::Error),
    /// Tremor common error
    #[error(transparent)]
    Common(#[from] tremor_common::Error),

    /// Invalid hostname
    #[error("Invalid hostname: {0}")]
    InvalidHostname(String),
    /// File not found
    #[error("File not found or not readable: {0}")]
    FileNotFound(String),
    /// some string
    // TODO: such an error should not exist one might argue
    #[error("{0}")]
    String(String),
    /*
     * ParserError
     */
    /// An unrecognized token
    #[error("Found the token `{token}` but expected {}", choices(.expected))]
    UnrecognizedToken {
        location: Box<ErrorLocation>,
        token: String,
        expected: Vec<String>,
    },
    /// An unexpected extra token
    #[error("Found an extra token: `{token}` that does not belong here")]
    ExtraToken {
        range: Span,
        loc: Span,
        token: String,
    },
    /// invalid token
    #[error("Invalid token")]
    InvalidToken { range: Span, loc: Span },
    /// invalid preprocessor
    #[error("Found the preprocessor directive `{directive}` but expected {}", choices(&["#!config"]))]
    InvalidPP {
        range: Span,
        loc: Span,
        directive: String,
    },
    /***
     * Generic
     */
    #[error("{msg}")]
    Generic {
        expr: Span,
        inner: Span,
        msg: String,
    },

    #[error("Cyclic dependency detected: {}", .uses.join(" -> "))]
    CyclicUse {
        expr: Span,
        inner: Span,
        uses: Vec<String>,
    },
    #[error("Type error: Expected {expected}, found {found}")]
    TypeError {
        expr: Option<Span>,
        inner: Option<Span>,
        expected: ValueType,
        found: ValueType,
    },
    #[error("No expressions were found in the script")]
    EmptyScript,
    #[error("The expression isn't constant and can't be evaluated at compile time")]
    NotConstant { expr: Span, inner: Span },
    #[error("Conflicting types, got {} but expected {}", t2s(*.got), choices(&.expected.iter().map(|v| t2s(*v).to_string()).collect::<Vec<String>>()))]
    TypeConflict {
        expr: Span,
        inner: Span,
        got: ValueType,
        expected: Vec<ValueType>,
    },
    #[error("Something went wrong and we're not sure what it was: {msg}")]
    Oops { expr: Span, id: u64, msg: String },
    #[error("Something wasn't found, aka NoneError.")]
    NotFound,
    /*
     * Functions
     */
    #[error("Bad arity for function {m}::{f}/{a:?} but was called with {calling_a} arguments")]
    BadArity {
        expr: Span,
        inner: Span,
        m: String,
        f: String,
        a: RangeInclusive<usize>,
        calling_a: usize,
    },
    #[error("Call to undefined module {m}")]
    MissingModule {
        outer: Span,
        inner: Span,
        m: String,
        suggestion: Option<(usize, String)>,
    },
    #[error("Call to undefined function {}::{f}", .m.join("::"))]
    MissingFunction {
        expr: Span,
        inner: Span,
        m: Vec<String>,
        f: String,
        suggestion: Option<(usize, String)>,
    },
    #[error("Aggregates can not be called inside of aggregates")]
    AggrInAggr { expr: Span, inner: Span },
    #[error("Bad type passed to function {m}::{f}/{a}")]
    BadType {
        expr: Span,
        inner: Span,
        m: String,
        f: String,
        a: usize,
    },
    #[error("Runtime error in function {m}::{f}/{a}: {c}")]
    RuntimeError {
        expr: Span,
        inner: Span,
        m: String,
        f: String,
        a: usize,
        c: String,
    },
    #[error("Can not recur from this location")]
    InvalidRecur { expr: Span, inner: Span },
    #[error("Recursion limit reached")]
    RecursionLimit { expr: Span, inner: Span },
    /*
     * Lexer, Preprocessor and Parser
     */
    #[error("It looks like you forgot to terminate an extractor with a closing '|'")]
    UnterminatedExtractor {
        expr: Span,
        inner: Span,
        extractor: UnfinishedToken,
    },
    #[error("It looks like you forgot to terminate a string with a closing '\"'")]
    UnterminatedStringLiteral {
        expr: Span,
        inner: Span,
        string: UnfinishedToken,
    },
    #[error("It looks like you forgot to terminate a here doc with with a closing '\"\"\"'")]
    UnterminatedHereDoc {
        expr: Span,
        inner: Span,
        string: UnfinishedToken,
    },
    #[error("It looks like you have characters tailing the here doc opening, it needs to be followed by a newline")]
    TailingHereDoc {
        expr: Span,
        inner: Span,
        hd: UnfinishedToken,
        ch: char,
    },
    #[error("It looks like you forgot to terminate a string interpolation with a closing '}}'")]
    UnterminatedInterpolation {
        expr: Span,
        inner: Span,
        string_with_interpolation: UnfinishedToken,
    },
    #[error("You have an interpolation without content.")]
    EmptyInterpolation {
        expr: Span,
        inner: Span,
        string_with_interpolation: UnfinishedToken,
    },
    #[error("It looks like you forgot to terminate an ident with a closing '`'")]
    UnterminatedIdentLiteral {
        expr: Span,
        inner: Span,
        ident: UnfinishedToken,
    },
    #[error("An unexpected character '{found}' was found")]
    UnexpectedCharacter {
        expr: Span,
        inner: Span,
        token: UnfinishedToken,
        found: char,
    },
    #[error("An unexpected escape code '{found}' was found")]
    UnexpectedEscapeCode {
        expr: Span,
        inner: Span,
        token: UnfinishedToken,
        found: char,
    },
    #[error("An invalid UTF8 escape sequence was found")]
    InvalidUtf8Sequence {
        expr: Span,
        inner: Span,
        token: UnfinishedToken,
    },
    #[error("An invalid hexadecimal")]
    InvalidHexLiteral {
        expr: Span,
        inner: Span,
        token: UnfinishedToken,
    },
    #[error("An invalid integer literal")]
    InvalidIntLiteral {
        expr: Span,
        inner: Span,
        token: UnfinishedToken,
    },
    #[error("An invalid float literal")]
    InvalidFloatLiteral {
        expr: Span,
        inner: Span,
        token: UnfinishedToken,
    },
    #[error("An unexpected end of stream was found")]
    UnexpectedEndOfStream { loc: Span },
    /*
     * Preprocessor
     */
    #[error("Module `{}` not found or not readable error in module path: {}",
                resolved_relative_file_path.trim(),
                expected.iter().fold(String::new(), |mut output, x|
                {
                    // ALLOW: if we can't allocate it's worse, we'd have the same problem with format
                    let _ = write!(output, "\n                         - {x}");
                    output

            }))]
    ModuleNotFound {
        range: Span,
        loc: Span,
        resolved_relative_file_path: String,
        expected: Vec<String>,
    },
    /*
     * Parser
     */
    #[error("Parser user error: {pos}")]
    ParserError { pos: String },
    /*
     * Resolve / Assign path walking
     */
    #[error("Unknown local variable: `{name}`")]
    UnknownLocal {
        outer: Span,
        inner: Span,
        name: String,
    },
    #[error("Trying to access a non existing local key `{key}`")]
    BadAccessInLocal {
        expr: Span,
        inner: Span,
        key: String,
        options: Vec<String>,
    },
    #[error("Trying to access a non existing global key `{key}`")]
    BadAccessInGlobal {
        expr: Span,
        inner: Span,
        key: String,
        options: Vec<String>,
    },
    #[error("Trying to access a non existing event key `{key}`")]
    BadAccessInEvent {
        expr: Span,
        inner: Span,
        key: String,
        options: Vec<String>,
    },
    #[error("Trying to access a non existing state key `{key}`")]
    BadAccessInState {
        expr: Span,
        inner: Span,
        key: String,
        options: Vec<String>,
    },
    #[error("Bad array index, got `{idx}` but expected an index in the range 0:{len}")]
    BadArrayIndex {
        expr: Span,
        inner: Span,
        idx: Value<'static>,
        len: usize,
    },
    #[error("A range's end cannot be smaller than its start, {start_idx}:{end_idx} is invalid")]
    DecreasingRange {
        expr: Span,
        inner: Span,
        start_idx: usize,
        end_idx: usize,
    },
    #[error("Array index out of bounds, got {} but expected {}",
                        if r.start == r.end {
                            format!("index {}", r.start)
                        } else {
                            format!("index range {}:{}", r.start, r.end)
                        },
                        if r.start == r.end {
                            format!("an index in the range 0:{len}")
                        } else {
                            format!("a subrange of 0:{len}")
                        })]
    ArrayOutOfRange {
        expr: Span,
        inner: Span,
        r: RangeExclusive<usize>,
        len: usize,
    },
    #[error("It is not supported to assign value into an array")]
    AssignIntoArray { expr: Span, inner: Span },
    #[error("You are trying to assign to a value that isn't valid")]
    InvalidAssign { expr: Span, inner: Span },
    #[error("Can't define a const here")]
    InvalidConst { expr: Span, inner: Span },
    #[error("Can't define a function here")]
    InvalidFn { expr: Span, inner: Span },
    #[error("Can't define the constant `{name}` twice")]
    DoubleConst {
        expr: Span,
        inner: Span,
        name: String,
    },
    #[error("Can't define the stream `{name}` twice")]
    DoubleStream {
        expr: Span,
        inner: Span,
        name: String,
    },
    #[error("Can't create the pipeline `{name}` twice")]
    DoublePipelineCreate {
        expr: Span,
        inner: Span,
        name: String,
    },
    #[error("Can't assign to a constant expression")]
    AssignToConst { expr: Span, inner: Span },
    /*
     * Emit & Drop
     */
    #[error("Can not emit from this location")]
    InvalidEmit { expr: Span, inner: Span },
    #[error("Can not drop from this location")]
    InvalidDrop { expr: Span, inner: Span },
    #[error("The expression can be read as a binary expression, please put the value you want to emit in parentheses.")]
    BinaryEmit { expr: Span, inner: Span },
    #[error("The expression can be read as a binary expression, please put the value you want to drop in parentheses.")]
    BinaryDrop { expr: Span, inner: Span },
    /*
     * Operators
     */
    #[error("The unary operation `{op}` is not defined for the type `{}`", t2s(*.val))]
    InvalidUnary {
        expr: Span,
        inner: Span,
        op: ast::UnaryOpKind,
        val: ValueType,
    },
    #[error("The binary operation `{op}` is not defined for the type `{}` and `{}`", t2s(*.left), t2s(*.right))]
    InvalidBinary {
        expr: Span,
        inner: Span,
        op: ast::BinOpKind,
        left: ValueType,
        right: ValueType,
    },
    #[error("The binary operation `{op}` must have a non zero RHS")]
    DivisionByZero {
        expr: Span,
        inner: Span,
        op: ast::BinOpKind,
    },
    #[error("The binary operation `{op}` caused an over- or underflow")]
    Overflow {
        expr: Span,
        inner: Span,
        op: ast::BinOpKind,
    },
    #[error("The binary operation `{op}` is not defined for the type `{}` and `{}`", t2s(*.left), .right.map_or_else(|| "<not executed>", t2s))]
    InvalidBinaryBoolean {
        expr: Span,
        inner: Span,
        op: ast::BooleanBinOpKind,
        left: ValueType,
        right: Option<ValueType>,
    },
    #[error("RHS value is larger than or equal to the number of bits in LHS value")]
    InvalidBitshift { expr: Span, inner: Span },
    /*
     * match
     */
    #[error("Invalid tilde predicate pattern: {error}")]
    InvalidExtractor {
        expr: Span,
        inner: Span,
        name: String,
        pattern: String,
        error: String,
    },
    #[error("A match expression executed but no clause matched")]
    NoClauseHit { expr: Span },
    #[error("The clause is missing a body")]
    MissingEffectors { expr: Span, inner: Span },
    /*
     * Patch
     */
    #[error("The key that is supposed to be written to already exists: {key}")]
    PatchKeyExists {
        expr: Span,
        inner: Span,
        key: String,
    },
    #[error("The key that is supposed to be updated does not exists: {key}")]
    UpdateKeyMissing {
        expr: Span,
        inner: Span,
        key: String,
    },
    #[error("Merge can only be performed on keys that either do not exist or are records but the key '{key}' has the type {}", t2s(*.val))]
    MergeTypeConflict {
        expr: Span,
        inner: Span,
        key: String,
        val: ValueType,
    },
    /*
     * Query stream definitions
     */
    #[error("Stream used in `from` or `into` is not defined: {name}/{port}")]
    QueryStreamNotDefined {
        stmt: Span,
        inner: Span,
        name: String,
        port: String,
    },
    #[error("Local variables are not allowed here")]
    NoLocalsAllowed { stmt: Span, inner: Span },
    #[error("Constants are not allowed here")]
    NoConstsAllowed { stmt: Span, inner: Span },
    #[error("References to `event` or `$` are not allowed in this context")]
    NoEventReferencesAllowed { stmt: Span, inner: Span },
    #[error("Failed to initialize window constant")]
    CantSetWindowConst,
    #[error("Failed to initialize group constant")]
    CantSetGroupConst,
    #[error("Failed to initialize args constant")]
    CantSetArgsConst,
    #[error("Name `{name}` is reserved for built-in nodes, please use another name.")]
    QueryNodeReservedName { stmt: Span, name: String },
    #[error("Name `{name}` is already in use for another node, please use another name.")]
    QueryNodeDuplicateName { stmt: Span, name: String },
    #[error("Query `{subq_name}` does not have port `{port_name}`")]
    PipelineUnknownPort {
        stmt: Span,
        inner: Span,
        subq_name: String,
        port_name: String,
    },
    /*
     * Troy statements
     */
    /// Deploy artefact not found
    #[error("Artefact `{name}` is not defined or not found, the following are defined: {}", .options.join(", "))]
    DeployArtefactNotDefined {
        /// error location
        location: Box<ErrorLocation>,
        /// name of the references artefact
        name: String,
        /// options
        options: Vec<String>,
    },
    // user provided with parameter that has no corresponding argument in the definition
    #[error("`with` parameter \"{param_name}\" does not correspond to an argument in the target definition \"{definition_name}\"")]
    WithParamNoArg {
        stmt: Span,
        inner: Span,
        param_name: String,
        definition_name: String,
        available_args: Vec<String>,
    },
    #[error("Argument `{name}` is required, but no defaults are provided in the definition and no final values in the instance")]
    DeployRequiredArgDoesNotResolve {
        stmt: Span,
        inner: Span,
        name: String,
    },
    #[error("Invalid `with` parameter \"{param}\" in definition of {definition}.")]
    InvalidDefinitionalWithParam {
        stmt: Span,
        inner: Span,
        definition: String,
        param: String,
        available_params: &'static [&'static str],
    },
}

impl From<Error> for std::io::Error {
    fn from(value: Error) -> Self {
        Self::other(value)
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::Poison(e.to_string())
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

#[derive(Debug)]
/// An error with a associated arena index
pub struct ErrorWithIndex(pub arena::Index, pub Error);

impl std::fmt::Display for ErrorWithIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.1.fmt(f)
    }
}

impl std::error::Error for ErrorWithIndex {}

impl From<ErrorWithIndex> for Error {
    fn from(e: ErrorWithIndex) -> Self {
        e.1
    }
}
impl From<Error> for ErrorWithIndex {
    fn from(e: Error) -> Self {
        ErrorWithIndex(arena::Index::INVALID, e)
    }
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

impl From<TryTypeError> for Error {
    fn from(e: TryTypeError) -> Self {
        Error::TypeError {
            expr: None,
            inner: None,
            expected: e.expected,
            found: e.got,
        }
    }
}

pub(crate) trait AddSpan<O, I>
where
    O: BaseExpr + Ranged,
    I: BaseExpr + Ranged,
{
    type Output;
    fn add_span(self, outer: &O, inner: &I) -> Self::Output;
}

impl<O, I> AddSpan<O, I> for Error
where
    O: BaseExpr + Ranged,
    I: BaseExpr + Ranged,
{
    type Output = Self;
    fn add_span(self, outer: &O, inner: &I) -> Self {
        match self {
            Error::TypeError {
                expected, found, ..
            } => Error::TypeError {
                expr: Some(outer.extent()),
                inner: Some(inner.extent()),
                expected,
                found,
            },
            _ => self,
        }
    }
}
impl<T, E, O, I> AddSpan<O, I> for std::result::Result<T, E>
where
    O: BaseExpr + Ranged,
    I: BaseExpr + Ranged,
    Error: From<E>,
{
    type Output = Result<T>;
    fn add_span(self, outer: &O, inner: &I) -> Self::Output {
        self.map_err(|e| Error::from(e).add_span(outer, inner))
    }
}

impl<'screw_lalrpop> From<ParserError<'screw_lalrpop>> for Error {
    fn from(error: ParserError<'screw_lalrpop>) -> Self {
        match error {
            LalrpopError::UnrecognizedToken {
                token: (start, token, end),
                expected,
            } => Error::UnrecognizedToken {
                location: Box::new(ErrorLocation {
                    expr: (start.move_up_lines(2), end.move_down_lines(2)).into(),
                    inner: (start, end).into(),
                }),
                token: token.to_string(),
                expected: expected
                    .into_iter()
                    .map(|s| match s.as_str() {
                        r#""heredoc_start""# | r#""heredoc_end""# => r#"`"""`"#.to_string(),
                        s => format!(
                            "`{}`",
                            s.strip_prefix('"')
                                .and_then(|s| s.strip_suffix('"'))
                                .unwrap_or(s)
                                .replace(r#"\""#, r#"""#)
                        ),
                    })
                    .collect(),
            },
            LalrpopError::ExtraToken {
                token: (start, token, end),
            } => Error::ExtraToken {
                range: (start.move_up_lines(2), end.move_down_lines(2)).into(),
                loc: (start, end).into(),
                token: token.to_string(),
            },
            LalrpopError::InvalidToken { location: start } => {
                let mut end = start;
                end.shift(' ');
                Error::InvalidToken {
                    range: (start.move_up_lines(2), end.move_down_lines(2)).into(),
                    loc: (start, end).into(),
                }
            }
            _ => Error::ParserError {
                pos: format!("{error:?}"),
            },
        }
    }
}

/// Location of an Error inside a source file
#[derive(Debug, PartialEq, Clone)]
pub struct ErrorLocation {
    /// The wider context
    pub expr: Span,
    /// More narrow location
    pub inner: Span,
}

// We need this since we call objects records
pub(crate) fn t2s(t: ValueType) -> &'static str {
    match t {
        ValueType::Null => "null",
        ValueType::Bool => "bool",
        ValueType::String => "string",
        ValueType::I64 | ValueType::U64 | ValueType::I128 | ValueType::U128 => "integer",
        ValueType::F64 => "float",
        ValueType::Array => "array",
        ValueType::Object => "record",
        ValueType::Custom(c) => c,
        ValueType::Extended(e) => match e {
            ExtendedValueType::I32
            | ExtendedValueType::I16
            | ExtendedValueType::I8
            | ExtendedValueType::U32
            | ExtendedValueType::U16
            | ExtendedValueType::U8
            | ExtendedValueType::Usize => "integer",
            ExtendedValueType::F32 => "float",
            ExtendedValueType::Char => "char",
            ExtendedValueType::None => "not even a value at all",
        },
    }
}

pub(crate) fn best_hint(
    given: &str,
    options: &[String],
    max_dist: usize,
) -> Option<(usize, String)> {
    options
        .iter()
        .map(|option| (distance::damerau_levenshtein(given, option), option))
        .filter(|(distance, _)| *distance <= max_dist)
        .min()
        .map(|(d, s)| (d, s.clone()))
}

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize, Eq)]
/// Token that might not be finished
pub struct UnfinishedToken {
    pub(crate) range: Span,
    pub(crate) value: String,
}

impl UnfinishedToken {
    pub(crate) fn new(range: Span, value: String) -> Self {
        Self { range, value }
    }
}

impl Error {
    pub(crate) fn aid(&self) -> arena::Index {
        self.expr().map(|loc| loc.expr.aid()).unwrap_or_default()
    }
    #[allow(clippy::too_many_lines)]
    pub(crate) fn expr(&self) -> Option<ErrorLocation> {
        match self {
            Self::NoClauseHit { expr: outer }
            | Self::UnexpectedEndOfStream { loc: outer }
            | Self::Oops { expr: outer, .. }
            | Self::QueryNodeDuplicateName { stmt: outer, .. }
            | Self::QueryNodeReservedName { stmt: outer, .. } => Some(ErrorLocation {
                expr: outer.expand_lines(2),
                inner: *outer,
            }),
            Self::AggrInAggr { expr: outer, inner }
            | Self::ArrayOutOfRange {
                expr: outer, inner, ..
            }
            | Self::AssignIntoArray { expr: outer, inner }
            | Self::AssignToConst { expr: outer, inner }
            | Self::BadAccessInEvent {
                expr: outer, inner, ..
            }
            | Self::BadAccessInGlobal {
                expr: outer, inner, ..
            }
            | Self::BadAccessInLocal {
                expr: outer, inner, ..
            }
            | Self::BadAccessInState {
                expr: outer, inner, ..
            }
            | Self::BadArity {
                expr: outer, inner, ..
            }
            | Self::BadArrayIndex {
                expr: outer, inner, ..
            }
            | Self::BadType {
                expr: outer, inner, ..
            }
            | Self::BinaryDrop { expr: outer, inner }
            | Self::BinaryEmit { expr: outer, inner }
            | Self::DecreasingRange {
                expr: outer, inner, ..
            }
            | Self::WithParamNoArg {
                stmt: outer, inner, ..
            }
            | Self::DeployRequiredArgDoesNotResolve {
                stmt: outer, inner, ..
            }
            | Self::DoubleConst {
                expr: outer, inner, ..
            }
            | Self::DoublePipelineCreate {
                expr: outer, inner, ..
            }
            | Self::DoubleStream {
                expr: outer, inner, ..
            }
            | Self::EmptyInterpolation {
                expr: outer, inner, ..
            }
            | Self::ExtraToken {
                range: outer,
                loc: inner,
                ..
            }
            | Self::Generic {
                expr: outer, inner, ..
            }
            | Self::InvalidAssign { expr: outer, inner }
            | Self::InvalidBinary {
                expr: outer, inner, ..
            }
            | Self::DivisionByZero {
                expr: outer, inner, ..
            }
            | Self::Overflow {
                expr: outer, inner, ..
            }
            | Self::InvalidBinaryBoolean {
                expr: outer, inner, ..
            }
            | Self::InvalidBitshift { expr: outer, inner }
            | Self::InvalidConst { expr: outer, inner }
            | Self::InvalidDrop { expr: outer, inner }
            | Self::InvalidEmit { expr: outer, inner }
            | Self::InvalidExtractor {
                expr: outer, inner, ..
            }
            | Self::InvalidFloatLiteral {
                expr: outer, inner, ..
            }
            | Self::InvalidFn { expr: outer, inner }
            | Self::InvalidHexLiteral {
                expr: outer, inner, ..
            }
            | Self::InvalidIntLiteral {
                expr: outer, inner, ..
            }
            | Self::InvalidPP {
                range: outer,
                loc: inner,
                ..
            }
            | Self::InvalidRecur { expr: outer, inner }
            | Self::InvalidToken {
                range: outer,
                loc: inner,
            }
            | Self::InvalidUnary {
                expr: outer, inner, ..
            }
            | Self::InvalidUtf8Sequence {
                expr: outer, inner, ..
            }
            | Self::MergeTypeConflict {
                expr: outer, inner, ..
            }
            | Self::MissingEffectors { expr: outer, inner }
            | Self::MissingFunction {
                expr: outer, inner, ..
            }
            | Self::MissingModule { outer, inner, .. }
            | Self::ModuleNotFound {
                range: outer,
                loc: inner,
                ..
            }
            | Self::NoConstsAllowed { stmt: outer, inner }
            | Self::NoEventReferencesAllowed { stmt: outer, inner }
            | Self::NoLocalsAllowed { stmt: outer, inner }
            | Self::NotConstant { expr: outer, inner }
            | Self::PatchKeyExists {
                expr: outer, inner, ..
            }
            | Self::PipelineUnknownPort {
                stmt: outer, inner, ..
            }
            | Self::QueryStreamNotDefined {
                stmt: outer, inner, ..
            }
            | Self::RecursionLimit { expr: outer, inner }
            | Self::RuntimeError {
                expr: outer, inner, ..
            }
            | Self::TailingHereDoc {
                expr: outer, inner, ..
            }
            | Self::TypeConflict {
                expr: outer, inner, ..
            }
            | Self::UnexpectedCharacter {
                expr: outer, inner, ..
            }
            | Self::UnexpectedEscapeCode {
                expr: outer, inner, ..
            }
            | Self::UnterminatedExtractor {
                expr: outer, inner, ..
            }
            | Self::UnterminatedHereDoc {
                expr: outer, inner, ..
            }
            | Self::UnterminatedIdentLiteral {
                expr: outer, inner, ..
            }
            | Self::UnterminatedInterpolation {
                expr: outer, inner, ..
            }
            | Self::UnterminatedStringLiteral {
                expr: outer, inner, ..
            }
            | Self::UnknownLocal { outer, inner, .. }
            | Self::CyclicUse {
                expr: outer, inner, ..
            }
            | Self::InvalidDefinitionalWithParam {
                stmt: outer, inner, ..
            }
            | Self::UpdateKeyMissing {
                expr: outer, inner, ..
            } => Some(ErrorLocation {
                expr: *outer,
                inner: *inner,
            }),

            Self::UnrecognizedToken { location, .. }
            | Self::DeployArtefactNotDefined { location, .. } => Some(*location.clone()),

            Self::TypeError {
                expr: outer, inner, ..
            } => outer
                .zip(*inner)
                .map(|(expr, inner)| ErrorLocation { expr, inner }),
            // Special cases
            Self::EmptyScript
            | Self::AccessError(_)
            | Self::CantSetArgsConst
            | Self::CantSetGroupConst
            | Self::CantSetWindowConst
            | Self::CodecError(_)
            | Self::Common(_)
            | Self::Grok(_)
            | Self::Io(_)
            | Self::JsonError(_)
            | Self::NotFound
            | Self::ParseIntError(_)
            | Self::ParserError { .. }
            | Self::Url(_)
            | Self::Poison(_)
            | Self::NoObjectError(_)
            | Self::Utf8Error(_)
            | Self::FromUtf8Error(_)
            | Self::InvalidHostname(_)
            | Self::FileNotFound(_)
            | Self::String(_)
            | Self::ValueError(_) => None,
        }
    }
    pub(crate) fn token(&self) -> Option<UnfinishedToken> {
        match self {
            Self::UnterminatedExtractor {
                extractor: token, ..
            }
            | Self::UnterminatedStringLiteral { string: token, .. }
            | Self::UnterminatedInterpolation {
                string_with_interpolation: token,
                ..
            }
            | Self::EmptyInterpolation {
                string_with_interpolation: token,
                ..
            }
            | Self::UnterminatedIdentLiteral { ident: token, .. }
            | Self::UnterminatedHereDoc { string: token, .. }
            | Self::TailingHereDoc { hd: token, .. }
            | Self::InvalidUtf8Sequence { token, .. }
            | Self::UnexpectedCharacter { token, .. }
            | Self::InvalidHexLiteral { token, .. }
            | Self::InvalidIntLiteral { token, .. }
            | Self::InvalidFloatLiteral { token, .. }
            | Self::UnexpectedEscapeCode { token, .. } => Some(token.clone()),
            _ => None,
        }
    }

    pub(crate) fn hint(&self) -> Option<String> {
        match self {
            Self::UnrecognizedToken{location, token, ..} if token.is_empty() && location.inner.start().absolute() == location.expr.start().absolute() => Some("It looks like a `;` is missing at the end of the script".into()),
            Self::UnrecognizedToken{token, ..} if token == "##" => Some(format!("`{token}` is as doc comment, it needs to be followed by a statement, did you want to use `#` here?")),
            Self::UnrecognizedToken{token, ..} if token == "default" || token == "case" => Some("You might have a trailing `;` in the prior statement".into()),
            Self::UnrecognizedToken{token, expected, .. } if token == "\"" && expected.contains(&("`<ident>`".to_string())) => Some("Did you mean to quote an ident? If so use ` (a back tick) not \" (a quote).".into()),
            Self::UnrecognizedToken{token, expected, ..} if !matches!(lexer::ident_to_token(token), lexer::Token::Ident(_, _)) && expected.contains(&("`<ident>`".to_string())) => Some(format!("It looks like you tried to use '{token}' as an ident, consider quoting it as `{token}` to make it an identifier.")),
            Self::UnrecognizedToken{token, expected, ..} if token == "-" && expected.contains(&("`(`".to_string())) => Some("Try wrapping this expression in parentheses `(` ... `)`".into()),
            Self::UnrecognizedToken{token, expected, ..} => {
                match best_hint(token, expected, 3) {
                    Some((_d, o)) if o == r#"`"`"# || o == r#"`"""`"#  => Some("Did you mean to use a string?".to_string()),
                    Some((_d, o)) if o != r#"`"`"# && o != r#"`"""`"# => Some(format!("Did you mean to use {o}?")),
                    _ => None
                }
            }
            Self::UnterminatedInterpolation {..} | Self::EmptyInterpolation {..} => {
                Some("Did you mean to write a literal '#{'? Escape it as '\\#{'.".to_string())
            }
            Self::BadAccessInLocal { key, ..} if key == "nil" => {
                Some("Did you mean null?".to_owned())
            }

            Self::BadAccessInLocal { key, options, .. } => {
                let mut options = options.clone();
                options.push("event".to_owned());
                options.push("true".to_owned());
                options.push("false".to_owned());
                options.push("null".to_owned());
                match best_hint(key, &options, 2) {
                    Some((_d, o)) => Some(format!("Did you mean to use `{o}`?")),
                    _ => None
                }
            }

            Self::BadAccessInEvent { key, options, .. } | Self::BadAccessInGlobal { key, options, .. } => {
                match best_hint(key, options, 2) {
                    Some((_d, o)) => Some(format!("Did you mean to use `{o}`?")),
                    _ => None
                }
            }
            Self::TypeConflict { got: ValueType::F64, expected, .. } => match expected.as_slice() {
                [ValueType::I64] => Some(
                    "You can use math::trunc() and related functions to ensure numbers are integers."
                        .to_owned(),
                ),
                _ => None
            },
            Self::MissingModule { m, ..} if m == "object" => Some("Did you mean to use the `record` module".into()),
            Self::MissingModule { suggestion: Some((_, suggestion)), .. } | Self::MissingFunction { suggestion: Some((_, suggestion)), .. } => Some(format!("Did you mean `{suggestion}`?")),

            Self::NoEventReferencesAllowed { .. } => Some("Here you operate in the whole window, not a single event. You need to wrap this reference in an aggregate function (e.g. aggr::win::last(...)) or use it in the group by clause of this query.".to_owned()),

            Self::NoClauseHit { .. } => Some("Consider adding a `case _ => null` clause at the end of your match or validate full coverage beforehand.".into()),
            Self::Oops { id, .. } => Some(format!("Please take the error output script and test data and open a ticket, this should not happen.\nhttps://github.com/tremor-rs/tremor-runtime/issues/new?labels=bug&template=bug_report.md&title=Opps%20{id}")),

            Self::InvalidDefinitionalWithParam { available_params, .. } => if available_params.is_empty() {
                Some(String::from("Definition does not allow any `with` parameters"))
            } else {
                Some(format!("Available parameters are: {}", available_params.join(", ")))
            },
            Self::WithParamNoArg { definition_name, available_args, .. } => if available_args.is_empty() {
                Some(format!("The definition of \"{definition_name}\" does not expose any args. Remove this `with`."))
            } else {
                Some(format!("Available args are: {}", available_args.join(", ")))
            }
            _ => None,
        }
    }
}

impl Error {
    /// the context of the error
    #[must_use]
    pub fn context(&self) -> Option<ErrorLocation> {
        self.expr()
    }

    /// If possible locate this error inside the given source.
    /// This is done without highlighting, for this, use an instance of `tremor_script::highlighter::Highlighter`,
    /// but it needs some more shenanigans than this in order to do proper highlighting.
    #[must_use]
    pub fn locate_in_source(&self, source: &str) -> Option<String> {
        match self.context() {
            Some(ErrorLocation {
                expr:
                    Span {
                        start: ctx_start,
                        end: ctx_end,
                    },
                inner:
                    Span {
                        start: error_loc_start,
                        end: error_loc_end,
                    },
            }) => {
                // display error we can locate in the source
                let start_line = ctx_start.line();
                let context_lines = source
                    .lines()
                    .skip(start_line.saturating_sub(1))
                    .take(ctx_end.line().saturating_sub(start_line).max(1))
                    .collect::<Vec<_>>();
                let error_line = error_loc_end.line();
                let mut cur_line_num = start_line;
                let mut error_lines = Vec::with_capacity(context_lines.len() + 1);
                for context_line in context_lines {
                    error_lines.push(format!("{cur_line_num:5} | {context_line}"));
                    if cur_line_num == error_line {
                        let err_msg = format!("{self}");
                        let (start_column, err_len) =
                            if error_loc_end.line() == error_loc_start.line() {
                                (
                                    error_loc_start.column(),
                                    error_loc_end
                                        .column()
                                        .saturating_sub(error_loc_start.column())
                                        .max(1),
                                )
                            } else {
                                (1, error_loc_end.column() - 1)
                            };

                        let prefix = " ".repeat(start_column.saturating_sub(1));
                        let underline = "^".repeat(err_len);
                        error_lines.push(format!("      | {prefix}{underline} {err_msg}"));
                    }
                    cur_line_num += 1;
                }
                Some(error_lines.join("\n"))
            }
            _ => None,
        }
    }
}

fn choices<T>(choices: &[T]) -> String
where
    T: ToString,
{
    if let [choice] = choices {
        choice.to_string()
    } else {
        format!(
            "one of {}",
            choices
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

/// Error for when something is already defined but another definition is found
pub fn already_defined_err<S>(e: &S, what: &str) -> Error
where
    S: BaseExpr + Ranged,
{
    error_generic(
        e,
        e,
        &format!(
            "Can't define the {what} `{}` twice",
            e.name().unwrap_or_default()
        ),
    )
}

/// Error when something is not defined but seems to be referenced
pub fn not_defined_err<S>(e: &S, what: &str) -> Error
where
    S: BaseExpr + Ranged,
{
    error_generic(
        e,
        e,
        &format!(
            "The {what} `{}` is not defined",
            e.name().unwrap_or_default()
        ),
    )
}

/// Creates a stream not defined error
#[allow(clippy::borrowed_box)]
pub fn query_stream_not_defined_err<S, I>(stmt: &S, inner: &I, name: String, port: String) -> Error
where
    S: Ranged,
    I: BaseExpr + Ranged,
{
    // Subqueries store unmangled `name` in `meta`
    // Use `name` from `meta` if it exists.
    let name = inner.meta().name().map_or(name, std::convert::Into::into);
    Error::QueryStreamNotDefined {
        stmt: stmt.extent(),
        inner: inner.extent(),
        name,
        port,
    }
}

/// Creates a query stream duplicate name error
pub fn query_stream_duplicate_name_err<S: Ranged, I: BaseExpr + Ranged>(
    stmt: &S,
    inner: &I,
    name: String,
) -> Error {
    let name = inner.meta().name().map_or(name, std::convert::Into::into);
    Error::DoubleStream {
        expr: stmt.extent(),
        inner: inner.extent(),
        name,
    }
}

/// Creates a pipeline stmt duplicate name error
pub fn pipeline_stmt_duplicate_name_err<S: Ranged, I: BaseExpr + Ranged>(
    stmt: &S,
    inner: &I,
    name: String,
) -> Error {
    let name = inner.meta().name().map_or(name, std::convert::Into::into);
    Error::DoublePipelineCreate {
        expr: stmt.extent(),
        inner: inner.extent(),
        name,
    }
}

/// Creates a pipeline unknown port error
pub fn pipeline_unknown_port_err<S: Ranged, I: BaseExpr + Ranged>(
    stmt: &S,
    inner: &I,
    subq_name: String,
    port_name: String,
) -> Error {
    let subq_name = inner
        .meta()
        .name()
        .map_or(subq_name, std::convert::Into::into);
    Error::PipelineUnknownPort {
        stmt: stmt.extent(),
        inner: inner.extent(),
        subq_name,
        port_name,
    }
}

/// Creates a query node reserved name error
pub fn query_node_reserved_name_err<S: BaseExpr + Ranged>(stmt: &S, name: String) -> Error {
    let name = stmt.meta().name().map_or(name, std::convert::Into::into);
    Error::QueryNodeReservedName {
        stmt: stmt.extent(),
        name,
    }
}

/// Creates a query node duplicate name error
pub fn query_node_duplicate_name_err<S: BaseExpr + Ranged>(stmt: &S, name: String) -> Error {
    let name = stmt.meta().name().map_or(name, std::convert::Into::into);
    Error::QueryNodeDuplicateName {
        stmt: stmt.extent(),
        name,
    }
}

/// Creates a guard not bool error
///
/// # Errors
/// always, this is a function to create errors
#[allow(clippy::borrowed_box)]
pub fn query_guard_not_bool<T, O: Ranged, I: Ranged>(
    stmt: &O,
    inner: &I,
    got: &Value,
) -> Result<T> {
    error_type_conflict_mult(stmt, inner, got.value_type(), vec![ValueType::Bool])
}

/// Error for when a tremor-query guard does not return a boolean
pub fn query_guard_not_bool_err<O: Ranged, I: Ranged>(stmt: &O, inner: &I, got: &Value) -> Error {
    err_type_conflict_mult(stmt, inner, got.value_type(), vec![ValueType::Bool])
}

/// A bad thing happened for which no specialized hygienic error handling strategy is defined
/// We can still be polite in our error reports!
/// # Errors
/// The parameters transformed into a generic error
pub fn err_generic<T, O: Ranged, I: Ranged, S: ToString>(
    outer: &O,
    inner: &I,
    error: &S,
) -> Result<T> {
    Err(error_generic(outer, inner, error))
}

/// A generic Error
pub fn error_generic<O: Ranged, I: Ranged, S: ToString>(outer: &O, inner: &I, error: &S) -> Error {
    Error::Generic {
        expr: outer.extent(),
        inner: inner.extent(),
        msg: error.to_string(),
    }
}

pub(crate) fn err_invalid_fold<O: Ranged, I: Ranged, Op: Display, T>(
    outer: &O,
    inner: &I,
    op: Op,
) -> Result<T> {
    err_generic(outer, inner, &format!("Invalid fold operation {op}"))
}
pub(crate) fn error_invalid_bool_op<O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: BooleanBinOpKind,
    left: ValueType,
    right: Option<ValueType>,
) -> Error {
    Error::InvalidBinaryBoolean {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
        left,
        right,
    }
}
pub(crate) fn error_type_conflict_mult<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: Vec<ValueType>,
) -> Result<T> {
    Err(err_type_conflict_mult(outer, inner, got, expected))
}

pub(crate) fn err_type_conflict_mult<O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: Vec<ValueType>,
) -> Error {
    Error::TypeConflict {
        expr: outer.extent(),
        inner: inner.extent(),
        got,
        expected,
    }
}

pub(crate) fn error_no_locals<T, O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Result<T> {
    Err(Error::NoLocalsAllowed {
        stmt: outer.extent(),
        inner: inner.extent(),
    })
}

pub(crate) fn error_event_ref_not_allowed<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
) -> Result<T> {
    Err(Error::NoEventReferencesAllowed {
        stmt: outer.extent(),
        inner: inner.extent(),
    })
}

pub(crate) fn error_need_obj<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    Err(err_need_obj(outer, inner, got))
}

pub(crate) fn err_need_obj<O: Ranged, I: Ranged>(outer: &O, inner: &I, got: ValueType) -> Error {
    err_type_conflict_mult(outer, inner, got, vec![ValueType::Object])
}

pub(crate) fn error_need_arr<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::Array])
}

pub(crate) fn error_need_str<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::String])
}

pub(crate) fn error_need_int<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    Err(err_need_int(outer, inner, got))
}

pub(crate) fn err_need_int<O: Ranged, I: Ranged>(outer: &O, inner: &I, got: ValueType) -> Error {
    err_type_conflict_mult(outer, inner, got, vec![ValueType::I64])
}

pub(crate) fn error_type_conflict<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: ValueType,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![expected])
}

pub(crate) fn error_guard_not_bool<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    got: &Value,
) -> Result<T> {
    error_type_conflict(outer, inner, got.value_type(), ValueType::Bool)
}

pub(crate) fn error_invalid_unary<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::UnaryOpKind,
    val: &Value,
) -> Result<T> {
    Err(err_invalid_unary(outer, inner, op, val))
}

pub(crate) fn err_invalid_unary<O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::UnaryOpKind,
    val: &Value,
) -> Error {
    Error::InvalidUnary {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
        val: val.value_type(),
    }
}

pub(crate) fn error_invalid_binary<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
    left: &Value,
    right: &Value,
) -> Result<T> {
    Err(Error::InvalidBinary {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
        left: left.value_type(),
        right: right.value_type(),
    })
}
pub(crate) fn error_division_by_zero<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
) -> Result<T> {
    Err(Error::DivisionByZero {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
    })
}
pub(crate) fn error_overflow<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
) -> Result<T> {
    Err(Error::Overflow {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
    })
}

pub(crate) fn error_invalid_bitshift<T, O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Result<T> {
    Err(Error::InvalidBitshift {
        expr: outer.extent(),
        inner: inner.extent(),
    })
}

pub(crate) fn error_no_clause_hit<T, O: Ranged>(outer: &O) -> Result<T> {
    Err(Error::NoClauseHit {
        expr: outer.extent(),
    })
}

pub(crate) fn error_oops<T, O: Ranged, S: ToString + ?Sized>(
    outer: &O,
    id: u64,
    msg: &S,
) -> Result<T> {
    Err(error_oops_err(outer, id, msg))
}

pub(crate) fn error_oops_err<O: Ranged, S: ToString + ?Sized>(
    outer: &O,
    id: u64,
    msg: &S,
) -> Error {
    Error::Oops {
        expr: outer.extent(),
        id,
        msg: msg.to_string(),
    }
}

pub(crate) fn error_patch_key_exists<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    key: String,
) -> Result<T> {
    Err(Error::PatchKeyExists {
        expr: outer.extent(),
        inner: inner.extent(),
        key,
    })
}

pub(crate) fn error_patch_update_key_missing<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    key: String,
) -> Result<T> {
    Err(Error::UpdateKeyMissing {
        expr: outer.extent(),
        inner: inner.extent(),
        key,
    })
}

pub(crate) fn error_missing_effector<O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Error {
    Error::MissingEffectors {
        expr: outer.extent(),
        inner: inner.extent(),
    }
}
pub(crate) fn error_patch_merge_type_conflict<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    key: String,
    val: &Value,
) -> Result<T> {
    Err(Error::MergeTypeConflict {
        expr: outer.extent(),
        inner: inner.extent(),
        key,
        val: val.value_type(),
    })
}

pub(crate) fn error_assign_array<T, O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Result<T> {
    Err(Error::AssignIntoArray {
        expr: outer.extent(),
        inner: inner.extent(),
    })
}
pub(crate) fn error_invalid_assign_target<T, O: Ranged>(outer: &O) -> Result<T> {
    let inner: Span = outer.extent();

    Err(Error::InvalidAssign {
        expr: inner.expand_lines(2),
        inner,
    })
}
pub(crate) fn error_assign_to_const<T, O: Ranged>(outer: &O) -> Result<T> {
    let inner: Span = outer.extent();

    Err(Error::AssignToConst {
        expr: inner.expand_lines(2),
        inner,
    })
}
pub(crate) fn error_array_out_of_bound<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    path: &ast::Path,
    r: RangeExclusive<usize>,
    len: usize,
) -> Result<T> {
    let expr: Span = outer.extent();
    // TODO: Why match on `path` when all arms do the same?!
    // -> ideally: put the `path` into the `ErrorKind::ArrayOutOfRange`, handle in display
    //        but: not trivial: `Path` is parametric in non-'static lifetime 'script
    Err(match path {
        ast::Path::Meta(_)
        | ast::Path::Event(_)
        | ast::Path::State(_)
        | ast::Path::Reserved(_)
        | ast::Path::Local(_)
        | ast::Path::Expr(_) => Error::ArrayOutOfRange {
            expr,
            inner: inner.extent(),
            r,
            len,
        },
    })
}

pub(crate) fn error_bad_array_index<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    path: &ast::Path,
    idx: &Value,
    len: usize,
) -> Result<T> {
    let expr: Span = outer.extent();
    let idx = idx.clone_static();
    Err(match path {
        ast::Path::Reserved(_)
        | ast::Path::State(_)
        | ast::Path::Event(_)
        | ast::Path::Meta(_)
        | ast::Path::Local(_)
        | ast::Path::Expr(_) => Error::BadArrayIndex {
            expr,
            inner: inner.extent(),
            idx,
            len,
        },
    })
}
pub(crate) fn error_decreasing_range<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    path: &ast::Path,
    start_idx: usize,
    end_idx: usize,
) -> Result<T> {
    let expr: Span = outer.extent();
    Err(match path {
        ast::Path::Meta(_)
        | ast::Path::Event(_)
        | ast::Path::State(_)
        | ast::Path::Reserved(_)
        | ast::Path::Local(_)
        | ast::Path::Expr(_) => Error::DecreasingRange {
            expr,
            inner: inner.extent(),
            start_idx,
            end_idx,
        },
    })
}

pub(crate) fn error_bad_key<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    path: &ast::Path,
    key: String,
    options: Vec<String>,
) -> Result<T> {
    Err(error_bad_key_err(outer, inner, path, key, options))
}

pub(crate) fn unknown_local<O: Ranged, I: BaseExpr>(outer: &O, inner: &I) -> Error {
    Error::UnknownLocal {
        outer: outer.extent(),
        inner: inner.extent(),
        name: inner.name_dflt().to_string(),
    }
}

pub(crate) fn error_bad_key_err<O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    path: &ast::Path,
    key: String,
    options: Vec<String>,
) -> Error {
    let expr: Span = outer.extent();
    match path {
        ast::Path::Reserved(_) | ast::Path::Local(_) | ast::Path::Expr(_) => {
            Error::BadAccessInLocal {
                expr,
                inner: inner.extent(),
                key,
                options,
            }
        }
        ast::Path::Meta(_p) => Error::BadAccessInGlobal {
            expr,
            inner: inner.extent(),
            key,
            options,
        },
        ast::Path::Event(_p) => Error::BadAccessInEvent {
            expr,
            inner: inner.extent(),
            key,
            options,
        },
        ast::Path::State(_p) => Error::BadAccessInState {
            expr,
            inner: inner.extent(),
            key,
            options,
        },
    }
}

pub(crate) fn unexpected_character<O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    tkn: UnfinishedToken,
    ch: char,
) -> Error {
    Error::UnexpectedCharacter {
        expr: outer.extent(),
        inner: inner.extent(),
        token: tkn,
        found: ch,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_type_error() {
        let r = Error::from(TryTypeError {
            expected: ValueType::Object,
            got: ValueType::String,
        });
        matches!(
            r,
            Error::TypeError {
                expr: None,
                inner: None,
                expected: ValueType::Object,
                found: ValueType::String
            }
        );
    }
}
