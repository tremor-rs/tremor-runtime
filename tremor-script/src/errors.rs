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
    lexer,
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
    /// Regex error
    #[error(transparent)]
    Regex(#[from] regex::Error),
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
    /// Parser Errors
    #[error(transparent)]
    Parser(#[from] Box<ParserError>),
    /// Lexer Errors
    #[error(transparent)]
    Lexer(#[from] Box<LexerError>),
    /// Runtime Errors
    #[error(transparent)]
    Runtime(#[from] Box<RuntimeError>),
    /// Function related Errors
    #[error(transparent)]
    Function(#[from] Box<FunctionError>),
    /// tremor-query / pipeline related Errors
    #[error(transparent)]
    Query(#[from] Box<QueryError>),
    /// Deploy / Troy related Errors
    #[error(transparent)]
    Deploy(#[from] Box<DeployError>),
    /// Oopsie Doopsie! Unrecoverable Error that should not happen
    #[error("Something went wrong and we're not sure what it was: {msg}")]
    Oops {
        /// Location of the oopsie
        expr: Span,
        /// Error id of the oopsie
        id: u64,
        /// Human readable message
        msg: String,
    },
    /// Something unspecified was not found *shrug*
    #[error("Something wasn't found, aka NoneError.")]
    NotFound,
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

impl From<TryTypeError> for Error {
    fn from(e: TryTypeError) -> Self {
        Error::Parser(Box::new(ParserError::TypeError {
            expr: None,
            inner: None,
            expected: e.expected,
            found: e.got,
        }))
    }
}

/// Parser Errors
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    /// Generic Parser Error
    #[error("{msg}")]
    Generic {
        /// extended / context location of the error
        expr: Span,
        /// exact / narrow location of the error
        inner: Span,
        /// generic error message
        msg: String,
    },

    /// cyclic use
    #[error("Cyclic dependency detected: {}", .uses.join(" -> "))]
    CyclicUse {
        /// extended / context location of the error
        expr: Span,
        /// exact / narrow location of the error
        inner: Span,
        /// the available uses
        uses: Vec<String>,
    },
    /// type error
    #[error("Type error: Expected {expected}, found {found}")]
    TypeError {
        /// extended / context location of the error
        expr: Option<Span>,
        /// exact / narrow location of the error
        inner: Option<Span>,
        /// expected type
        expected: ValueType,
        /// found type
        found: ValueType,
    },
    /// the given expression is not a constant
    #[error("The expression isn't constant and can't be evaluated at compile time")]
    NotConstant {
        /// extended / context location of the error
        expr: Span,
        /// exact / narrow location of the error
        inner: Span,
    },
    /// type conflict
    #[error("Conflicting types, got {} but expected {}", t2s(*.got), choices(&.expected.iter().map(|v| t2s(*v).to_string()).collect::<Vec<String>>()))]
    TypeConflict {
        /// extended / context location of the error
        expr: Span,
        /// exact / narrow location of the error
        inner: Span,
        /// actual type
        got: ValueType,
        /// list of expected possible types
        expected: Vec<ValueType>,
    },
    /// Invalid Recur usage
    #[error("Can not recur from this location")]
    InvalidRecur {
        /// extended / context location of the error
        expr: Span,
        /// extended / context location of the error
        inner: Span,
    },
    /// module not found
    #[error("Module `{}` not found or not readable error in module path: {}",
                resolved_relative_file_path.trim(),
                expected.iter().fold(String::new(), |mut output, x|
                {
                    // ALLOW: if we can't allocate it's worse, we'd have the same problem with format
                    let _ = write!(output, "\n                         - {x}");
                    output

            }))]
    ModuleNotFound {
        /// extended / context location of the error
        expr: Span,

        /// extended / context location of the error
        inner: Span,
        /// resolved file path - might not exist
        resolved_relative_file_path: String,
        /// expected
        expected: Vec<String>,
    },
    /// invalid usage of emit
    #[error("Can not emit from this location")]
    InvalidEmit {
        /// extended / context location of the error
        expr: Span,
        /// extended / context location of the error
        inner: Span,
    },
    /// invalid usage of drop
    #[error("Can not drop from this location")]
    InvalidDrop {
        /// extended / context location of the error
        expr: Span,
        /// extended / context location of the error
        inner: Span,
    },
    /// invalid extractor
    #[error("Invalid tilde predicate pattern: {error}")]
    InvalidExtractor {
        /// extended / context location of the error
        expr: Span,
        /// extended / context location of the error
        inner: Span,
        /// name
        name: String,
        /// pattern
        pattern: String,
        /// error
        error: String,
    },
    /// effectors are missing
    #[error("The clause is missing a body")]
    MissingEffectors {
        /// extended / context location of the error
        expr: Span,
        /// extended / context location of the error
        inner: Span,
    },
}

impl From<ParserError> for Error {
    fn from(value: ParserError) -> Self {
        Error::Parser(Box::new(value))
    }
}

/// Lexer Errors
#[derive(Debug, thiserror::Error)]
pub enum LexerError {
    /// An unrecognized token
    #[error("Found the token `{token}` but expected {}", choices(.expected))]
    UnrecognizedToken {
        /// Error location
        location: ErrorLocation,
        /// found token
        token: String,
        /// possible tokens for the given location
        expected: Vec<String>,
    },
    /// An unexpected extra token
    #[error("Found an extra token: `{token}` that does not belong here")]
    ExtraToken {
        /// Error location
        location: ErrorLocation,
        /// extra token
        token: String,
    },
    /// invalid token
    #[error("Invalid token")]
    InvalidToken {
        /// Error location
        location: ErrorLocation,
    },
    /// invalid preprocessor
    #[error("Found the preprocessor directive `{directive}` but expected {}", choices(&["#!config"]))]
    InvalidPP {
        /// Error location
        location: ErrorLocation,
        /// invalid preprocessor directive
        directive: String,
    },
    /// unerminated extractor
    #[error("It looks like you forgot to terminate an extractor with a closing '|'")]
    UnterminatedExtractor {
        /// Error location
        location: ErrorLocation,
        /// unterminated extractor token
        extractor: UnfinishedToken,
    },
    /// unterminated string
    #[error("It looks like you forgot to terminate a string with a closing '\"'")]
    UnterminatedStringLiteral {
        /// Error location
        location: ErrorLocation,
        /// unterminated string token
        string: UnfinishedToken,
    },
    /// unterminated heredoc
    #[error("It looks like you forgot to terminate a here doc with with a closing '\"\"\"'")]
    UnterminatedHereDoc {
        /// Error location
        location: ErrorLocation,
        /// unterminated heredoc token
        string: UnfinishedToken,
    },
    /// tailing heredoc
    #[error("It looks like you have characters tailing the here doc opening, it needs to be followed by a newline")]
    TailingHereDoc {
        /// Error location
        location: ErrorLocation,
        /// unterminated heredoc token
        hd: UnfinishedToken,
        /// some character
        ch: char,
    },
    /// unterminated interpolation
    #[error("It looks like you forgot to terminate a string interpolation with a closing '}}'")]
    UnterminatedInterpolation {
        /// Error location
        location: ErrorLocation,
        /// interpolation token
        string_with_interpolation: UnfinishedToken,
    },
    /// empty interpolation
    #[error("You have an interpolation without content.")]
    EmptyInterpolation {
        /// Error location
        location: ErrorLocation,
        /// token
        string_with_interpolation: UnfinishedToken,
    },
    /// unterminated ident
    #[error("It looks like you forgot to terminate an ident with a closing '`'")]
    UnterminatedIdentLiteral {
        /// Error location
        location: ErrorLocation,
        /// unterminated ident
        ident: UnfinishedToken,
    },
    /// unexpected character
    #[error("An unexpected character '{found}' was found")]
    UnexpectedCharacter {
        /// Error location
        location: ErrorLocation,
        /// token with bad character
        token: UnfinishedToken,
        /// unexpected character
        found: char,
    },
    /// invalid escape code
    #[error("An unexpected escape code '{found}' was found")]
    UnexpectedEscapeCode {
        /// Error location
        location: ErrorLocation,
        /// token
        token: UnfinishedToken,
        /// unexpected escape code
        found: char,
    },
    /// invalid utf8
    #[error("An invalid UTF8 escape sequence was found")]
    InvalidUtf8Sequence {
        /// Error location
        location: ErrorLocation,
        /// invalid utf8 token
        token: UnfinishedToken,
    },
    /// invalid hex int
    #[error("An invalid hexadecimal")]
    InvalidHexLiteral {
        /// Error location
        location: ErrorLocation,
        /// invalid hex token
        token: UnfinishedToken,
    },
    /// invalid literal int
    #[error("An invalid integer literal")]
    InvalidIntLiteral {
        /// Error location
        location: ErrorLocation,
        /// invalid int token
        token: UnfinishedToken,
    },
    /// invalid literal float
    #[error("An invalid float literal")]
    InvalidFloatLiteral {
        /// Error location
        location: ErrorLocation,
        /// invalid float token
        token: UnfinishedToken,
    },
    /// EOS
    #[error("An unexpected end of stream was found")]
    UnexpectedEndOfStream {
        /// Error location
        location: ErrorLocation,
    },

    /// Unspecified lexer Error
    #[error("Lexer error: {pos}")]
    Lexer {
        /// human readable position
        pos: String,
    },
}

impl From<LexerError> for Error {
    fn from(value: LexerError) -> Self {
        Error::Lexer(Box::new(value))
    }
}

/// Error related to function definition and usage
#[derive(Debug, thiserror::Error)]
pub enum FunctionError {
    /// Bad arity
    #[error("Bad arity for function {m}::{f}/{a:?} but was called with {calling_a} arguments")]
    BadArity {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// module
        m: String,
        /// function
        f: String,
        /// definition arity
        a: RangeInclusive<usize>,
        /// arity of the call-site
        calling_a: usize,
    },
    /// module missing/undefined
    #[error("Call to undefined module {m}")]
    MissingModule {
        /// wider context location of the error
        outer: Span,
        /// more specific location of the error
        inner: Span,
        /// module
        m: String,
        /// suggested module names
        suggestion: Option<(usize, String)>,
    },
    /// function missing/undefined
    #[error("Call to undefined function {}::{f}", .m.join("::"))]
    MissingFunction {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// module
        m: Vec<String>,
        /// function
        f: String,
        /// suggested function names
        suggestion: Option<(usize, String)>,
    },
    /// aggregate used inside aggregate
    #[error("Aggregates can not be called inside of aggregates")]
    AggrInAggr {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
    },
    /// bad argument type
    #[error("Bad type passed to function {m}::{f}/{a}")]
    BadType {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// module
        m: String,
        /// function
        f: String,
        /// arity
        a: usize,
    },
}

impl From<FunctionError> for Error {
    fn from(value: FunctionError) -> Self {
        Error::Function(Box::new(value))
    }
}

/// Errors raised from the tremor-script interpreter
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// runtime error during function execution
    #[error("Runtime error in function {m}::{f}/{a}: {c}")]
    RuntimeError {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// module
        m: String,
        /// function
        f: String,
        /// arity
        a: usize,
        /// error
        c: String,
    },
    /// recursion limit reached
    #[error("Recursion limit reached")]
    RecursionLimit {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
    },
    /// unknown local variable/binding
    #[error("Unknown local variable: `{name}`")]
    UnknownLocal {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// name
        name: String,
    },
    /// bad access into a local variable
    #[error("Trying to access a non existing local key `{key}`")]
    BadAccessInLocal {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// access key
        key: String,
        /// possible existing keys
        options: Vec<String>,
    },
    /// bad access into a global variable
    #[error("Trying to access a non existing global key `{key}`")]
    BadAccessInGlobal {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// access key
        key: String,
        /// possibly existing keys
        options: Vec<String>,
    },
    /// bad access into event
    #[error("Trying to access a non existing event key `{key}`")]
    BadAccessInEvent {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// access key
        key: String,
        /// possible existing keys
        options: Vec<String>,
    },
    #[error("Trying to access a non existing state key `{key}`")]
    /// bad access into state
    BadAccessInState {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// access key
        key: String,
        /// possible existing keys
        options: Vec<String>,
    },
    /// bad array index
    #[error("Bad array index, got `{idx}` but expected an index in the range 0:{len}")]
    BadArrayIndex {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// array index
        idx: Value<'static>,
        /// actual array length
        len: usize,
    },
    /// decreasing range
    #[error("A range's end cannot be smaller than its start, {start_idx}:{end_idx} is invalid")]
    DecreasingRange {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// range start index
        start_idx: usize,
        /// range end index
        end_idx: usize,
    },
    /// assigning a value into an array
    #[error("It is not supported to assign value into an array")]
    AssignIntoArray {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
    },
    /// cannot assign to this value
    #[error("You are trying to assign to a value that isn't valid")]
    InvalidAssign {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
    },
    /// Cannot assign to a const (more than once upon definition)
    #[error("Can't assign to a constant expression")]
    AssignToConst {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
    },
    /// invalid unary operation
    #[error("The unary operation `{op}` is not defined for the type `{}`", t2s(*.val))]
    InvalidUnary {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// operator
        op: ast::UnaryOpKind,
        /// value type
        val: ValueType,
    },
    /// invalid binary operator
    #[error("The binary operation `{op}` is not defined for the type `{}` and `{}`", t2s(*.left), t2s(*.right))]
    InvalidBinary {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// operator
        op: ast::BinOpKind,
        /// lhs value type
        left: ValueType,
        /// rhs value type
        right: ValueType,
    },
    /// division by zero
    #[error("The binary operation `{op}` must have a non zero RHS")]
    DivisionByZero {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// operator
        op: ast::BinOpKind,
    },
    /// integer overflow or underflow
    #[error("The binary operation `{op}` caused an over- or underflow")]
    Overflow {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// operator
        op: ast::BinOpKind,
    },
    /// invalid binary operator
    #[error("The binary operation `{op}` is not defined for the type `{}` and `{}`", t2s(*.left), .right.map_or_else(|| "<not executed>", t2s))]
    InvalidBinaryBoolean {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// operator
        op: ast::BooleanBinOpKind,
        /// lhs type
        left: ValueType,
        /// rhs type
        right: Option<ValueType>,
    },
    /// invalid bitshift
    #[error("RHS value is larger than or equal to the number of bits in LHS value")]
    InvalidBitshift {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
    },
    /// no match clause was executed
    #[error("A match expression executed but no clause matched")]
    NoClauseHit {
        /// wider context location of the error
        expr: Span,
    },
    /// key used in patch already exists
    #[error("The key that is supposed to be written to already exists: {key}")]
    PatchKeyExists {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// key to patch
        key: String,
    },
    #[error("The key that is supposed to be updated does not exists: {key}")]
    /// key to update is missing
    UpdateKeyMissing {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// missing key
        key: String,
    },
    /// type conflict in merge
    #[error("Merge can only be performed on keys that either do not exist or are records but the key '{key}' has the type {}", t2s(*.val))]
    MergeTypeConflict {
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// key to be merged
        key: String,
        /// type
        val: ValueType,
    },
    /// Array index out of bounds
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
        /// wider context location of the error
        expr: Span,
        /// more specific location of the error
        inner: Span,
        /// array index-range
        r: RangeExclusive<usize>,
        /// actual length of the array
        len: usize,
    },
}

impl From<RuntimeError> for Error {
    fn from(value: RuntimeError) -> Self {
        Error::Runtime(Box::new(value))
    }
}

/// Tremor-query / pipeline definition errors
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// Queyr stream not defined
    #[error("Stream used in `from` or `into` is not defined: {name}/{port}")]
    QueryStreamNotDefined {
        /// error location
        location: ErrorLocation,
        /// name of the stream
        name: String,
        /// port of the stream
        port: String,
    },
    /// no locals allowerd here
    #[error("Local variables are not allowed here")]
    NoLocalsAllowed {
        /// error location
        location: ErrorLocation,
    },
    /// no consts allowed here
    #[error("Constants are not allowed here")]
    NoConstsAllowed {
        /// error location
        location: ErrorLocation,
    },
    /// No references to `event` allowed here
    #[error("References to `event` or `$` are not allowed in this context")]
    NoEventReferencesAllowed {
        /// error location
        location: ErrorLocation,
    },
    /// failed to initialize window constant
    #[error("Failed to initialize window constant")]
    CantSetWindowConst,
    /// failed to initialize group constant
    #[error("Failed to initialize group constant")]
    CantSetGroupConst,
    /// failed to initialize args constant
    #[error("Failed to initialize args constant")]
    CantSetArgsConst,
    /// given name is reserved
    #[error("Name `{name}` is reserved for built-in nodes, please use another name.")]
    QueryNodeReservedName {
        /// error location
        location: ErrorLocation,
        /// reserved name
        name: String,
    },
    /// Name is already in use
    #[error("Name `{name}` is already in use for another node, please use another name.")]
    QueryNodeDuplicateName {
        /// error location
        location: ErrorLocation,
        /// duplicate name
        name: String,
    },
    /// pipeline port is not known / defined
    #[error("Query `{subq_name}` does not have port `{port_name}`")]
    PipelineUnknownPort {
        /// error location
        location: ErrorLocation,
        /// subquery name
        subq_name: String,
        /// port name
        port_name: String,
    },
    /// stream already defined
    #[error("Can't define the stream `{name}` twice")]
    DoubleStream {
        /// error location
        location: ErrorLocation,
        /// already defined name
        name: String,
    },
}

impl From<QueryError> for Error {
    fn from(value: QueryError) -> Self {
        Error::Query(Box::new(value))
    }
}

/// Troy / tremor-deploy errors
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    /// Deploy artefact not found
    #[error("Artefact `{name}` is not defined or not found, the following are defined: {}", .options.join(", "))]
    DeployArtefactNotDefined {
        /// error location
        location: ErrorLocation,
        /// name of the references artefact
        name: String,
        /// options
        options: Vec<String>,
    },
    /// user provided with parameter that has no corresponding argument in the definition
    #[error("`with` parameter \"{param_name}\" does not correspond to an argument in the target definition \"{definition_name}\"")]
    WithParamNoArg {
        /// error location
        location: ErrorLocation,
        /// param name
        param_name: String,
        /// param definition name
        definition_name: String,
        /// available arguments
        available_args: Vec<String>,
    },
    /// with param invalid
    #[error("Invalid `with` parameter \"{param}\" in definition of {definition}.")]
    InvalidDefinitionalWithParam {
        /// error location
        location: ErrorLocation,
        /// defined param
        definition: String,
        /// provided param
        param: String,
        /// available arguments
        available_params: &'static [&'static str],
    },
}

impl From<DeployError> for Error {
    fn from(value: DeployError) -> Self {
        Error::Deploy(Box::new(value))
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
        if let Error::Parser(parser_error) = &self {
            match **parser_error {
                ParserError::TypeError {
                    expected, found, ..
                } => Error::Parser(Box::new(ParserError::TypeError {
                    expr: Some(outer.extent()),
                    inner: Some(inner.extent()),
                    expected,
                    found,
                })),
                _ => self,
            }
        } else {
            self
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

type LalrPopError<'screw_lalrpop> =
    lalrpop_util::ParseError<pos::Location, lexer::Token<'screw_lalrpop>, Error>;

impl<'screw_lalrpop> From<LalrPopError<'screw_lalrpop>> for Error {
    fn from(error: LalrPopError<'screw_lalrpop>) -> Self {
        match error {
            LalrpopError::UnrecognizedToken {
                token: (start, token, end),
                expected,
            } => Error::Lexer(Box::new(LexerError::UnrecognizedToken {
                location: ErrorLocation {
                    expr: (start.move_up_lines(2), end.move_down_lines(2)).into(),
                    inner: (start, end).into(),
                },
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
            })),
            LalrpopError::ExtraToken {
                token: (start, token, end),
            } => Error::Lexer(Box::new(LexerError::ExtraToken {
                location: ErrorLocation {
                    expr: (start.move_up_lines(2), end.move_down_lines(2)).into(),
                    inner: (start, end).into(),
                },
                token: token.to_string(),
            })),
            LalrpopError::InvalidToken { location: start } => {
                let mut end = start;
                end.shift(' ');
                Error::Lexer(Box::new(LexerError::InvalidToken {
                    location: ErrorLocation {
                        expr: (start.move_up_lines(2), end.move_down_lines(2)).into(),
                        inner: (start, end).into(),
                    },
                }))
            }
            _ => Error::Lexer(Box::new(LexerError::Lexer {
                pos: format!("{error:?}"),
            })),
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

impl From<&Span> for ErrorLocation {
    fn from(value: &Span) -> Self {
        Self {
            expr: value.expand_lines(2),
            inner: *value,
        }
    }
}

impl From<Span> for ErrorLocation {
    fn from(value: Span) -> Self {
        Self {
            expr: value.expand_lines(2),
            inner: value,
        }
    }
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
    //pub(crate) fn aid(&self) -> arena::Index {
    //    self.expr().map(|loc| loc.expr.aid()).unwrap_or_default()
    //}
    #[allow(clippy::too_many_lines)]
    pub(crate) fn expr(&self) -> Option<ErrorLocation> {
        match self {
            Self::Runtime(runtime_err) => match **runtime_err {
                RuntimeError::NoClauseHit { expr } => Some(ErrorLocation::from(&expr)),
                RuntimeError::RuntimeError { expr, inner, .. }
                | RuntimeError::RecursionLimit { expr, inner }
                | RuntimeError::UnknownLocal { expr, inner, .. }
                | RuntimeError::BadAccessInLocal { expr, inner, .. }
                | RuntimeError::BadAccessInGlobal { expr, inner, .. }
                | RuntimeError::BadAccessInEvent { expr, inner, .. }
                | RuntimeError::BadAccessInState { expr, inner, .. }
                | RuntimeError::BadArrayIndex { expr, inner, .. }
                | RuntimeError::DecreasingRange { expr, inner, .. }
                | RuntimeError::AssignIntoArray { expr, inner }
                | RuntimeError::InvalidAssign { expr, inner }
                | RuntimeError::AssignToConst { expr, inner }
                | RuntimeError::InvalidUnary { expr, inner, .. }
                | RuntimeError::InvalidBinary { expr, inner, .. }
                | RuntimeError::DivisionByZero { expr, inner, .. }
                | RuntimeError::Overflow { expr, inner, .. }
                | RuntimeError::InvalidBinaryBoolean { expr, inner, .. }
                | RuntimeError::InvalidBitshift { expr, inner }
                | RuntimeError::PatchKeyExists { expr, inner, .. }
                | RuntimeError::UpdateKeyMissing { expr, inner, .. }
                | RuntimeError::ArrayOutOfRange { expr, inner, .. }
                | RuntimeError::MergeTypeConflict { expr, inner, .. } => {
                    Some(ErrorLocation { expr, inner })
                }
            },
            Self::Lexer(lexer_err) => match &**lexer_err {
                LexerError::UnrecognizedToken { location, .. }
                | LexerError::ExtraToken { location, .. }
                | LexerError::InvalidToken { location }
                | LexerError::InvalidPP { location, .. }
                | LexerError::UnterminatedExtractor { location, .. }
                | LexerError::UnterminatedStringLiteral { location, .. }
                | LexerError::UnterminatedHereDoc { location, .. }
                | LexerError::TailingHereDoc { location, .. }
                | LexerError::UnterminatedInterpolation { location, .. }
                | LexerError::EmptyInterpolation { location, .. }
                | LexerError::UnterminatedIdentLiteral { location, .. }
                | LexerError::UnexpectedCharacter { location, .. }
                | LexerError::UnexpectedEscapeCode { location, .. }
                | LexerError::InvalidUtf8Sequence { location, .. }
                | LexerError::InvalidHexLiteral { location, .. }
                | LexerError::InvalidIntLiteral { location, .. }
                | LexerError::InvalidFloatLiteral { location, .. }
                | LexerError::UnexpectedEndOfStream { location } => Some(location.clone()),
                LexerError::Lexer { .. } => None,
            },
            Self::Oops { expr: outer, .. } => Some(ErrorLocation::from(outer)),
            Self::Query(query_err) => match &**query_err {
                QueryError::QueryStreamNotDefined { location, .. }
                | QueryError::NoLocalsAllowed { location }
                | QueryError::NoConstsAllowed { location }
                | QueryError::NoEventReferencesAllowed { location }
                | QueryError::QueryNodeReservedName { location, .. }
                | QueryError::QueryNodeDuplicateName { location, .. }
                | QueryError::DoubleStream { location, .. }
                | QueryError::PipelineUnknownPort { location, .. } => Some(location.clone()),
                QueryError::CantSetWindowConst
                | QueryError::CantSetGroupConst
                | QueryError::CantSetArgsConst => None,
            },
            Self::Function(function_err) => match **function_err {
                FunctionError::BadArity { expr, inner, .. }
                | FunctionError::MissingModule {
                    outer: expr, inner, ..
                }
                | FunctionError::MissingFunction { expr, inner, .. }
                | FunctionError::AggrInAggr { expr, inner }
                | FunctionError::BadType { expr, inner, .. } => Some(ErrorLocation { expr, inner }),
            },
            Self::Parser(parser_err) => match **parser_err {
                ParserError::Generic { expr, inner, .. }
                | ParserError::CyclicUse { expr, inner, .. }
                | ParserError::NotConstant { expr, inner }
                | ParserError::TypeConflict { expr, inner, .. }
                | ParserError::InvalidRecur { expr, inner }
                | ParserError::ModuleNotFound { expr, inner, .. }
                | ParserError::InvalidEmit { expr, inner }
                | ParserError::InvalidDrop { expr, inner }
                | ParserError::InvalidExtractor { expr, inner, .. }
                | ParserError::MissingEffectors { expr, inner } => {
                    Some(ErrorLocation { expr, inner })
                }
                ParserError::TypeError { expr, inner, .. } => expr
                    .zip(inner)
                    .map(|(expr, inner)| ErrorLocation { expr, inner }),
            },
            Self::Deploy(deploy_err) => match &**deploy_err {
                DeployError::InvalidDefinitionalWithParam { location, .. }
                | DeployError::WithParamNoArg { location, .. }
                | DeployError::DeployArtefactNotDefined { location, .. } => Some(location.clone()),
            },
            Error::Url(_)
            | Error::Poison(_)
            | Error::Grok(_)
            | Error::Regex(_)
            | Error::Io(_)
            | Error::JsonError(_)
            | Error::ValueError(_)
            | Error::ParseIntError(_)
            | Error::Utf8Error(_)
            | Error::FromUtf8Error(_)
            | Error::NoObjectError(_)
            | Error::AccessError(_)
            | Error::CodecError(_)
            | Error::Common(_)
            | Error::InvalidHostname(_)
            | Error::FileNotFound(_)
            | Error::String(_)
            | Error::NotFound => None,
        }
    }

    pub(crate) fn token(&self) -> Option<UnfinishedToken> {
        match self {
            Self::Lexer(lexer_err) => match &**lexer_err {
                LexerError::UnterminatedExtractor {
                    extractor: token, ..
                }
                | LexerError::UnterminatedStringLiteral { string: token, .. }
                | LexerError::UnterminatedInterpolation {
                    string_with_interpolation: token,
                    ..
                }
                | LexerError::EmptyInterpolation {
                    string_with_interpolation: token,
                    ..
                }
                | LexerError::UnterminatedIdentLiteral { ident: token, .. }
                | LexerError::UnterminatedHereDoc { string: token, .. }
                | LexerError::TailingHereDoc { hd: token, .. }
                | LexerError::InvalidUtf8Sequence { token, .. }
                | LexerError::UnexpectedCharacter { token, .. }
                | LexerError::InvalidHexLiteral { token, .. }
                | LexerError::InvalidIntLiteral { token, .. }
                | LexerError::InvalidFloatLiteral { token, .. }
                | LexerError::UnexpectedEscapeCode { token, .. } => Some(token.clone()),
                _ => None,
            },

            _ => None,
        }
    }

    pub(crate) fn hint(&self) -> Option<String> {
        match self {
            Self::Lexer(lexer_err) =>
                match &**lexer_err {
                    LexerError::UnrecognizedToken{location, token, ..} if token.is_empty() && location.inner.start().absolute() == location.expr.start().absolute() => Some("It looks like a `;` is missing at the end of the script".into()),
                    LexerError::UnrecognizedToken{token, ..} if token == "##" => Some(format!("`{token}` is as doc comment, it needs to be followed by a statement, did you want to use `#` here?")),
                    LexerError::UnrecognizedToken{token, ..} if token == "default" || token == "case" => Some("You might have a trailing `;` in the prior statement".into()),
                    LexerError::UnrecognizedToken{token, expected, .. } if token == "\"" && expected.contains(&("`<ident>`".to_string())) => Some("Did you mean to quote an ident? If so use ` (a back tick) not \" (a quote).".into()),
                    LexerError::UnrecognizedToken{token, expected, ..} if !matches!(lexer::ident_to_token(token), lexer::Token::Ident(_, _)) && expected.contains(&("`<ident>`".to_string())) => Some(format!("It looks like you tried to use '{token}' as an ident, consider quoting it as `{token}` to make it an identifier.")),
                    LexerError::UnrecognizedToken{token, expected, ..} if token == "-" && expected.contains(&("`(`".to_string())) => Some("Try wrapping this expression in parentheses `(` ... `)`".into()),
                    LexerError::UnrecognizedToken{token, expected, ..} => {
                        match best_hint(token, expected, 3) {
                            Some((_d, o)) if o == r#"`"`"# || o == r#"`"""`"#  => Some("Did you mean to use a string?".to_string()),
                            Some((_d, o)) if o != r#"`"`"# && o != r#"`"""`"# => Some(format!("Did you mean to use {o}?")),
                            _ => None
                        }
                    }
                    LexerError::UnterminatedInterpolation { .. } | LexerError::EmptyInterpolation { .. } => {
                        Some("Did you mean to write a literal '#{'? Escape it as '\\#{'.".to_string())
                    }
                    _ => None
                },
            Self::Oops { id, .. } => Some(format!("Please take the error output script and test data and open a ticket, this should not happen.\nhttps://github.com/tremor-rs/tremor-runtime/issues/new?labels=bug&template=bug_report.md&title=Opps%20{id}")),
            Self::Runtime(runtime_err) => {
                match &**runtime_err {
                    RuntimeError::BadAccessInLocal { key, ..} if key == "nil" => {
                        Some("Did you mean null?".to_owned())
                    }

                    RuntimeError::BadAccessInLocal { key, options, .. } => {
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

                    RuntimeError::BadAccessInEvent { key, options, .. } | RuntimeError::BadAccessInGlobal { key, options, .. } => {
                        match best_hint(key, options, 2) {
                            Some((_d, o)) => Some(format!("Did you mean to use `{o}`?")),
                            _ => None
                        }
                    }
                    RuntimeError::NoClauseHit { .. } => Some("Consider adding a `case _ => null` clause at the end of your match or validate full coverage beforehand.".into()),
                    _ => None
                }
            },
            Self::Parser(parser_err) => {
                match &**parser_err {
                    ParserError::TypeConflict { got: ValueType::F64, expected, .. } => match expected.as_slice() {
                        [ValueType::I64] => Some(
                            "You can use math::trunc() and related functions to ensure numbers are integers."
                                .to_owned(),
                        ),
                        _ => None
                    },
                    _ => None
                }
            },
            Self::Function(function_err) => {
                match &**function_err {
                    FunctionError::MissingModule { m, ..} if m == "object" => Some("Did you mean to use the `record` module".into()),
                    FunctionError::MissingModule { suggestion: Some((_, suggestion)), .. } | FunctionError::MissingFunction { suggestion: Some((_, suggestion)), .. } => Some(format!("Did you mean `{suggestion}`?")),
                    _ => None
                }
            },
            Self::Query(query_err) => {
                match &**query_err {
                    QueryError::NoEventReferencesAllowed { .. } => Some("Here you operate in the whole window, not a single event. You need to wrap this reference in an aggregate function (e.g. aggr::win::last(...)) or use it in the group by clause of this query.".to_owned()),
                    _ => None
                }
            },
            Self::Deploy(deploy_err) => {
                match &**deploy_err {
                    DeployError::InvalidDefinitionalWithParam { available_params, .. } => if available_params.is_empty() {
                        Some(String::from("Definition does not allow any `with` parameters"))
                    } else {
                        Some(format!("Available parameters are: {}", available_params.join(", ")))
                    },
                    DeployError::WithParamNoArg { definition_name, available_args, .. } => if available_args.is_empty() {
                        Some(format!("The definition of \"{definition_name}\" does not expose any args. Remove this `with`."))
                    } else {
                        Some(format!("Available args are: {}", available_args.join(", ")))
                    },

                DeployError::DeployArtefactNotDefined{ .. } => None
                }
            },
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
    QueryError::QueryStreamNotDefined {
        location: ErrorLocation {
            expr: stmt.extent(),
            inner: inner.extent(),
        },
        name,
        port,
    }
    .into()
}

/// Creates a query stream duplicate name error
pub fn query_stream_duplicate_name_err<S: Ranged, I: BaseExpr + Ranged>(
    stmt: &S,
    inner: &I,
    name: String,
) -> Error {
    let name = inner.meta().name().map_or(name, std::convert::Into::into);
    QueryError::DoubleStream {
        location: ErrorLocation {
            expr: stmt.extent(),
            inner: inner.extent(),
        },
        name,
    }
    .into()
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
    QueryError::PipelineUnknownPort {
        location: ErrorLocation {
            expr: stmt.extent(),
            inner: inner.extent(),
        },
        subq_name,
        port_name,
    }
    .into()
}

/// Creates a query node reserved name error
pub fn query_node_reserved_name_err<S: BaseExpr + Ranged>(stmt: &S, name: String) -> Error {
    let name = stmt.meta().name().map_or(name, std::convert::Into::into);
    QueryError::QueryNodeReservedName {
        location: ErrorLocation::from(&stmt.extent()),
        name,
    }
    .into()
}

/// Creates a query node duplicate name error
pub fn query_node_duplicate_name_err<S: BaseExpr + Ranged>(stmt: &S, name: String) -> Error {
    let name = stmt.meta().name().map_or(name, std::convert::Into::into);
    QueryError::QueryNodeDuplicateName {
        location: ErrorLocation::from(&stmt.extent()),
        name,
    }
    .into()
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
    ParserError::Generic {
        expr: outer.extent(),
        inner: inner.extent(),
        msg: error.to_string(),
    }
    .into()
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
    RuntimeError::InvalidBinaryBoolean {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
        left,
        right,
    }
    .into()
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
    ParserError::TypeConflict {
        expr: outer.extent(),
        inner: inner.extent(),
        got,
        expected,
    }
    .into()
}

pub(crate) fn error_no_locals<T, O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Result<T> {
    Err(QueryError::NoLocalsAllowed {
        location: ErrorLocation {
            expr: outer.extent(),
            inner: inner.extent(),
        },
    }
    .into())
}

pub(crate) fn error_event_ref_not_allowed<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
) -> Result<T> {
    Err(QueryError::NoEventReferencesAllowed {
        location: ErrorLocation {
            expr: outer.extent(),
            inner: inner.extent(),
        },
    }
    .into())
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
    RuntimeError::InvalidUnary {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
        val: val.value_type(),
    }
    .into()
}

pub(crate) fn error_invalid_binary<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
    left: &Value,
    right: &Value,
) -> Result<T> {
    Err(RuntimeError::InvalidBinary {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
        left: left.value_type(),
        right: right.value_type(),
    }
    .into())
}
pub(crate) fn error_division_by_zero<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
) -> Result<T> {
    Err(RuntimeError::DivisionByZero {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
    }
    .into())
}
pub(crate) fn error_overflow<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    op: ast::BinOpKind,
) -> Result<T> {
    Err(RuntimeError::Overflow {
        expr: outer.extent(),
        inner: inner.extent(),
        op,
    }
    .into())
}

pub(crate) fn error_invalid_bitshift<T, O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Result<T> {
    Err(RuntimeError::InvalidBitshift {
        expr: outer.extent(),
        inner: inner.extent(),
    }
    .into())
}

pub(crate) fn error_no_clause_hit<T, O: Ranged>(outer: &O) -> Result<T> {
    Err(RuntimeError::NoClauseHit {
        expr: outer.extent(),
    }
    .into())
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
    Err(RuntimeError::PatchKeyExists {
        expr: outer.extent(),
        inner: inner.extent(),
        key,
    }
    .into())
}

pub(crate) fn error_patch_update_key_missing<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    key: String,
) -> Result<T> {
    Err(RuntimeError::UpdateKeyMissing {
        expr: outer.extent(),
        inner: inner.extent(),
        key,
    }
    .into())
}

pub(crate) fn error_missing_effector<O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Error {
    ParserError::MissingEffectors {
        expr: outer.extent(),
        inner: inner.extent(),
    }
    .into()
}
pub(crate) fn error_patch_merge_type_conflict<T, O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    key: String,
    val: &Value,
) -> Result<T> {
    Err(RuntimeError::MergeTypeConflict {
        expr: outer.extent(),
        inner: inner.extent(),
        key,
        val: val.value_type(),
    }
    .into())
}

pub(crate) fn error_assign_array<T, O: Ranged, I: Ranged>(outer: &O, inner: &I) -> Result<T> {
    Err(RuntimeError::AssignIntoArray {
        expr: outer.extent(),
        inner: inner.extent(),
    }
    .into())
}
pub(crate) fn error_invalid_assign_target<T, O: Ranged>(outer: &O) -> Result<T> {
    let inner: Span = outer.extent();

    Err(RuntimeError::InvalidAssign {
        expr: inner.expand_lines(2),
        inner,
    }
    .into())
}
pub(crate) fn error_assign_to_const<T, O: Ranged>(outer: &O) -> Result<T> {
    let inner: Span = outer.extent();

    Err(RuntimeError::AssignToConst {
        expr: inner.expand_lines(2),
        inner,
    }
    .into())
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
        | ast::Path::Expr(_) => RuntimeError::ArrayOutOfRange {
            expr,
            inner: inner.extent(),
            r,
            len,
        }
        .into(),
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
        | ast::Path::Expr(_) => RuntimeError::BadArrayIndex {
            expr,
            inner: inner.extent(),
            idx,
            len,
        }
        .into(),
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
        | ast::Path::Expr(_) => RuntimeError::DecreasingRange {
            expr,
            inner: inner.extent(),
            start_idx,
            end_idx,
        }
        .into(),
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
    RuntimeError::UnknownLocal {
        expr: outer.extent(),
        inner: inner.extent(),
        name: inner.name_dflt().to_string(),
    }
    .into()
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
            RuntimeError::BadAccessInLocal {
                expr,
                inner: inner.extent(),
                key,
                options,
            }
        }
        ast::Path::Meta(_p) => RuntimeError::BadAccessInGlobal {
            expr,
            inner: inner.extent(),
            key,
            options,
        },
        ast::Path::Event(_p) => RuntimeError::BadAccessInEvent {
            expr,
            inner: inner.extent(),
            key,
            options,
        },
        ast::Path::State(_p) => RuntimeError::BadAccessInState {
            expr,
            inner: inner.extent(),
            key,
            options,
        },
    }
    .into()
}

pub(crate) fn unexpected_character<O: Ranged, I: Ranged>(
    outer: &O,
    inner: &I,
    tkn: UnfinishedToken,
    ch: char,
) -> Error {
    LexerError::UnexpectedCharacter {
        location: ErrorLocation {
            expr: outer.extent(),
            inner: inner.extent(),
        },
        token: tkn,
        found: ch,
    }
    .into()
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
        if let Error::Parser(parser_err) = r {
            match *parser_err {
                ParserError::TypeError {
                    expr: None,
                    inner: None,
                    expected: ValueType::Object,
                    found: ValueType::String,
                } => {}
                other => {
                    panic!("Expected TypeError, got {other:?}");
                }
            }
        } else {
            panic!("Expected TypeError, got {r:?}");
        }
    }
}
