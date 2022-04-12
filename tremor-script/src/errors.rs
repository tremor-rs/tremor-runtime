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

// NOTE: We need this because of error_chain
#![allow(clippy::large_enum_variant)]
#![allow(deprecated)]
#![allow(missing_docs)]

pub use crate::prelude::ValueType;
use crate::{
    arena,
    ast::{self, BaseExpr},
    errors, lexer,
    pos::{self, Span},
    prelude::*,
    Value,
};
use error_chain::error_chain;
use lalrpop_util::ParseError as LalrpopError;

use std::num;
use std::ops::{Range as RangeExclusive, RangeInclusive};

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

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e))
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {:?}", e))
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
                expected
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
                end.shift(' ');
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
        ValueType::Custom(c) => c,
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
pub(crate) type ErrorLocation = (Option<Span>, Option<Span>);

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub struct UnfinishedToken {
    pub(crate) range: Span,
    pub(crate) value: String,
}

impl UnfinishedToken {
    pub(crate) fn new(range: Span, value: String) -> Self {
        Self { range, value }
    }
}

impl ErrorKind {
    pub(crate) fn aid(&self) -> arena::Index {
        self.expr().0.map(Span::aid).unwrap_or_default()
    }
    #[allow(clippy::too_many_lines)]
    pub(crate) fn expr(&self) -> ErrorLocation {
        use ErrorKind::{
            AccessError, AggrInAggr, ArrayOutOfRange, AssignIntoArray, AssignToConst,
            BadAccessInEvent, BadAccessInGlobal, BadAccessInLocal, BadAccessInState, BadArity,
            BadArrayIndex, BadType, BinaryDrop, BinaryEmit, CantSetArgsConst, CantSetGroupConst,
            CantSetWindowConst, Common, DecreasingRange, DeployArgNotSpecified,
            DeployArtefactNotDefined, DeployRequiredArgDoesNotResolve, DoubleConst,
            DoublePipelineCreate, DoubleStream, EmptyInterpolation, EmptyScript, ExtraToken,
            Generic, Grok, InvalidAssign, InvalidBinary, InvalidBitshift, InvalidConst,
            InvalidDrop, InvalidEmit, InvalidExtractor, InvalidFloatLiteral, InvalidFn,
            InvalidHexLiteral, InvalidIntLiteral, InvalidMod, InvalidPP, InvalidRecur,
            InvalidToken, InvalidUnary, InvalidUtf8Sequence, Io, JsonError, MergeTypeConflict,
            MissingEffectors, MissingFunction, MissingModule, ModuleNotFound, Msg, NoClauseHit,
            NoConstsAllowed, NoEventReferencesAllowed, NoLocalsAllowed, NoObjectError, NotConstant,
            NotFound, Oops, ParseIntError, ParserError, PatchKeyExists, PipelineUnknownPort,
            PreprocessorError, QueryNodeDuplicateName, QueryNodeReservedName,
            QueryStreamNotDefined, RecursionLimit, RuntimeError, TailingHereDoc, TypeConflict,
            UnexpectedCharacter, UnexpectedEndOfStream, UnexpectedEscapeCode, UnrecognizedToken,
            UnterminatedExtractor, UnterminatedHereDoc, UnterminatedIdentLiteral,
            UnterminatedInterpolation, UnterminatedStringLiteral, UpdateKeyMissing, Utf8Error,
            ValueError,
        };
        match self {
            NoClauseHit(outer)
            | Oops(outer, _, _)
            | QueryNodeDuplicateName(outer, _)
            | QueryNodeReservedName(outer, _) => (Some(outer.expand_lines(2)), Some(*outer)),
            AggrInAggr(outer, inner)
            | ArrayOutOfRange(outer, inner, _, _)
            | AssignIntoArray(outer, inner)
            | AssignToConst(outer, inner)
            | BadAccessInEvent(outer, inner, _, _)
            | BadAccessInGlobal(outer, inner, _, _)
            | BadAccessInLocal(outer, inner, _, _)
            | BadAccessInState(outer, inner, _, _)
            | BadArity(outer, inner, _, _, _, _)
            | BadArrayIndex(outer, inner, _, _)
            | BadType(outer, inner, _, _, _)
            | BinaryDrop(outer, inner)
            | BinaryEmit(outer, inner)
            | DecreasingRange(outer, inner, _, _)
            | DeployArgNotSpecified(outer, inner, _)
            | DeployArtefactNotDefined(outer, inner, _, _)
            | DeployRequiredArgDoesNotResolve(outer, inner, _)
            | DoubleConst(outer, inner, _)
            | DoublePipelineCreate(outer, inner, _)
            | DoubleStream(outer, inner, _)
            | EmptyInterpolation(outer, inner, _)
            | ExtraToken(outer, inner, _)
            | Generic(outer, inner, _)
            | InvalidAssign(outer, inner)
            | InvalidBinary(outer, inner, _, _, _)
            | InvalidBitshift(outer, inner)
            | InvalidConst(outer, inner)
            | InvalidDrop(outer, inner)
            | InvalidEmit(outer, inner)
            | InvalidExtractor(outer, inner, _, _, _)
            | InvalidFloatLiteral(outer, inner, _)
            | InvalidFn(outer, inner)
            | InvalidHexLiteral(outer, inner, _)
            | InvalidIntLiteral(outer, inner, _)
            | InvalidMod(outer, inner)
            | InvalidPP(outer, inner, _)
            | InvalidRecur(outer, inner)
            | InvalidToken(outer, inner)
            | InvalidUnary(outer, inner, _, _)
            | InvalidUtf8Sequence(outer, inner, _)
            | MergeTypeConflict(outer, inner, _, _)
            | MissingEffectors(outer, inner)
            | MissingFunction(outer, inner, _, _, _)
            | MissingModule(outer, inner, _, _)
            | ModuleNotFound(outer, inner, _, _)
            | NoConstsAllowed(outer, inner)
            | NoEventReferencesAllowed(outer, inner)
            | NoLocalsAllowed(outer, inner)
            | NotConstant(outer, inner)
            | PatchKeyExists(outer, inner, _)
            | PipelineUnknownPort(outer, inner, _, _)
            | QueryStreamNotDefined(outer, inner, _)
            | RecursionLimit(outer, inner)
            | RuntimeError(outer, inner, _, _, _, _)
            | TailingHereDoc(outer, inner, _, _)
            | TypeConflict(outer, inner, _, _)
            | UnexpectedCharacter(outer, inner, _, _)
            | UnexpectedEscapeCode(outer, inner, _, _)
            | UnrecognizedToken(outer, inner, _, _)
            | UnterminatedExtractor(outer, inner, _)
            | UnterminatedHereDoc(outer, inner, _)
            | UnterminatedIdentLiteral(outer, inner, _)
            | UnterminatedInterpolation(outer, inner, _)
            | UnterminatedStringLiteral(outer, inner, _)
            | UpdateKeyMissing(outer, inner, _) => (Some(*outer), Some(*inner)),
            // Special cases
            EmptyScript
            | AccessError(_)
            | CantSetArgsConst
            | CantSetGroupConst
            | CantSetWindowConst
            | Common(_)
            | Grok(_)
            | Io(_)
            | JsonError(_)
            | Msg(_)
            | NoObjectError(_)
            | NotFound
            | ParseIntError(_)
            | ParserError(_)
            | PreprocessorError(_)
            | Self::__Nonexhaustive { .. }
            | UnexpectedEndOfStream
            | Utf8Error(_)
            | ValueError(_) => (Some(Span::default()), None),
        }
    }
    pub(crate) fn token(&self) -> Option<UnfinishedToken> {
        use ErrorKind::{
            EmptyInterpolation, InvalidFloatLiteral, InvalidHexLiteral, InvalidIntLiteral,
            InvalidUtf8Sequence, TailingHereDoc, UnexpectedCharacter, UnexpectedEscapeCode,
            UnterminatedExtractor, UnterminatedHereDoc, UnterminatedIdentLiteral,
            UnterminatedInterpolation, UnterminatedStringLiteral,
        };
        match self {
            UnterminatedExtractor(_, _, token)
            | UnterminatedStringLiteral(_, _, token)
            | UnterminatedInterpolation(_, _, token)
            | EmptyInterpolation(_, _, token)
            | UnterminatedIdentLiteral(_, _, token)
            | UnterminatedHereDoc(_, _, token)
            | TailingHereDoc(_, _, token, _)
            | InvalidUtf8Sequence(_, _, token)
            | UnexpectedCharacter(_, _, token, _)
            | InvalidHexLiteral(_, _, token)
            | InvalidIntLiteral(_, _, token)
            | InvalidFloatLiteral(_, _, token)
            | UnexpectedEscapeCode(_, _, token, _) => Some(token.clone()),
            _ => None,
        }
    }

    pub(crate) fn hint(&self) -> Option<String> {
        use ErrorKind::{
            BadAccessInEvent, BadAccessInGlobal, BadAccessInLocal, EmptyInterpolation,
            MissingFunction, MissingModule, NoClauseHit, NoEventReferencesAllowed, Oops,
            TypeConflict, UnrecognizedToken, UnterminatedInterpolation,
        };
        match self {
            UnrecognizedToken(outer, inner, t, _) if t.is_empty() && inner.start().absolute() == outer.start().absolute() => Some("It looks like a `;` is missing at the end of the script".into()),
            UnrecognizedToken(_, _, t, _) if t == "##"  || t == "##"   => Some(format!("`{t}` is as doc comment, it needs to be followed by a statement, did you want to use `#` here?")),
            UnrecognizedToken(_, _, t, _) if t == "default" || t == "case" => Some("You might have a trailing `;` in the prior statement".into()),
            UnrecognizedToken(_, _, t, l) if !matches!(lexer::ident_to_token(t), lexer::Token::Ident(_, _)) && l.contains(&("`<ident>`".to_string())) => Some(format!("It looks like you tried to use the '{}' as a ident, consider quoting it as `{}` to make it an identifier.", t, t)),
            UnrecognizedToken(_, _, t, l) if t == "-" && l.contains(&("`(`".to_string())) => Some("Try wrapping this expression in parentheses `(` ... `)`".into()),
            UnrecognizedToken(_, _, key, options) => {
                match best_hint(key, options, 3) {
                    Some((_d, o)) if o == r#"`"`"# || o == r#"`"""`"#  => Some("Did you mean to use a string?".to_string()),
                    Some((_d, o)) if o != r#"`"`"# && o != r#"`"""`"# => Some(format!("Did you mean to use {}?", o)),
                    _ => None
                }
            }
            UnterminatedInterpolation(_, _, _) | EmptyInterpolation(_, _, _) => {
                Some("Did you mean to write a literal '#{'? Escape it as '\\#{'.".to_string())
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
                match best_hint(key, &options, 2) {
                    Some((_d, o)) => Some(format!("Did you mean to use `{}`?", o)),
                    _ => None
                }
            }

            BadAccessInEvent(_, _, key, options) |BadAccessInGlobal(_, _, key, options) => {
                match best_hint(key, options, 2) {
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

            NoEventReferencesAllowed(_, _) => Some("Here you operate in the whole window, not a single event. You need to wrap this reference in an aggregate function (e.g. aggr::win::last(...)) or use it in the group by clause of this query.".to_owned()),

            NoClauseHit(_) => Some("Consider adding a `default => null` clause at the end of your match or validate full coverage beforehand.".into()),
            Oops(_, id, _) => Some(format!("Please take the error output script and test data and open a ticket, this should not happen.\nhttps://github.com/tremor-rs/tremor-runtime/issues/new?labels=bug&template=bug_report.md&title=Opps%20{}", id)),
            _ => None,
        }
    }
}

impl Error {
    /// the context of the error
    #[must_use]
    pub fn context(&self) -> ErrorLocation {
        self.0.expr()
    }
    /// The compilation unit
    #[must_use]
    pub fn aid(&self) -> arena::Index {
        self.0.aid()
    }

    pub(crate) fn hint(&self) -> Option<String> {
        self.0.hint()
    }
    pub(crate) fn token(&self) -> Option<UnfinishedToken> {
        self.0.token()
    }

    /// If possible locate this error inside the given source.
    /// This is done without highlighting, for this, use an instance of `tremor_script::highlighter::Highlighter`,
    /// but it needs some more shenanigans than this in order to do proper highlighting.
    #[must_use]
    pub fn locate_in_source(&self, source: &str) -> Option<String> {
        match self.context() {
            (
                Some(Span {
                    start: ctx_start,
                    end: ctx_end,
                    ..
                }),
                Some(Span {
                    start: error_loc_start,
                    end: error_loc_end,
                    ..
                }),
            ) => {
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
                    error_lines.push(format!("{:5} | {}", cur_line_num, context_line));
                    if cur_line_num == error_line {
                        let err_msg = format!("{}", self);
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
                        error_lines.push(format!("      | {}{} {}", prefix, underline, err_msg));
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
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
pub type Kind = ErrorKind;

error_chain! {
    foreign_links {
        Grok(grok::Error);
        Io(std::io::Error);
        JsonError(simd_json::Error);
        ValueError(tremor_value::Error);
        ParseIntError(num::ParseIntError);
        Utf8Error(std::str::Utf8Error);
        NoObjectError(tremor_value::KnownKeyError);
        AccessError(value_trait::AccessError);
        Common(tremor_common::Error);
    }
    errors {
        /*
         * ParserError
         */
         /// An unrecognized token
        UnrecognizedToken(range: Span, loc: Span, token: String, expected: Vec<String>) {
            description("Unrecognized token")
                display("Found the token `{}` but expected {}", token, choices(expected))
        }
        ExtraToken(range: Span, loc: Span, token: String) {
            description("Extra token")
                display("Found an extra token: `{}` that does not belong there", token)
        }
        InvalidToken(range: Span, loc: Span) {
            description("Invalid token")
                display("Invalid token")
        }
        InvalidPP(range: Span, loc: Span, directive: String) {
            description("Invalid preprocessor directive")
                display("Found the preprocessor directive `{}` but expected {}", directive, choices(&["#!config"]))
        }
        /*
         * Generic
         */
        Generic(expr: Span, inner: Span, msg: String) {
            description("Generic error")
                display("{}", msg)
        }

        EmptyScript {
            description("No expressions were found in the script")
                display("No expressions were found in the script")
        }
        NotConstant(expr: Span, inner: Span) {
            description("The expression isn't constant and can't be evaluated at compile time")
                display("The expression isn't constant and can't be evaluated at compile time")
        }
        TypeConflict(expr: Span, inner: Span, got: ValueType, expected: Vec<ValueType>) {
            description("Conflicting types")
                display("Conflicting types, got {} but expected {}", t2s(*got), choices(&expected.iter().map(|v| t2s(*v).to_string()).collect::<Vec<String>>()))
        }
        Oops(expr: Span, id: u64, msg: String) {
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
        BadArity(expr: Span, inner: Span, m: String, f: String, a: RangeInclusive<usize>, calling_a: usize) {
            description("Bad arity for function")
                display("Bad arity for function {}::{}/{:?} but was called with {} arguments", m, f, a, calling_a)
        }
        MissingModule(expr: Span, inner: Span, m: String, suggestion: Option<(usize, String)>) {
            description("Call to undefined module")
                display("Call to undefined module {}", m)
        }
        MissingFunction(expr: Span, inner: Span, m: Vec<String>, f: String, suggestion: Option<(usize, String)>) {
            description("Call to undefined function")
                display("Call to undefined function {}::{}", m.join("::"), f)
        }
        AggrInAggr(expr: Span, inner: Span) {
            description("Aggregates can not be called inside of aggregates")
                display("Aggregates can not be called inside of aggregates")
        }
        BadType(expr: Span, inner: Span, m: String, f: String, a: usize) {
            description("Bad type passed to function")
                display("Bad type passed to function {}::{}/{}", m, f, a)
        }
        RuntimeError(expr: Span, inner: Span, m: String, f: String,  a: usize, c: String) {
            description("Runtime error in function")
                display("Runtime error in function {}::{}/{}: {}", m, f, a, c)
        }
        InvalidRecur(expr: Span, inner: Span) {
            description("Can not recur from this location")
                display("Can not recur from this location")
        }
        RecursionLimit(expr: Span, inner: Span) {
            description("Recursion limit Reached")
                display("Recursion limit Reached")
        }

        /*
         * Lexer, Preprocessor and Parser
         */
        UnterminatedExtractor(expr: Span, inner: Span, extractor: UnfinishedToken) {
            description("Unterminated extractor")
                display("It looks like you forgot to terminate an extractor with a closing '|'")
        }

        UnterminatedStringLiteral(expr: Span, inner: Span, string: UnfinishedToken) {
            description("Unterminated string")
                display("It looks like you forgot to terminate a string with a closing '\"'")
        }

        UnterminatedHereDoc(expr: Span, inner: Span, string: UnfinishedToken) {
            description("Unterminated heredoc")
                display("It looks like you forgot to terminate a here doc with with a closing '\"\"\"'")
        }
        TailingHereDoc(expr: Span, inner: Span, hd: UnfinishedToken, ch: char) {
            description("Tailing Characters after opening a here doc")
                display("It looks like you have characters tailing the here doc opening, it needs to be followed by a newline")
        }
        UnterminatedInterpolation(expr: Span, inner: Span, string_with_interpolation: UnfinishedToken) {
            description("Unterminated String interpolation")
                display("It looks like you forgot to terminate a string interpolation with a closing '}}'")
        }
        EmptyInterpolation(expr: Span, inner: Span, string_with_interpolation: UnfinishedToken) {
            description("Empty interpolation")
                display("You have an interpolation without content.")
        }

        UnterminatedIdentLiteral(expr: Span, inner: Span, ident: UnfinishedToken)
        {
            description("Unterminated ident")
                display("It looks like you forgot to terminate an ident with a closing '`'")
        }

        UnexpectedCharacter(expr: Span, inner: Span, token: UnfinishedToken, found: char){
            description("An unexpected character was found")
                display("An unexpected character '{}' was found", found)
        }

        UnexpectedEscapeCode(expr: Span, inner: Span, token: UnfinishedToken, found: char){
            description("An unexpected escape code was found")
                display("An unexpected escape code '{}' was found", found)

        }
        InvalidUtf8Sequence(expr: Span, inner: Span, token: UnfinishedToken){
            description("An invalid UTF8 escape sequence was found")
                display("An invalid UTF8 escape sequence was found")

        }
        InvalidHexLiteral(expr: Span, inner: Span, token: UnfinishedToken){
            description("An invalid hexadecimal")
                display("An invalid hexadecimal")

        }

        InvalidIntLiteral(expr: Span, inner: Span, token: UnfinishedToken) {
            description("An invalid integer literal")
                display("An invalid integer literal")

        }
        InvalidFloatLiteral(expr: Span, inner: Span, token: UnfinishedToken) {
            description("An invalid float literal")
                display("An invalid float literal")

        }

        UnexpectedEndOfStream {
            description("An unexpected end of stream was found")
                display("An unexpected end of stream was found")

        }

        /*
         * Preprocessor
         */
         PreprocessorError(msg: String) {
            description("Preprocessor due to user error")
                display("Preprocessor due to user error: {}", msg)
         }

        ModuleNotFound(range: Span, loc: Span, resolved_relative_file_path: String, expected: Vec<String>) {
            description("Module not found")
                display("Module `{}` not found or not readable error in module path: {}",
                resolved_relative_file_path.trim(),
                expected.iter().map(|x| format!("\n                         - {}", x)).collect::<Vec<String>>().join(""))
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

        BadAccessInLocal(expr: Span, inner: Span, key: String, options: Vec<String>) {
            description("Trying to access a non existing local key")
                display("Trying to access a non existing local key `{}`", key)
        }
        BadAccessInGlobal(expr: Span, inner: Span, key: String, options: Vec<String>) {
            description("Trying to access a non existing global key")
                display("Trying to access a non existing global key `{}`", key)
        }
        BadAccessInEvent(expr: Span, inner: Span, key: String, options: Vec<String>) {
            description("Trying to access a non existing event key")
                display("Trying to access a non existing event key `{}`", key)
        }
        BadAccessInState(expr: Span, inner: Span, key: String, options: Vec<String>) {
            description("Trying to access a non existing state key")
                display("Trying to access a non existing state key `{}`", key)
        }
        BadArrayIndex(expr: Span, inner: Span, idx: Value<'static>, len: usize) {
            description("Trying to index into an array with invalid index")
                display("Bad array index, got `{}` but expected an index in the range 0:{}",
                        idx, len)
        }
        DecreasingRange(expr: Span, inner: Span, start_idx: usize, end_idx: usize) {
            description("A range's end cannot be smaller than its start")
                display("A range's end cannot be smaller than its start, {}:{} is invalid",
                        start_idx, end_idx)
        }
        ArrayOutOfRange(expr: Span, inner: Span, r: RangeExclusive<usize>, len: usize) {
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
        AssignIntoArray(expr: Span, inner: Span) {
            description("Can not assign into an array")
                display("It is not supported to assign value into an array")
        }
        InvalidAssign(expr: Span, inner: Span) {
            description("You can not assign that")
                display("You are trying to assign to a value that isn't valid")
        }
        InvalidConst(expr: Span, inner: Span) {
            description("Can't declare a const here")
                display("Can't declare a const here")
        }
        InvalidMod(expr: Span, inner: Span) {
            description("Can't declare a module here")
                display("Can't declare a module here")
        }
        InvalidFn(expr: Span, inner: Span) {
            description("Can't declare a function here")
                display("Can't declare a function here")
        }
        DoubleConst(expr: Span, inner: Span, name: String) {
            description("Can't declare a constant twice")
                display("Can't declare the constant `{}` twice", name)
        }
        DoubleStream(expr: Span, inner: Span, name: String) {
            description("Can't declare a stream twice")
                display("Can't declare the stream `{}` twice", name)
        }
        DoublePipelineCreate(expr: Span, inner: Span, name: String) {
            description("Can't create a query twice")
                display("Can't create the pipeline `{}` twice", name)
        }
        AssignToConst(expr: Span, inner: Span) {
            description("Can't assign to a constant expression")
                display("Can't assign to a constant expression")
        }
        /*
         * Emit & Drop
         */
        InvalidEmit(expr: Span, inner: Span) {
            description("Can not emit from this location")
                display("Can not emit from this location")
        }
        InvalidDrop(expr: Span, inner: Span) {
            description("Can not drop from this location")
                display("Can not drop from this location")

        }
        BinaryEmit(expr: Span, inner: Span) {
            description("Please enclose the value you want to emit")
                display("The expression can be read as a binary expression, please put the value you want to emit in parentheses.")
        }
        BinaryDrop(expr: Span, inner: Span) {
            description("Please enclose the value you want to drop")
                display("The expression can be read as a binary expression, please put the value you want to drop in parentheses.")
        }
        /*
         * Operators
         */
        InvalidUnary(expr: Span, inner: Span, op: ast::UnaryOpKind, val: ValueType) {
            description("Invalid unary operation")
                display("The unary operation `{}` is not defined for the type `{}`", op, t2s(*val))
        }
        InvalidBinary(expr: Span, inner: Span, op: ast::BinOpKind, left: ValueType, right: ValueType) {
            description("Invalid binary operation")
                display("The binary operation `{}` is not defined for the type `{}` and `{}`", op, t2s(*left), t2s(*right))
        }
        InvalidBitshift(expr: Span, inner: Span) {
            description("Invalid value for bitshift")
                display("RHS value is larger than or equal to the number of bits in LHS value")
        }

        /*
         * match
         */
        InvalidExtractor(expr: Span, inner: Span, name: String, pattern: String, error: String){
            description("Invalid tilde predicate pattern")
                display("Invalid tilde predicate pattern: {}", error)
        }
        NoClauseHit(expr: Span){
            description("A match expression executed but no clause matched")
                display("A match expression executed but no clause matched")
        }
        MissingEffectors(expr: Span, inner: Span) {
            description("The clause has no effectors")
                display("The clause is missing a body")
        }
        /*
         * Patch
         */
        PatchKeyExists(expr: Span, inner: Span, key: String) {
            description("The key that is supposed to be written to already exists")
                display("The key that is supposed to be written to already exists: {}", key)
        }
        UpdateKeyMissing(expr: Span, inner: Span, key:String) {
            description("The key that is supposed to be updated does not exists")
                display("The key that is supposed to be updated does not exists: {}", key)
        }

        MergeTypeConflict(expr: Span, inner: Span, key:String, val: ValueType) {
            description("Merge can only be performed on keys that either do not exist or are records")
                display("Merge can only be performed on keys that either do not exist or are records but the key '{}' has the type {}", key, t2s(*val))
        }

        /*
         * Query stream declarations
         */
        QueryStreamNotDefined(stmt: Span, inner: Span, name: String) {
            description("Stream is not defined")
                display("Stream used in `from` or `into` is not defined: {}", name)
        }
        NoLocalsAllowed(stmt: Span, inner: Span) {
            description("Local variables are not allowed here")
                display("Local variables are not allowed here")
        }
        NoConstsAllowed(stmt: Span, inner: Span) {
            description("Constants are not allowed here")
                display("Constants are not allowed here")
        }
        NoEventReferencesAllowed(stmt: Span, inner: Span) {
            description("References to `event` or `$` are not allowed in this context")
                display("References to `event` or `$` are not allowed in this context")
        }

        CantSetWindowConst {
            description("Failed to initialize window constant")
                display("Failed to initialize window constant")
        }
        CantSetGroupConst {
            description("Failed to initialize group constant")
                display("Failed to initialize group constant")
        }
        CantSetArgsConst {
            description("Failed to initialize args constant")
                display("Failed to initialize args constant")
        }

        QueryNodeReservedName(stmt: Span, name: String) {
            description("Reserved name used for the node")
                display("Name `{}` is reserved for built-in nodes, please use another name.", name)
        }
        QueryNodeDuplicateName(stmt: Span, name: String) {
            description("Duplicate name used for the node")
                // TODO would be nice to include location of the node where the name was already used
                display("Name `{}` is already in use for another node, please use another name.", name)
        }

        PipelineUnknownPort(stmt: Span, inner: Span, subq_name: String, port_name: String) {
            description("Query does not have this port")
                display("Query `{}` does not have port `{}`", subq_name, port_name)
        }
        /*
         * Troy statements
         */
        DeployArtefactNotDefined(stmt: Span, inner: Span, name: String, options: Vec<String>) {
            description("Deployment artefact is not defined")
                display("Artefact `{}` is not defined or not found, the following are defined: {}", name, options.join(", "))
        }
        DeployArgNotSpecified(stmt: Span, inner: Span, name: String) {
            description("Deployment artefact has unknown argument")
                display("Argument used was not formally specified in originating definition: {}", name)
        }
        DeployRequiredArgDoesNotResolve(stmt: Span, inner: Span, name: String) {
            description("Deployment artefact argument has no value bound")
                display("Argument `{}` is required, but no defaults are provided in the definition and no final values in the instance", name)
        }
    }
}

/// Creates a stream not defined error
#[allow(clippy::borrowed_box)]
pub fn query_stream_not_defined_err<S: BaseExpr, I: BaseExpr>(
    stmt: &S,
    inner: &I,
    name: String,
) -> Error {
    // Subqueries store unmangled `name` in `meta`
    // Use `name` from `meta` if it exists.
    let name = inner.meta().name().map_or(name, |s| s.into());
    ErrorKind::QueryStreamNotDefined(stmt.extent(), inner.extent(), name).into()
}

/// Creates a query stream duplicate name error
pub fn query_stream_duplicate_name_err<S: BaseExpr, I: BaseExpr>(
    stmt: &S,
    inner: &I,
    name: String,
) -> Error {
    let name = inner.meta().name().map_or(name, |s| s.into());
    ErrorKind::DoubleStream(stmt.extent(), inner.extent(), name).into()
}

/// Creates a pipeline stmt duplicate name error
pub fn pipeline_stmt_duplicate_name_err<S: BaseExpr, I: BaseExpr>(
    stmt: &S,
    inner: &I,
    name: String,
) -> Error {
    let name = inner.meta().name().map_or(name, |s| s.into());
    ErrorKind::DoublePipelineCreate(stmt.extent(), inner.extent(), name).into()
}

/// Creates a pipeline unknown port error
pub fn pipeline_unknown_port_err<S: BaseExpr, I: BaseExpr>(
    stmt: &S,
    inner: &I,
    subq_name: String,
    port_name: String,
) -> Error {
    let subq_name = inner.meta().name().map_or(subq_name, |s| s.into());
    ErrorKind::PipelineUnknownPort(stmt.extent(), inner.extent(), subq_name, port_name).into()
}

/// Creates a query node reserved name error
pub fn query_node_reserved_name_err<S: BaseExpr>(stmt: &S, name: String) -> Error {
    let name = stmt.meta().name().map_or(name, |s| s.into());
    ErrorKind::QueryNodeReservedName(stmt.extent(), name).into()
}

/// Creates a query node duplicate name error
pub fn query_node_duplicate_name_err<S: BaseExpr>(stmt: &S, name: String) -> Error {
    let name = stmt.meta().name().map_or(name, |s| s.into());
    ErrorKind::QueryNodeDuplicateName(stmt.extent(), name).into()
}

/// Creates a guard not bool error
///
/// # Errors
/// always, this is a function to create errors
#[allow(clippy::borrowed_box)]
pub fn query_guard_not_bool<T, O: BaseExpr, I: BaseExpr>(
    stmt: &O,
    inner: &I,
    got: &Value,
) -> Result<T> {
    error_type_conflict_mult(stmt, inner, got.value_type(), vec![ValueType::Bool])
}

pub fn query_guard_not_bool_err<O: BaseExpr, I: BaseExpr>(
    stmt: &O,
    inner: &I,
    got: &Value,
) -> Error {
    err_type_conflict_mult(stmt, inner, got.value_type(), vec![ValueType::Bool])
}

/// A bad thing happened for which no specialized hygienic error handling strategy is defined
/// We can still be polite in our error reports!
/// # Errors
/// The parameters transformed into a generic error
pub fn error_generic<T, O: BaseExpr, I: BaseExpr, S: ToString>(
    outer: &O,
    inner: &I,
    error: &S,
) -> Result<T> {
    Err(err_generic(outer, inner, error))
}

pub(crate) fn err_generic<O: BaseExpr, I: BaseExpr, S: ToString>(
    outer: &O,
    inner: &I,
    error: &S,
) -> Error {
    ErrorKind::Generic(outer.extent(), inner.extent(), error.to_string()).into()
}

pub(crate) fn error_type_conflict_mult<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: Vec<ValueType>,
) -> Result<T> {
    Err(err_type_conflict_mult(outer, inner, got, expected))
}

pub(crate) fn err_type_conflict_mult<O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: Vec<ValueType>,
) -> Error {
    ErrorKind::TypeConflict(outer.extent(), inner.extent(), got, expected).into()
}

pub(crate) fn error_no_locals<T, O: BaseExpr, I: BaseExpr>(outer: &O, inner: &I) -> Result<T> {
    Err(ErrorKind::NoLocalsAllowed(outer.extent(), inner.extent()).into())
}

pub(crate) fn error_event_ref_not_allowed<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
) -> Result<T> {
    Err(ErrorKind::NoEventReferencesAllowed(outer.extent(), inner.extent()).into())
}

pub(crate) fn error_need_obj<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    Err(err_need_obj(outer, inner, got))
}

pub(crate) fn err_need_obj<O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Error {
    err_type_conflict_mult(outer, inner, got, vec![ValueType::Object])
}

pub(crate) fn error_need_arr<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::Array])
}

pub(crate) fn error_need_str<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![ValueType::String])
}

pub(crate) fn error_need_int<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Result<T> {
    Err(err_need_int(outer, inner, got))
}

pub(crate) fn err_need_int<O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
) -> Error {
    err_type_conflict_mult(outer, inner, got, vec![ValueType::I64])
}

pub(crate) fn error_type_conflict<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: ValueType,
    expected: ValueType,
) -> Result<T> {
    error_type_conflict_mult(outer, inner, got, vec![expected])
}

pub(crate) fn error_guard_not_bool<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    got: &Value,
) -> Result<T> {
    error_type_conflict(outer, inner, got.value_type(), ValueType::Bool)
}

pub(crate) fn error_invalid_unary<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    op: ast::UnaryOpKind,
    val: &Value,
) -> Result<T> {
    Err(err_invalid_unary(outer, inner, op, val))
}

pub(crate) fn err_invalid_unary<O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    op: ast::UnaryOpKind,
    val: &Value,
) -> Error {
    ErrorKind::InvalidUnary(outer.extent(), inner.extent(), op, val.value_type()).into()
}

pub(crate) fn error_invalid_binary<T, O: BaseExpr, I: BaseExpr>(
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
        left.value_type(),
        right.value_type(),
    )
    .into())
}

pub(crate) fn error_invalid_bitshift<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
) -> Result<T> {
    Err(ErrorKind::InvalidBitshift(outer.extent(), inner.extent()).into())
}

pub(crate) fn error_no_clause_hit<T, O: BaseExpr>(outer: &O) -> Result<T> {
    Err(ErrorKind::NoClauseHit(outer.extent()).into())
}

pub(crate) fn error_oops<T, O: BaseExpr, S: ToString + ?Sized>(
    outer: &O,
    id: u64,
    msg: &S,
) -> Result<T> {
    Err(error_oops_err(outer, id, msg))
}

pub(crate) fn error_oops_err<O: BaseExpr, S: ToString + ?Sized>(
    outer: &O,
    id: u64,
    msg: &S,
) -> Error {
    ErrorKind::Oops(outer.extent(), id, msg.to_string()).into()
}

pub(crate) fn error_patch_key_exists<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
) -> Result<T> {
    Err(ErrorKind::PatchKeyExists(outer.extent(), inner.extent(), key).into())
}

pub(crate) fn error_patch_update_key_missing<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
) -> Result<T> {
    Err(ErrorKind::UpdateKeyMissing(outer.extent(), inner.extent(), key).into())
}

pub(crate) fn error_missing_effector<O: BaseExpr, I: BaseExpr>(outer: &O, inner: &I) -> Error {
    ErrorKind::MissingEffectors(outer.extent(), inner.extent()).into()
}
pub(crate) fn error_patch_merge_type_conflict<T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    key: String,
    val: &Value,
) -> Result<T> {
    Err(ErrorKind::MergeTypeConflict(outer.extent(), inner.extent(), key, val.value_type()).into())
}

pub(crate) fn error_assign_array<T, O: BaseExpr, I: BaseExpr>(outer: &O, inner: &I) -> Result<T> {
    Err(ErrorKind::AssignIntoArray(outer.extent(), inner.extent()).into())
}
pub(crate) fn error_invalid_assign_target<T, O: BaseExpr>(outer: &O) -> Result<T> {
    let inner: Span = outer.extent();

    Err(ErrorKind::InvalidAssign(inner.expand_lines(2), inner).into())
}
pub(crate) fn error_assign_to_const<T, O: BaseExpr>(outer: &O) -> Result<T> {
    let inner: Span = outer.extent();

    Err(ErrorKind::AssignToConst(inner.expand_lines(2), inner).into())
}
pub(crate) fn error_array_out_of_bound<'script, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
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
        | ast::Path::Expr(_) => ErrorKind::ArrayOutOfRange(expr, inner.extent(), r, len).into(),
    })
}

pub(crate) fn error_bad_array_index<'script, 'idx, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
    idx: &Value<'idx>,
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
        | ast::Path::Expr(_) => ErrorKind::BadArrayIndex(expr, inner.extent(), idx, len).into(),
    })
}
pub(crate) fn error_decreasing_range<'script, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
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
        | ast::Path::Expr(_) => {
            ErrorKind::DecreasingRange(expr, inner.extent(), start_idx, end_idx).into()
        }
    })
}

pub(crate) fn error_bad_key<'script, T, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
    key: String,
    options: Vec<String>,
) -> Result<T> {
    Err(error_bad_key_err(outer, inner, path, key, options))
}

pub(crate) fn error_bad_key_err<'script, O: BaseExpr, I: BaseExpr>(
    outer: &O,
    inner: &I,
    path: &ast::Path<'script>,
    key: String,
    options: Vec<String>,
) -> Error {
    let expr: Span = outer.extent();
    match path {
        ast::Path::Reserved(_) | ast::Path::Local(_) | ast::Path::Expr(_) => {
            ErrorKind::BadAccessInLocal(expr, inner.extent(), key, options).into()
        }
        ast::Path::Meta(_p) => {
            ErrorKind::BadAccessInGlobal(expr, inner.extent(), key, options).into()
        }
        ast::Path::Event(_p) => {
            ErrorKind::BadAccessInEvent(expr, inner.extent(), key, options).into()
        }
        ast::Path::State(_p) => {
            ErrorKind::BadAccessInState(expr, inner.extent(), key, options).into()
        }
    }
}
