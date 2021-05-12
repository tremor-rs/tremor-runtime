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

// Note: We ignore the is_* functions for coverage as they effectively are
// only lists

use crate::errors::{Error, ErrorKind, Result, ResultExt, UnfinishedToken};
#[cfg_attr(feature = "cargo-clippy", allow(clippy::all, clippy::unwrap_used))]
use crate::parser::g::__ToTriple;
use crate::path::ModulePath;
pub use crate::pos::*;
use crate::Value;
use beef::Cow;
use simd_json::Writable;
use std::ffi::OsStr;
use std::fmt;
use std::io::Read;
use std::iter::Peekable;
use std::path::{Path, PathBuf};
use std::str::Chars;
use std::{collections::VecDeque, mem};
use tremor_common::file;
use unicode_xid::UnicodeXID;

/// Source for a parser
pub trait ParserSource {
    /// The source as a str
    fn src(&self) -> &str;
    /// the initial index
    fn start_index(&self) -> BytePos;
}

impl ParserSource for str {
    fn src(&self) -> &str {
        self
    }
    fn start_index(&self) -> BytePos {
        BytePos::from(0)
    }
}

/// A token with a span to indicate its location
pub type TokenSpan<'input> = Spanned<Token<'input>>;

fn is_ws(ch: char) -> bool {
    ch.is_whitespace() && ch != '\n'
}

fn is_ident_start(ch: char) -> bool {
    UnicodeXID::is_xid_start(ch) || ch == '_' || ch == '-'
}

fn is_ident_continue(ch: char) -> bool {
    let ch = ch as char;
    UnicodeXID::is_xid_continue(ch) || ch == '_' || ch == '-'
}

fn is_test_start(ch: char) -> bool {
    ch == '|'
}

fn is_dec_digit(ch: char) -> bool {
    ch.is_digit(10)
}

fn is_hex(ch: char) -> bool {
    ch.is_digit(16)
}

pub(crate) fn ident_to_token(ident: &str) -> Token {
    match ident {
        "intrinsic" => Token::Intrinsic,
        "mod" => Token::Module,
        "const" => Token::Const,
        "let" => Token::Let,
        "match" => Token::Match,
        "end" => Token::End,
        "fn" => Token::Fun,
        "null" => Token::Nil,
        "case" => Token::Case,
        "of" => Token::Of,
        "when" => Token::When,
        "default" => Token::Default,
        "patch" => Token::Patch,
        "insert" => Token::Insert,
        "upsert" => Token::Upsert,
        "update" => Token::Update,
        "erase" => Token::Erase,
        "move" => Token::Move,
        "copy" => Token::Copy,
        "merge" => Token::Merge,
        "for" => Token::For,
        "event" => Token::Event,
        "state" => Token::State,
        "present" => Token::Present,
        "absent" => Token::Absent,
        "true" => Token::BoolLiteral(true),
        "false" => Token::BoolLiteral(false),
        "and" => Token::And,
        "or" => Token::Or,
        "xor" => Token::Xor,
        "not" => Token::Not,
        "drop" => Token::Drop,
        "emit" => Token::Emit,
        "select" => Token::Select,
        "from" => Token::From,
        "where" => Token::Where,
        "with" => Token::With,
        // "order" => Token::Order,
        "group" => Token::Group,
        "by" => Token::By,
        "having" => Token::Having,
        "into" => Token::Into,
        "create" => Token::Create,
        "tumbling" => Token::Tumbling,
        "sliding" => Token::Sliding,
        "window" => Token::Window,
        "stream" => Token::Stream,
        "operator" => Token::Operator,
        "script" => Token::Script,
        "set" => Token::Set,
        "each" => Token::Each,
        "define" => Token::Define,
        "args" => Token::Args,
        "use" => Token::Use,
        "as" => Token::As,
        "recur" => Token::Recur,
        src => Token::Ident(src.into(), false),
    }
}

/// A token in the source ( file, byte stream ), to be emitted by the `Lexer`
/// The LALRPOP grammar uses these tokens and this custom lexer
/// as it does not have a facility to ignore special tokens, to
/// easily disambiguate tokens ( eg: ':' vs '::' vs ':=' ), nor
/// to inject magic tokens ( bad token ) or support lexical streams
/// ( different token streams drive by users, eg: emit ignorable tokens when
/// syntax highlighting or error highlighting
#[derive(Clone, Debug, PartialEq)]
pub enum Token<'input> {
    ///  Ignorable when parsing, significant when highlighting
    Whitespace(&'input str),
    /// a new line
    NewLine,
    /// a singe line comment
    SingleLineComment(&'input str),
    /// a mod comment
    ModComment(&'input str),
    /// a doc comment
    DocComment(&'input str),
    /// a BAD TOKEN
    Bad(String), // Mark bad tokens in lexical stream

    /// an ident
    Ident(Cow<'input, str>, bool), // bool marks whether the ident is surrounded by '`' (escape notation)
    /// the `$` sign
    Dollar,
    /// a `.`
    Dot,
    /// null
    Nil,
    /// a boolean
    BoolLiteral(bool),
    /// an integer
    IntLiteral(i64),
    /// an float
    FloatLiteral(f64, String),
    /// a test literal
    TestLiteral(usize, Vec<String>),

    /// a heredoc start token `"""`
    HereDocStart,
    /// a heredoc end token `"""`
    HereDocEnd,
    /// a heredoc literal, within triple quotes with special newline handling
    HereDocLiteral(Cow<'input, str>),

    /// a double quote `"`
    DQuote,
    /// a string literal
    StringLiteral(Cow<'input, str>),

    // Keywords
    /// the `let` keyword
    Let,
    /// the `const` keyword
    Const,
    /// the `match` keyword
    Match,
    /// the `case` keyword
    Case,
    /// the `of` keyword
    Of,
    /// the `when` keyword
    When,
    /// the `end` keyword
    End,
    /// the `patch` keyword
    Patch,
    /// the `insert` keyword
    Insert,
    /// the `upsert` keyword
    Upsert,
    /// the `update` keyword
    Update,
    /// the `erase` keyword
    Erase,
    /// the `move` keyword
    Move,
    /// the `copy` keyword
    Copy,
    /// the `merge` keyword
    Merge,
    /// the `drop` keyword
    Drop,
    /// the `default` keyword
    Default,
    /// the `emit` keyword
    Emit,
    /// the `for` keyword
    For,
    /// the `event` keyword
    Event,
    /// the `state` keyword
    State,
    /// the `present` keyword
    Present,
    /// the `absent` keyword
    Absent,
    /// the `fun` keyword
    Fun,
    /// the `intrinsic` keyword
    Intrinsic,
    /// the `mod` keyword
    Module,
    /// the `_` token
    DontCare,
    /// the `recur` token
    Recur,

    // Symbols
    /// the `\` backslash
    BSlash,
    /// the `,` comma
    Comma,

    // Op
    /// the `not` keyword
    Not,
    /// bitwise not `!`
    BitNot,
    /// the `and` keyword
    And,
    /// the `or` keyword
    Or,
    /// the `xor` keyword
    Xor,
    /// bitwise and `&`
    BitAnd,
    /// bitwise or `|`
    // BitOr,
    /// bitwise xor `^`
    BitXor,
    /// equal `=`
    Eq,
    /// double equal `==`
    EqEq,
    /// not equal `!=`
    NotEq,
    /// tilde equal `~=`
    TildeEq,
    /// tilde `~`
    Tilde,
    /// greater than equal `>=`
    Gte,
    /// grater than `>`
    Gt,
    /// lower than equal `<=`
    Lte,
    /// lower then `<`
    Lt,
    /// Right bit shift signed `>>`
    RBitShiftSigned,
    /// Right bit shift unsigned `>>>`
    RBitShiftUnsigned,
    /// Left bit shift `<<`
    LBitShift,
    /// Add `+`
    Add,
    /// Substract `-`
    Sub,
    /// Multiply `*`
    Mul,
    /// Divide `/`
    Div,
    /// Moduly `%`
    Mod,

    // Symbols
    /// Colon `:`
    Colon,
    /// Double coilon `::`
    ColonColon,
    /// Effector array `=>`
    EqArrow,
    /// Semicolon `;`
    Semi,
    /// Left Paren `%(`
    LPatParen,
    /// Left Paren `(`
    LParen,
    /// Right Paren `)`
    RParen,
    /// Left brace `{`
    LBrace,
    /// Interpolation Start `#{`
    Interpol,
    /// Escaped Hash `\#`
    EscapedHash,
    /// Left pattern Bracket `%{`
    LPatBrace,
    /// Right Brace `}`
    RBrace,
    /// Left Bracket `[`
    LBracket,
    /// Left pattern Bracket `%[`
    LPatBracket,
    /// Right bracket `]`
    RBracket,

    /// End of stream token
    EndOfStream,

    // Query
    /// The `select` keyword
    Select,
    /// The `from` keyword
    From,
    /// The `where` keyword
    Where,
    /// The `with` keyword
    With,
    /// The `order` keyword
    // Order,
    /// the `group` keyword
    Group,
    /// The `by` keyword
    By,
    /// The `having` keyword
    Having,
    /// The `into` keyword
    Into,
    /// The `create` keyword
    Create,
    /// The `tumbling` keyword
    Tumbling,
    /// The `sliding` keyword
    Sliding,
    /// The `window` keyword
    Window,
    /// The `stream` keyword
    Stream,
    /// The `operator` keyword
    Operator,
    /// The `script` keyword
    Script,
    /// The `set` keyword
    Set,
    /// The `each` keyword
    Each,
    /// The `define` keyword
    Define,
    /// The `args` keyword
    Args,
    /// The `use` keyword
    Use,
    /// The `as` keyword
    As,
    /// Preprocessor directives
    LineDirective(Location, Cow<'input, str>),
    /// Config Directive
    ConfigDirective,
}

impl<'input> Token<'input> {
    /// Is the token ignorable except when syntax or error highlighting.
    /// Is the token insignificant when parsing ( a correct ... ) source.
    #[cfg(not(tarpaulin_include))] // matches is not supported
    pub(crate) fn is_ignorable(&self) -> bool {
        matches!(
            *self,
            Token::SingleLineComment(_)
                | Token::Whitespace(_)
                | Token::NewLine
                | Token::LineDirective(_, _)
        )
    }

    /// Is the token a keyword, excluding keyword literals ( eg: true, nil )
    #[cfg(not(tarpaulin_include))] // matches is not supported
    pub(crate) fn is_keyword(&self) -> bool {
        matches!(
            *self,
            Token::Absent
                | Token::Args
                | Token::By
                | Token::Case
                | Token::Const
                | Token::Copy
                | Token::Create
                | Token::Default
                | Token::Define
                | Token::Drop
                | Token::Each
                | Token::Emit
                | Token::End
                | Token::Erase
                | Token::Event
                | Token::For
                | Token::From
                | Token::Fun
                | Token::Group
                | Token::Having
                | Token::Insert
                | Token::Into
                | Token::Intrinsic
                | Token::Let
                | Token::Match
                | Token::Merge
                | Token::Module
                | Token::Move
                | Token::Of
                | Token::Operator
                // | Token::Order
                | Token::Patch
                | Token::Present
                | Token::Script
                | Token::Select
                | Token::Set
                | Token::Use
                | Token::As
                | Token::Sliding
                | Token::State
                | Token::Stream
                | Token::Tumbling
                | Token::Update
                | Token::Upsert
                | Token::When
                | Token::Where
                | Token::Window
                | Token::With
        )
    }

    /// Is the token a literal, excluding list and record literals
    #[cfg(not(tarpaulin_include))] // matches is not supported
    pub(crate) fn is_literal(&self) -> bool {
        matches!(
            *self,
            Token::DontCare
                | Token::Nil
                | Token::BoolLiteral(_)
                | Token::IntLiteral(_)
                | Token::FloatLiteral(_, _)
        )
    }

    // It's text-like or string-like notation such as String, char, regex ...
    #[cfg(not(tarpaulin_include))] // matches is not supported
    pub(crate) fn is_string_like(&self) -> bool {
        matches!(
            *self,
            Token::StringLiteral(_)
                | Token::DQuote
                | Token::TestLiteral(_, _)
                | Token::HereDocStart
                | Token::HereDocEnd
                | Token::HereDocLiteral(_)
        )
    }

    /// Is the token a builtin delimiter symbol
    #[cfg(not(tarpaulin_include))] // matches is not supported
    pub(crate) fn is_symbol(&self) -> bool {
        matches!(
            *self,
            Token::BSlash
                | Token::Colon
                | Token::ColonColon
                | Token::Comma
                | Token::Eq
                | Token::EqArrow
                | Token::LBrace
                | Token::LBracket
                | Token::LineDirective(_, _)
                | Token::ConfigDirective
                | Token::LParen
                | Token::LPatBrace
                | Token::LPatBracket
                | Token::RBrace
                | Token::RBracket
                | Token::RParen
                | Token::Semi
                | Token::TildeEq
        )
    }

    /// Is the token a builtin expression operator ( excludes forms such as 'match', 'let'
    #[cfg(not(tarpaulin_include))] // matches is not supported
    pub(crate) fn is_operator(&self) -> bool {
        matches!(
            *self,
            Token::Not
                | Token::BitNot
                | Token::Or
                | Token::Xor
                | Token::And
                // | Token::BitOr
                | Token::BitXor
                | Token::BitAnd
                | Token::Eq
                | Token::EqEq
                | Token::NotEq
                | Token::TildeEq
                | Token::Tilde
                | Token::Gte
                | Token::Gt
                | Token::Lte
                | Token::Lt
                | Token::RBitShiftSigned
                | Token::RBitShiftUnsigned
                | Token::LBitShift
                | Token::Add
                | Token::Sub
                | Token::Mul
                | Token::Div
                | Token::Mod
                | Token::Dollar
                | Token::Dot
        )
    }
}

// LALRPOP requires a means to convert spanned tokens to triple form
impl<'input> __ToTriple<'input> for Spanned<Token<'input>> {
    fn to_triple(
        value: Self,
    ) -> std::result::Result<
        (Location, Token<'input>, Location),
        lalrpop_util::ParseError<Location, Token<'input>, Error>,
    > {
        Ok((value.span.start(), value.value, value.span.end()))
    }
}

// Format a token for display
//

impl<'input> fmt::Display for Token<'input> {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Whitespace(ref ws) => write!(f, "{}", ws),
            Token::NewLine => writeln!(f),
            Token::Ident(ref name, true) => write!(f, "`{}`", name),
            Token::Ident(ref name, false) => write!(f, "{}", name),
            Token::ModComment(ref comment) => write!(f, "### {}", comment),
            Token::DocComment(ref comment) => write!(f, "## {}", comment),
            Token::SingleLineComment(ref comment) => write!(f, "# {}", comment),
            Token::IntLiteral(value) => write!(f, "{}", value),
            Token::FloatLiteral(_, txt) => write!(f, "{}", txt),
            Token::DQuote => write!(f, "\""),
            Token::Interpol => write!(f, "#{{"),
            Token::EscapedHash => write!(f, "\\#"),
            Token::StringLiteral(value) => {
                // We do those to ensure proper escaping
                let value: &str = &value;
                let s = Value::from(value).encode();
                // Strip the quotes
                write!(f, "{}", s.get(1..s.len() - 1).unwrap_or_default())
            }
            Token::HereDocStart => {
                // here we write the following linebreak
                writeln!(f, r#"""""#)
            }
            Token::HereDocEnd => {
                // here we do not
                write!(f, r#"""""#)
            }
            Token::HereDocLiteral(value) => {
                // do not escape linebreaks
                let (value, add_linebreak) = value
                    .strip_suffix('\n')
                    .map_or_else(|| (&value as &str, false), |stripped| (stripped, true));
                let s = Value::from(value).encode();
                // Strip the quotes
                if let Some(s) = s.get(1..s.len() - 1) {
                    write!(f, "{}", s)?;
                }
                if add_linebreak {
                    writeln!(f)?;
                }
                Ok(())
            }
            Token::TestLiteral(indent, values) => {
                let mut first = true;
                write!(f, "|")?;
                if let [v] = values.as_slice() {
                    write!(f, "{}", v.replace('\\', "\\\\").replace('|', "\\|"))?;
                } else {
                    for l in values {
                        if first {
                            first = false;
                        } else {
                            write!(f, "\\\n{}", " ".repeat(*indent))?;
                        };
                        write!(f, "{}", l.replace('\\', "\\\\").replace('|', "\\|"))?;
                    }
                }
                write!(f, "|")
            }
            Token::BoolLiteral(true) => write!(f, "true"),
            Token::BoolLiteral(false) => write!(f, "false"),
            Token::Bad(value) => write!(f, "{}", value),
            Token::Let => write!(f, "let"),
            Token::Const => write!(f, "const"),
            Token::Match => write!(f, "match"),
            Token::Patch => write!(f, "patch"),
            Token::Insert => write!(f, "insert"),
            Token::Upsert => write!(f, "upsert"),
            Token::Update => write!(f, "update"),
            Token::Erase => write!(f, "erase"),
            Token::Move => write!(f, "move"),
            Token::Copy => write!(f, "copy"),
            Token::Merge => write!(f, "merge"),
            Token::For => write!(f, "for"),
            Token::Event => write!(f, "event"),
            Token::State => write!(f, "state"),
            Token::Present => write!(f, "present"),
            Token::Absent => write!(f, "absent"),
            //            Token::Return => write!(f, "return"),
            //            Token::Todo => write!(f, "todo"),
            Token::Drop => write!(f, "drop"),
            Token::Emit => write!(f, "emit"),
            Token::Fun => write!(f, "fn"),
            Token::End => write!(f, "end"),
            Token::Nil => write!(f, "null"),
            Token::Case => write!(f, "case"),
            Token::Of => write!(f, "of"),
            Token::When => write!(f, "when"),
            Token::Default => write!(f, "default"),
            Token::Intrinsic => write!(f, "intrinsic"),
            Token::Module => write!(f, "mod"),
            Token::BSlash => write!(f, "\\"),
            Token::Colon => write!(f, ":"),
            Token::ColonColon => write!(f, "::"),
            Token::Comma => write!(f, ","),
            //            Token::DotDotDot => write!(f, "..."),
            //            Token::DotDot => write!(f, ".."),
            Token::Dot => write!(f, "."),
            Token::Dollar => write!(f, "$"),
            //            Token::Question => write!(f, "?"),
            //            Token::Pipe => write!(f, "|"),
            Token::Not => write!(f, "not"),
            Token::BitNot => write!(f, "!"),
            //            Token::Tilde => write!(f, "~"),
            Token::DontCare => write!(f, "_"),
            Token::EqArrow => write!(f, "=>"),
            Token::Semi => write!(f, ";"),
            Token::LPatParen => write!(f, "%("),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LPatBrace => write!(f, "%{{"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::LBracket => write!(f, "["),
            Token::LPatBracket => write!(f, "%["),
            Token::RBracket => write!(f, "]"),
            Token::And => write!(f, "and"),
            Token::Or => write!(f, "or"),
            Token::Xor => write!(f, "xor"),
            Token::BitAnd => write!(f, "&"),
            // Token::BitOr => write!(f, "|"),
            Token::BitXor => write!(f, "^"),
            Token::Eq => write!(f, "="),
            Token::EqEq => write!(f, "=="),
            Token::NotEq => write!(f, "!="),
            Token::TildeEq => write!(f, "~="),
            Token::Tilde => write!(f, "~"),
            Token::Gte => write!(f, ">="),
            Token::Gt => write!(f, ">"),
            Token::Lte => write!(f, "<="),
            Token::Lt => write!(f, "<"),
            Token::RBitShiftSigned => write!(f, ">>"),
            Token::RBitShiftUnsigned => write!(f, ">>>"),
            Token::LBitShift => write!(f, "<<"),
            Token::Mul => write!(f, "*"),
            Token::Div => write!(f, "/"),
            Token::Add => write!(f, "+"),
            Token::Sub => write!(f, "-"),
            Token::Mod => write!(f, "%"),
            Token::EndOfStream => write!(f, ""),
            // Query
            Token::Select => write!(f, "select"),
            Token::From => write!(f, "from"),
            Token::Where => write!(f, "where"),
            Token::With => write!(f, "with"),
            // Token::Order => write!(f, "order"),
            Token::Group => write!(f, "group"),
            Token::By => write!(f, "by"),
            Token::Having => write!(f, "having"),
            Token::Into => write!(f, "into"),
            Token::Create => write!(f, "create"),
            Token::Tumbling => write!(f, "tumbling"),
            Token::Sliding => write!(f, "sliding"),
            Token::Window => write!(f, "window"),
            Token::Stream => write!(f, "stream"),
            Token::Operator => write!(f, "operator"),
            Token::Script => write!(f, "script"),
            Token::Set => write!(f, "set"),
            Token::Each => write!(f, "each"),
            Token::Define => write!(f, "define"),
            Token::Args => write!(f, "args"),
            Token::Use => write!(f, "use"),
            Token::As => write!(f, "as"),
            Token::Recur => write!(f, "recur"),
            Token::ConfigDirective => write!(f, "#!config "),
            Token::LineDirective(l, file) => write!(
                f,
                "#!line {} {} {} {} {}",
                l.absolute(),
                l.line(),
                l.column(),
                l.unit_id,
                file
            ),
        }
    }
}

struct CharLocations<'input> {
    location: Location,
    chars: Peekable<Chars<'input>>,
}

impl<'input> CharLocations<'input> {
    pub(crate) fn new<S>(input: &'input S) -> CharLocations<'input>
    where
        S: ?Sized + ParserSource,
    {
        CharLocations {
            location: Location::new(1, 1, input.start_index().to_usize(), 0),
            chars: input.src().chars().peekable(),
        }
    }

    pub(crate) fn current(&self) -> &Location {
        &self.location
    }
}

impl<'input> Iterator for CharLocations<'input> {
    type Item = (Location, char);

    fn next(&mut self) -> Option<(Location, char)> {
        self.chars.next().map(|ch| {
            let location = self.location;
            self.location.shift(ch);
            (location, ch)
        })
    }
}

/// A Tremor tokeniser
pub struct Tokenizer<'input> {
    //cu: usize,
    eos: bool,
    pos: Span,
    iter: Peekable<Lexer<'input>>,
}

impl<'input> Tokenizer<'input> {
    /// Creates a new tokeniser
    #[must_use]
    pub fn new(input: &'input str) -> Self {
        let lexer = Lexer::new(input);
        let start = Location::default();
        let end = Location::default();
        Tokenizer {
            //cu: 0,
            eos: false,
            iter: lexer.peekable(),
            pos: span(start, end, start, end),
        }
    }

    /// Turn this Tokenizer into an `Iterator` of tokens that are produced until the first error is hit
    ///
    /// The purpose of this method is to not accidentally consume tokens after an error,
    /// which might be completely incorrect, but would still show up in the resulting iterator if
    /// used with `filter_map(Result::ok)` for example.
    pub fn tokenize_until_err(self) -> impl Iterator<Item = Spanned<Token<'input>>> {
        // i wanna use map_while here, but it is still unstable :(
        self.scan((), |_, item| item.ok()).fuse()
    }
}

impl<'input> Iterator for Tokenizer<'input> {
    type Item = Result<TokenSpan<'input>>;

    fn next(&mut self) -> Option<Self::Item> {
        let token = self.iter.next();
        match (self.eos, token) {
            (false, None) => {
                self.eos = true;
                Some(Ok(Spanned {
                    value: Token::EndOfStream,
                    span: self.pos,
                }))
            }
            (true, _) => None,
            (_, Some(Err(x))) => Some(Err(x)),
            (_, Some(Ok(t))) => {
                self.pos = t.clone().span;
                Some(Ok(t))
            }
        }
    }
}

/// A lexical preprocessor in the spirit of cpp
///
pub struct Preprocessor {}

macro_rules! take_while {
    ($let:ident, $token:pat, $iter:expr) => {
        $let = $iter.next().ok_or(ErrorKind::UnexpectedEndOfStream)??;
        loop {
            if let Spanned { value: $token, .. } = $let {
                $let = $iter.next().ok_or(ErrorKind::UnexpectedEndOfStream)??;
                continue;
            }
            break;
        }
    };
}

/// A compilation unit
#[derive(Clone, Debug)]
pub struct CompilationUnit {
    file_path: Box<Path>,
}

impl CompilationUnit {
    fn from_file(file: &Path) -> Self {
        let mut p = PathBuf::new();
        p.push(file);
        Self {
            file_path: p.into_boxed_path(),
        }
    }

    /// Returns the path of the file for this
    #[must_use]
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }
}
impl PartialEq for CompilationUnit {
    fn eq(&self, other: &Self) -> bool {
        self.file_path == other.file_path
    }
}

impl fmt::Display for CompilationUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.file_path.to_string_lossy())
    }
}

/// Tracks set of included files in a given logical compilation unit
pub struct IncludeStack {
    elements: Vec<CompilationUnit>,
    pub(crate) cus: Vec<CompilationUnit>,
}
impl Default for IncludeStack {
    fn default() -> Self {
        Self {
            elements: Vec::new(),
            cus: Vec::new(),
        }
    }
}

impl IncludeStack {
    fn pop(&mut self) {
        self.elements.pop();
    }

    /// Returns set of compilation units
    #[must_use]
    pub fn into_cus(self) -> Vec<CompilationUnit> {
        self.cus
    }

    /// Pushes a a compilation unit onto the include stack
    ///
    /// # Errors
    /// if a cyclic dependency is detected
    pub fn push<S: AsRef<OsStr> + ?Sized>(&mut self, file: &S) -> Result<usize> {
        let e = CompilationUnit::from_file(Path::new(file));
        if self.contains(&e) {
            let es = self
                .elements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" -> ");
            Err(format!("Cyclic dependency detected: {} -> {}", es, e.to_string()).into())
        } else {
            let cu = self.cus.len();
            self.cus.push(e.clone());
            self.elements.push(e);
            Ok(cu)
        }
    }
    fn contains(&self, e: &CompilationUnit) -> bool {
        self.elements.contains(e)
    }
}

fn urt(next: Spanned<Token<'_>>, alt: &[&'static str]) -> Error {
    ErrorKind::UnrecognizedToken(
        Range::from(next.span).expand_lines(2),
        Range::from(next.span),
        next.value.to_string(),
        alt.iter().map(ToString::to_string).collect(),
    )
    .into()
}

fn as_ident(next: Spanned<Token<'_>>) -> Result<Cow<'_, str>> {
    if let Token::Ident(id, _) = next.value {
        Ok(id)
    } else {
        Err(urt(next, &["`<ident>`"]))
    }
}
impl<'input> Preprocessor {
    pub(crate) fn resolve(
        module_path: &ModulePath,
        use_span: Span,
        span2: Span,
        rel_module_path: &Path,
        include_stack: &mut IncludeStack,
    ) -> Result<(usize, Box<Path>)> {
        let mut file = PathBuf::from(rel_module_path);
        file.set_extension("tremor");

        if let Some(path) = module_path.resolve(&file) {
            if path.is_file() {
                let cu = include_stack.push(path.as_os_str())?;
                return Ok((cu, path));
            }
        }
        file.set_extension("trickle");

        if let Some(path) = module_path.resolve(&file) {
            if path.is_file() {
                let cu = include_stack.push(path.as_os_str())?;
                return Ok((cu, path));
            }
        }

        let outer = Range::from((use_span.start, use_span.end)).expand_lines(2);
        let inner = Range::from((span2.start, span2.end));
        let path = rel_module_path.to_string_lossy().to_string();
        Err(ErrorKind::ModuleNotFound(outer, inner, path, module_path.mounts.clone()).into())
    }

    #[allow(clippy::too_many_lines)]
    /// Preprocess a possibly nested set of related sources into a single compilation unit
    ///
    /// # Errors
    /// on preprocessor failreus
    pub fn preprocess<S: AsRef<OsStr> + ?Sized>(
        module_path: &ModulePath,
        file_name: &S,
        input: &'input mut std::string::String,
        cu: usize,
        include_stack: &mut IncludeStack,
    ) -> Result<Vec<Result<TokenSpan<'input>>>> {
        let file_name = Path::new(file_name);

        let top = input.clone();
        let lexemes: Vec<Result<TokenSpan>> = Tokenizer::new(top.as_str()).collect();
        input.clear();

        let mut iter = lexemes.into_iter();

        let mut next;
        loop {
            next = iter.next().ok_or(ErrorKind::UnexpectedEndOfStream)??;
            match next {
                //
                // use <module_path> [as <alias>] ;
                //
                Spanned {
                    value: Token::Use,
                    span: use_span,
                    ..
                } => {
                    let mut rel_module_path = PathBuf::new();
                    let mut alias;
                    take_while!(next, Token::Whitespace(_), iter);

                    let id = as_ident(next)?;
                    alias = id.to_string();
                    rel_module_path.push(&alias);

                    take_while!(next, Token::Whitespace(_), iter);

                    loop {
                        if next.value == Token::ColonColon {
                            // module path of the form:
                            // <ident> [ :: <ident> ]*;
                            //
                            take_while!(next, Token::Whitespace(_), iter);

                            let id = as_ident(next)?;
                            alias = id.to_string();
                            rel_module_path.push(&alias);
                            take_while!(next, Token::Whitespace(_), iter);
                            match next.value {
                                Token::As => (),
                                Token::ColonColon => continue,
                                Token::Semi => break,
                                _ => {
                                    return Err(urt(next, &["`as`", "`::`", "`;`"]));
                                }
                            }
                        } else if next.value == Token::As || next.value == Token::Semi {
                            break;
                        } else {
                            return Err(urt(next, &["`as`", "`::`", "`;`"]));
                        }
                    }

                    //
                    // As <alias:ident>
                    //

                    if next.value == Token::As {
                        take_while!(next, Token::Whitespace(_), iter);

                        alias = as_ident(next)?.to_string();

                        // ';'
                        take_while!(next, Token::Whitespace(_), iter);

                        // Semi
                        take_while!(next, Token::Whitespace(_), iter);
                    }

                    if next.value == Token::Semi {
                        take_while!(next, Token::Whitespace(_), iter);
                    }

                    if next.value == Token::NewLine {
                        //let file_path = rel_module_path.clone();
                        let (inner_cu, file_path) = Preprocessor::resolve(
                            module_path,
                            use_span,
                            next.span,
                            &rel_module_path,
                            include_stack,
                        )?;
                        let file_path2 = file_path.clone();

                        let mut file = file::open(&file_path)?;
                        let mut s = String::new();
                        file.read_to_string(&mut s)?;

                        s.push(' ');
                        let s = Preprocessor::preprocess(
                            &module_path,
                            file_path.as_os_str(),
                            &mut s,
                            inner_cu,
                            include_stack,
                        )?;
                        let y = s
                            .into_iter()
                            .filter_map(|x| x.map(|x| format!("{}", x.value)).ok())
                            .collect::<Vec<String>>()
                            .join("");
                        input.push_str(&format!(
                            "#!line 0 0 0 {} {}\n",
                            inner_cu,
                            &file_path2.to_string_lossy()
                        ));
                        input.push_str(&format!("mod {} with\n", &alias));
                        input.push_str(&format!(
                            "#!line 0 0 0 {} {}\n",
                            inner_cu,
                            &file_path2.to_string_lossy()
                        ));
                        input.push_str(&format!("{}\n", y.trim()));
                        input.push_str("end;\n");
                        input.push_str(&format!(
                            "#!line {} {} {} {} {}\n",
                            next.span.end.absolute(),
                            next.span.end.line() + 1,
                            0,
                            cu,
                            file_name.to_string_lossy(),
                        ));

                        include_stack.pop();
                    } else {
                        return Err(urt(next, &["`<newline>`"]));
                    }
                }
                Spanned {
                    value: Token::EndOfStream,
                    ..
                } => break,
                other => {
                    input.push_str(&format!("{}", other.value));
                }
            }
        }
        //input.push_str(" ");
        let tokens = Tokenizer::new(input).collect();

        Ok(tokens)
    }
}

/// An iterator over a source string that yeilds `Token`s for subsequent use by
/// the parser
pub struct Lexer<'input> {
    input: &'input str,
    chars: CharLocations<'input>,
    start_index: BytePos,
    file_offset: Location,
    cu: usize,
    stored_tokens: VecDeque<TokenSpan<'input>>,
}

type Lexeme = Option<(Location, char)>;

impl<'input> Lexer<'input> {
    pub(crate) fn spanned2<T>(
        &self,
        mut start: Location,
        mut end: Location,
        value: T,
    ) -> Spanned<T> {
        start.unit_id = self.cu;
        end.unit_id = self.cu;
        Spanned {
            span: span(start - self.file_offset, end - self.file_offset, start, end),
            value,
        }
    }

    /// Create a new lexer from the source string
    pub(crate) fn new<S>(input: &'input S) -> Self
    where
        S: ?Sized + ParserSource,
    {
        let chars = CharLocations::new(input);
        Lexer {
            input: input.src(),
            chars,
            start_index: input.start_index(),
            file_offset: Location::default(),
            cu: 0,
            stored_tokens: VecDeque::new(),
        }
    }

    /// Return the next character in the source string
    fn lookahead(&mut self) -> Lexeme {
        let loc = self.chars.location;
        self.chars.chars.peek().map(|b| (loc, *b))
    }

    fn starts_with(&mut self, start: Location, s: &str) -> Option<(Location, &'input str)> {
        let mut end = start;
        for c in s.chars() {
            end.shift(c)
        }

        if let Some(head) = self.slice(start, end) {
            if head == s {
                for _ in 0..s.len() - 1 {
                    self.bump();
                }
                Some((end, head))
            } else {
                None
            }
        } else {
            None
        }
    }

    fn bump(&mut self) -> Lexeme {
        self.chars.next()
    }

    /// Return a slice of the source string
    fn slice(&self, start: Location, end: Location) -> Option<&'input str> {
        let start = start.absolute() - self.start_index.to_usize();
        let end = end.absolute() - self.start_index.to_usize();

        // Our indexes are in characters as we are iterating over codepoints
        // string indexes however are in bytes and might lead to either
        // invalid regions or even invalid strings
        // That is terrible, what are you thinking rust!
        // But at lease we can work around it using `indices`
        self.input.get(start..end)
    }

    /// return a slice from `start` to eol of the line `end` is on
    fn slice_full_lines(&self, start: &Location, end: &Location) -> Option<String> {
        let start_idx = start.absolute() - self.start_index.to_usize();
        let take_lines = end.line().saturating_sub(start.line()) + 1;

        self.input.get(start_idx..).map(|f| {
            f.split('\n')
                .take(take_lines)
                .collect::<Vec<&'input str>>()
                .join("\n")
        })
    }

    /// return a slice from start to the end of the line
    fn slice_until_eol(&self, start: &Location) -> Option<&'input str> {
        let start = start.absolute() - self.start_index.to_usize();

        self.input.get(start..).and_then(|f| f.split('\n').next())
    }

    // return a slice from start to the end of the input
    fn slice_until_eof(&self, start: &Location) -> Option<&'input str> {
        let start = start.absolute() - self.start_index.to_usize();
        self.input.get(start..)
    }

    // return a String without any seperators
    fn extract_number<F>(&mut self, start: Location, mut is_valid: F) -> (Location, String)
    where
        F: FnMut(char) -> bool,
    {
        let seperator = '_';
        let (end, number) = self.take_while(start, |c| is_valid(c) || c == seperator);
        // Remove seperators from number
        (end, number.replace(seperator, ""))
    }

    fn take_while<F>(&mut self, start: Location, mut keep_going: F) -> (Location, &'input str)
    where
        F: FnMut(char) -> bool,
    {
        self.take_until(start, |c| !keep_going(c))
    }

    fn take_until<F>(&mut self, start: Location, mut terminate: F) -> (Location, &'input str)
    where
        F: FnMut(char) -> bool,
    {
        let mut e = start;
        while let Some((end, ch)) = self.lookahead() {
            e = end;
            if terminate(ch) {
                if let Some(slice) = self.slice(start, e) {
                    return (end, slice);
                }
                // Invalid start end case :(
                return (end, "<ERROR>");
            }
            self.bump();
        }
        // EOF
        // we need to shift by 1, otherwise we take the location of the previous char.
        e.shift(0 as char);
        self.slice(start, e)
            .map_or((e, "<ERROR>"), |slice| (e, slice))
    }

    /// handle comments
    fn cx(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        if let Some((end, _)) = self.starts_with(start, "#!config ") {
            Ok(self.spanned2(start, end, Token::ConfigDirective))
        } else {
            let (end, lexeme) = self.take_until(start, |ch| ch == '\n');

            if let Some(l) = lexeme.strip_prefix("###") {
                let doc = Token::ModComment(l);
                Ok(self.spanned2(start, end, doc))
            } else if let Some(l) = lexeme.strip_prefix("##") {
                let doc = Token::DocComment(l);
                Ok(self.spanned2(start, end, doc))
            } else if let Some(directive) = lexeme.strip_prefix("#!line") {
                let directive = directive.trim();
                let splitted: Vec<_> = directive.split(' ').collect();

                if let Some(&[absolute, line, column, cu, file]) = splitted.get(0..5) {
                    let cu = cu.parse()?;
                    // we need to move up 1 line as the line directive itself occupies its own line
                    let l = Location::new(line.parse()?, column.parse()?, absolute.parse()?, cu)
                        .move_up_lines(1);

                    let line_directive = Token::LineDirective(l, file.into());

                    // set the file offset to the difference between the included file (line directive)
                    // and the current position in the source so we point to the correct location
                    // in the included file
                    self.file_offset = (start - l).start_of_line();
                    // update the cu to the included file
                    self.file_offset.unit_id = cu;
                    self.cu = cu;
                    Ok(self.spanned2(start, end, line_directive))
                } else {
                    Err("failed to parse line directive!".into())
                }
            } else {
                Ok(self.spanned2(
                    start,
                    end,
                    Token::SingleLineComment(lexeme.trim_start_matches('#')),
                ))
            }
        }
    }

    /// handle equals
    fn eq(&mut self, start: Location) -> TokenSpan<'input> {
        match self.lookahead() {
            Some((end, '>')) => {
                self.bump();
                self.spanned2(start, end + '>', Token::EqArrow)
            }
            Some((end, '=')) => {
                self.bump();
                self.spanned2(start, end + '=', Token::EqEq)
            }
            _ => self.spanned2(start, start + '=', Token::Eq),
        }
    }

    /// handle colons
    fn cn(&mut self, start: Location) -> TokenSpan<'input> {
        let (end, lexeme) = self.take_while(start, |ch| ch == ':');

        match lexeme {
            "::" => self.spanned2(start, end, Token::ColonColon),
            ":" => self.spanned2(start, end, Token::Colon),
            _ => self.spanned2(start, end, Token::Bad(lexeme.to_string())),
        }
    }

    fn gt(&mut self, start: Location) -> TokenSpan<'input> {
        match self.lookahead() {
            Some((end, '>')) => {
                self.bump();
                if let Some((end, '>')) = self.lookahead() {
                    self.bump();
                    self.spanned2(start, end + '>', Token::RBitShiftUnsigned)
                } else {
                    self.spanned2(start, end + '>', Token::RBitShiftSigned)
                }
            }
            Some((end, '=')) => {
                self.bump();
                self.spanned2(start, end + '=', Token::Gte)
            }
            Some((end, _)) => self.spanned2(start, end, Token::Gt),
            None => self.spanned2(start, start, Token::Bad(">".to_string())),
        }
    }

    fn lt(&mut self, start: Location) -> TokenSpan<'input> {
        match self.lookahead() {
            Some((end, '<')) => {
                self.bump();
                self.spanned2(start, end + '<', Token::LBitShift)
            }
            Some((end, '=')) => {
                self.bump();
                self.spanned2(start, end + '=', Token::Lte)
            }
            Some((end, _)) => self.spanned2(start, end, Token::Lt),
            None => self.spanned2(start, start, Token::Bad("<".to_string())),
        }
    }

    /// handle pattern begin
    fn pb(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '[')) => {
                self.bump();
                Ok(self.spanned2(start, end + '[', Token::LPatBracket))
            }
            Some((end, '(')) => {
                self.bump();
                Ok(self.spanned2(start, end + '(', Token::LPatParen))
            }
            Some((end, '{')) => {
                self.bump();
                Ok(self.spanned2(start, end + '{', Token::LPatBrace))
            }
            Some((end, _)) => Ok(self.spanned2(start, end, Token::Mod)),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    /// handle pattern end
    fn pe(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '=')) => {
                self.bump();
                Ok(self.spanned2(start, end + '=', Token::NotEq))
            }
            Some((end, _ch)) => Ok(self.spanned2(start, end, Token::BitNot)),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    /// handle tilde
    fn tl(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '=')) => {
                self.bump();
                Ok(self.spanned2(start, end + '=', Token::TildeEq))
            }
            Some((end, _)) => Ok(self.spanned2(start, end, Token::Tilde)),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    fn id(&mut self, start: Location) -> TokenSpan<'input> {
        let (end, ident) = self.take_while(start, is_ident_continue);

        let token = ident_to_token(ident);

        self.spanned2(start, end, token)
    }

    fn next_index(&mut self) -> Result<Location> {
        match self.chars.next() {
            Some((loc, _)) => Ok(loc),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    fn escape_code(
        &mut self,
        string_start: &Location,
        start: Location,
    ) -> Result<(Location, char)> {
        match self.bump() {
            Some((e, '\'')) => Ok((e, '\'')),
            Some((e, '"')) => Ok((e, '"')),
            Some((e, '\\')) => Ok((e, '\\')),
            // Some((e, '{')) => Ok((e, '{')),
            // Some((e, '}')) => Ok((e, '}')),
            Some((e, '#')) => Ok((e, '#')),
            Some((e, '/')) => Ok((e, '/')),
            Some((e, 'b')) => Ok((e, '\u{8}')), // Backspace
            Some((e, 'f')) => Ok((e, '\u{c}')), // Form Feed
            Some((e, 'n')) => Ok((e, '\n')),
            Some((e, 'r')) => Ok((e, '\r')),
            Some((e, 't')) => Ok((e, '\t')),
            // TODO: Unicode escape codes
            Some((mut end, 'u')) => {
                let mut escape_start = end;
                escape_start.extend_left('u');
                let mut digits = String::with_capacity(4);
                for _i in 0..4 {
                    if let Some((mut e, c)) = self.bump() {
                        digits.push(c);
                        if u32::from_str_radix(&format!("{}", c), 16).is_err() {
                            e.shift(' ');
                            let token_str =
                                self.slice_full_lines(string_start, &e).unwrap_or_default();
                            let range = Range::from((escape_start, e));
                            return Err(ErrorKind::InvalidUtf8Sequence(
                                Range::from((start, e)).expand_lines(2),
                                range,
                                UnfinishedToken::new(range, token_str),
                            )
                            .into());
                        }
                        end = e;
                    } else {
                        end.shift(' ');

                        let token_str = self
                            .slice_full_lines(string_start, &end)
                            .unwrap_or_default();
                        let range = Range::from((escape_start, end));

                        return Err(ErrorKind::InvalidUtf8Sequence(
                            Range::from((escape_start, end)).expand_lines(2),
                            range,
                            UnfinishedToken::new(range, token_str),
                        )
                        .into());
                    }
                }

                if let Ok(Some(c)) = u32::from_str_radix(&digits, 16).map(std::char::from_u32) {
                    Ok((end, c))
                } else {
                    let token_str = self
                        .slice_full_lines(string_start, &end)
                        .unwrap_or_default();
                    end.shift(' ');
                    let range = Range::from((escape_start, end));
                    Err(ErrorKind::InvalidUtf8Sequence(
                        Range::from((escape_start, end)).expand_lines(2),
                        range,
                        UnfinishedToken::new(range, token_str),
                    )
                    .into())
                }
            }
            Some((mut end, ch)) => {
                let token_str = self
                    .slice_full_lines(string_start, &end)
                    .unwrap_or_default();
                end.shift(' ');
                let range = Range::from((start, end));
                Err(ErrorKind::UnexpectedEscapeCode(
                    Range::from((start, end)).expand_lines(2),
                    range,
                    UnfinishedToken::new(range, token_str),
                    ch,
                )
                .into())
            }
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    /// handle quoted idents `\`` ...
    fn id2(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let mut string = String::new();
        let mut end = start;

        loop {
            match self.bump() {
                Some((mut end, '`')) => {
                    // we got to bump end by one so we claim the tailing `"`
                    let e = end;
                    let mut s = start;
                    s.shift('`');
                    end.shift('`');
                    if let Some(slice) = self.slice(s, e) {
                        let token = Token::Ident(slice.into(), true);
                        return Ok(self.spanned2(start, end, token));
                    }
                    // Invalid start end case :(
                    let token = Token::Ident(string.into(), true);
                    return Ok(self.spanned2(start, end, token));
                }
                Some((end, '\n')) => {
                    let range = Range::from((start, end));
                    return Err(ErrorKind::UnterminatedIdentLiteral(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(range, format!("`{}", string)),
                    )
                    .into());
                }

                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => {
                    let range = Range::from((start, end));
                    return Err(ErrorKind::UnterminatedIdentLiteral(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(range, format!("`{}", string)),
                    )
                    .into());
                }
            }
        }
    }

    /// handle quoted strings or heredoc strings
    fn qs_or_hd(&mut self, start: Location) -> Result<Vec<TokenSpan<'input>>> {
        let mut end = start;
        let heredoc_start = start;
        end.shift('"');

        let q1 = self.spanned2(start, end, Token::DQuote);
        let mut res = vec![q1];
        let mut string = String::new();

        match self.lookahead() {
            // This would be the second quote
            Some((mut end, '"')) => {
                self.bump();
                if let Some((end, '"')) = self.lookahead() {
                    self.bump();
                    // We don't allow anything tailing the initial `"""`
                    match self.bump() {
                        Some((mut newline_loc, '\n')) => {
                            res = vec![self.spanned2(start, end + '"', Token::HereDocStart)]; // (0, vec![]))];
                            string = String::new();
                            newline_loc.shift('\n'); // content starts after newline
                            self.hd(heredoc_start, newline_loc, end, false, &string, res)
                        }
                        Some((end, ch)) => {
                            let token_str = self
                                .slice_until_eol(&start)
                                .map_or_else(|| format!(r#""""{}"#, ch), ToString::to_string);
                            let range = Range::from((start, end));
                            Err(ErrorKind::TailingHereDoc(
                                range.expand_lines(2),
                                range,
                                UnfinishedToken::new(range, token_str),
                                ch,
                            )
                            .into())
                        }
                        None => {
                            let token_str = self
                                .slice_until_eol(&start)
                                .map_or_else(|| r#"""""#.to_string(), ToString::to_string);
                            let range = Range::from((start, end));
                            Err(ErrorKind::UnterminatedHereDoc(
                                range.expand_lines(2),
                                range,
                                UnfinishedToken::new(range, token_str),
                            )
                            .into())
                        }
                    }
                } else {
                    // We had two quotes followed by something not a quote so
                    // it is an empty string.
                    //TODO :make slice
                    let start = end;
                    end.shift('"');
                    res.push(self.spanned2(start, end, Token::DQuote));
                    Ok(res)
                }
            }
            Some(_) => self.qs(heredoc_start, end, end, false, string, res),
            None => {
                let range = Range::from((start, start));
                Err(ErrorKind::UnterminatedStringLiteral(
                    range.expand_lines(2),
                    range,
                    UnfinishedToken::new(range, format!("\"{}", string)),
                )
                .into())
            }
        }
    }

    #[allow(clippy::too_many_lines, clippy::clippy::too_many_arguments)]
    fn handle_interpol<F>(
        &mut self,
        is_hd: bool,
        error_prefix: &str,
        has_escapes: &mut bool,
        total_start: Location,
        end_inner: Location,
        segment_start: &mut Location,
        end: &mut Location,
        res: &mut Vec<TokenSpan<'input>>,
        content: &mut String,
        token_constructor: F,
    ) -> Result<()>
    where
        F: Fn(Cow<'input, str>) -> Token,
    {
        end.shift('{');
        self.bump();
        if !content.is_empty() {
            let mut c = String::new();
            mem::swap(&mut c, content);
            let token = if *has_escapes {
                // The string was modified so we can't use the slice
                token_constructor(c.into())
            } else {
                token_constructor(
                    self.slice(*segment_start, end_inner)
                        .map_or_else(|| Cow::from(c), Cow::from),
                )
            };
            res.push(self.spanned2(*segment_start, end_inner, token));
            *has_escapes = false;
        }
        *segment_start = end_inner;
        res.push(self.spanned2(*segment_start, *end, Token::Interpol));
        let mut pcount = 0;
        let mut first = true;
        loop {
            match self.next() {
                Some(Ok(s)) => {
                    match &s.value {
                        Token::RBrace if pcount == 0 => {
                            let start = *segment_start;
                            *segment_start = s.span.pp_end;

                            res.push(s);
                            if first {
                                let end_location = *segment_start;
                                let token_str = self
                                    .slice_full_lines(&total_start, &end_location)
                                    .unwrap_or_else(|| format!("{}{}", error_prefix, content));

                                let unfinished_token = UnfinishedToken::new(
                                    Range::from((start, end_location)),
                                    token_str,
                                );

                                return Err(ErrorKind::EmptyInterpolation(
                                    Range::from((total_start, end_location)),
                                    Range::from((start, end_location)),
                                    unfinished_token,
                                )
                                .into());
                            }
                            break;
                        }
                        Token::RBrace => {
                            pcount -= 1;
                        }
                        Token::LBrace | Token::Interpol => {
                            pcount += 1;
                        }
                        _ => {}
                    };
                    res.push(s);
                    first = false;
                }
                // intercept error and extend the token to match this outer heredoc
                // with interpolation
                // otherwise we will not get the whole heredoc in error messages
                Some(Err(error)) => {
                    let end_location = error.context().1.map_or_else(
                        || res.last().map_or(*end, |last| last.span.end),
                        |inner_error_range| inner_error_range.1,
                    );

                    let Error(kind, ..) = error;

                    let token_str = self
                        .slice_full_lines(&total_start, &end_location)
                        .unwrap_or_else(|| format!("{}{}", error_prefix, content));
                    let mut end = total_start;
                    end.shift_str(&token_str);
                    let unfinished_token =
                        UnfinishedToken::new(Range::from((total_start, end_location)), token_str);
                    let error = match kind {
                        ErrorKind::UnterminatedExtractor(outer, location, _) => {
                            // expand to start line of heredoc, so we get a proper context
                            let outer = outer
                                .expand_lines(outer.0.line().saturating_sub(total_start.line()));
                            ErrorKind::UnterminatedExtractor(outer, location, unfinished_token)
                        }
                        ErrorKind::UnterminatedIdentLiteral(outer, location, _) => {
                            // expand to start line of heredoc, so we get a proper context
                            let outer = outer
                                .expand_lines(outer.0.line().saturating_sub(total_start.line()));
                            ErrorKind::UnterminatedIdentLiteral(outer, location, unfinished_token)
                        }
                        ErrorKind::UnterminatedHereDoc(outer, location, _)
                        | ErrorKind::TailingHereDoc(outer, location, _, _) => {
                            if is_hd {
                                // unterminated heredocs within interpolation are better reported
                                // as unterminated interpolation
                                ErrorKind::UnterminatedInterpolation(
                                    Range::from((total_start, end.move_down_lines(2))),
                                    Range::from((total_start, end)),
                                    unfinished_token,
                                )
                            } else {
                                let outer = outer.expand_lines(
                                    outer.0.line().saturating_sub(total_start.line()),
                                );

                                ErrorKind::UnterminatedHereDoc(outer, location, unfinished_token)
                            }
                        }
                        ErrorKind::UnterminatedInterpolation(outer, location, _) => {
                            // expand to start line of heredoc, so we get a proper context
                            let outer = outer
                                .expand_lines(outer.0.line().saturating_sub(total_start.line()));
                            ErrorKind::UnterminatedInterpolation(outer, location, unfinished_token)
                        }
                        ErrorKind::UnexpectedEscapeCode(outer, location, _, found) => {
                            // expand to start line of heredoc, so we get a proper context
                            let outer = outer
                                .expand_lines(outer.0.line().saturating_sub(total_start.line()));
                            ErrorKind::UnexpectedEscapeCode(
                                outer,
                                location,
                                unfinished_token,
                                found,
                            )
                        }
                        ErrorKind::UnterminatedStringLiteral(outer, location, _) => {
                            // expand to start line of heredoc, so we get a proper context
                            let outer = outer
                                .expand_lines(outer.0.line().saturating_sub(total_start.line()));
                            if is_hd {
                                ErrorKind::UnterminatedStringLiteral(
                                    outer,
                                    location,
                                    unfinished_token,
                                )
                            } else {
                                let mut toekn_end = *segment_start;
                                toekn_end.shift('#');
                                toekn_end.shift('{');
                                ErrorKind::UnterminatedInterpolation(
                                    outer,
                                    Range::from((*segment_start, toekn_end)),
                                    unfinished_token,
                                )
                            }
                        }
                        ErrorKind::InvalidUtf8Sequence(outer, location, _) => {
                            // expand to start line of heredoc, so we get a proper context
                            let outer = outer
                                .expand_lines(outer.0.line().saturating_sub(total_start.line()));
                            ErrorKind::InvalidUtf8Sequence(outer, location, unfinished_token)
                        }
                        e => e,
                    };
                    return Err(error.into());
                }
                None => {
                    let end_location = self.chars.current();
                    let token_str = self
                        .slice_full_lines(&total_start, end_location)
                        .unwrap_or_else(|| format!("{}{}", error_prefix, content));
                    return Err(ErrorKind::UnterminatedInterpolation(
                        Range::from((total_start, end.move_down_lines(2))),
                        Range::from((*segment_start, *end)),
                        UnfinishedToken::new(Range::from((total_start, *end_location)), token_str),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::clippy::too_many_arguments)]
    fn handle_qs_hd_generic<F>(
        &mut self,
        is_hd: bool,
        error_prefix: &str,
        has_escapes: &mut bool,
        total_start: Location,
        segment_start: &mut Location,
        end: &mut Location,
        res: &mut Vec<TokenSpan<'input>>,
        content: &mut String,
        lc: (Location, char),
        token_constructor: F,
    ) -> Result<()>
    where
        F: Fn(Cow<'input, str>) -> Token,
    {
        match lc {
            (end_inner, '\\') => {
                let (mut e, c) = self.escape_code(&total_start, end_inner)?;
                if c == '#' {
                    if !content.is_empty() {
                        let mut c = String::new();
                        mem::swap(&mut c, content);
                        let token = if *has_escapes {
                            // The string was modified so we can't use the slice
                            token_constructor(c.into())
                        } else {
                            self.slice(*segment_start, end_inner).map_or_else(
                                || token_constructor(c.into()),
                                |slice| token_constructor(slice.into()),
                            )
                        };
                        res.push(self.spanned2(*segment_start, end_inner, token));
                        *has_escapes = false;
                    }
                    res.push(self.spanned2(end_inner, e, Token::EscapedHash));
                    e.shift(c);
                    *segment_start = e;
                    *end = e;
                } else {
                    *has_escapes = true;
                    content.push(c);
                    *end = e;
                }
            }
            (end_inner, '#') => {
                if let Some((e, '{')) = self.lookahead() {
                    *end = e;
                    self.handle_interpol(
                        is_hd,
                        error_prefix,
                        has_escapes,
                        total_start,
                        end_inner,
                        segment_start,
                        end,
                        res,
                        content,
                        token_constructor,
                    )?;
                } else {
                    content.push('#');
                    *end = end_inner;
                }
            }
            (e, other) => {
                content.push(other);
                *end = e;
                end.shift(other);
            }
        }
        Ok(())
    }

    /// Handle heredoc strings `"""`  ...
    fn hd(
        &mut self,
        heredoc_start: Location,
        mut segment_start: Location,
        mut end: Location,
        mut has_escapes: bool,
        _string: &str,
        mut res: Vec<TokenSpan<'input>>,
    ) -> Result<Vec<TokenSpan<'input>>> {
        // TODO: deduplicate by encapsulating all state in a struct and have some common operations on it
        let mut content = String::new();
        loop {
            match self.bump() {
                Some((e, '"')) => {
                    // If the current line is just a `"""` then we are at the end of the heredoc
                    res.push(self.spanned2(
                        segment_start,
                        e,
                        Token::HereDocLiteral(content.into()),
                    ));
                    segment_start = e;
                    content = String::new();
                    content.push('"');
                    end = if let Some((e, '"')) = self.lookahead() {
                        self.bump();
                        content.push('"');
                        if let Some((e, '"')) = self.lookahead() {
                            self.bump();
                            let mut heredoc_end = e;
                            heredoc_end.shift('"');
                            res.push(self.spanned2(segment_start, heredoc_end, Token::HereDocEnd));
                            return Ok(res);
                        }
                        e
                    } else {
                        e
                    };
                    end.shift('"');
                }
                Some((e, '\n')) => {
                    end = e;
                    content.push('\n');
                    res.push(self.spanned2(
                        segment_start,
                        end,
                        Token::HereDocLiteral(content.into()),
                    ));
                    end.shift('\n');
                    segment_start = end;
                    content = String::new();
                }
                Some(lc) => {
                    self.handle_qs_hd_generic(
                        true,
                        "\"\"\"\n",
                        &mut has_escapes,
                        heredoc_start,
                        &mut segment_start,
                        &mut end,
                        &mut res,
                        &mut content,
                        lc,
                        Token::HereDocLiteral,
                    )?;
                }
                None => {
                    // We reached EOF
                    let token_str = self
                        .slice_until_eof(&heredoc_start)
                        .map_or_else(|| format!(r#""""\n{}"#, content), ToString::to_string);
                    let range = Range::from((heredoc_start, end));
                    return Err(ErrorKind::UnterminatedHereDoc(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(range, token_str),
                    )
                    .into());
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    /// Handle quote strings `"`  ...
    fn qs(
        &mut self,
        total_start: Location,
        mut segment_start: Location,
        mut end: Location,
        mut has_escapes: bool,
        mut string: String,
        mut res: Vec<TokenSpan<'input>>,
    ) -> Result<Vec<TokenSpan<'input>>> {
        loop {
            match self.bump() {
                Some((mut end, '"')) => {
                    // If the string is empty we kind of don't need it.
                    if !string.is_empty() {
                        let token = if has_escapes {
                            // The string was modified so we can't use the slice
                            Token::StringLiteral(string.into())
                        } else {
                            self.slice(segment_start, end).map_or_else(
                                || Token::StringLiteral(string.into()),
                                |slice| Token::StringLiteral(slice.into()),
                            )
                        };
                        res.push(self.spanned2(segment_start, end, token));
                    }
                    let start = end;
                    end.shift('"');
                    res.push(self.spanned2(start, end, Token::DQuote));
                    return Ok(res);
                }
                Some((end, '\n')) => {
                    let token_str = self
                        .slice_until_eol(&total_start)
                        .map_or_else(|| format!("\"{}", string), ToString::to_string);

                    let mut token_end = total_start;
                    token_end.shift_str(&token_str);
                    let range = Range::from((total_start, end));
                    return Err(ErrorKind::UnterminatedStringLiteral(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(Range::from((total_start, token_end)), token_str),
                    )
                    .into());
                }
                Some(lc) => {
                    self.handle_qs_hd_generic(
                        false,
                        "\"",
                        &mut has_escapes,
                        total_start,
                        &mut segment_start,
                        &mut end,
                        &mut res,
                        &mut string,
                        lc,
                        Token::StringLiteral,
                    )?;
                }
                None => {
                    let token_str = self
                        .slice_until_eol(&total_start)
                        .map_or_else(|| format!("\"{}", string), ToString::to_string);
                    let mut token_end = total_start;
                    token_end.shift_str(&token_str);
                    let range = Range::from((total_start, end));
                    return Err(ErrorKind::UnterminatedStringLiteral(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(Range::from((total_start, token_end)), token_str),
                    )
                    .into());
                }
            }
        }
    }

    fn test_escape_code(&mut self, start: &Location, s: &str) -> Result<Option<char>> {
        match self.bump() {
            Some((_, '\\')) => Ok(Some('\\')),
            Some((_, '|')) => Ok(Some('|')),
            Some((_end, '\n')) => Ok(None),
            Some((end, ch)) => {
                let token_str = format!("|{}\\{}", s, ch);
                let mut token_end = end;
                token_end.shift(ch);
                let range = Range::from((end, end));
                Err(ErrorKind::UnexpectedEscapeCode(
                    range.expand_lines(2),
                    range,
                    UnfinishedToken::new(Range::from((*start, token_end)), token_str),
                    ch,
                )
                .into())
            }
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    /// handle test/extractor '|...'
    fn pl(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let mut string = String::new();
        let mut strings = Vec::new();

        let mut end = start;
        loop {
            match self.bump() {
                Some((e, '\\')) => {
                    if let Some(ch) = self.test_escape_code(&start, &string)? {
                        string.push(ch);
                    } else {
                        strings.push(string);
                        string = String::new();
                    }
                    end = e;
                }
                Some((mut end, '|')) => {
                    strings.push(string);
                    end.shift('|');
                    let indent = indentation(&strings);
                    let strings = strings
                        .iter()
                        .map(|s| {
                            if s.is_empty() {
                                s.clone()
                            } else {
                                s.split_at(indent).1.to_string()
                            }
                        })
                        .collect();
                    let token = Token::TestLiteral(indent, strings);
                    return Ok(self.spanned2(start, end, token));
                }
                Some((end, '\n')) => {
                    let token_str = self
                        .slice_until_eol(&start)
                        .map_or_else(|| format!("|{}", string), ToString::to_string);
                    let range = Range::from((start, end));
                    return Err(ErrorKind::UnterminatedExtractor(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(range, token_str),
                    )
                    .into());
                }
                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => {
                    let token_str = self
                        .slice_until_eol(&start)
                        .map_or_else(|| format!("|{}", string), ToString::to_string);
                    let mut token_end = start;
                    token_end.shift_str(&token_str);
                    end.shift(' ');
                    let range = Range::from((start, end));
                    return Err(ErrorKind::UnterminatedExtractor(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(Range::from((start, token_end)), token_str),
                    )
                    .into());
                }
            }
        }
    }

    /// handle numbers (with or without leading '-')
    #[allow(clippy::too_many_lines)]
    fn nm(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, int) = self.extract_number(start, is_dec_digit);
        let (start, end, token) = match self.lookahead() {
            Some((_, '.')) => {
                self.bump(); // Skip '.'
                let (end, float) = self.extract_number(start, is_dec_digit);
                match self.lookahead() {
                    Some((_, 'e')) => {
                        self.bump();
                        if let Some((exp_location, _)) = self.bump() {
                            // handle sign
                            let (exp_location, sign) = match self.lookahead() {
                                Some((loc, '+')) | Some((loc, '-')) => {
                                    self.bump();
                                    self.slice(exp_location, loc).map(|s| (loc, s))
                                }
                                _ => Some((exp_location, "")),
                            }
                            .unwrap_or((exp_location, ""));
                            let (end, exp) = self.extract_number(exp_location, is_dec_digit);
                            let float = &format!("{}e{}{}", float, sign, exp);
                            (
                                start,
                                end,
                                Token::FloatLiteral(
                                    float.parse().chain_err(|| {
                                        ErrorKind::InvalidFloatLiteral(
                                            Range::from((start, end)).expand_lines(2),
                                            Range::from((start, end)),
                                            UnfinishedToken::new(
                                                Range::from((start, end)),
                                                self.slice_until_eol(&start).map_or_else(
                                                    || float.to_string(),
                                                    ToString::to_string,
                                                ),
                                            ),
                                        )
                                    })?,
                                    float.to_string(),
                                ),
                            )
                        } else {
                            return Err(ErrorKind::InvalidFloatLiteral(
                                Range::from((start, end)).expand_lines(2),
                                Range::from((start, end)),
                                UnfinishedToken::new(
                                    Range::from((start, end)),
                                    self.slice_until_eol(&start)
                                        .map_or_else(|| float.to_string(), ToString::to_string),
                                ),
                            )
                            .into());
                        }
                    }
                    Some((end, ch)) if is_ident_start(ch) => {
                        return Err(ErrorKind::UnexpectedCharacter(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((end, end)),
                            UnfinishedToken::new(
                                Range::from((start, end)),
                                self.slice_until_eol(&start)
                                    .map_or_else(|| int.to_string(), ToString::to_string),
                            ),
                            ch,
                        )
                        .into());
                    }
                    _ => (
                        start,
                        end,
                        Token::FloatLiteral(
                            float.parse().chain_err(|| {
                                ErrorKind::InvalidFloatLiteral(
                                    Range::from((start, end)).expand_lines(2),
                                    Range::from((start, end)),
                                    UnfinishedToken::new(
                                        Range::from((start, end)),
                                        self.slice_until_eol(&start)
                                            .map_or_else(|| float.to_string(), ToString::to_string),
                                    ),
                                )
                            })?,
                            float.to_string(),
                        ),
                    ),
                }
            }
            Some((_, 'x')) => {
                self.bump(); // Skip 'x'
                let int_start = self.next_index()?;
                let (end, hex) = self.extract_number(int_start, is_hex);
                // ALLOW: this takes the whole string and can not panic
                match &int[..] {
                    "0" | "-0" => match self.lookahead() {
                        Some((_, ch)) if is_ident_start(ch) => {
                            return Err(ErrorKind::UnexpectedCharacter(
                                Range::from((start, end)).expand_lines(2),
                                Range::from((start, end)),
                                UnfinishedToken::new(
                                    Range::from((start, end)),
                                    self.slice_until_eol(&start)
                                        .map_or_else(|| hex.to_string(), ToString::to_string),
                                ),
                                ch,
                            )
                            .into());
                        }
                        _ => {
                            if hex.is_empty() {
                                return Err(ErrorKind::InvalidHexLiteral(
                                    Range::from((start, end)).expand_lines(2),
                                    Range::from((start, end)),
                                    UnfinishedToken::new(
                                        Range::from((start, end)),
                                        self.slice_until_eol(&start)
                                            .map_or_else(|| hex.to_string(), ToString::to_string),
                                    ),
                                )
                                .into());
                            }
                            let is_positive = int == "0";
                            // ALLOW: this takes the whole string and can not panic
                            match i64_from_hex(&hex[..], is_positive) {
                                Ok(val) => (start, end, Token::IntLiteral(val)),
                                Err(_err) => {
                                    return Err(ErrorKind::InvalidHexLiteral(
                                        Range::from((start, end)).expand_lines(2),
                                        Range::from((start, end)),
                                        UnfinishedToken::new(
                                            Range::from((start, end)),
                                            self.slice_until_eol(&start).map_or_else(
                                                || hex.to_string(),
                                                ToString::to_string,
                                            ),
                                        ),
                                    )
                                    .into());
                                }
                            }
                        }
                    },
                    _ => {
                        return Err(ErrorKind::InvalidHexLiteral(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            UnfinishedToken::new(
                                Range::from((start, end)),
                                self.slice_until_eol(&start)
                                    .map_or_else(|| int.to_string(), ToString::to_string),
                            ),
                        )
                        .into());
                    }
                }
            }
            Some((char_loc, ch)) if is_ident_start(ch) => {
                return Err(ErrorKind::UnexpectedCharacter(
                    Range::from((start, end)).expand_lines(2),
                    Range::from((char_loc, char_loc)),
                    UnfinishedToken::new(
                        Range::from((start, end)),
                        self.slice_until_eol(&start)
                            .map_or_else(|| int.to_string(), ToString::to_string),
                    ),
                    ch,
                )
                .into());
            }
            None | Some(_) => {
                if let Ok(val) = int.parse() {
                    (start, end, Token::IntLiteral(val))
                } else {
                    return Err(ErrorKind::InvalidIntLiteral(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        UnfinishedToken::new(
                            Range::from((start, end)),
                            self.slice_until_eol(&start)
                                .map_or_else(|| int.to_string(), ToString::to_string),
                        ),
                    )
                    .into());
                }
            }
        };

        Ok(self.spanned2(start, end, token))
    }

    /// Consume whitespace
    fn ws(&mut self, start: Location) -> TokenSpan<'input> {
        let (end, src) = self.take_while(start, is_ws);
        self.spanned2(start, end, Token::Whitespace(src))
    }
}

/// Converts partial hex literal (i.e. part after `0x` or `-0x`) to 64 bit signed integer.
///
/// This is basically a copy and adaptation of `std::num::from_str_radix`.
fn i64_from_hex(hex: &str, is_positive: bool) -> Result<i64> {
    let r = i64::from_str_radix(hex, 16)?;

    Ok(if is_positive { r } else { -r })
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Result<TokenSpan<'input>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.stored_tokens.pop_front() {
            return Some(Ok(next));
        }
        let lexeme = self.bump();
        match lexeme {
            None => None,
            Some((start, ch)) => {
                match ch as char {
                    // '...' =>  Some(Ok(self.spanned2(start, self.next_index(), Token::DotDotDot))),
                    // ".." =>  Some(Ok(self.spanned2(start, self.next_index(), Token::DotDot))),
                    ',' => Some(Ok(self.spanned2(start, start + ch, Token::Comma))),
                    '$' => Some(Ok(self.spanned2(start, start + ch, Token::Dollar))),
                    '.' => Some(Ok(self.spanned2(start, start + ch, Token::Dot))),
                    //                        '?' => Some(Ok(self.spanned2(start, start, Token::Question))),
                    '_' => Some(Ok(self.spanned2(start, start + ch, Token::DontCare))),
                    ';' => Some(Ok(self.spanned2(start, start + ch, Token::Semi))),
                    '+' => Some(Ok(self.spanned2(start, start + ch, Token::Add))),
                    '*' => Some(Ok(self.spanned2(start, start + ch, Token::Mul))),
                    '\\' => Some(Ok(self.spanned2(start, start + ch, Token::BSlash))),
                    '(' => Some(Ok(self.spanned2(start, start + ch, Token::LParen))),
                    ')' => Some(Ok(self.spanned2(start, start + ch, Token::RParen))),
                    '{' => Some(Ok(self.spanned2(start, start + ch, Token::LBrace))),
                    '}' => Some(Ok(self.spanned2(start, start + ch, Token::RBrace))),
                    '[' => Some(Ok(self.spanned2(start, start + ch, Token::LBracket))),
                    ']' => Some(Ok(self.spanned2(start, start + ch, Token::RBracket))),
                    '/' => Some(Ok(self.spanned2(start, start + ch, Token::Div))),
                    // TODO account for extractors which use | to mark format boundaries
                    //'|' => Some(Ok(self.spanned2(start, start, Token::BitOr))),
                    '^' => Some(Ok(self.spanned2(start, start + ch, Token::BitXor))),
                    '&' => Some(Ok(self.spanned2(start, start + ch, Token::BitAnd))),
                    ':' => Some(Ok(self.cn(start))),
                    '-' => match self.lookahead() {
                        Some((_loc, c)) if is_dec_digit(c) => Some(self.nm(start)),
                        _ => Some(Ok(self.spanned2(start, start + ch, Token::Sub))),
                    },
                    '#' => Some(self.cx(start)),
                    '=' => Some(Ok(self.eq(start))),
                    '<' => Some(Ok(self.lt(start))),
                    '>' => Some(Ok(self.gt(start))),
                    '%' => Some(self.pb(start)),
                    '~' => Some(self.tl(start)),
                    '`' => Some(self.id2(start)),
                    // TODO account for bitwise not operator
                    '!' => Some(self.pe(start)),
                    '\n' => Some(Ok(self.spanned2(start, start, Token::NewLine))),
                    ch if is_ident_start(ch) => Some(Ok(self.id(start))),
                    '"' => match self.qs_or_hd(start) {
                        Ok(mut tokens) => {
                            for t in tokens.drain(..) {
                                self.stored_tokens.push_back(t)
                            }
                            self.next()
                        }
                        Err(e) => Some(Err(e)),
                    },
                    ch if is_test_start(ch) => Some(self.pl(start)),
                    ch if is_dec_digit(ch) => Some(self.nm(start)),
                    ch if ch.is_whitespace() => Some(Ok(self.ws(start))),
                    _ => {
                        let str = format!("{}", ch);
                        Some(Ok(self.spanned2(start, start, Token::Bad(str))))
                    }
                }
            }
        }
    }
}

fn indentation(strings: &[String]) -> usize {
    let mut indent = None;
    for s in strings {
        if !s.is_empty() {
            let l = s.len() - s.trim_start().len();
            if let Some(i) = indent {
                if i > l {
                    indent = Some(l);
                }
            } else {
                indent = Some(l);
            }
        }
    }
    indent.unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! lex_ok {
        ($src:expr, $($span:expr => $token:expr,)*) => {{
            let lexed_tokens: Vec<_> = Tokenizer::new($src).filter(|t| match t {
                Ok(Spanned { value: Token::EndOfStream, .. }) => false,
                Ok(Spanned { value: t, .. }) => !t.is_ignorable(),
                Err(_) => true
            }).map(|t| match t {
                Ok(Spanned { value: t, span: tspan }) => Ok((t, tspan.start.column(), tspan.end.column())),
                Err(e) => Err(e)
            }).collect();
            // locations are 1-based, so we need to add 1 to every loc, end position needs another +1
            let expected_tokens = vec![$({
                Ok(($token, $span.find("~").unwrap() + 1, $span.rfind("~").unwrap() + 2))
            }),*];

            assert_eq!(lexed_tokens, expected_tokens);
        }};
    }

    #[rustfmt::skip]
    #[test]
    fn interpolate() -> Result<()> {
        lex_ok! {
            r#"  "" "#,
            r#"  ~ "# => Token::DQuote,
            r#"   ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#"  "hello" "#,
            r#"  ~ "# => Token::DQuote,
            r#"   ~~~~~ "# => Token::StringLiteral("hello".into()),
            r#"        ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#"  "hello #{7}" "#,
            r#"  ~ "# => Token::DQuote,
            r#"   ~~~~~~ "# => Token::StringLiteral("hello ".into()),
            r#"         ~~ "# => Token::Interpol,
            r#"           ~ "# => Token::IntLiteral(7),
            r#"            ~ "# => Token::RBrace,
            r#"             ~ "# => Token::DQuote,

        };
        // We can't use `r#""#` for the string since we got a a `"#` in it
        lex_ok! {
            "  \"#{7} hello\" ",
            r#"  ~ "# => Token::DQuote,
            r#"   ~~ "# => Token::Interpol,
            r#"     ~ "# => Token::IntLiteral(7),
            r#"      ~ "# => Token::RBrace,
            r#"       ~~~~~~ "# => Token::StringLiteral(" hello".into()),
            r#"             ~ "# => Token::DQuote,

        };
        lex_ok! {
            r#"  "hello #{ "snot #{7}" }" "#,
            r#"  ~ "# => Token::DQuote,
            r#"   ~~~~~~ "# => Token::StringLiteral("hello ".into()),
            r#"         ~~ "# => Token::Interpol,
            r#"            ~ "# => Token::DQuote,
            r#"             ~~~~~ "# => Token::StringLiteral("snot ".into()),
            r#"                  ~~ "# => Token::Interpol,
            r#"                    ~ "# => Token::IntLiteral(7),
            r#"                     ~ "# => Token::RBrace,
            r#"                      ~ "# => Token::DQuote,
            r#"                        ~ "# => Token::RBrace,
            r#"                         ~ "# => Token::DQuote,
        };
        Ok(())
    }

    #[rustfmt::skip]
    #[test]
    fn number_parsing() -> Result<()> {
        lex_ok! {
        "1_000_000",
        "~~~~~~~~~" => Token::IntLiteral(1000000), };
        lex_ok! {
        "1_000_000_",
        "~~~~~~~~~~" => Token::IntLiteral(1000000), };

        lex_ok! {
        "100.0000",
        "~~~~~~~~" => Token::FloatLiteral(100.0000, "100.0000".to_string()), };
        lex_ok! {
        "1_00.0_000_",
        "~~~~~~~~~~~" => Token::FloatLiteral(100.0000, "100.0000".to_string()), };

        lex_ok! {
        "0xFFAA00",
        "~~~~~~~~" => Token::IntLiteral(16755200), };
        lex_ok! {
        "0xFF_AA_00",
        "~~~~~~~~~~" => Token::IntLiteral(16755200), };
        Ok(())
    }

    #[rustfmt::skip]
    #[test]
    fn paths() -> Result<()> {
        lex_ok! {
            "  hello-hahaha8ABC ",
            "  ~~~~~~~~~~~~~~~~ " => Token::Ident("hello-hahaha8ABC".into(), false),
        };
        lex_ok! {
            "  .florp ",
            "  ~ " => Token::Dot,
            "   ~~~~~ " => Token::Ident("florp".into(), false),
        };
        lex_ok! {
            "  $borp ",
            "  ~ " => Token::Dollar,
            "   ~~~~ " => Token::Ident("borp".into(), false),
        };
        lex_ok! {
            "  $`borp`",
            "  ~ " => Token::Dollar,
            "   ~~~~~~ " => Token::Ident("borp".into(), true),
        };
        Ok(())
    }

    #[rustfmt::skip]
    #[test]
    fn keywords() -> Result<()> {
        lex_ok! {
        " let ",
        " ~~~ " => Token::Let, };
        lex_ok! {
            " match ",
            " ~~~~~ " => Token::Match, };
        lex_ok! {
            " case ",
            " ~~~~ " => Token::Case, };
        lex_ok! {
            " of ",
            " ~~ " => Token::Of, };
        lex_ok! {
            " end ",
            " ~~~ " => Token::End, };
        lex_ok! {
            " drop ",
            " ~~~~ " => Token::Drop, };
        lex_ok! {
            " emit ",
            " ~~~~ " => Token::Emit, };
        lex_ok! {
            " event ",
            " ~~~~~ " => Token::Event, };
        lex_ok! {
            " state ",
            " ~~~~~ " => Token::State, };
        lex_ok! {
            " set ",
            " ~~~ " => Token::Set, };
        lex_ok! {
            " each ",
            " ~~~~ " => Token::Each, };
        lex_ok! {
            " intrinsic ",
            " ~~~~~~~~~ " => Token::Intrinsic, };
        Ok(())
    }

    #[rustfmt::skip]
    #[test]
    fn operators() -> Result<()> {
        lex_ok! {
            " not null ",
            " ~~~ " => Token::Not,
            "     ~~~~ " => Token::Nil,
        };
        lex_ok! {
            " != null ",
            " ~~ " => Token::NotEq,
            "    ~~~~ " => Token::Nil,
        };
        lex_ok! {
            " !1 ",
            " ~  " => Token::BitNot,
            "  ~ " => Token::IntLiteral(1),
        };
        lex_ok! {
            " ! ",
            " ~ " => Token::BitNot,
        };
        lex_ok! {
            " and ",
            " ~~~ " => Token::And,
        };
        lex_ok! {
            " or ",
            " ~~ " => Token::Or,
        };
        lex_ok! {
            " xor ",
            " ~~~ " => Token::Xor,
        };
        lex_ok! {
            " & ",
            " ~ " => Token::BitAnd,
        };
        /* TODO: enable
        lex_ok! {
            " | ",
            " ~ " => Token::BitOr,
        };*/
        lex_ok! {
            " ^ ",
            " ~ " => Token::BitXor,
        };
        lex_ok! {
            " = ",
            " ~ " => Token::Eq,
        };
        lex_ok! {
            " == ",
            " ~~ " => Token::EqEq,
        };
        lex_ok! {
            " != ",
            " ~~ " => Token::NotEq,
        };
        lex_ok! {
            " >= ",
            " ~~ " => Token::Gte,
        };
        lex_ok! {
            " > ",
            " ~ " => Token::Gt,
        };
        lex_ok! {
            " <= ",
            " ~~ " => Token::Lte,
        };
        lex_ok! {
            " < ",
            " ~ " => Token::Lt,
        };
        lex_ok! {
            " >> ",
            " ~~ " => Token::RBitShiftSigned,
        };
        lex_ok! {
            " >>> ",
            " ~~~ " => Token::RBitShiftUnsigned,
        };
        lex_ok! {
            " << ",
            " ~~ " => Token::LBitShift,
        };
        lex_ok! {
            " + ",
            " ~ " => Token::Add,
        };
        lex_ok! {
            " - ",
            " ~ " => Token::Sub,
        };
        lex_ok! {
            " * ",
            " ~ " => Token::Mul,
        };
        lex_ok! {
            " / ",
            " ~ " => Token::Div,
        };
        lex_ok! {
            " % ",
            " ~ " => Token::Mod,
        };
        Ok(())
    }

    #[test]
    fn should_disambiguate() -> Result<()> {
        // Starts with ':'
        lex_ok! {
            " : ",
            " ~ " => Token::Colon,
        };
        lex_ok! {
            " :: ",
            " ~~ " => Token::ColonColon,
        };

        // Starts with '-'
        lex_ok! {
            " - ",
            " ~ " => Token::Sub,
        };

        // Starts with '='
        lex_ok! {
            " = ",
            " ~ " => Token::Eq,
        };
        lex_ok! {
            " == ",
            " ~~ " => Token::EqEq,
        };
        lex_ok! {
            " => ",
            " ~~ " => Token::EqArrow,
        };

        // Starts with '%'
        lex_ok! {
            " % ",
            " ~ " => Token::Mod,
        }
        lex_ok! {
            " %{ ",
            " ~~ " => Token::LPatBrace,
        }
        lex_ok! {
            " %[ ",
            " ~~ " => Token::LPatBracket,
        }
        lex_ok! {
            " %( ",
            " ~~ " => Token::LPatParen,
        }
        // Starts with '.'
        lex_ok! {
            " . ",
            " ~ " => Token::Dot,
        };

        Ok(())
    }

    #[test]
    fn delimiters() -> Result<()> {
        lex_ok! {
                    " ( ) { } [ ] ",
                    " ~           " => Token::LParen,
                    "   ~         " => Token::RParen,
                    "     ~       " => Token::LBrace,
                    "       ~     " => Token::RBrace,
                    "         ~   " => Token::LBracket,
                    "           ~ " => Token::RBracket,
        };
        Ok(())
    }

    #[test]
    fn string() -> Result<()> {
        lex_ok! {
            r#" "\n" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~~  "# => Token::StringLiteral("\n".into()),
            r#"    ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#" "\r" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~~  "# => Token::StringLiteral("\r".into()),
            r#"    ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#" "\t" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~~  "# => Token::StringLiteral("\t".into()),
            r#"    ~ "# => Token::DQuote,
        };
        lex_ok! {
             r#" "\\" "#,
             r#" ~    "# => Token::DQuote,
             r#"  ~~  "# => Token::StringLiteral("\\".into()),
             r#"    ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#" "\"" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~~  "# => Token::StringLiteral("\"".into()),
            r#"    ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#" "{" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~  "# => Token::StringLiteral("{".into()),
            r#"   ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#" "}" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~  "# => Token::StringLiteral("}".into()),
            r#"   ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#" "{}" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~~  "# => Token::StringLiteral("{}".into()),
            r#"    ~ "# => Token::DQuote,
        };
        lex_ok! {
            r#" "}" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~   "# => Token::StringLiteral("}".into()),
            r#"   ~  "# => Token::DQuote,
        };

        lex_ok! {
            r#" "a\nb}}" "#,
            r#" ~        "# => Token::DQuote,
            r#"  ~~~~~~  "# => Token::StringLiteral("a\nb}}".into()),
            r#"        ~ "# => Token::DQuote,
        };

        lex_ok! {
            r#" "\"\"" "#,
            r#" ~      "# => Token::DQuote,
            r#"  ~~~~  "# => Token::StringLiteral("\"\"".into()),
            r#"      ~ "# => Token::DQuote,
        };

        lex_ok! {
            r#" "\\\"" "#,
            r#" ~      "# => Token::DQuote,
            r#"  ~~~~  "# => Token::StringLiteral("\\\"".into()),
            r#"      ~ "# => Token::DQuote,
        };

        lex_ok! {
            r#" "\"\"""\"\"" "#,
            r#" ~            "# => Token::DQuote,
            r#"  ~~~~        "# => Token::StringLiteral("\"\"".into()),
            r#"      ~       "# => Token::DQuote,
            r#"       ~      "# => Token::DQuote,
            r#"        ~~~~  "# => Token::StringLiteral("\"\"".into()),
            r#"            ~ "# => Token::DQuote,
        };
        //lex_ko! { r#" "\\\" "#, " ~~~~~ " => ErrorKind::UnterminatedStringLiteral { start: Location::new(1,2,2), end: Location::new(1,7,7) } }
        Ok(())
    }

    #[rustfmt::skip]
    #[test]
    fn heredoc() -> Result<()> {
        lex_ok! {
            r#""""
              """"#,
            r#"~~~"# => Token::HereDocStart,
            r#"~~~~~~~~~~~~~~"# => Token::HereDocLiteral("              ".into()),
            r#"              ~~~"# => Token::HereDocEnd,
        };

        Ok(())
    }

    #[test]
    fn test_preprocessor() -> Result<()> {
        lex_ok! {
            r#"use "foo.tremor" ;"#,
            r#"~~~               "# => Token::Use,
            r#"    ~             "# => Token::DQuote,
            r#"     ~~~~~~~~~~   "# => Token::StringLiteral("foo.tremor".into()),
            r#"               ~  "# => Token::DQuote,
            r#"                 ~"# => Token::Semi,
        };
        Ok(())
    }

    #[test]
    fn test_test_literal_format_bug_regression() -> Result<()> {
        let snot = "match %{ test ~= base64|| } of default => \"badger\" end ".to_string();
        let mut snot2 = snot.clone();
        let badger: Vec<_> = Tokenizer::new(&snot).collect();
        let mut include_stack = IncludeStack::default();
        let badger2 = Preprocessor::preprocess(
            &ModulePath { mounts: vec![] },
            "foo",
            &mut snot2,
            0,
            &mut include_stack,
        )?;
        let mut res = String::new();
        for b in badger
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<TokenSpan>>()
        {
            res.push_str(&format!("{}", b.value));
        }
        assert_eq!(snot, res);
        let mut res2 = String::new();
        for b in badger2
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<TokenSpan>>()
        {
            res2.push_str(&format!("{}", b.value));
        }
        assert_eq!(snot, res2);
        Ok(())
    }

    #[test]
    fn lexer_long_float() -> Result<()> {
        let f = 48354865651623290000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.0;
        let source = format!("{:.1}", f); // ensure we keep the .0
        match Tokenizer::new(&source).next() {
            Some(Ok(token)) => match token.value {
                Token::FloatLiteral(f_token, _) => {
                    assert_eq!(f, f_token);
                }
                t => assert!(false, "{:?} not a float", t),
            },
            e => assert!(false, "{:?} unexpected", e),
        };
        Ok(())
    }
    use proptest::prelude::*;

    proptest! {
        // negative floats are constructed in the AST later
        #[test]
        fn float_literals_precision(f in 0f64..f64::MAX) {
            if f.round() != f {
                let float = format!("{:.}", f);
                for token in Tokenizer::new(& float) {
                    let _ = token?;
                }
            }
        }
    }

    proptest! {
        // negative floats are constructed in the AST later
        #[test]
        fn float_literals_scientific(f in 0f64..f64::MAX) {
            let float = format!("{:e}", f);
            for token in Tokenizer::new(& float) {
                match token {
                    Ok(spanned) =>
                        match spanned.value {
                            Token::FloatLiteral(f_token, _f_str) => assert_eq!(f, f_token),
                            _ => ()
                        }
                    _ => ()
                }
            }
        }
    }
}
