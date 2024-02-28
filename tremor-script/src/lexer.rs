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

#[allow(clippy::all, clippy::unwrap_used)]
use crate::parser::g::__ToTriple;
pub use crate::pos::*;
use crate::Value;
use crate::{
    arena,
    errors::{Error, Kind as ErrorKind, Result, ResultExt, UnfinishedToken},
};
use crate::{errors::unexpected_character, pos};
use beef::Cow;
use std::fmt;
use std::iter::Peekable;
use std::str::Chars;
use std::{collections::VecDeque, mem};
use unicode_xid::UnicodeXID;
use value_trait::prelude::Writable;

/// Source for a parser
pub trait ParserSource {
    /// The source as a str
    fn src(&self) -> &str;
    /// the initial index
    fn start_index(&self) -> pos::Byte;
}

impl ParserSource for str {
    fn src(&self) -> &str {
        self
    }
    fn start_index(&self) -> pos::Byte {
        pos::Byte::from(0)
    }
}

/// A token with a span to indicate its location
pub type TokenSpan<'input> = Spanned<'input>;

fn is_ws(ch: char) -> bool {
    ch.is_whitespace() && ch != '\n'
}

fn is_ident_start(ch: char) -> bool {
    UnicodeXID::is_xid_start(ch) || ch == '_'
}

fn is_ident_continue(ch: char) -> bool {
    UnicodeXID::is_xid_continue(ch) || ch == '_'
}

fn is_test_start(ch: char) -> bool {
    ch == '|'
}

fn is_dec_digit(ch: char) -> bool {
    ch.is_ascii_digit()
}

fn is_hex(ch: char) -> bool {
    ch.is_ascii_hexdigit()
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
        "pipeline" => Token::Pipeline,
        "connector" => Token::Connector,
        "connect" => Token::Connect,
        "flow" => Token::Flow,
        "to" => Token::To,
        "deploy" => Token::Deploy,
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
    IntLiteral(u64),
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
    /// Config Directive
    ConfigDirective,
    /// The `pipeline` keyword
    Pipeline,
    /// The `connector` keyword
    Connector,
    /// The `flow` keyword
    Flow,
    /// The `connect` keyword
    Connect,
    /// The `to` keyword
    To,
    /// The `deploy` keyword
    Deploy,
}

impl<'input> Token<'input> {
    /// Is the token ignorable except when syntax or error highlighting.
    /// Is the token insignificant when parsing ( a correct ... ) source.
    pub(crate) fn is_ignorable(&self) -> bool {
        matches!(
            *self,
            Token::SingleLineComment(_) | Token::Whitespace(_) | Token::NewLine
        )
    }

    /// Is the token a keyword, excluding keyword literals ( eg: true, nil )
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
                | Token::Pipeline
                | Token::Connector
                | Token::Flow
                | Token::Connect
                | Token::To
                | Token::Deploy
        )
    }

    /// Is the token a literal, excluding list and record literals
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
#[allow(clippy::wrong_self_convention)]
impl<'input> __ToTriple<'input> for Spanned<'input> {
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
// #[cfg_attr(coverage, no_coverage)]
impl<'input> fmt::Display for Token<'input> {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Whitespace(ws) => write!(f, "{}", &ws),
            Token::NewLine => writeln!(f),
            Token::Ident(ref name, true) => write!(f, "`{name}`"),
            Token::Ident(ref name, false) => write!(f, "{name}"),
            Token::ModComment(comment) => write!(f, "### {}", &comment),
            Token::DocComment(comment) => write!(f, "## {}", &comment),
            Token::SingleLineComment(comment) => write!(f, "# {}", &comment),
            Token::IntLiteral(value) => write!(f, "{value}"),
            Token::FloatLiteral(_, txt) => write!(f, "{txt}"),
            Token::DQuote => write!(f, "\""),
            Token::Interpol => write!(f, "#{{"),
            Token::EscapedHash => write!(f, "\\#"),
            Token::StringLiteral(value) => {
                // We do those to ensure proper escaping
                let value: &str = value;
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
                    .map_or_else(|| (value as &str, false), |stripped| (stripped, true));
                let s = Value::from(value).encode();
                // Strip the quotes
                if let Some(s) = s.get(1..s.len() - 1) {
                    write!(f, "{s}")?;
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
            Token::Bad(value) => write!(f, "{value}"),
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
            // Preprocessor
            Token::ConfigDirective => write!(f, "#!config "),
            // Troy
            Token::Pipeline => write!(f, "pipeline"),
            Token::Connector => write!(f, "connector"),
            Token::Flow => write!(f, "flow"),
            Token::Connect => write!(f, "connect"),
            Token::To => write!(f, "to"),
            Token::Deploy => write!(f, "deploy"),
        }
    }
}

struct CharLocations<'input> {
    location: Location,
    chars: Peekable<Chars<'input>>,
}

impl<'input> CharLocations<'input> {
    pub(crate) fn new<S>(input: &'input S, aid: arena::Index) -> CharLocations<'input>
    where
        S: ?Sized + ParserSource,
    {
        CharLocations {
            location: Location::with_byte_index(input.start_index().to_usize(), aid),
            chars: input.src().chars().peekable(),
        }
    }

    pub(crate) fn current(&self) -> Location {
        self.location
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

/// An iterator over a source string that yeilds `Token`s for subsequent use by
/// the parser
pub struct Lexer<'input> {
    input: &'input str,
    chars: CharLocations<'input>,
    stored_tokens: VecDeque<TokenSpan<'input>>,
}

type Lexeme = Option<(Location, char)>;

pub(crate) fn spanned(start: Location, end: Location, value: Token) -> Spanned {
    Spanned {
        span: span(start, end),
        value,
    }
}
impl<'input> Lexer<'input> {
    /// The purpose of this method is to not accidentally consume tokens after an error,
    /// which might be completely incorrect, but would still show up in the resulting iterator if
    /// used with `filter_map(Result::ok)` for example.
    pub fn tokenize_until_err(self) -> impl Iterator<Item = Spanned<'input>> {
        // i wanna use map_while here, but it is still unstable :(
        self.scan((), |_, item| item.ok()).fuse()
    }

    /// Create a new lexer from the source string
    #[must_use]
    pub fn new(input: &'input str, aid: arena::Index) -> Self {
        let chars = CharLocations::new(input, aid);
        Lexer {
            input: input.src(),
            chars,
            stored_tokens: VecDeque::new(),
        }
    }

    /// Return the next character in the source string
    fn lookahead(&mut self) -> Lexeme {
        let loc = self.chars.location;
        self.chars.chars.peek().map(|b| (loc, *b))
    }

    fn must_lookahead(&mut self) -> Result<(Location, char)> {
        self.lookahead()
            .ok_or_else(|| ErrorKind::UnexpectedEndOfStream(self.chars.current().into()).into())
    }
    fn must_bump(&mut self) -> Result<(Location, char)> {
        self.bump()
            .ok_or_else(|| ErrorKind::UnexpectedEndOfStream(self.chars.current().into()).into())
    }

    fn starts_with(&mut self, start: Location, s: &str) -> Option<(Location, &'input str)> {
        let mut end = start;
        for c in s.chars() {
            end.shift(c);
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
        let start = start.absolute();
        let end = end.absolute();

        // Our indexes are in characters as we are iterating over codepoints
        // string indexes however are in bytes and might lead to either
        // invalid regions or even invalid strings
        // That is terrible, what are you thinking rust!
        // But at lease we can work around it using `indices`
        self.input.get(start..end)
    }

    /// return a slice from `start` to eol of the line `end` is on
    fn slice_full_lines(&self, start: Location, end: Location) -> Option<String> {
        let start_idx = start.absolute();
        let take_lines = end.line().saturating_sub(start.line()) + 1;

        self.input.get(start_idx..).map(|f| {
            let v: Vec<_> = f.split('\n').take(take_lines).collect();
            v.join("\n")
        })
    }

    /// return a slice from start to the end of the line
    fn slice_until_eol(&self, start: Location) -> Option<&'input str> {
        let start = start.absolute();

        self.input.get(start..).and_then(|f| f.split('\n').next())
    }

    // return a slice from start to the end of the input
    fn slice_until_eof(&self, start: Location) -> Option<&'input str> {
        let start = start.absolute();
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
    fn comment(&mut self, start: Location) -> TokenSpan<'input> {
        if let Some((end, _)) = self.starts_with(start, "#!config ") {
            spanned(start, end, Token::ConfigDirective)
        } else if let Some((end, lexeme)) = self.starts_with(start, "#!") {
            spanned(start, end, Token::Bad(lexeme.to_string()))
        } else {
            let (end, lexeme) = self.take_until(start, |ch| ch == '\n');

            if let Some(l) = lexeme.strip_prefix("###") {
                let doc = Token::ModComment(l);
                spanned(start, end, doc)
            } else if let Some(l) = lexeme.strip_prefix("##") {
                let doc = Token::DocComment(l);
                spanned(start, end, doc)
            } else {
                spanned(
                    start,
                    end,
                    Token::SingleLineComment(lexeme.trim_start_matches('#')),
                )
            }
        }
    }

    /// handle equals
    fn eq(&mut self, start: Location) -> TokenSpan<'input> {
        match self.lookahead() {
            Some((end, '>')) => {
                self.bump();
                spanned(start, end + '>', Token::EqArrow)
            }
            Some((end, '=')) => {
                self.bump();
                spanned(start, end + '=', Token::EqEq)
            }
            _ => spanned(start, start + '=', Token::Eq),
        }
    }

    /// handle colons
    fn colon(&mut self, start: Location) -> TokenSpan<'input> {
        match self.take_while(start, |ch| ch == ':') {
            (end, "::") => spanned(start, end, Token::ColonColon),
            (end, ":") => spanned(start, end, Token::Colon),
            (end, lexeme) => spanned(start, end, Token::Bad(lexeme.to_string())),
        }
    }

    fn gt(&mut self, start: Location) -> TokenSpan<'input> {
        match self.lookahead() {
            Some((end, '>')) => {
                self.bump();
                if let Some((end, '>')) = self.lookahead() {
                    self.bump();
                    spanned(start, end + '>', Token::RBitShiftUnsigned)
                } else {
                    spanned(start, end + '>', Token::RBitShiftSigned)
                }
            }
            Some((end, '=')) => {
                self.bump();
                spanned(start, end + '=', Token::Gte)
            }
            Some((end, _)) => spanned(start, end, Token::Gt),
            None => spanned(start, start, Token::Bad(">".to_string())),
        }
    }

    fn lt(&mut self, start: Location) -> TokenSpan<'input> {
        match self.lookahead() {
            Some((end, '<')) => {
                self.bump();
                spanned(start, end + '<', Token::LBitShift)
            }
            Some((end, '=')) => {
                self.bump();
                spanned(start, end + '=', Token::Lte)
            }
            Some((end, _)) => spanned(start, end, Token::Lt),
            None => spanned(start, start, Token::Bad("<".to_string())),
        }
    }

    /// handle pattern begin
    fn pb(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.must_lookahead()? {
            (end, '[') => {
                self.bump();
                Ok(spanned(start, end + '[', Token::LPatBracket))
            }
            (end, '(') => {
                self.bump();
                Ok(spanned(start, end + '(', Token::LPatParen))
            }
            (end, '{') => {
                self.bump();
                Ok(spanned(start, end + '{', Token::LPatBrace))
            }
            (end, _) => Ok(spanned(start, end, Token::Mod)),
        }
    }

    /// handle pattern end
    fn pe(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.must_lookahead()? {
            (end, '=') => {
                self.bump();
                Ok(spanned(start, end + '=', Token::NotEq))
            }
            (end, _ch) => Ok(spanned(start, end, Token::BitNot)),
        }
    }

    /// handle tilde
    fn tl(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.must_lookahead()? {
            (end, '=') => {
                self.bump();
                Ok(spanned(start, end + '=', Token::TildeEq))
            }
            (end, _) => Ok(spanned(start, end, Token::Tilde)),
        }
    }

    fn id(&mut self, start: Location) -> TokenSpan<'input> {
        let (end, ident) = self.take_while(start, is_ident_continue);

        let token = ident_to_token(ident);

        spanned(start, end, token)
    }

    fn next_index(&mut self) -> Result<Location> {
        let (loc, _) = self.must_bump()?;
        Ok(loc)
    }

    fn escape_code(&mut self, str_start: Location, start: Location) -> Result<(Location, char)> {
        match self.must_bump()? {
            (e, '"') => Ok((e, '"')),
            (e, '\\') => Ok((e, '\\')),
            (e, '#') => Ok((e, '#')),
            (e, 'b') => Ok((e, '\u{8}')), // Backspace
            (e, 'f') => Ok((e, '\u{c}')), // Form Feed
            (e, 'n') => Ok((e, '\n')),
            (e, 'r') => Ok((e, '\r')),
            (e, 't') => Ok((e, '\t')),
            // TODO: Unicode escape codes
            (mut end, 'u') => {
                let mut escape_start = end;
                escape_start.shift_left('u');
                let mut digits = String::with_capacity(4);
                let r = (|| {
                    for _i in 0..4 {
                        if let Some((e, c)) = self.bump() {
                            digits.push(c);
                            end = e;
                            if u32::from_str_radix(&format!("{c}"), 16).is_err() {
                                return None;
                            }
                        } else {
                            return None;
                        }
                    }
                    u32::from_str_radix(&digits, 16)
                        .map(std::char::from_u32)
                        .ok()
                        .flatten()
                        .map(|c| (end, c))
                })();
                r.ok_or_else(|| {
                    end.shift(' ');
                    let token_str = self.slice_full_lines(str_start, end).unwrap_or_default();
                    let inner = Span::new(escape_start, end);
                    let outer = inner.expand_lines(2);
                    let tkn = UnfinishedToken::new(inner, token_str);
                    ErrorKind::InvalidUtf8Sequence(outer, inner, tkn).into()
                })
            }
            (mut end, ch) => {
                end.shift(' ');
                let token_str = self.slice_full_lines(str_start, end).unwrap_or_default();
                let inner = Span::new(start, end);
                let outer = inner.expand_lines(2);
                let tkn = UnfinishedToken::new(inner, token_str);
                Err(ErrorKind::UnexpectedEscapeCode(outer, inner, tkn, ch).into())
            }
        }
    }

    /// handle quoted idents `\` ...
    fn quoted_ident(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let mut string = String::new();
        let mut end = start;

        loop {
            match self.bump().ok_or_else(|| {
                let range = Span::new(start, end);
                ErrorKind::UnterminatedIdentLiteral(
                    range.expand_lines(2),
                    range,
                    UnfinishedToken::new(range, format!("`{string}")),
                )
            })? {
                (mut end, '`') => {
                    // we got to bump end by one so we claim the tailing `"`
                    let e = end;
                    let mut s = start;
                    s.shift('`');
                    end.shift('`');
                    if let Some(slice) = self.slice(s, e) {
                        let token = Token::Ident(slice.into(), true);
                        return Ok(spanned(start, end, token));
                    }
                    // Invalid start end case :(
                    let token = Token::Ident(string.into(), true);
                    return Ok(spanned(start, end, token));
                }
                (end, '\n') => {
                    let range = Span::new(start, end);
                    return Err(ErrorKind::UnterminatedIdentLiteral(
                        range.expand_lines(2),
                        range,
                        UnfinishedToken::new(range, format!("`{string}")),
                    )
                    .into());
                }

                (e, other) => {
                    string.push(other);
                    end = e;
                    continue;
                }
            }
        }
    }

    /// handle quoted strings or heredoc strings
    fn string_or_heredoc(&mut self, start: Location) -> Result<Vec<TokenSpan<'input>>> {
        let mut end = start;
        let heredoc_start = start;
        end.shift('"');

        let q1 = spanned(start, end, Token::DQuote);
        let mut res = vec![q1];
        let mut string = String::new();

        if let (mut end, '"') = self.lookahead().ok_or_else(|| {
            let range = Span::new(start, end);
            ErrorKind::UnterminatedStringLiteral(
                range.expand_lines(2),
                range,
                UnfinishedToken::new(range, format!("\"{string}")),
            )
        })? {
            // This would be the second quote
            self.bump();
            if let Some((end, '"')) = self.lookahead() {
                self.bump();
                // We don't allow anything tailing the initial `"""`
                match self.bump().ok_or_else(|| {
                    let tkn = self.slice_until_eol(start).unwrap_or(r#"""""#).into();
                    let inner = Span::new(start, end);
                    let outer = inner.expand_lines(2);
                    ErrorKind::UnterminatedHereDoc(outer, inner, UnfinishedToken::new(inner, tkn))
                })? {
                    (mut newline_loc, '\n') => {
                        res = vec![spanned(start, end + '"', Token::HereDocStart)]; // (0, vec![]))];
                        string = String::new();
                        newline_loc.shift('\n'); // content starts after newline
                        self.heredoc(heredoc_start, newline_loc, end, false, &string, res)
                    }
                    (end, ch) => {
                        let tnk = self
                            .slice_until_eol(start)
                            .map_or_else(|| format!(r#""""{ch}"#), ToString::to_string);
                        let inner = Span::new(start, end);
                        let outer = inner.expand_lines(2);
                        let tkn = UnfinishedToken::new(inner, tnk);
                        Err(ErrorKind::TailingHereDoc(outer, inner, tkn, ch).into())
                    }
                }
            } else {
                // We had two quotes followed by something not a quote so
                // it is an empty string.
                //TODO :make slice
                let start = end;
                end.shift('"');
                res.push(spanned(start, end, Token::DQuote));
                Ok(res)
            }
        } else {
            self.string(heredoc_start, end, end, false, string, res)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn intercept_heredoc_error(
        &self,
        is_hd: bool,
        error_prefix: &str,
        total_start: Location,
        segment_start: &mut Location,
        end: &mut Location,
        res: &mut [TokenSpan<'input>],
        content: &mut String,
        error: Error,
    ) -> ErrorKind {
        // intercept error and extend the token to match this outer heredoc
        // with interpolation
        // otherwise we will not get the whole heredoc in error messages

        let end_location = error.context().1.map_or_else(
            || res.last().map_or(*end, |last| last.span.end()),
            pos::Span::end,
        );

        let Error(kind, ..) = error;

        let tkn = self.slice_full_lines(total_start, end_location);
        let tkn = tkn.unwrap_or_else(|| format!("{error_prefix}{content}"));
        let mut end = total_start;
        end.shift_str(&tkn);
        let unfinished_token = UnfinishedToken::new(Span::new(total_start, end_location), tkn);
        match kind {
            ErrorKind::UnterminatedExtractor(o, location, _) => {
                // expand to start line of heredoc, so we get a proper context
                let outer = o.expand_lines(o.start().line().saturating_sub(total_start.line()));
                ErrorKind::UnterminatedExtractor(outer, location, unfinished_token)
            }
            ErrorKind::UnterminatedIdentLiteral(o, location, _) => {
                // expand to start line of heredoc, so we get a proper context
                let outer = o.expand_lines(o.start().line().saturating_sub(total_start.line()));
                ErrorKind::UnterminatedIdentLiteral(outer, location, unfinished_token)
            }
            ErrorKind::UnterminatedHereDoc(o, location, _)
            | ErrorKind::TailingHereDoc(o, location, _, _) => {
                if is_hd {
                    // unterminated heredocs within interpolation are better reported
                    // as unterminated interpolation
                    ErrorKind::UnterminatedInterpolation(
                        Span::new(total_start, end.move_down_lines(2)),
                        Span::new(total_start, end),
                        unfinished_token,
                    )
                } else {
                    let outer = o.expand_lines(o.start().line().saturating_sub(total_start.line()));
                    ErrorKind::UnterminatedHereDoc(outer, location, unfinished_token)
                }
            }
            ErrorKind::UnterminatedInterpolation(o, location, _) => {
                // expand to start line of heredoc, so we get a proper context
                let outer = o.expand_lines(o.start().line().saturating_sub(total_start.line()));
                ErrorKind::UnterminatedInterpolation(outer, location, unfinished_token)
            }
            ErrorKind::UnexpectedEscapeCode(o, location, _, found) => {
                // expand to start line of heredoc, so we get a proper context
                let outer = o.expand_lines(o.start().line().saturating_sub(total_start.line()));
                ErrorKind::UnexpectedEscapeCode(outer, location, unfinished_token, found)
            }
            ErrorKind::UnterminatedStringLiteral(o, location, _) => {
                // expand to start line of heredoc, so we get a proper context
                let outer = o.expand_lines(o.start().line().saturating_sub(total_start.line()));
                if is_hd {
                    ErrorKind::UnterminatedStringLiteral(outer, location, unfinished_token)
                } else {
                    let mut toekn_end = *segment_start;
                    toekn_end.shift('#');
                    toekn_end.shift('{');
                    ErrorKind::UnterminatedInterpolation(
                        outer,
                        Span::new(*segment_start, toekn_end),
                        unfinished_token,
                    )
                }
            }
            ErrorKind::InvalidUtf8Sequence(o, location, _) => {
                // expand to start line of heredoc, so we get a proper context
                let outer = o.expand_lines(o.start().line().saturating_sub(total_start.line()));
                ErrorKind::InvalidUtf8Sequence(outer, location, unfinished_token)
            }
            e => e,
        }
    }

    #[allow(clippy::too_many_arguments)]
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
            res.push(spanned(*segment_start, end_inner, token));
            *has_escapes = false;
        }
        *segment_start = end_inner;
        res.push(spanned(*segment_start, *end, Token::Interpol));
        let mut pcount = 0;
        let mut first = true;
        loop {
            let next = self.next().ok_or_else(|| {
                let end_location = self.chars.current();
                let token_str = self
                    .slice_full_lines(total_start, end_location)
                    .unwrap_or_else(|| format!("{error_prefix}{content}"));
                ErrorKind::UnterminatedInterpolation(
                    Span::new(total_start, end.move_down_lines(2)),
                    Span::new(*segment_start, *end),
                    UnfinishedToken::new(Span::new(total_start, end_location), token_str),
                )
            })?;

            let s = next.map_err(|error| {
                self.intercept_heredoc_error(
                    is_hd,
                    error_prefix,
                    total_start,
                    segment_start,
                    end,
                    res,
                    content,
                    error,
                )
            })?;
            match &s.value {
                Token::RBrace if pcount == 0 => {
                    let start = *segment_start;
                    *segment_start = s.span.end();

                    res.push(s);
                    if first {
                        let end_location = *segment_start;
                        let tkn = self.slice_full_lines(total_start, end_location);
                        let tkn = tkn.unwrap_or_else(|| format!("{error_prefix}{content}"));
                        let tkn = UnfinishedToken::new(Span::new(start, end_location), tkn);
                        let outer = Span::new(total_start, end_location);
                        let inner = Span::new(start, end_location);
                        return Err(ErrorKind::EmptyInterpolation(outer, inner, tkn).into());
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
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_string_heredoc_generic<F>(
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
                let (mut e, c) = self.escape_code(total_start, end_inner)?;
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
                        res.push(spanned(*segment_start, end_inner, token));
                        *has_escapes = false;
                    }
                    res.push(spanned(end_inner, e, Token::EscapedHash));
                    e.shift(c);
                    *segment_start = e;
                } else {
                    *has_escapes = true;
                    content.push(c);
                }
                *end = e;
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
    fn heredoc(
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
            let next = self.bump().ok_or_else(|| {
                // We reached EOF
                let token_str = self
                    .slice_until_eof(heredoc_start)
                    .map_or_else(|| format!(r#""""\n{content}"#), ToString::to_string);
                let range = Span::new(heredoc_start, end);
                ErrorKind::UnterminatedHereDoc(
                    range.expand_lines(2),
                    range,
                    UnfinishedToken::new(range, token_str),
                )
            })?;
            match next {
                (e, '"') => {
                    // If the current line is just a `"""` then we are at the end of the heredoc
                    res.push(spanned(
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
                            res.push(spanned(segment_start, heredoc_end, Token::HereDocEnd));
                            return Ok(res);
                        }
                        e
                    } else {
                        e
                    };
                    end.shift('"');
                }
                (e, '\n') => {
                    end = e;
                    content.push('\n');
                    res.push(spanned(
                        segment_start,
                        end,
                        Token::HereDocLiteral(content.into()),
                    ));
                    end.shift('\n');
                    segment_start = end;
                    content = String::new();
                }
                lc => {
                    self.handle_string_heredoc_generic(
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
            }
        }
    }

    /// Handle quote strings `"`  ...
    fn string(
        &mut self,
        total_start: Location,
        mut segment_start: Location,
        mut end: Location,
        mut has_escapes: bool,
        mut string: String,
        mut res: Vec<TokenSpan<'input>>,
    ) -> Result<Vec<TokenSpan<'input>>> {
        loop {
            let next = self
                .bump()
                .ok_or_else(|| self.unfinished_string(&string, total_start))?;

            match next {
                (mut end, '"') => {
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
                        res.push(spanned(segment_start, end, token));
                    }
                    let start = end;
                    end.shift('"');
                    res.push(spanned(start, end, Token::DQuote));
                    return Ok(res);
                }
                (end, '\n') => {
                    let tkn = self.slice_until_eol(total_start);
                    let tkn = tkn.map_or_else(|| format!("\"{string}"), ToString::to_string);
                    let mut token_end = total_start;
                    token_end.shift_str(&tkn);
                    let inner = Span::new(total_start, end);
                    let outer = inner.expand_lines(2);
                    let tkn = UnfinishedToken::new(Span::new(total_start, token_end), tkn);
                    return Err(ErrorKind::UnterminatedStringLiteral(outer, inner, tkn).into());
                }
                lc => {
                    self.handle_string_heredoc_generic(
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
            }
        }
    }

    fn test_escape_code(&mut self, start: Location, s: &str) -> Result<Option<char>> {
        match self.must_bump()? {
            (_, '\\') => Ok(Some('\\')),
            (_, '|') => Ok(Some('|')),
            (_end, '\n') => Ok(None),
            (end, ch) => {
                let token_str = format!("|{s}\\{ch}");
                let mut token_end = end;
                token_end.shift(ch);
                let inner = Span::new(end, end);
                let outer = inner.expand_lines(2);
                let tkn = UnfinishedToken::new(Span::new(start, token_end), token_str);
                Err(ErrorKind::UnexpectedEscapeCode(outer, inner, tkn, ch).into())
            }
        }
    }

    fn unfinished_extractor(&self, string: &str, start: Location) -> ErrorKind {
        let token_str = self
            .slice_until_eol(start)
            .map_or_else(|| format!("|{string}"), ToString::to_string);
        let mut token_end = start;
        token_end.shift_str(&token_str);
        let range = Span::new(start, token_end);
        ErrorKind::UnterminatedExtractor(
            range.expand_lines(2),
            range,
            UnfinishedToken::new(Span::new(start, token_end), token_str),
        )
    }
    fn unfinished_string(&self, string: &str, start: Location) -> ErrorKind {
        let tkn = self.slice_until_eol(start);
        let tkn = tkn.map_or_else(|| format!("\"{string}"), ToString::to_string);
        let mut token_end = start;
        token_end.shift_str(&tkn);
        let range = Span::new(start, token_end);
        ErrorKind::UnterminatedStringLiteral(
            range.expand_lines(2),
            range,
            UnfinishedToken::new(Span::new(start, token_end), tkn),
        )
    }

    /// handle test/extractor '|...'
    fn extractor(&mut self, total_start: Location) -> Result<TokenSpan<'input>> {
        let mut string = String::new();
        let mut strings = Vec::new();

        loop {
            let next = self
                .bump()
                .ok_or_else(|| self.unfinished_extractor(&string, total_start))?;
            match next {
                (_e, '\\') => {
                    if let Some(ch) = self.test_escape_code(total_start, &string)? {
                        string.push(ch);
                    } else {
                        strings.push(string);
                        string = String::new();
                    }
                }
                (mut end, '|') => {
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
                    return Ok(spanned(total_start, end, token));
                }
                (end, '\n') => {
                    let tkn = self.slice_until_eol(total_start);
                    let tkn = tkn.map_or_else(|| format!("|{string}"), ToString::to_string);
                    let inner = Span::new(total_start, end);
                    let outer = inner.expand_lines(2);
                    let tkn = UnfinishedToken::new(inner, tkn);
                    return Err(ErrorKind::UnterminatedExtractor(outer, inner, tkn).into());
                }
                (_e, other) => {
                    string.push(other);
                }
            }
        }
    }

    fn float(&mut self, start: Location, int: &str) -> Result<TokenSpan<'input>> {
        self.bump(); // Skip '.'
        let (end, float) = self.extract_number(start, is_dec_digit);
        match self.lookahead() {
            Some((_, 'e')) => {
                self.bump();
                if let Some((exp_location, _)) = self.bump() {
                    // handle sign
                    let (exp_location, sign) = match self.lookahead() {
                        Some((loc, '+' | '-')) => {
                            self.bump();
                            self.slice(exp_location, loc).map(|s| (loc, s))
                        }
                        _ => Some((exp_location, "")),
                    }
                    .unwrap_or((exp_location, ""));
                    let (end, exp) = self.extract_number(exp_location, is_dec_digit);
                    let float = &format!("{float}e{sign}{exp}");
                    Ok(spanned(
                        start,
                        end,
                        Token::FloatLiteral(
                            float.parse().chain_err(|| {
                                ErrorKind::InvalidFloatLiteral(
                                    Span::new(start, end).expand_lines(2),
                                    Span::new(start, end),
                                    UnfinishedToken::new(
                                        Span::new(start, end),
                                        self.slice_until_eol(start)
                                            .map_or_else(|| float.to_string(), ToString::to_string),
                                    ),
                                )
                            })?,
                            float.to_string(),
                        ),
                    ))
                } else {
                    Err(ErrorKind::InvalidFloatLiteral(
                        Span::new(start, end).expand_lines(2),
                        Span::new(start, end),
                        UnfinishedToken::new(
                            Span::new(start, end),
                            self.slice_until_eol(start)
                                .map_or_else(|| float.to_string(), ToString::to_string),
                        ),
                    )
                    .into())
                }
            }
            Some((end, ch)) if is_ident_start(ch) => Err(ErrorKind::UnexpectedCharacter(
                Span::new(start, end).expand_lines(2),
                Span::new(start, end),
                UnfinishedToken::new(
                    Span::new(start, end),
                    self.slice_until_eol(start)
                        .map_or_else(|| int.to_string(), ToString::to_string),
                ),
                ch,
            )
            .into()),
            _ => Ok(spanned(
                start,
                end,
                Token::FloatLiteral(
                    float.parse().chain_err(|| {
                        ErrorKind::InvalidFloatLiteral(
                            Span::new(start, end).expand_lines(2),
                            Span::new(start, end),
                            UnfinishedToken::new(
                                Span::new(start, end),
                                self.slice_until_eol(start)
                                    .map_or_else(|| float.to_string(), ToString::to_string),
                            ),
                        )
                    })?,
                    float.to_string(),
                ),
            )),
        }
    }

    fn hex(&mut self, start: Location, int: &str) -> Result<TokenSpan<'input>> {
        self.bump(); // Skip 'x'
        let int_start = self.next_index()?;
        let (end, hex) = self.extract_number(int_start, is_hex);
        // ALLOW: this takes the whole string and can not panic
        match int {
            "0" => match self.lookahead() {
                Some((_, ch)) if is_ident_start(ch) => {
                    let r = Span::new(start, end);
                    Err(unexpected_character(
                        &r.expand_lines(2),
                        &r,
                        UnfinishedToken::new(
                            r,
                            self.slice_until_eol(start)
                                .map_or_else(|| hex.to_string(), ToString::to_string),
                        ),
                        ch,
                    ))
                }
                _ => {
                    if hex.is_empty() {
                        Err(ErrorKind::InvalidHexLiteral(
                            Span::new(start, end).expand_lines(2),
                            Span::new(start, end),
                            UnfinishedToken::new(
                                Span::new(start, end),
                                self.slice_until_eol(start)
                                    .map_or_else(|| hex.to_string(), ToString::to_string),
                            ),
                        )
                        .into())
                    } else {
                        // ALLOW: this takes the whole string and can not panic
                        match u64::from_str_radix(&hex[..], 16) {
                            Ok(val) => Ok(spanned(start, end, Token::IntLiteral(val))),
                            Err(_err) => Err(ErrorKind::InvalidHexLiteral(
                                Span::new(start, end).expand_lines(2),
                                Span::new(start, end),
                                UnfinishedToken::new(
                                    Span::new(start, end),
                                    self.slice_until_eol(start)
                                        .map_or_else(|| hex.to_string(), ToString::to_string),
                                ),
                            )
                            .into()),
                        }
                    }
                }
            },
            _ => Err(ErrorKind::InvalidHexLiteral(
                Span::new(start, end).expand_lines(2),
                Span::new(start, end),
                UnfinishedToken::new(
                    Span::new(start, end),
                    self.slice_until_eol(start)
                        .map_or_else(|| int.to_string(), ToString::to_string),
                ),
            )
            .into()),
        }
    }

    /// handle numbers
    #[allow(clippy::too_many_lines)]
    fn number(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, int) = self.extract_number(start, is_dec_digit);
        match self.lookahead() {
            Some((_, '.')) => self.float(start, &int),
            Some((_, 'x')) => self.hex(start, &int),
            Some((char_loc, ch)) if is_ident_start(ch) => Err(ErrorKind::UnexpectedCharacter(
                Span::new(start, end).expand_lines(2),
                Span::new(char_loc, char_loc),
                UnfinishedToken::new(
                    Span::new(start, end),
                    self.slice_until_eol(start)
                        .map_or_else(|| int.to_string(), ToString::to_string),
                ),
                ch,
            )
            .into()),
            None | Some(_) => int
                .parse()
                .map(|val| spanned(start, end, Token::IntLiteral(val)))
                .map_err(|_| {
                    Error::from(ErrorKind::InvalidIntLiteral(
                        Span::new(start, end).expand_lines(2),
                        Span::new(start, end),
                        UnfinishedToken::new(
                            Span::new(start, end),
                            self.slice_until_eol(start)
                                .map_or_else(|| int.to_string(), ToString::to_string),
                        ),
                    ))
                }),
        }
    }

    /// Consume whitespace
    fn whitespace(&mut self, start: Location) -> TokenSpan<'input> {
        let (end, src) = self.take_while(start, is_ws);
        spanned(start, end, Token::Whitespace(src))
    }
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Result<TokenSpan<'input>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.stored_tokens.pop_front() {
            return Some(Ok(next));
        }
        let (start, ch) = self.bump()?;
        match ch {
            // '...' =>  Some(Ok(spanned2(start, self.next_index(), Token::DotDotDot))),
            // ".." =>  Some(Ok(spanned2(start, self.next_index(), Token::DotDot))),
            ',' => Some(Ok(spanned(start, start + ch, Token::Comma))),
            '$' => Some(Ok(spanned(start, start + ch, Token::Dollar))),
            '.' => Some(Ok(spanned(start, start + ch, Token::Dot))),
            //                        '?' => Some(Ok(spanned2(start, start, Token::Question))),
            '_' => Some(Ok(spanned(start, start + ch, Token::DontCare))),
            ';' => Some(Ok(spanned(start, start + ch, Token::Semi))),
            '+' => Some(Ok(spanned(start, start + ch, Token::Add))),
            '*' => Some(Ok(spanned(start, start + ch, Token::Mul))),
            '\\' => Some(Ok(spanned(start, start + ch, Token::BSlash))),
            '(' => Some(Ok(spanned(start, start + ch, Token::LParen))),
            ')' => Some(Ok(spanned(start, start + ch, Token::RParen))),
            '{' => Some(Ok(spanned(start, start + ch, Token::LBrace))),
            '}' => Some(Ok(spanned(start, start + ch, Token::RBrace))),
            '[' => Some(Ok(spanned(start, start + ch, Token::LBracket))),
            ']' => Some(Ok(spanned(start, start + ch, Token::RBracket))),
            '/' => Some(Ok(spanned(start, start + ch, Token::Div))),
            // TODO account for extractors which use | to mark format boundaries
            //'|' => Some(Ok(spanned2(start, start, Token::BitOr))),
            '^' => Some(Ok(spanned(start, start + ch, Token::BitXor))),
            '&' => Some(Ok(spanned(start, start + ch, Token::BitAnd))),
            ':' => Some(Ok(self.colon(start))),
            '-' => Some(Ok(spanned(start, start + ch, Token::Sub))),
            '#' => Some(Ok(self.comment(start))),
            '=' => Some(Ok(self.eq(start))),
            '<' => Some(Ok(self.lt(start))),
            '>' => Some(Ok(self.gt(start))),
            '%' => Some(self.pb(start)),
            '~' => Some(self.tl(start)),
            '`' => Some(self.quoted_ident(start)),
            // TODO account for bitwise not operator
            '!' => Some(self.pe(start)),
            '\n' => Some(Ok(spanned(start, start, Token::NewLine))),
            ch if is_ident_start(ch) => Some(Ok(self.id(start))),
            '"' => match self.string_or_heredoc(start) {
                Ok(mut tokens) => {
                    for t in tokens.drain(..) {
                        self.stored_tokens.push_back(t);
                    }
                    self.next()
                }
                Err(e) => Some(Err(e)),
            },
            ch if is_test_start(ch) => Some(self.extractor(start)),
            ch if is_dec_digit(ch) => Some(self.number(start)),
            ch if ch.is_whitespace() => Some(Ok(self.whitespace(start))),
            _ => {
                let str = format!("{ch}");
                Some(Ok(spanned(start, start, Token::Bad(str))))
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
mod test;
