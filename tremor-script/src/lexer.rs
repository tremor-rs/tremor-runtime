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

#[cfg_attr(
    feature = "cargo-clippy",
    allow(clippy::all, clippy::result_unwrap_used, clippy::unnecessary_unwrap)
)]
use crate::parser::grammar::__ToTriple;
use codespan::ByteIndex;
use codespan::ByteOffset;
use codespan::ColumnIndex;
use codespan::LineIndex;
use codespan::Span;
use failure::Fail;
use lalrpop_util;
use std::fmt;
use std::iter::Peekable;
use unicode_xid::UnicodeXID;

pub use crate::pos::*;
use crate::str_suffix;

pub trait ParserSource {
    fn src(&self) -> &str;
    fn start_index(&self) -> BytePos;

    fn span(&self) -> Span<BytePos> {
        let start = self.start_index();
        Span::new(start, start + ByteOffset::from(self.src().len() as i64))
    }
}

impl<'a, S> ParserSource for &'a S
where
    S: ?Sized + ParserSource,
{
    fn src(&self) -> &str {
        (**self).src()
    }
    fn start_index(&self) -> BytePos {
        (**self).start_index()
    }
}

impl ParserSource for str {
    fn src(&self) -> &str {
        self
    }
    fn start_index(&self) -> BytePos {
        BytePos::from(1)
    }
}

impl ParserSource for codespan::FileMap {
    fn src(&self) -> &str {
        codespan::FileMap::src(self)
    }
    fn start_index(&self) -> BytePos {
        codespan::FileMap::span(self).start()
    }
}

pub type TokenSpan<'input> = Spanned<Token<'input>, Location>;

fn is_ws(ch: u8) -> bool {
    let ch = ch as char;
    ch.is_whitespace() && ch != '\n'
}

fn is_ident_start(ch: u8) -> bool {
    let ch = ch as char;
    UnicodeXID::is_xid_start(ch) || ch == '_' || ch == '-'
}

fn is_ident_continue(ch: u8) -> bool {
    let ch = ch as char;
    UnicodeXID::is_xid_continue(ch) || ch == '_' || ch == '-'
}

fn is_string_start(ch: char) -> bool {
    ch == '\"'
}

fn is_test_start(ch: char) -> bool {
    ch == '|'
}

fn is_dec_digit(ch: u8) -> bool {
    let ch = ch as char;
    ch.is_digit(10)
}

fn is_hex(ch: u8) -> bool {
    let ch = ch as char;
    ch.is_digit(16)
}

/// An error that occurred while lexing the source file
#[derive(Fail, Debug, Clone, PartialEq, Eq)]
pub enum LexerError {
    #[fail(
        display = "An unexpected character {:?} was found at location {}.",
        found, start
    )]
    UnexpectedCharacter { start: Location, found: char },

    #[fail(
        display = "An unterminated string literal starting/ending at {}/{}.",
        start, end
    )]
    UnterminatedStringLiteral { start: Location, end: Location },

    #[fail(
        display = "An unterminated ident literal starting/ending at {}/{}.",
        start, end
    )]
    UnterminatedIdentLiteral { start: Location, end: Location },

    #[fail(
        display = "An unexpected escape code {:?} was found at location {}.",
        found, start
    )]
    UnexpectedEscapeCode { start: Location, found: char },

    #[fail(
        display = "An invalid hexadecimal literal at location {}/{}.",
        start, end
    )]
    InvalidHexLiteral { start: Location, end: Location },

    #[fail(
        display = "An invalid integer literal starting at location {}/{}.",
        start, end
    )]
    InvalidIntLiteral { start: Location, end: Location },

    #[fail(display = "An unexpected end of stream was found.")]
    UnexpectedEndOfStream {},

    #[fail(display = "Hex literal underflow.")]
    HexOverFlow {},

    #[fail(display = "Hex literal underflow.")]
    HexUnderFlow {},
}

/// A token in the source ( file, byte stream ), to be emitted by the `Lexer`
/// The LALRPOP grammar uses these tokens and this custom lexer
/// as it does not have a facility to ignore special tokens, to
/// easily disambiguate tokens ( eg: ':' vs '::' vs ':=' ), nor
/// to inject magic tokens ( bad token ) or support lexical streams
/// ( different token streams drive by users, eg: emit ignorable tokens when
/// syntax highlighting or error highlighting
///
#[derive(Clone, Debug, PartialEq)]
pub enum Token<'input> {
    //  Ignorable when parsing, significant when highlighting
    Whitespace(&'input str),
    NewLine,
    SingleLineComment(&'input str),
    DocComment(&'input str),
    BadToken(String), // Mark bad tokens in lexical stream
    LineToken(usize),

    // Path
    Ident(String, bool),
    Dollar,
    Dot,

    // Literals
    Nil,
    BoolLiteral(bool),
    IntLiteral(i64),
    FloatLiteral(f64),
    StringLiteral(String),
    TestLiteral(String),

    // Keywords
    Let,
    Match,
    Case,
    Of,
    When,
    End,
    Patch,
    Insert,
    Upsert,
    Update,
    Erase,
    Merge,
    Drop,
    Default,
    Emit,
    For,
    Event,
    //    Return,
    //   Todo,
    Fun,

    // Symbols
    BSlash,
    Comma,
    //    Pipe,
    //    Tilde,
    //    DotDotDot, // ...
    //    DotDot,    // ..
    //    Question,

    // Wildcards
    // DontCare,

    // Op
    Not,
    And,
    Or,
    Eq,
    EqEq,
    NotEq,
    TildeEq,
    Tilde,
    Gte,
    Gt,
    Lte,
    Lt,
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // Symbols
    Colon,
    ColonColon,
    EqArrow,
    Semi,
    LParen,
    RParen,
    LBrace,
    LPatBrace,
    RBrace,
    LBracket,
    LPatBracket,
    RBracket,

    EndOfStream,
}

pub trait TokenFuns {
    fn is_ignorable(&self) -> bool;
    fn is_keyword(&self) -> bool;
    fn is_literal(&self) -> bool;
    fn is_string_like(&self) -> bool;
    fn is_symbol(&self) -> bool;
    fn is_operator(&self) -> bool;
}

impl<'input> TokenFuns for Token<'input> {
    /// Is the token ignorable except when syntax or error highlighting.
    /// Is the token insignificant when parsing ( a correct ... ) source.
    ///
    fn is_ignorable(&self) -> bool {
        match *self {
            Token::DocComment(_) => true,
            Token::SingleLineComment(_) => true,
            Token::Whitespace(_) => true,
            Token::NewLine => true,
            Token::LineToken(_) => true,
            _ => false,
        }
    }

    /// Is the token a keyword, excluding keyword literals ( eg: true, nil )
    fn is_keyword(&self) -> bool {
        match *self {
            Token::Match => true,
            Token::End => true,
            Token::Fun => true,
            Token::Let => true,
            Token::Case => true,
            Token::Of => true,
            Token::When => true,
            Token::Drop => true,
            Token::Emit => true,
            Token::Default => true,
            Token::Patch => true,
            Token::Insert => true,
            Token::Upsert => true,
            Token::Update => true,
            Token::Erase => true,
            Token::Merge => true,
            Token::For => true,
            Token::Event => true,
            _ => false,
        }
    }

    // Is the token a literal, excluding list and record literals
    fn is_literal(&self) -> bool {
        match *self {
            // Token::DontCare => true,
            Token::Nil => true,
            Token::BoolLiteral(_) => true,
            Token::IntLiteral(_) => true,
            Token::FloatLiteral(_) => true,
            _ => false,
        }
    }

    // It's text-like or string-like notation such as String, char, regex ...
    fn is_string_like(&self) -> bool {
        match *self {
            Token::StringLiteral(_) => true,
            Token::TestLiteral(_) => true,
            _ => false,
        }
    }

    // Is the token a builtin delimiter symbol
    fn is_symbol(&self) -> bool {
        match *self {
            Token::Colon => true,
            Token::ColonColon => true,
            Token::EqArrow => true,
            Token::Semi => true,
            Token::LParen => true,
            Token::RParen => true,
            Token::LPatBrace => true,
            Token::LBrace => true,
            Token::RBrace => true,
            Token::LBracket => true,
            Token::LPatBracket => true,
            Token::RBracket => true,
            Token::BSlash => true,
            Token::Comma => true,
            //            Token::Pipe => true,
            //            Token::Tilde => true,
            //            Token::DotDotDot => true,
            //            Token::DotDot => true,
            //            Token::Question => true,
            _ => false,
        }
    }

    // Is the token a builtin expression operator ( excludes forms such as 'match', 'let'
    fn is_operator(&self) -> bool {
        match *self {
            Token::Not => true,
            Token::Or => true,
            Token::And => true,
            Token::Eq => true,
            Token::EqEq => true,
            Token::NotEq => true,
            Token::TildeEq => true,
            Token::Tilde => true,
            Token::Gte => true,
            Token::Gt => true,
            Token::Lte => true,
            Token::Lt => true,
            Token::Add => true,
            Token::Sub => true,
            Token::Mul => true,
            Token::Div => true,
            Token::Mod => true,
            Token::Dollar => true,
            Token::Dot => true,
            _ => false,
        }
    }
}

// LALRPOP requires a means to convert spanned tokens to triple form
impl<'input> __ToTriple<'input> for Result<Spanned<Token<'input>, Location>, LexerError> {
    fn to_triple(
        value: Self,
    ) -> Result<
        (Location, Token<'input>, Location),
        lalrpop_util::ParseError<Location, Token<'input>, LexerError>,
    > {
        match value {
            Ok(span) => Ok((span.span.start(), span.value, span.span.end())),
            Err(error) => Err(lalrpop_util::ParseError::User { error }),
        }
    }
}

// Format a token for display
//
impl<'input> fmt::Display for Token<'input> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Whitespace(ref ws) => write!(f, "{}", ws),
            Token::NewLine => write!(f, "\n"),
            Token::LineToken(n) => write!(f, "{}: ", n),
            Token::Ident(ref name, true) => write!(f, "`{}`", name),
            Token::Ident(ref name, false) => write!(f, "{}", name),
            Token::DocComment(ref comment) => write!(f, "## {}", comment),
            Token::SingleLineComment(ref comment) => write!(f, "# {}", comment),
            Token::IntLiteral(value) => write!(f, "{}", value),
            Token::FloatLiteral(value) => write!(f, "{}", value),
            Token::StringLiteral(value) => write!(f, "\"{}\"", value),
            Token::TestLiteral(value) => write!(f, "|{}|", value),
            Token::BoolLiteral(value) => write!(f, "{}", value),
            Token::BadToken(value) => write!(f, "\"{}\"", value),
            Token::Let => write!(f, "let"),
            Token::Match => write!(f, "match"),
            Token::Patch => write!(f, "patch"),
            Token::Insert => write!(f, "insert"),
            Token::Upsert => write!(f, "upsert"),
            Token::Update => write!(f, "update"),
            Token::Erase => write!(f, "erase"),
            Token::Merge => write!(f, "merge"),
            Token::For => write!(f, "for"),
            Token::Event => write!(f, "event"),
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
            Token::Not => write!(f, "!"),
            //            Token::Tilde => write!(f, "~"),
            // Token::DontCare => write!(f, "_"),
            Token::EqArrow => write!(f, "=>"),
            Token::Semi => write!(f, ";"),
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
            Token::Gte => write!(f, ">="),
            Token::Eq => write!(f, "="),
            Token::EqEq => write!(f, "=="),
            Token::NotEq => write!(f, "!="),
            Token::TildeEq => write!(f, "~="),
            Token::Tilde => write!(f, "~"),
            Token::Gt => write!(f, ">"),
            Token::Lte => write!(f, "<="),
            Token::Lt => write!(f, "<"),
            Token::Mul => write!(f, "*"),
            Token::Div => write!(f, "/"),
            Token::Add => write!(f, "+"),
            Token::Sub => write!(f, "-"),
            Token::Mod => write!(f, "%"),
            Token::EndOfStream => write!(f, ""),
        }
    }
}

struct CharLocations<'input> {
    location: Location,
    chars: str_suffix::Iter<'input>,
}

impl<'input> CharLocations<'input> {
    pub fn new<S>(input: &'input S) -> CharLocations<'input>
    where
        S: ?Sized + ParserSource,
    {
        CharLocations {
            location: Location {
                line: Line::from(1),
                column: Column::from(1),
                absolute: input.start_index(),
            },
            chars: str_suffix::StrSuffix::new(input.src()).iter(),
        }
    }
}

impl<'input> Iterator for CharLocations<'input> {
    type Item = (Location, u8);

    fn next(&mut self) -> Option<(Location, u8)> {
        self.chars.next().map(|ch| {
            let location = self.location;
            self.location.shift(ch);
            (location, ch)
        })
    }
}

pub struct Tokenizer<'input> {
    eos: bool,
    pos: Span<Location>,
    iter: Peekable<Lexer<'input>>,
    //line: usize,
}

impl<'input> Tokenizer<'input> {
    fn new(input: &'input str) -> Self {
        let lexer = Lexer::new(input);
        let start = Location {
            line: LineIndex(0),
            column: ColumnIndex(0),
            absolute: ByteIndex(0),
        };
        let end = Location {
            line: LineIndex(0),
            column: ColumnIndex(0),
            absolute: ByteIndex(0),
        };
        Tokenizer {
            eos: false,
            iter: lexer.peekable(),
            pos: span(start, end),
            //line: 0,
        }
    }
}

impl<'input> Iterator for Tokenizer<'input> {
    type Item = Result<TokenSpan<'input>, LexerError>;

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

pub fn tokenizer(input: &str) -> Tokenizer {
    Tokenizer::new(input)
}

/// An iterator over a source string that yeilds `Token`s for subsequent use by
/// the parser
pub struct Lexer<'input> {
    input: &'input str,
    chars: CharLocations<'input>,
    start_index: BytePos,
}

type Lexeme = Option<(Location, u8)>;

impl<'input> Lexer<'input> {
    /// Create a new lexer from the source string
    pub fn new<S>(input: &'input S) -> Self
    where
        S: ?Sized + ParserSource,
    {
        let chars = CharLocations::new(input);

        Lexer {
            input: input.src(),
            chars,
            start_index: input.start_index(),
        }
    }

    /// Return the next character in the source string
    fn lookahead(&self) -> Lexeme {
        self.chars
            .chars
            .as_str_suffix()
            .first()
            .map(|b| (self.chars.location, b))
    }

    fn bump(&mut self) -> Lexeme {
        self.chars.next()
    }

    /// Return a slice of the source string
    fn slice(&self, start: Location, end: Location) -> &'input str {
        let start = start.absolute - ByteOffset::from(self.start_index.to_usize() as i64);
        let end = end.absolute - ByteOffset::from(self.start_index.to_usize() as i64);

        &self.input[start.to_usize()..end.to_usize()]
    }

    fn take_while<F>(&mut self, start: Location, mut keep_going: F) -> (Location, &'input str)
    where
        F: FnMut(u8) -> bool,
    {
        self.take_until(start, |c| !keep_going(c))
    }

    fn take_until<F>(&mut self, start: Location, mut terminate: F) -> (Location, &'input str)
    where
        F: FnMut(u8) -> bool,
    {
        while let Some((end, ch)) = self.lookahead() {
            if terminate(ch) {
                return (end, self.slice(start, end));
            } else {
                self.bump();
            }
        }

        (start, "")
    }

    fn cx(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let (end, lexeme) = self.take_until(start, |ch| ch == b'\n');

        if lexeme.starts_with("##") {
            let doc = Token::DocComment(&lexeme[2..]);
            Ok(spanned2(start, end, doc))
        } else {
            Ok(spanned2(start, end, Token::SingleLineComment(&lexeme[1..])))
        }
    }

    fn eq(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        match self.lookahead() {
            Some((end, b'>')) => {
                self.bump();
                Ok(spanned2(start, end, Token::EqArrow))
            }
            Some((end, b'=')) => {
                self.bump();
                Ok(spanned2(start, end, Token::EqEq))
            }
            _ => Ok(spanned2(start, start, Token::Eq)),
        }
    }

    fn cn(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let (end, lexeme) = self.take_while(start, |ch| ch == b':' || ch == b'=');

        match lexeme {
            "::" => Ok(spanned2(start, end, Token::ColonColon)),
            ":" => Ok(spanned2(start, end, Token::Colon)),
            _ => Ok(spanned2(start, end, Token::BadToken(lexeme.to_string()))),
        }
    }

    fn sb(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let (end, lexeme) = self.take_while(start, |ch| ch == b'-' || ch == b'>');

        match lexeme {
            "-" => Ok(spanned2(start, end, Token::Sub)),
            _ => Ok(spanned2(start, end, Token::BadToken(lexeme.to_string()))),
        }
    }

    fn an(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let (end, lexeme) = self.take_while(start, |ch| {
            ch == b'>' || ch == b'<' || ch == b'=' || is_ident_continue(ch)
        });

        match lexeme {
            "<" => Ok(spanned2(start, end, Token::Lt)),
            "<=" => Ok(spanned2(start, end, Token::Lte)),
            ">" => Ok(spanned2(start, end, Token::Gt)),
            ">=" => Ok(spanned2(start, end, Token::Gte)),
            _ => Ok(spanned2(start, end, Token::BadToken(lexeme.to_string()))),
        }
    }

    fn pb(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        match self.lookahead() {
            Some((end, b'[')) => {
                self.bump();
                Ok(spanned2(start, end, Token::LPatBracket))
            }
            Some((end, b'{')) => {
                self.bump();
                Ok(spanned2(start, end, Token::LPatBrace))
            }
            Some((end, _)) => Ok(spanned2(start, end, Token::Mod)),
            None => Err(LexerError::UnexpectedEndOfStream {}),
        }
    }

    fn pe(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        match self.lookahead() {
            Some((end, b'=')) => {
                self.bump();
                Ok(spanned2(start, end, Token::NotEq))
            }
            Some((end, ch)) => Ok(spanned2(start, end, Token::BadToken(format!("!{}", ch)))),
            None => Err(LexerError::UnexpectedEndOfStream {}),
        }
    }

    fn tl(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        match self.lookahead() {
            Some((end, b'=')) => {
                self.bump();
                Ok(spanned2(start, end, Token::TildeEq))
            }
            Some((end, _)) => Ok(spanned2(start, end, Token::Tilde)),
            None => Err(LexerError::UnexpectedEndOfStream {}),
        }
    }

    fn id(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let (end, ident) = self.take_while(start, is_ident_continue);

        let token = match ident {
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
            "merge" => Token::Merge,
            "for" => Token::For,
            "event" => Token::Event,
            "true" => Token::BoolLiteral(true),
            "false" => Token::BoolLiteral(false),
            "and" => Token::And,
            "or" => Token::Or,
            "not" => Token::Not,
            "drop" => Token::Drop,
            "emit" => Token::Emit,
            src => Token::Ident(src.to_string(), false),
        };

        Ok(spanned2(start, end, token))
    }

    fn next_index(&mut self) -> Result<Location, LexerError> {
        match self.chars.next() {
            Some((loc, _)) => Ok(loc),
            None => Err(LexerError::UnexpectedEndOfStream {}),
        }
    }

    fn escape_code(&mut self) -> Result<u8, LexerError> {
        match self.bump() {
            Some((_, b'\'')) => Ok(b'\''),
            Some((_, b'"')) => Ok(b'"'),
            Some((_, b'\\')) => Ok(b'\\'),
            Some((_, b'/')) => Ok(b'/'),
            Some((_, b'n')) => Ok(b'\n'),
            Some((_, b'r')) => Ok(b'\r'),
            Some((_, b't')) => Ok(b'\t'),
            // TODO: Unicode escape codes
            Some((end, ch)) => {
                let ch = self.chars.chars.as_str_suffix().restore_char(&[ch]);
                Err(LexerError::UnexpectedEscapeCode {
                    start: end,
                    found: ch,
                })
            }
            None => Err(LexerError::UnexpectedEndOfStream {}),
        }
    }

    fn id2(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let mut string = String::new();
        let mut end = start;

        loop {
            match self.bump() {
                Some((end, b'`')) => {
                    let token = Token::Ident(string, true);
                    return Ok(spanned2(start, end, token));
                }
                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => return Err(LexerError::UnterminatedIdentLiteral { start, end }),
            }
        }
    }

    fn qs(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let mut string = String::new();
        let mut end = start;
        loop {
            match self.bump() {
                Some((e, b'\\')) => {
                    string.push(self.escape_code()? as char);
                    end = e;
                }
                Some((mut end, b'"')) => {
                    let token = Token::StringLiteral(string);
                    end.column.0 += 1; // we got to bump end by one so we claim the tailing `"`
                    return Ok(spanned2(start, end, token));
                }
                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => return Err(LexerError::UnterminatedStringLiteral { start, end }),
            }
        }
    }

    fn test_escape_code(&mut self) -> Result<u8, LexerError> {
        match self.bump() {
            Some((_, b'\\')) => Ok(b'\\'),
            Some((_, b'|')) => Ok(b'|'),
            Some((_end, ch)) => Ok(ch),
            None => Err(LexerError::UnexpectedEndOfStream {}),
        }
    }

    fn pl(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let mut string = String::new();

        let mut end = start;
        loop {
            match self.bump() {
                Some((e, b'\\')) => {
                    string.push('\\');
                    string.push(self.test_escape_code()? as char);
                    end = e;
                }
                Some((end, b'|')) => {
                    let token = Token::TestLiteral(string);
                    return Ok(spanned2(start, end, token));
                }
                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => return Err(LexerError::UnterminatedStringLiteral { start, end }),
            }
        }
    }

    // NOTE rs is a quoted string variant that interprets escapes whereas qs above passes them through
    #[allow(unused)]
    fn rs(&mut self, start: Location) -> Result<TokenSpan, LexerError> {
        let mut delimiters = 0;
        while let Some((end, ch)) = self.bump() {
            match ch {
                b'#' => delimiters += 1,
                b'"' => break,
                _ => return Err(LexerError::UnterminatedStringLiteral { start, end }),
            }
        }

        let content_start = self.next_index().expect("");
        loop {
            self.take_until(content_start, |b| b == b'"');
            match self.bump() {
                Some((_, b'"')) => {
                    let mut found_delimiters = 0;
                    while let Some((_, ch)) = self.bump() {
                        match ch {
                            b'#' => found_delimiters += 1,
                            b'"' => found_delimiters = 0,
                            _ => break,
                        }
                        if found_delimiters == delimiters {
                            let end = self.next_index().expect("");
                            let mut content_end = end;
                            content_end.absolute.0 -= delimiters + 1;
                            let string = self.slice(content_start, content_end).into();

                            let token = Token::StringLiteral(string);
                            return Ok(spanned2(start, end, token));
                        }
                    }
                }
                _ => continue,
            }
        }
    }

    fn nm(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let (end, int) = self.take_while(start, is_dec_digit);

        let (start, end, token) = match self.lookahead() {
            Some((_, b'.')) => {
                self.bump(); // Skip b'.'
                let (end, float) = self.take_while(start, is_dec_digit);
                match self.lookahead() {
                    Some((_, b'e')) => {
                        self.bump();
                        self.bump();
                        let (end, exp) = self.take_while(end, is_dec_digit);
                        let float = &format!("{}{}", float, exp);
                        (
                            start,
                            end,
                            Token::FloatLiteral(float.parse().expect("unable to parse float")),
                        )
                    }
                    Some((end, ch)) if is_ident_start(ch) => {
                        let ch = self.chars.chars.as_str_suffix().restore_char(&[ch]);
                        return Err(LexerError::UnexpectedCharacter {
                            start: end,
                            found: ch,
                        });
                    }
                    _ => (
                        start,
                        end,
                        Token::FloatLiteral(float.parse().expect("unable to parse float")),
                    ),
                }
            }
            Some((_, b'x')) => {
                self.bump(); // Skip b'x'
                let int_start = self.next_index()?;
                let (end, hex) = self.take_while(int_start, is_hex);
                match int {
                    "0" | "-0" => match self.lookahead() {
                        Some((_, ch)) if is_ident_start(ch) => {
                            let ch = self.chars.chars.as_str_suffix().restore_char(&[ch]);
                            return Err(LexerError::UnexpectedCharacter {
                                start: end,
                                found: ch,
                            });
                        }
                        _ => {
                            if hex.is_empty() {
                                return Err(LexerError::InvalidHexLiteral { start, end });
                            }
                            let is_positive = int == "0";
                            match i64_from_hex(hex, is_positive) {
                                Ok(val) => (start, end, Token::IntLiteral(val)),
                                Err(_err) => {
                                    return Err(LexerError::InvalidHexLiteral { start, end });
                                }
                            }
                        }
                    },
                    _ => {
                        return Err(LexerError::InvalidHexLiteral { start, end });
                    }
                }
            }
            // Some((_, b'b')) => {
            //     self.bump(); // Skip b'b'
            //     let end = self.next_index()?;
            //     match self.lookahead() {
            //         Some((pos, ch)) if is_ident_start(ch) => {
            //             let ch = self.chars.chars.as_str_suffix().restore_char(&[ch]);
            //             // HACK
            //             return Err(LexerError::UnterminatedStringLiteral {
            //                 start: ByteIndex(0),
            //                 end: ByteIndex(0),
            //             });
            //             //                        return self.error(pos, UnexpectedChar(ch));
            //         }
            //         _ => {
            //             if let Ok(val) = int.parse() {
            //                 (start, end, Token::ByteLiteral(val))
            //             } else {
            //                 //                            return self.error(start, NonParseableInt);
            //                 // HACK
            //                 return Err(LexerError::UnterminatedStringLiteral {
            //                     start: ByteIndex(0),
            //                     end: ByteIndex(0),
            //                 });
            //             }
            //         }
            //     }
            // }
            Some((_, ch)) if is_ident_start(ch) => {
                let ch = self.chars.chars.as_str_suffix().restore_char(&[ch]);
                return Err(LexerError::UnexpectedCharacter {
                    start: end,
                    found: ch,
                });
            }
            None | Some(_) => {
                if let Ok(val) = int.parse() {
                    (start, end, Token::IntLiteral(val))
                } else {
                    // return self.error(start, NonParseableInt);
                    return Err(LexerError::InvalidIntLiteral { start, end });
                }
            }
        };

        Ok(spanned2(start, end, token))
    }

    /// Consume whitespace
    fn ws(&mut self, start: Location) -> Result<TokenSpan<'input>, LexerError> {
        let (end, src) = self.take_while(start, is_ws);
        Ok(spanned2(start, end, Token::Whitespace(src)))
    }
}

/// Converts partial hex literal (i.e. part after `0x` or `-0x`) to 64 bit signed integer.
///
/// This is basically a copy and adaptation of `std::num::from_str_radix`.
fn i64_from_hex(hex: &str, is_positive: bool) -> Result<i64, LexerError> {
    const RADIX: u32 = 16;
    let digits = hex.as_bytes();
    let sign: i64 = if is_positive { 1 } else { -1 };
    let mut result = 0i64;
    for &c in digits {
        let x = (c as char).to_digit(RADIX).expect("valid hex literal");
        let _y: i64 = result
            .checked_mul(i64::from(RADIX))
            .and_then(|result| result.checked_add(i64::from(x) * sign))
            .ok_or_else(|| {
                if is_positive {
                    LexerError::HexOverFlow {}
                } else {
                    LexerError::HexUnderFlow {}
                }
            })?;
        result = result;
    }
    Ok(result)
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Result<TokenSpan<'input>, LexerError>;

    fn next(&mut self) -> Option<Self::Item> {
        let lexeme = self.bump();
        match lexeme {
            None => None,
            Some((start, ch)) => {
                match ch as char {
                    // '...' =>  Some(Ok(spanned2(start, self.next_index(), Token::DotDotDot))),
                    // ".." =>  Some(Ok(spanned2(start, self.next_index(), Token::DotDot))),
                    ',' => Some(Ok(spanned2(start, start, Token::Comma))),
                    '$' => Some(Ok(spanned2(start, start, Token::Dollar))),
                    '.' => Some(Ok(spanned2(start, start, Token::Dot))),
                    //                        '?' => Some(Ok(spanned2(start, start, Token::Question))),
                    //'_' => Some(Ok(spanned2(start, start, Token::DontCare))),
                    ';' => Some(Ok(spanned2(start, start, Token::Semi))),
                    '+' => Some(Ok(spanned2(start, start, Token::Add))),
                    '*' => Some(Ok(spanned2(start, start, Token::Mul))),
                    '\\' => Some(Ok(spanned2(start, start, Token::BSlash))),
                    '(' => Some(Ok(spanned2(start, start, Token::LParen))),
                    ')' => Some(Ok(spanned2(start, start, Token::RParen))),
                    '{' => Some(Ok(spanned2(start, start, Token::LBrace))),
                    '}' => Some(Ok(spanned2(start, start, Token::RBrace))),
                    '[' => Some(Ok(spanned2(start, start, Token::LBracket))),
                    ']' => Some(Ok(spanned2(start, start, Token::RBracket))),
                    '/' => Some(Ok(spanned2(start, start, Token::Div))),
                    ':' => Some(self.cn(start)),
                    '-' => Some(self.sb(start)),
                    '#' => Some(self.cx(start)),
                    '=' => Some(self.eq(start)),
                    '<' => Some(self.an(start)),
                    '>' => Some(self.an(start)),
                    '%' => Some(self.pb(start)),
                    '~' => Some(self.tl(start)),
                    '`' => Some(self.id2(start)),
                    '!' => Some(self.pe(start)),
                    '\n' => Some(Ok(spanned2(start, start, Token::NewLine))),
                    ch if is_ident_start(ch as u8) => Some(self.id(start)),
                    ch if is_string_start(ch) => Some(self.qs(start)),
                    ch if is_test_start(ch) => Some(self.pl(start)),
                    ch if is_dec_digit(ch as u8) => Some(self.nm(start)),
                    ch if ch.is_whitespace() => Some(self.ws(start)),
                    _ => {
                        let str = format!("{}", ch as char);
                        Some(Ok(spanned2(start, start, Token::BadToken(str))))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! lex_ok {
        ($src:expr, $($span:expr => $token:expr,)*) => {{
            let lexed_tokens: Vec<_> = tokenizer($src).filter(|t| match t {
                Ok(Spanned { value: Token::EndOfStream, .. }) => false,
                Ok(Spanned { value: t, .. }) => !t.is_ignorable(),
                Err(_) => true
            }).map(|t| match t {
                Ok(Spanned { value: t, .. }) => Ok(t),
                Err(e) => Err(e)
            }).collect();
            let expected_tokens = vec![$({
                Ok($token)
            }),*];

            assert_eq!(lexed_tokens, expected_tokens);
        }};
    }

    macro_rules! lex_ko {
        ($src:expr, $span:expr => $error:expr) => {{
            let lexed_tokens: Vec<Result<Token, LexerError>> = tokenizer($src)
                .filter(|t| match t {
                    Ok(Spanned { .. }) => false,
                    Err(_) => true,
                })
                .map(|t| match t {
                    Ok(Spanned { value: t, .. }) => Ok(t),
                    Err(e) => Err(e),
                })
                .collect();

            assert_eq!(lexed_tokens, vec![Err($error)]);
        }};
    }

    #[test]
    fn paths() {
        lex_ok! {
            "  hello-hahaha8ABC ",
            "  ~~~~~~~~~~~~~~~~ " => Token::Ident("hello-hahaha8ABC".to_string(), false),
        };
        lex_ok! {
            "  .florp ", "  ~ " => Token::Dot, "   ~~~~~ " => Token::Ident("florp".to_string(), false),
        }
        lex_ok! {
            "  $borp ", "  ~ " => Token::Dollar, "   ~~~~~ " => Token::Ident("borp".to_string(), false),
        }
        lex_ok! {
            "  $`borp`", "  ~ " => Token::Dollar, "   ~~~~~~~ " => Token::Ident("borp".to_string(), true),
        }
    }

    #[test]
    fn ignorable() {}

    #[test]
    fn keywords() {
        lex_ok! { " let ", " ~~~~~~ " => Token::Let, };
        lex_ok! { " match ", " ~~~~~~ " => Token::Match, };
        lex_ok! { " case ", " ~~~~~~ " => Token::Case, };
        lex_ok! { " of ", " ~~~~~~ " => Token::Of, };
        lex_ok! { " end ", " ~~~~~~ " => Token::End, };
        lex_ok! { " drop ", " ~~~~~~ " => Token::Drop, };
        lex_ok! { " emit ", " ~~~~~~ " => Token::Emit, };
        lex_ok! { " event ", " ~~~~~~ " => Token::Event, };
    }

    #[test]
    fn operators() {
        lex_ok! { " not null ", "  ~ " => Token::Not, "  ~ " => Token::Nil, };
        lex_ok! { " != null ", "  ~~ " => Token::NotEq, "   ~ " => Token::Nil, };

        lex_ok! { " and ", " ~ " => Token::And, };
        lex_ok! { " or ", " ~ " => Token::Or, };
        lex_ok! { " = ", " ~ " => Token::Eq, };
        lex_ok! { " == ", " ~ " => Token::EqEq, };
        lex_ok! { " != ", " ~ " => Token::NotEq, };
        lex_ok! { " >= ", " ~ " => Token::Gte, };
        lex_ok! { " > ", " ~ " => Token::Gt, };
        lex_ok! { " <= ", " ~ " => Token::Lte, };
        lex_ok! { " < ", " ~ " => Token::Lt, };
        lex_ok! { " + ", " ~ " => Token::Add, };
        lex_ok! { " - ", " ~ " => Token::Sub, };
        lex_ok! { " * ", " ~ " => Token::Mul, };
        lex_ok! { " / ", " ~ " => Token::Div, };
        lex_ok! { " % ", " ~ " => Token::Mod, };
    }

    #[test]
    fn should_disambiguate() {
        // Starts with ':'
        lex_ok! { " : ", " ~ " => Token::Colon, };
        lex_ok! { " :: ", " ~~ " => Token::ColonColon, };

        // Starts with '-'
        lex_ok! { " - ", " ~ " => Token::Sub, };

        // Starts with '='
        lex_ok! { " = ", " ~ " => Token::Eq, };
        lex_ok! { " == ", " ~~ " => Token::EqEq, };
        lex_ok! { " => ", " ~~ " => Token::EqArrow, };

        // Starts with '%'
        lex_ok! { " % ", " ~ " => Token::Mod, }
        lex_ok! { " %{ ", " ~~ " => Token::LPatBrace, }

        // Starts with '.'
        lex_ok! { " . ", " ~ " => Token::Dot, }
    }

    #[test]
    fn delimiters() {
        lex_ok! {
                    " ( ) { } [ ] ",
                    " ~           " => Token::LParen,
                    "   ~         " => Token::RParen,
                    "     ~       " => Token::LBrace,
                    "       ~     " => Token::RBrace,
                    "         ~   " => Token::LBracket,
                    "           ~ " => Token::RBracket,
        }
    }

    #[test]
    fn string() {
        lex_ok! { r#" "\n" "#, " ~~~~ " => Token::StringLiteral("\n".to_string()), }
        lex_ok! { r#" "\r" "#, " ~~~~  " => Token::StringLiteral("\r".to_string()), }
        lex_ok! { r#" "\t" "#, " ~~~  " => Token::StringLiteral("\t".to_string()), }
        lex_ok! { r#" "\\" "#, " ~~~~  " => Token::StringLiteral("\\".to_string()), }
        lex_ok! { r#" "\"" "#, " ~~~~ " => Token::StringLiteral("\"".to_string()), }
        lex_ok! { r#" "\"\"" "#, " ~~~~~ " => Token::StringLiteral("\"\"".to_string()), }
        lex_ok! {
            r#" "\"\"""\"\"" "#,
            " ~~~~~ " => Token::StringLiteral("\"\"".to_string()),
            "       ~~~~~ " => Token::StringLiteral("\"\"".to_string()),
        }
        lex_ok! { r#" "\\\"" "#, " ~~~~ " => Token::StringLiteral("\\\"".to_string()), }
        lex_ko! { r#" "\\\" "#, " ~~~~~ " => LexerError::UnterminatedStringLiteral { start: Location::new(1,2,2), end: Location::new(1,7,7) } }
    }
}
