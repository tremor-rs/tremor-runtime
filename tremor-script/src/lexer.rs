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

use crate::errors::*;
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
use lalrpop_util;
use std::borrow::Cow;
use std::fmt;
use std::iter::Peekable;
use std::str::Chars;
use unicode_xid::UnicodeXID;

pub use crate::pos::*;
//use crate::str_suffix;

pub trait ParserSource {
    fn src(&self) -> &str;
    fn start_index(&self) -> BytePos;

    fn span(&self) -> Span<BytePos> {
        let start = self.start_index();
        Span::new(
            start,
            start + ByteOffset::from(self.src().chars().count() as i64),
        )
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
        BytePos::from(0)
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
    Ident(Cow<'input, str>, bool),
    Dollar,
    Dot,

    // Literals
    Nil,
    BoolLiteral(bool),
    IntLiteral(i64),
    FloatLiteral(f64, String),
    StringLiteral(Cow<'input, str>),
    TestLiteral(usize, Vec<String>),
    HereDoc(usize, Vec<String>),

    // Keywords
    Let,
    Const,
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
    //Move,
    Merge,
    Drop,
    Default,
    Emit,
    For,
    Event,
    Present,
    Absent,
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
            Token::Const => true,
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
            //Token::Move => true,
            Token::Merge => true,
            Token::For => true,
            Token::Event => true,
            Token::Present => true,
            Token::Absent => true,
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
            Token::FloatLiteral(_, _) => true,
            _ => false,
        }
    }

    // It's text-like or string-like notation such as String, char, regex ...
    fn is_string_like(&self) -> bool {
        match *self {
            Token::StringLiteral(_) => true,
            Token::TestLiteral(_, _) => true,
            Token::HereDoc(_, _) => true,
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
impl<'input> __ToTriple<'input> for Result<Spanned<Token<'input>, Location>> {
    fn to_triple(
        value: Self,
    ) -> std::result::Result<
        (Location, Token<'input>, Location),
        lalrpop_util::ParseError<Location, Token<'input>, Error>,
    > {
        match value {
            Ok(span) => Ok((span.span.start(), span.value, span.span.end())),
            Err(error) => Err(lalrpop_util::ParseError::User { error }),
        }
    }
}

// fn unformat_str(s: &str) -> String {
//     s.replace(r#"\"#, r#"\\"#)
//         .replace('"', r#"\\""#)
//         .replace('\n', "\\n")
//         .replace('\t', "\\t")
// }
// Format a token for display
//
impl<'input> fmt::Display for Token<'input> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Whitespace(ref ws) => write!(f, "{}", ws),
            Token::NewLine => writeln!(f),
            Token::LineToken(n) => write!(f, "{}: ", n),
            Token::Ident(ref name, true) => write!(f, "`{}`", name),
            Token::Ident(ref name, false) => write!(f, "{}", name),
            Token::DocComment(ref comment) => write!(f, "## {}", comment),
            Token::SingleLineComment(ref comment) => write!(f, "# {}", comment),
            Token::IntLiteral(value) => write!(f, "{}", value),
            Token::FloatLiteral(_, txt) => write!(f, "{}", txt),
            Token::StringLiteral(value) => write!(
                f,
                "{}",
                simd_json::OwnedValue::from(value.to_string()).to_string()
            ),
            Token::HereDoc(indent, lines) => {
                writeln!(f, r#"""""#)?;
                let space = String::from(" ");
                for l in lines {
                    writeln!(f, "{}{}", space.repeat(*indent), l)?
                }
                write!(f, r#"""""#)
            }
            Token::TestLiteral(indent, values) => {
                let mut first = true;
                let space = String::from(" ");
                write!(f, "|")?;
                for l in values {
                    if first {
                        write!(f, "|")?;
                        first = false;
                    } else {
                        writeln!(f)?;
                    };
                    writeln!(f, "{}{}", space.repeat(*indent), l)?;
                }
                write!(f, "|")
            }

            Token::BoolLiteral(value) => write!(f, "{}", value),
            Token::BadToken(value) => write!(f, "{}", value),
            Token::Let => write!(f, "let"),
            Token::Const => write!(f, "const"),
            Token::Match => write!(f, "match"),
            Token::Patch => write!(f, "patch"),
            Token::Insert => write!(f, "insert"),
            Token::Upsert => write!(f, "upsert"),
            Token::Update => write!(f, "update"),
            Token::Erase => write!(f, "erase"),
            //Token::Move => write!(f, "move"),
            Token::Merge => write!(f, "merge"),
            Token::For => write!(f, "for"),
            Token::Event => write!(f, "event"),
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
    chars: Peekable<Chars<'input>>,
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
            chars: input.src().chars().peekable(),
        }
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
        }
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

type Lexeme = Option<(Location, char)>;

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
    fn lookahead(&mut self) -> Lexeme {
        let loc = self.chars.location;
        self.chars.chars.peek().map(|b| (loc, *b))
    }

    fn bump(&mut self) -> Lexeme {
        self.chars.next()
    }

    /// Return a slice of the source string
    fn slice(&self, start: Location, end: Location) -> Option<&'input str> {
        let start =
            (start.absolute - ByteOffset::from(self.start_index.to_usize() as i64)).to_usize();
        let end = (end.absolute - ByteOffset::from(self.start_index.to_usize() as i64)).to_usize();

        // Our indexes are in characters as we are iterating over codepoints
        // string indexes however are in bytes and might lead to either
        // invalid regions or even invalid strings
        // That is terrible, what are you thinking rust!
        // But at lease we can work around it using `indices`
        let start = self.input.char_indices().nth(start)?.0;
        let end = self.input.char_indices().nth(end)?.0;
        self.input.get(start..end)
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
        while let Some((end, ch)) = self.lookahead() {
            if terminate(ch) {
                if let Some(slice) = self.slice(start, end) {
                    return (end, slice);
                } else {
                    // Invalid start end case :(
                    return (start, "");
                }
            } else {
                self.bump();
            }
        }

        (start, "")
    }

    fn cx(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, lexeme) = self.take_until(start, |ch| ch == '\n');

        if lexeme.starts_with("##") {
            let doc = Token::DocComment(&lexeme[2..]);
            Ok(spanned2(start, end, doc))
        } else {
            Ok(spanned2(start, end, Token::SingleLineComment(&lexeme[1..])))
        }
    }

    fn eq(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '>')) => {
                self.bump();
                Ok(spanned2(start, end, Token::EqArrow))
            }
            Some((end, '=')) => {
                self.bump();
                Ok(spanned2(start, end, Token::EqEq))
            }
            _ => Ok(spanned2(start, start, Token::Eq)),
        }
    }

    fn cn(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, lexeme) = self.take_while(start, |ch| ch == ':');

        match lexeme {
            "::" => Ok(spanned2(start, end, Token::ColonColon)),
            ":" => Ok(spanned2(start, end, Token::Colon)),
            _ => Ok(spanned2(start, end, Token::BadToken(lexeme.to_string()))),
        }
    }

    fn sb(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, lexeme) = self.take_while(start, |ch| ch == '-');

        match lexeme {
            "-" => Ok(spanned2(start, end, Token::Sub)),
            _ => Ok(spanned2(start, end, Token::BadToken(lexeme.to_string()))),
        }
    }

    fn an(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, lexeme) = self.take_while(start, |ch| {
            ch == '>' || ch == '<' || ch == '=' || is_ident_continue(ch)
        });

        match lexeme {
            "<" => Ok(spanned2(start, end, Token::Lt)),
            "<=" => Ok(spanned2(start, end, Token::Lte)),
            ">" => Ok(spanned2(start, end, Token::Gt)),
            ">=" => Ok(spanned2(start, end, Token::Gte)),
            _ => Ok(spanned2(start, end, Token::BadToken(lexeme.to_string()))),
        }
    }

    fn pb(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '[')) => {
                self.bump();
                Ok(spanned2(start, end, Token::LPatBracket))
            }
            Some((end, '{')) => {
                self.bump();
                Ok(spanned2(start, end, Token::LPatBrace))
            }
            Some((end, _)) => Ok(spanned2(start, end, Token::Mod)),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    fn pe(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '=')) => {
                self.bump();
                Ok(spanned2(start, end, Token::NotEq))
            }
            Some((end, ch)) => Ok(spanned2(start, end, Token::BadToken(format!("!{}", ch)))),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    fn tl(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '=')) => {
                self.bump();
                Ok(spanned2(start, end, Token::TildeEq))
            }
            Some((end, _)) => Ok(spanned2(start, end, Token::Tilde)),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    fn id(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, ident) = self.take_while(start, is_ident_continue);

        let token = match ident {
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
            //"move" => Token::Move,
            "merge" => Token::Merge,
            "for" => Token::For,
            "event" => Token::Event,
            "present" => Token::Present,
            "absent" => Token::Absent,
            "true" => Token::BoolLiteral(true),
            "false" => Token::BoolLiteral(false),
            "and" => Token::And,
            "or" => Token::Or,
            "not" => Token::Not,
            "drop" => Token::Drop,
            "emit" => Token::Emit,
            src => Token::Ident(src.into(), false),
        };

        Ok(spanned2(start, end, token))
    }

    fn next_index(&mut self) -> Result<Location> {
        match self.chars.next() {
            Some((loc, _)) => Ok(loc),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    fn escape_code(&mut self, s: &str, start: Location) -> Result<Option<char>> {
        match self.bump() {
            Some((_, '\'')) => Ok(Some('\'')),
            Some((_, '"')) => Ok(Some('"')),
            Some((_, '\\')) => Ok(Some('\\')),
            Some((_, '/')) => Ok(Some('/')),
            Some((_, 'b')) => Ok(Some('\u{8}')), // Backspace
            Some((_, 'f')) => Ok(Some('\u{c}')), // Form Feed
            Some((_, 'n')) => Ok(Some('\n')),
            Some((_, 'r')) => Ok(Some('\r')),
            Some((_, 't')) => Ok(Some('\t')),
            // TODO: Unicode escape codes
            Some((end, 'u')) => {
                let mut end = end;
                let mut digits = String::new();
                for _i in 0..4 {
                    if let Some((e, c)) = self.bump() {
                        end = e;
                        digits.push(c);
                    } else {
                        return Err(ErrorKind::UnterminatedIdentLiteral(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            format!("\"{}\\u{}\n", s, digits),
                        )
                        .into());
                    }
                }

                if let Ok(Some(c)) = u32::from_str_radix(&digits, 16).map(std::char::from_u32) {
                    Ok(Some(c))
                } else {
                    Err(ErrorKind::UnexpectedEscapeCode(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!("\"{}\\u{}\n", s, digits),
                        'u',
                    )
                    .into())
                }
            }
            Some((end, ch)) => Err(ErrorKind::UnexpectedEscapeCode(
                Range::from((start, end)).expand_lines(2),
                Range::from((start, end)),
                format!("\"{}\\{}\n", s, ch),
                ch,
            )
            .into()),
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
                    s.column.0 += 1;
                    s.absolute.0 += 1;
                    end.column.0 += 1;
                    end.absolute.0 += 1;
                    if let Some(slice) = self.slice(s, e) {
                        let token = Token::Ident(slice.into(), true);
                        return Ok(spanned2(start, end, token));
                    } else {
                        // Invalid start end case :(
                        let token = Token::Ident(string.into(), true);
                        return Ok(spanned2(start, end, token));
                    }
                }
                Some((end, '\n')) => {
                    return Err(ErrorKind::UnterminatedIdentLiteral(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!("`{}\n", string),
                    )
                    .into());
                }

                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => {
                    return Err(ErrorKind::UnterminatedIdentLiteral(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!("`{}", string),
                    )
                    .into())
                }
            }
        }
    }

    fn qs_or_hd(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let mut string = String::new();
        match self.bump() {
            // This would be the second quote
            Some((mut end, '"')) => match self.lookahead() {
                Some((end, '"')) => {
                    self.bump();
                    // We don't allow anything tailing the initial `"""`
                    match self.bump() {
                        Some((_end, '\n')) => self.hd(start),
                        Some((end, ch)) => Err(ErrorKind::TailingHereDoc(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            format!(r#""""{}\n"#, ch),
                            ch,
                        )
                        .into()),
                        None => Err(ErrorKind::UnterminatedHereDoc(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            r#""""\n"#.to_string(),
                        )
                        .into()),
                    }
                }
                // We had two quotes followed by something not a quote so
                // it is an empty string.
                _ => {
                    //TODO :make slice
                    let token = Token::StringLiteral(string.into());
                    end.column.0 += 1;
                    end.absolute.0 += 1;
                    Ok(spanned2(start, end, token))
                }
            },
            Some((end, '\\')) => {
                if let Some(c) = self.escape_code(&string, start)? {
                    string.push(c);
                };
                self.qs(start, end, true, string)
            }
            Some((end, '\n')) => Err(ErrorKind::UnterminatedStringLiteral(
                Range::from((start, end)).expand_lines(2),
                Range::from((start, end)),
                format!("\"{}\n", string),
            )
            .into()),
            Some((end, ch)) => {
                string.push(ch);
                self.qs(start, end, false, string)
            }
            None => Err(ErrorKind::UnterminatedStringLiteral(
                Range::from((start, start)).expand_lines(2),
                Range::from((start, start)),
                format!("\"{}", string),
            )
            .into()),
        }
    }

    fn hd(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let mut string = String::new();
        let mut strings = Vec::new();
        let mut end = start;
        loop {
            match self.bump() {
                Some((e, '\n')) => {
                    end = e;
                    strings.push(string);
                    string = String::new();
                }
                Some((mut end, ch)) => {
                    string.push(ch);
                    // If the current line is just a `"""` then we are at the end of the heardoc
                    if string.trim_start() == r#"""""# {
                        let indent = indentation(&strings);
                        let strings = strings
                            .iter()
                            .map(|s| s.split_at(indent).1.to_string())
                            .collect();
                        let token = Token::HereDoc(indent, strings);
                        end.column.0 += 1;
                        end.absolute.0 += 1;
                        return Ok(spanned2(start, end, token));
                    }
                }
                None => {
                    return Err(ErrorKind::UnterminatedHereDoc(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!(r#""""{}\n"#, strings.join("\n")).to_string(),
                    )
                    .into())
                }
            }
        }
        //let mut strings = Vec::new();
    }
    /// Handle quote strings `"`  ...
    fn qs(
        &mut self,
        start: Location,
        mut end: Location,
        mut has_escapes: bool,
        mut string: String,
    ) -> Result<TokenSpan<'input>> {
        loop {
            match self.bump() {
                Some((e, '\\')) => {
                    has_escapes = true;
                    if let Some(c) = self.escape_code(&string, start)? {
                        string.push(c);
                    };
                    end = e;
                }
                Some((mut end, '"')) => {
                    // we got to bump end by one so we claim the tailing `"`
                    let e = end;
                    let mut s = start;
                    s.column.0 += 1;
                    s.absolute.0 += 1;
                    end.column.0 += 1;
                    end.absolute.0 += 1;
                    let token = if has_escapes {
                        // The string was modified so we can't use the slice
                        Token::StringLiteral(string.into())
                    } else if let Some(slice) = self.slice(s, e) {
                        Token::StringLiteral(slice.into())
                    } else {
                        // Invalid start end case :(
                        Token::StringLiteral(string.into())
                    };

                    return Ok(spanned2(start, end, token));
                }
                Some((end, '\n')) => {
                    return Err(ErrorKind::UnterminatedStringLiteral(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!("\"{}\n", string),
                    )
                    .into());
                }
                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => {
                    return Err(ErrorKind::UnterminatedStringLiteral(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!("\"{}", string),
                    )
                    .into())
                }
            }
        }
    }

    fn test_escape_code(&mut self, s: &str) -> Result<Option<char>> {
        match self.bump() {
            Some((_, '\\')) => Ok(Some('\\')),
            Some((_, '|')) => Ok(Some('|')),
            Some((_end, '\n')) => Ok(None),
            Some((end, ch)) => Err(ErrorKind::UnexpectedEscapeCode(
                Range::from((end, end)).expand_lines(2),
                Range::from((end, end)),
                format!("|{}\\{}\n", s, ch as char),
                ch,
            )
            .into()),
            None => Err(ErrorKind::UnexpectedEndOfStream.into()),
        }
    }

    /// handle escaped
    fn pl(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let mut string = String::new();
        let mut strings = Vec::new();

        let mut end = start;
        loop {
            match self.bump() {
                Some((e, '\\')) => {
                    if let Some(ch) = self.test_escape_code(&string)? {
                        string.push(ch);
                    } else {
                        strings.push(string);
                        string = String::new();
                    }
                    end = e;
                }
                Some((mut end, '|')) => {
                    strings.push(string);
                    end.absolute.0 += 1;
                    end.column.0 += 1;
                    let indent = indentation(&strings);
                    let strings = strings
                        .iter()
                        .map(|s| {
                            if s == "" {
                                s.to_owned()
                            } else {
                                s.split_at(indent).1.to_string()
                            }
                        })
                        .collect();
                    let token = Token::TestLiteral(indent, strings);
                    return Ok(spanned2(start, end, token));
                }
                Some((end, '\n')) => {
                    return Err(ErrorKind::UnterminatedExtractor(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!("|{}\n", string),
                    )
                    .into());
                }
                Some((e, other)) => {
                    string.push(other as char);
                    end = e;
                    continue;
                }
                None => {
                    return Err(ErrorKind::UnterminatedExtractor(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!("|{}\n", string),
                    )
                    .into())
                }
            }
        }
    }

    /*
    // NOTE rs is a quoted string variant that interprets escapes whereas qs above passes them through
    #[allow(unused)]
    fn rs(&mut self, start: Location) -> Result<TokenSpan> {
        let mut delimiters = 0;
        while let Some((end, ch)) = self.bump() {
            match ch {
                '#' => delimiters += 1,
                '"' => break,
                _ => {
                    return Err(ErrorKind::UnterminatedStringLiteral(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                    )
                    .into())
                }
            }
        }

        let content_start = self.next_index()?;
        loop {
            self.take_until(content_start, |b| b == '"');
            match self.bump() {
                Some((_, '"')) => {
                    let mut found_delimiters = 0;
                    while let Some((_, ch)) = self.bump() {
                        match ch {
                            '#' => found_delimiters += 1,
                            '"' => found_delimiters = 0,
                            _ => break,
                        }
                        if found_delimiters == delimiters {
                            let end = self.next_index()?;
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
    */

    fn nm(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, int) = self.take_while(start, is_dec_digit);

        let (start, end, token) = match self.lookahead() {
            Some((_, '.')) => {
                self.bump(); // Skip '.'
                let (end, float) = self.take_while(start, is_dec_digit);
                match self.lookahead() {
                    Some((_, 'e')) => {
                        self.bump();
                        self.bump();
                        let (end, exp) = self.take_while(end, is_dec_digit);
                        let float = &format!("{}{}", float, exp);
                        (
                            start,
                            end,
                            Token::FloatLiteral(
                                float.parse().map_err(|_| {
                                    Error::from(ErrorKind::InvalidFloatLiteral(
                                        Range::from((start, end)).expand_lines(2),
                                        Range::from((start, end)),
                                    ))
                                })?,
                                float.to_string(),
                            ),
                        )
                    }
                    Some((end, ch)) if is_ident_start(ch) => {
                        return Err(ErrorKind::UnexpectedCharacter(
                            Range::from((start, end)).expand_lines(2),
                            Range::from((start, end)),
                            ch,
                        )
                        .into());
                    }
                    _ => (
                        start,
                        end,
                        Token::FloatLiteral(
                            float.parse().map_err(|_| {
                                Error::from(ErrorKind::InvalidFloatLiteral(
                                    Range::from((start, end)).expand_lines(2),
                                    Range::from((start, end)),
                                ))
                            })?,
                            float.to_string(),
                        ),
                    ),
                }
            }
            Some((_, 'x')) => {
                self.bump(); // Skip 'x'
                let int_start = self.next_index()?;
                let (end, hex) = self.take_while(int_start, is_hex);
                match int {
                    "0" | "-0" => match self.lookahead() {
                        Some((_, ch)) if is_ident_start(ch) => {
                            return Err(ErrorKind::UnexpectedCharacter(
                                Range::from((start, end)).expand_lines(2),
                                Range::from((start, end)),
                                ch,
                            )
                            .into());
                        }
                        _ => {
                            if hex.is_empty() {
                                return Err(ErrorKind::InvalidHexLiteral(
                                    Range::from((start, end)).expand_lines(2),
                                    Range::from((start, end)),
                                )
                                .into());
                            }
                            let is_positive = int == "0";
                            match i64_from_hex(hex, is_positive) {
                                Ok(val) => (start, end, Token::IntLiteral(val)),
                                Err(_err) => {
                                    return Err(ErrorKind::InvalidHexLiteral(
                                        Range::from((start, end)).expand_lines(2),
                                        Range::from((start, end)),
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
                        )
                        .into());
                    }
                }
            }
            // Some((_, '')) => {
            //     self.bump(); // Skip ''
            //     let end = self.next_index()?;
            //     match self.lookahead() {
            //         Some((pos, ch)) if is_ident_start(ch) => {
            //             let ch = self.chars.chars.as_str_suffix().restore_char(&[ch]);
            //             // HACK
            //             return Err(ErrorKind::UnterminatedStringLiteral {
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
            //                 return Err(ErrorKind::UnterminatedStringLiteral {
            //                     start: ByteIndex(0),
            //                     end: ByteIndex(0),
            //                 });
            //             }
            //         }
            //     }
            // }
            Some((_, ch)) if is_ident_start(ch) => {
                return Err(ErrorKind::UnexpectedCharacter(
                    Range::from((start, end)).expand_lines(2),
                    Range::from((start, end)),
                    ch,
                )
                .into());
            }
            None | Some(_) => {
                if let Ok(val) = int.parse() {
                    (start, end, Token::IntLiteral(val))
                } else {
                    // return self.error(start, NonParseableInt);
                    return Err(ErrorKind::InvalidIntLiteral(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                    )
                    .into());
                }
            }
        };

        Ok(spanned2(start, end, token))
    }

    /// Consume whitespace
    fn ws(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, src) = self.take_while(start, is_ws);
        Ok(spanned2(start, end, Token::Whitespace(src)))
    }
}

/// Converts partial hex literal (i.e. part after `0x` or `-0x`) to 64 bit signed integer.
///
/// This is basically a copy and adaptation of `std::num::from_str_radix`.
fn i64_from_hex(hex: &str, is_positive: bool) -> Result<i64> {
    let r = i64::from_str_radix(hex, 16)?;

    Ok(if is_positive { r } else { -r })
}

fn inc_loc(mut l: Location) -> Location {
    l.column.0 += 1;
    l.absolute.0 += 1;
    l
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Result<TokenSpan<'input>>;

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
                    ')' => Some(Ok(spanned2(start, inc_loc(start), Token::RParen))),
                    '{' => Some(Ok(spanned2(start, start, Token::LBrace))),
                    '}' => Some(Ok(spanned2(start, inc_loc(start), Token::RBrace))),
                    '[' => Some(Ok(spanned2(start, start, Token::LBracket))),
                    ']' => Some(Ok(spanned2(start, inc_loc(start), Token::RBracket))),
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
                    ch if is_ident_start(ch) => Some(self.id(start)),
                    '"' => Some(self.qs_or_hd(start)),
                    ch if is_test_start(ch) => Some(self.pl(start)),
                    ch if is_dec_digit(ch) => Some(self.nm(start)),
                    ch if ch.is_whitespace() => Some(self.ws(start)),
                    _ => {
                        let str = format!("{}", ch);
                        Some(Ok(spanned2(start, start, Token::BadToken(str))))
                    }
                }
            }
        }
    }
}

fn indentation(strings: &[String]) -> usize {
    let mut indent = None;
    for s in strings {
        if s != "" {
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

    /*
    macro_rules! lex_ko {
        ($src:expr, $span:expr => $error:expr) => {{
            let lexed_tokens: Vec<Result<Token>> = tokenizer($src)
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
    */
    #[test]
    fn paths() {
        lex_ok! {
            "  hello-hahaha8ABC ",
            "  ~~~~~~~~~~~~~~~~ " => Token::Ident("hello-hahaha8ABC".into(), false),
        };
        lex_ok! {
            "  .florp ", "  ~ " => Token::Dot, "   ~~~~~ " => Token::Ident("florp".into(), false),
        }
        lex_ok! {
            "  $borp ", "  ~ " => Token::Dollar, "   ~~~~~ " => Token::Ident("borp".into(), false),
        }
        lex_ok! {
            "  $`borp`", "  ~ " => Token::Dollar, "   ~~~~~~~ " => Token::Ident("borp".into(), true),
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
        lex_ok! { r#" "\n" "#, " ~~~~ " => Token::StringLiteral("\n".into()), }
        lex_ok! { r#" "\r" "#, " ~~~~  " => Token::StringLiteral("\r".into()), }
        lex_ok! { r#" "\t" "#, " ~~~  " => Token::StringLiteral("\t".into()), }
        lex_ok! { r#" "\\" "#, " ~~~~  " => Token::StringLiteral("\\".into()), }
        lex_ok! { r#" "\"" "#, " ~~~~ " => Token::StringLiteral("\"".into()), }
        lex_ok! { r#" "\"\"" "#, " ~~~~~ " => Token::StringLiteral("\"\"".into()), }
        lex_ok! {
            r#" "\"\"""\"\"" "#,
            " ~~~~~ " => Token::StringLiteral("\"\"".into()),
            "       ~~~~~ " => Token::StringLiteral("\"\"".into()),
        }
        lex_ok! { r#" "\\\"" "#, " ~~~~ " => Token::StringLiteral("\\\"".into()), }
        //lex_ko! { r#" "\\\" "#, " ~~~~~ " => ErrorKind::UnterminatedStringLiteral { start: Location::new(1,2,2), end: Location::new(1,7,7) } }
    }
}
