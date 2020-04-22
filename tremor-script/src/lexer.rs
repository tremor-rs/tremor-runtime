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

// Note: We ignore the is_* functions for coverage as they effectively are
// only lists

use crate::errors::*;
#[cfg_attr(
    feature = "cargo-clippy",
    allow(clippy::all, clippy::result_unwrap_used, clippy::unnecessary_unwrap)
)]
use crate::parser::g::__ToTriple;
use crate::path::ModulePath;
pub use crate::pos::*;
use lalrpop_util;
use simd_json::prelude::*;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::iter::Peekable;
use std::str::Chars;
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
    /// an heredoc
    HereDoc(usize, Vec<String>),

    /// a double quote `"`
    DQuote,
    /// a string literal
    StringLiteral(Cow<'input, str>),

    // Keywords
    /// the `let` keywrod
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
    /// the `recure` token
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
    BitOr,
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
    Order,
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
    LineDirective(Cow<'input, str>),
}

impl<'input> Token<'input> {
    /// Is the token ignorable except when syntax or error highlighting.
    /// Is the token insignificant when parsing ( a correct ... ) source.
    #[cfg_attr(tarpaulin, skip)]
    pub(crate) fn is_ignorable(&self) -> bool {
        match *self {
            Token::SingleLineComment(_) | Token::Whitespace(_) | Token::NewLine => true,
            _ => false,
        }
    }

    /// Is the token a keyword, excluding keyword literals ( eg: true, nil )
    #[cfg_attr(tarpaulin, skip)]
    pub(crate) fn is_keyword(&self) -> bool {
        match *self {
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
            | Token::Order
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
            | Token::With => true,
            _ => false,
        }
    }

    /// Is the token a literal, excluding list and record literals
    #[cfg_attr(tarpaulin, skip)]
    pub(crate) fn is_literal(&self) -> bool {
        match *self {
            Token::DontCare
            | Token::Nil
            | Token::BoolLiteral(_)
            | Token::IntLiteral(_)
            | Token::FloatLiteral(_, _) => true,
            _ => false,
        }
    }

    // It's text-like or string-like notation such as String, char, regex ...
    #[cfg_attr(tarpaulin, skip)]
    pub(crate) fn is_string_like(&self) -> bool {
        match *self {
            Token::StringLiteral(_)
            | Token::DQuote
            | Token::TestLiteral(_, _)
            | Token::HereDoc(_, _) => true,
            _ => false,
        }
    }

    /// Is the token a builtin delimiter symbol
    #[cfg_attr(tarpaulin, skip)]
    pub(crate) fn is_symbol(&self) -> bool {
        match *self {
            Token::LineDirective(_) => true,
            Token::Colon
            | Token::ColonColon
            | Token::EqArrow
            | Token::Semi
            | Token::LParen
            | Token::RParen
            | Token::LPatBrace
            | Token::LBrace
            | Token::RBrace
            | Token::LBracket
            | Token::LPatBracket
            | Token::RBracket
            | Token::BSlash
            | Token::Comma => true,
            _ => false,
        }
    }

    /// Is the token a builtin expression operator ( excludes forms such as 'match', 'let'
    #[cfg_attr(tarpaulin, skip)]
    pub(crate) fn is_operator(&self) -> bool {
        match *self {
            Token::Not
            | Token::BitNot
            | Token::Or
            | Token::Xor
            | Token::And
            | Token::BitOr
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
            | Token::Dot => true,
            _ => false,
        }
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
#[cfg_attr(tarpaulin, skip)]
impl<'input> fmt::Display for Token<'input> {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Whitespace(ref ws) => write!(f, "{}", ws),
            Token::NewLine => writeln!(f),
            Token::Ident(ref name, true) => write!(f, "`{}`", name),
            Token::Ident(ref name, false) => write!(f, "{}", name),
            Token::ModComment(ref comment) => write!(f, "##! {}", comment),
            Token::DocComment(ref comment) => write!(f, "## {}", comment),
            Token::SingleLineComment(ref comment) => write!(f, "# {}", comment),
            Token::IntLiteral(value) => write!(f, "{}", value),
            Token::FloatLiteral(_, txt) => write!(f, "{}", txt),
            Token::DQuote => write!(f, "\""),
            Token::StringLiteral(value) => {
                // We do thos to ensure proper escaping
                let s = simd_json::BorrowedValue::String(value.clone()).encode();
                // Strip the quotes
                write!(f, "{}", &s[1..s.len() - 1])
            }
            Token::HereDoc(indent, lines) => {
                writeln!(f, r#"""""#)?;
                for l in lines {
                    writeln!(f, "{}{}", " ".repeat(*indent), l)?
                }
                write!(f, r#"""""#)
            }
            Token::TestLiteral(indent, values) => {
                let mut first = true;
                write!(f, "|")?;
                if values.len() == 1 {
                    write!(f, "{}", values[0])?;
                } else {
                    for l in values {
                        if first {
                            first = false;
                        } else {
                            write!(f, "\\\n{}", " ".repeat(*indent))?;
                        };
                        write!(f, "{}", l)?;
                    }
                }
                write!(f, "|")
            }
            Token::BoolLiteral(value) => write!(f, "{}", value),
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
            Token::BitOr => write!(f, "|"),
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
            Token::Order => write!(f, "order"),
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

            Token::LineDirective(s) => write!(f, "#!line {}", s),
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
            location: Location {
                line: 1,
                column: 1,
                absolute: input.start_index().to_usize(),
                unit_id: 0, // FIXME cpp
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

/// A Tremor tokeniser
pub struct Tokenizer<'input> {
    eos: bool,
    pos: Span,
    iter: Peekable<Lexer<'input>>,
}

impl<'input> Tokenizer<'input> {
    /// Creates a new tokeniser
    pub fn new(input: &'input str) -> Self {
        let lexer = Lexer::new(input);
        let start = Location::default();
        let end = Location::default();
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

/// A lexical preprocessor in the spirit of cpp
///
pub struct Preprocessor {}

rental! {
    mod rentals {
        use super::*;
        #[rental_mut(covariant,debug)]
        pub(crate) struct Preprocessor {
            pre: Box<(String, Vec<String>)>,
            parsed: Vec<Result<TokenSpan<'pre>>>,
        }
    }
}

macro_rules! take_while {
    ($let:ident, $token:pat, $iter:expr) => {
        $let = $iter.next();
        loop {
            if let Some(Ok(Spanned { value: $token, .. })) = $let {
                $let = $iter.next();
                continue;
            } else {
                break;
            }
        }
    };
}

impl<'input> Preprocessor {
    pub(crate) fn resolve(module_path: &ModulePath, rel_module_path: &str) -> Option<String> {
        use std::path::Path;
        let trickle = &format!("{}.trickle", rel_module_path);
        let tremor = &format!("{}.tremor", rel_module_path);
        // FIXME consider raw JSON: let json = Path::new(format!("{}.json", rel_module_path));

        let test = module_path.resolve(tremor.trim());
        if let Some(path) = test {
            if Path::new(&path).is_file() {
                return Some(path);
            }
        }

        let test = module_path.resolve(trickle.trim());
        if let Some(path) = test {
            if Path::new(&path).is_file() {
                return Some(path);
            }
        }

        return None;
    }

    pub(crate) fn preprocess(
        module_path: &ModulePath,
        file_name: std::string::String,
        input: &'input mut std::string::String,
    ) -> Result<Vec<Result<TokenSpan<'input>>>> {
        use std::fs;
        use std::io::Read;

        input.push_str(" ");

        let top = input.clone();
        let lexemes: Vec<Result<TokenSpan>> = Tokenizer::new(top.as_str()).collect();
        input.clear();

        let mut iter = lexemes.into_iter();

        let mut next;
        loop {
            next = iter.next();
            match next {
                //
                // use <module_path> [as <alias>] ;
                //
                Some(Ok(Spanned {
                    value: Token::Use,
                    span: use_span,
                    ..
                })) => {
                    let mut rel_module_path = String::new();
                    let mut alias = String::from("<NOMODULE>");
                    take_while!(next, Token::Whitespace(_), iter);

                    if let Some(Ok(Spanned {
                        value: Token::Ident(ref id, _),
                        ..
                    })) = &next
                    {
                        rel_module_path.push_str(&id);
                        alias = id.to_string();

                        take_while!(next, Token::Whitespace(_), iter);

                        loop {
                            if let Some(Ok(Spanned {
                                value: Token::ColonColon,
                                ..
                            })) = next
                            {
                                // module path of the form:
                                // <ident> [ :: <ident> ]*;
                                //
                                take_while!(next, Token::Whitespace(_), iter);

                                if let Some(Ok(Spanned { ref value, .. })) = next {
                                    if let Token::Ident(id, ..) = &value {
                                        rel_module_path.push('/');
                                        rel_module_path.push_str(&format!("{}", &id));
                                        alias = id.to_string();
                                        take_while!(next, Token::Whitespace(_), iter);

                                        if let Some(Ok(Spanned {
                                            ref value, span, ..
                                        })) = next
                                        {
                                            match value {
                                                Token::As => (),
                                                Token::ColonColon => continue,
                                                Token::Semi => break,
                                                bad_token => {
                                                    return Err(ErrorKind::UnrecognizedToken(
                                                        Range::from((span.start, span.end))
                                                            .expand_lines(2),
                                                        Range::from((span.start, span.end)),
                                                        bad_token.to_string(),
                                                        vec![
                                                            "`as`".to_string(),
                                                            "`::`".to_string(),
                                                            "`;`".to_string(),
                                                        ],
                                                    )
                                                    .into());
                                                }
                                            }
                                        }
                                    } else if let Some(Ok(Spanned {
                                        value: Token::As, ..
                                    })) = next
                                    {
                                        if let Some(Ok(Spanned {
                                            value: bad_token,
                                            span,
                                            ..
                                        })) = next
                                        {
                                            return Err(ErrorKind::UnrecognizedToken(
                                                Range::from((span.start, span.end)).expand_lines(2),
                                                Range::from((span.start, span.end)),
                                                bad_token.to_string(),
                                                vec!["`<ident>`".to_string()],
                                            )
                                            .into());
                                        }
                                    } else if let Some(Ok(Spanned {
                                        value: Token::Semi, ..
                                    })) = next
                                    {
                                        break;
                                    } else {
                                        // We expect either ';' or 'as'
                                        // At this point we have a parse error / bad token
                                        //
                                        match next {
                                            Some(Ok(Spanned { value, span, .. })) => {
                                                let bad_token = value;
                                                return Err(ErrorKind::UnrecognizedToken(
                                                    Range::from((span.start, span.end))
                                                        .expand_lines(2),
                                                    Range::from((span.start, span.end)),
                                                    bad_token.to_string(),
                                                    vec!["`<ident>`".to_string()],
                                                )
                                                .into());
                                            }
                                            Some(Err(e)) => {
                                                return Err(e);
                                            }
                                            None => {
                                                return Err(ErrorKind::UnexpectedEndOfStream.into());
                                            }
                                        }
                                    }
                                } else if let Some(Ok(Spanned { span, value })) = next {
                                    let bad_token = value;
                                    return Err(ErrorKind::UnrecognizedToken(
                                        Range::from((span.start, span.end)).expand_lines(2),
                                        Range::from((span.start, span.end)),
                                        bad_token.to_string(),
                                        vec!["<ident>".to_string()],
                                    )
                                    .into());
                                } else if let Some(Err(e)) = next {
                                    // Capture lexical errors from the initial lexing phase and bubble back directly
                                    //
                                    return Err(e);
                                }
                            } else if let Some(Ok(Spanned {
                                value: Token::As, ..
                            })) = next
                            {
                                break;
                            } else if let Some(Ok(Spanned {
                                value: Token::Semi, ..
                            })) = next
                            {
                                break;
                            } else {
                                if let Some(Ok(Spanned {
                                    value: bad_token,
                                    span,
                                    ..
                                })) = next
                                {
                                    return Err(ErrorKind::UnrecognizedToken(
                                        Range::from((span.start, span.end)).expand_lines(2),
                                        Range::from((span.start, span.end)),
                                        bad_token.to_string(),
                                        vec!["::".to_string(), "as".to_string(), ";".to_string()],
                                    )
                                    .into());
                                }
                            }
                        }

                        //
                        // As <alias:ident>
                        //

                        if let Some(Ok(Spanned {
                            value: Token::As, ..
                        })) = next
                        {
                            take_while!(next, Token::Whitespace(_), iter);
                            if let Some(Ok(Spanned {
                                value: Token::Ident(alias_id, ..),
                                ..
                            })) = &next
                            {
                                alias = alias_id.to_string();
                            } else {
                                if let Some(Ok(Spanned {
                                    value: bad_token,
                                    span,
                                    ..
                                })) = next
                                {
                                    return Err(ErrorKind::UnrecognizedToken(
                                        Range::from((span.start, span.end)).expand_lines(2),
                                        Range::from((span.start, span.end)),
                                        bad_token.to_string(),
                                        vec!["`<ident>`".to_string()],
                                    )
                                    .into());
                                }
                            }

                            // ';'
                            take_while!(next, Token::Whitespace(_), iter);

                            // Semi
                            take_while!(next, Token::Whitespace(_), iter);
                        }
                    } else {
                        if let Some(Ok(Spanned {
                            ref value, span, ..
                        })) = next
                        {
                            return Err(ErrorKind::UnrecognizedToken(
                                Range::from((span.start, span.end)).expand_lines(2),
                                Range::from((span.start, span.end)),
                                value.to_string(),
                                vec!["`<ident>`".to_string()],
                            )
                            .into());
                        }
                    }
                    if let Some(Ok(Spanned {
                        value: Token::Semi, ..
                    })) = next
                    {
                        take_while!(next, Token::Whitespace(_), iter);
                    }

                    if let Some(Ok(Spanned {
                        value: Token::NewLine,
                        span: span2,
                        ..
                    })) = next
                    {
                        let file_path = &rel_module_path;
                        match Preprocessor::resolve(module_path, &rel_module_path) {
                            Some(file_path) => {
                                let file_path2 = file_path.clone();

                                match fs::File::open(&file_path) {
                                    Ok(mut file) => {
                                        let mut s = String::new();
                                        match file.read_to_string(&mut s) {
                                            _size => {
                                                s.push(' ');
                                                let s = Preprocessor::preprocess(
                                                    &module_path,
                                                    file_path,
                                                    &mut s,
                                                )?;
                                                let y = s
                                                    .into_iter()
                                                    .filter_map(&Result::ok)
                                                    .map(|ref x| format!("{}", x.value))
                                                    .collect::<Vec<String>>()
                                                    .join("");
                                                input.push_str(&format!(
                                                    "#!line {} 0\n",
                                                    &file_path2
                                                ));
                                                input.push_str(&format!("mod {} with ", &alias));
                                                input.push_str(&format!(
                                                    "#!line {} 0\n",
                                                    &file_path2
                                                ));
                                                input.push_str(&format!("{}\n", y.trim()));
                                                input.push_str("end;\n");
                                                input.push_str(&format!(
                                                    "#!line {} {}\n",
                                                    file_name,
                                                    span2.end.line + 1
                                                ));
                                            }
                                        }
                                        file.read_to_string(&mut s)?;
                                    }
                                    Err(e) => {
                                        return Err(e.into());
                                    }
                                }
                            }
                            None => {
                                return Err(ErrorKind::ModuleNotFound(
                                    Range::from((use_span.start, use_span.end)).expand_lines(2),
                                    Range::from((span2.start, span2.end)),
                                    file_path.to_string(),
                                    module_path.mounts.clone(),
                                )
                                .into());
                            }
                        }
                    } else {
                        if let Some(Ok(Spanned {
                            value: bad_token,
                            span,
                            ..
                        })) = next
                        {
                            return Err(ErrorKind::UnrecognizedToken(
                                Range::from((span.start, span.end)).expand_lines(2),
                                Range::from((span.start, span.end)),
                                bad_token.to_string(),
                                vec!["<ident>".to_string()],
                            )
                            .into());
                        }
                    }
                }
                Some(Ok(other)) => {
                    input.push_str(&format!("{}", other.value));
                }
                Some(Err(e)) => {
                    return Err(e.into());
                }
                None => break,
            }
        }

        input.push_str(" ");
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
    stored_tokens: VecDeque<TokenSpan<'input>>,
}

type Lexeme = Option<(Location, char)>;

impl<'input> Lexer<'input> {
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
            stored_tokens: VecDeque::new(),
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
        let start = start.absolute - self.start_index.to_usize();
        let end = end.absolute - self.start_index.to_usize();

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
        let mut e = start;
        while let Some((end, ch)) = self.lookahead() {
            e = end;
            if terminate(ch) {
                if let Some(slice) = self.slice(start, end) {
                    return (end, slice);
                } else {
                    // Invalid start end case :(
                    return (end, "<ERROR>");
                }
            } else {
                self.bump();
            }
        }
        if let Some(slice) = self.slice(start, e) {
            (e, slice)
        } else {
            // Invalid start end case :(
            (e, "<ERROR>")
        }
    }

    fn cx(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, lexeme) = self.take_until(start, |ch| ch == '\n');

        if lexeme.starts_with("##!") {
            let doc = Token::ModComment(&lexeme[3..]);
            Ok(spanned2(start, end, doc))
        } else if lexeme.starts_with("##") {
            let doc = Token::DocComment(&lexeme[2..]);
            Ok(spanned2(start, end, doc))
        } else if lexeme.starts_with("#!line") {
            let directive = &lexeme[6..];
            let directive = directive.trim();
            let line_directive = Token::LineDirective(Cow::from(directive));
            Ok(spanned2(start, end, line_directive))
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
            _ => Ok(spanned2(start, end, Token::Bad(lexeme.to_string()))),
        }
    }

    fn sb(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        let (end, lexeme) = self.take_while(start, |ch| ch == '-');

        match lexeme {
            "-" => Ok(spanned2(start, end, Token::Sub)),
            _ => Ok(spanned2(start, end, Token::Bad(lexeme.to_string()))),
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
            "<<" => Ok(spanned2(start, end, Token::LBitShift)),
            ">>" => Ok(spanned2(start, end, Token::RBitShiftSigned)),
            ">>>" => Ok(spanned2(start, end, Token::RBitShiftUnsigned)),
            _ => Ok(spanned2(start, end, Token::Bad(lexeme.to_string()))),
        }
    }

    fn pb(&mut self, start: Location) -> Result<TokenSpan<'input>> {
        match self.lookahead() {
            Some((end, '[')) => {
                self.bump();
                Ok(spanned2(start, end, Token::LPatBracket))
            }
            Some((end, '(')) => {
                self.bump();
                Ok(spanned2(start, end, Token::LPatParen))
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
            Some((end, ch)) => Ok(spanned2(start, end, Token::Bad(format!("!{}", ch)))),
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
            "order" => Token::Order,
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
                    s.column += 1;
                    s.absolute += 1;
                    end.column += 1;
                    end.absolute += 1;
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

    fn qs_or_hd(&mut self, start: Location) -> Result<Vec<TokenSpan<'input>>> {
        let mut end = start;
        end.column += 1;
        end.absolute += 1;
        let q1 = spanned2(start, end, Token::DQuote);
        let mut res = vec![q1];
        let string = String::new();

        match self.lookahead() {
            // This would be the second quote
            Some((mut end, '"')) => {
                self.bump();
                if let Some((end, '"')) = self.lookahead() {
                    self.bump();
                    // We don't allow anything tailing the initial `"""`
                    match self.bump() {
                        Some((_, '\n')) => self.hd(start).map(|e| vec![e]),
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
                } else {
                    // We had two quotes followed by something not a quote so
                    // it is an empty string.
                    //TODO :make slice
                    let start = end;
                    end.column += 1;
                    end.absolute += 1;
                    res.push(spanned2(start, end, Token::DQuote));
                    Ok(res)
                }
            }
            Some(_) => self.qs(start, end, false, string, res),
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
                        end.column += 1;
                        end.absolute += 1;
                        return Ok(spanned2(start, end, token));
                    }
                }
                None => {
                    return Err(ErrorKind::UnterminatedHereDoc(
                        Range::from((start, end)).expand_lines(2),
                        Range::from((start, end)),
                        format!(r#""""{}\n"#, strings.join("\n")),
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
        mut start: Location,
        mut end: Location,
        mut has_escapes: bool,
        mut string: String,
        mut res: Vec<TokenSpan<'input>>,
    ) -> Result<Vec<TokenSpan<'input>>> {
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
                    // If the string is empty we kind of don't need it.
                    if !string.is_empty() {
                        // we got to bump end by one so we claim the tailing `"`

                        let e = end;
                        let mut s = start;
                        s.column += 1;
                        s.absolute += 1;
                        let token = if has_escapes {
                            // The string was modified so we can't use the slice
                            Token::StringLiteral(string.into())
                        } else if let Some(slice) = self.slice(s, e) {
                            Token::StringLiteral(slice.into())
                        } else {
                            // Invalid start end case :(
                            Token::StringLiteral(string.into())
                        };
                        res.push(spanned2(start, end, token));
                    }
                    let start = end;
                    end.column += 1;
                    end.absolute += 1;
                    res.push(spanned2(start, end, Token::DQuote));
                    return Ok(res);
                }
                Some((end_inner, '{')) => {
                    if let Some((e, '}')) = self.lookahead() {
                        string.push('}');
                        end = e;
                        continue;
                    }
                    let e = end_inner;
                    let mut s = start;
                    s.column += 1;
                    s.absolute += 1;
                    if !string.is_empty() {
                        let token = if has_escapes {
                            // The string was modified so we can't use the slice
                            Token::StringLiteral(string.into())
                        } else if let Some(slice) = self.slice(s, e) {
                            Token::StringLiteral(slice.into())
                        } else {
                            // Invalid start end case :(
                            Token::StringLiteral(string.into())
                        };
                        res.push(spanned2(start, end_inner, token));
                        string = String::new();
                    }
                    start = end_inner;
                    end = end_inner;
                    end.column += 1;
                    end.absolute += 1;
                    res.push(spanned2(start, end, Token::LBrace));
                    let mut pcount = 0;
                    // We can't use for because of the borrow checker ...
                    #[allow(clippy::while_let_on_iterator)]
                    while let Some(s) = self.next() {
                        let s = s?;
                        match &s.value {
                            Token::RBrace if pcount == 0 => {
                                start = s.span.start();
                                res.push(s);
                                break;
                            }
                            Token::RBrace => {
                                pcount -= 1;
                            }
                            Token::LBrace => {
                                pcount += 1;
                            }
                            _ => {}
                        };
                        res.push(s);
                    }
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
                    end.absolute += 1;
                    end.column += 1;
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

    #[allow(clippy::too_many_lines)]
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
    l.column += 1;
    l.absolute += 1;
    l
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
                    // '...' =>  Some(Ok(spanned2(start, self.next_index(), Token::DotDotDot))),
                    // ".." =>  Some(Ok(spanned2(start, self.next_index(), Token::DotDot))),
                    ',' => Some(Ok(spanned2(start, start, Token::Comma))),
                    '$' => Some(Ok(spanned2(start, start, Token::Dollar))),
                    '.' => Some(Ok(spanned2(start, start, Token::Dot))),
                    //                        '?' => Some(Ok(spanned2(start, start, Token::Question))),
                    '_' => Some(Ok(spanned2(start, start, Token::DontCare))),
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
                    // TODO account for extractors which use | to mark format boundaries
                    //'|' => Some(Ok(spanned2(start, start, Token::BitOr))),
                    '^' => Some(Ok(spanned2(start, start, Token::BitXor))),
                    '&' => Some(Ok(spanned2(start, start, Token::BitAnd))),
                    ':' => Some(self.cn(start)),
                    '-' => Some(self.sb(start)),
                    '#' => Some(self.cx(start)),
                    '=' => Some(self.eq(start)),
                    '<' | '>' => Some(self.an(start)),
                    '%' => Some(self.pb(start)),
                    '~' => Some(self.tl(start)),
                    '`' => Some(self.id2(start)),
                    // TODO account for bitwise not operator
                    '!' => Some(self.pe(start)),
                    '\n' => Some(Ok(spanned2(start, start, Token::NewLine))),
                    ch if is_ident_start(ch) => Some(self.id(start)),
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
                    ch if ch.is_whitespace() => Some(self.ws(start)),
                    _ => {
                        let str = format!("{}", ch);
                        Some(Ok(spanned2(start, start, Token::Bad(str))))
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
            let lexed_tokens: Vec<_> = Tokenizer::new($src).filter(|t| match t {
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

    #[test]
    fn interpolat() -> Result<()> {
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
            r#"  "hello {7}" "#,
            r#"  ~ "# => Token::DQuote,
            r#"   ~~~~~~ "# => Token::StringLiteral("hello ".into()),
            r#"         ~ "# => Token::LBrace,
            r#"          ~ "# => Token::IntLiteral(7),
            r#"           ~ "# => Token::RBrace,
            r#"            ~ "# => Token::DQuote,

        };
        lex_ok! {
            r#"  "{7} hello" "#,
            r#"  ~ "# => Token::DQuote,
            r#"   ~ "# => Token::LBrace,
            r#"    ~ "# => Token::IntLiteral(7),
            r#"     ~ "# => Token::RBrace,
            r#"      ~~~~~~ "# => Token::StringLiteral(" hello".into()),
            r#"            ~ "# => Token::DQuote,

        };
        lex_ok! {
            r#"  "hello { "snot {7}" }" "#,
            r#"  ~ "# => Token::DQuote,
            r#"   ~~~~~~ "# => Token::StringLiteral("hello ".into()),
            r#"         ~ "# => Token::LBrace,
            r#"            ~ "# => Token::DQuote,
            r#"             ~~~~ "# => Token::StringLiteral("snot ".into()),
            r#"                  ~ "# => Token::LBrace,
            r#"                   ~ "# => Token::IntLiteral(7),
            r#"                    ~ "# => Token::RBrace,
            r#"                     ~ "# => Token::DQuote,
            r#"                        ~ "# => Token::RBrace,
            r#"                         ~ "# => Token::DQuote,
        };
        Ok(())
    }
    #[test]
    fn paths() -> Result<()> {
        lex_ok! {
            "  hello-hahaha8ABC ",
            "  ~~~~~~~~~~~~~~~~ " => Token::Ident("hello-hahaha8ABC".into(), false),
        };
        lex_ok! {
            "  .florp ", "  ~ " => Token::Dot, "   ~~~~~ " => Token::Ident("florp".into(), false),
        };
        lex_ok! {
            "  $borp ", "  ~ " => Token::Dollar, "   ~~~~~ " => Token::Ident("borp".into(), false),
        };
        lex_ok! {
            "  $`borp`", "  ~ " => Token::Dollar, "   ~~~~~~~ " => Token::Ident("borp".into(), true),
        };
        Ok(())
    }

    #[test]
    fn ignorable() {}

    #[test]
    fn keywords() -> Result<()> {
        lex_ok! { " let ", " ~~~~~~ " => Token::Let, };
        lex_ok! { " match ", " ~~~~~~ " => Token::Match, };
        lex_ok! { " case ", " ~~~~~~ " => Token::Case, };
        lex_ok! { " of ", " ~~~~~~ " => Token::Of, };
        lex_ok! { " end ", " ~~~~~~ " => Token::End, };
        lex_ok! { " drop ", " ~~~~~~ " => Token::Drop, };
        lex_ok! { " emit ", " ~~~~~~ " => Token::Emit, };
        lex_ok! { " event ", " ~~~~~~ " => Token::Event, };
        lex_ok! { " state ", " ~~~~~~ " => Token::State, };
        lex_ok! { " set ", " ~~~~~~ " => Token::Set, };
        lex_ok! { " each ", " ~~~~~~ " => Token::Each, };
        lex_ok! { " intrinsic ", " ~~~~~~~~~ " => Token::Intrinsic, };
        Ok(())
    }

    #[test]
    fn operators() -> Result<()> {
        lex_ok! {
        " not null ", "  ~ " => Token::Not, "  ~ " => Token::Nil, };
        lex_ok! { " != null ", "  ~~ " => Token::NotEq, "   ~ " => Token::Nil, };
        // TODO fix this
        //lex_ok! { " !1 ", " ~  " => Token::BitNot, "  ~ " => Token::IntLiteral(1), };

        lex_ok! { " and ", " ~ " => Token::And, };
        lex_ok! { " or ", " ~ " => Token::Or, };
        lex_ok! { " xor ", " ~ " => Token::Xor, };
        lex_ok! { " & ", " ~ " => Token::BitAnd, };
        // TODO enable
        //lex_ok! { " | ", " ~ " => Token::BitOr, };
        lex_ok! { " ^ ", " ~ " => Token::BitXor, };
        lex_ok! { " = ", " ~ " => Token::Eq, };
        lex_ok! { " == ", " ~ " => Token::EqEq, };
        lex_ok! { " != ", " ~ " => Token::NotEq, };
        lex_ok! { " >= ", " ~ " => Token::Gte, };
        lex_ok! { " > ", " ~ " => Token::Gt, };
        lex_ok! { " <= ", " ~ " => Token::Lte, };
        lex_ok! { " < ", " ~ " => Token::Lt, };
        lex_ok! { " >> ", " ~ " => Token::RBitShiftSigned, };
        lex_ok! { " >>> ", " ~ " => Token::RBitShiftUnsigned, };
        lex_ok! { " << ", " ~ " => Token::LBitShift, };
        lex_ok! { " + ", " ~ " => Token::Add, };
        lex_ok! { " - ", " ~ " => Token::Sub, };
        lex_ok! { " * ", " ~ " => Token::Mul, };
        lex_ok! { " / ", " ~ " => Token::Div, };
        lex_ok! { " % ", " ~ " => Token::Mod, };
        Ok(())
    }

    #[test]
    fn should_disambiguate() -> Result<()> {
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
        lex_ok! { " %[ ", " ~~ " => Token::LPatBracket, }
        lex_ok! { " %( ", " ~~ " => Token::LPatParen, }
        // Starts with '.'
        lex_ok! { " . ", " ~ " => Token::Dot, };

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
            r#" "\"\"" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~~  "# => Token::StringLiteral("\"\"".into()),
            r#"    ~ "# => Token::DQuote,
        };

        lex_ok! {
            r#" "\\\"" "#,
            r#" ~    "# => Token::DQuote,
            r#"  ~~  "# => Token::StringLiteral("\\\"".into()),
            r#"    ~ "# => Token::DQuote,
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

    #[test]
    fn test_preprocessor() -> Result<()> {
        lex_ok! {
            r#"use "foo.tremor" ;"#,
            r#"~"# => Token::Use,
            r#"    ~"# => Token::DQuote,
            r#"      ~"# => Token::StringLiteral("foo.tremor".into()),
            r#"               ~"# => Token::DQuote,
            r#"                 ~"# => Token::Semi,
        };

        Ok(())
    }

    #[test]
    fn test_test_literal_format_bug_regression() -> Result<()> {
        let mut snot = "match %{ test ~= base64|| } of default => \"badger\" end ".to_string();
        let mut snot2 = snot.clone();
        let badger: Vec<_> = Tokenizer::new(&mut snot).collect();
        let badger2 = Preprocessor::preprocess(
            &ModulePath { mounts: vec![] },
            "foo".to_string(),
            &mut snot2,
        )?;
        let mut res = String::new();
        for b in badger
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<TokenSpan>>()
        {
            res.push_str(&format!("{}", b.value));
        }
        assert_eq!(snot.trim(), res);
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
}
