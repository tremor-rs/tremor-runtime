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
use crate::errors::Error;
use crate::lexer::{LexerError, Token, TokenFuns, TokenSpan};
use crate::pos::*;
use lalrpop_util::ParseError;
use serde::{Deserialize, Serialize};
use std::io::Write;
use termcolor::{Buffer, BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

#[derive(Serialize, Deserialize, Debug)]
pub struct HighlighterError {
    pub start: Location,
    pub end: Location,
    pub callout: String,
    pub hint: Option<String>,
}

impl From<&Error> for HighlighterError {
    fn from(error: &Error) -> Self {
        let (start, end) = match error.context() {
            (_, Some(inner)) => (inner.0, inner.1),
            _ => (Location::default(), Location::default()),
        };
        Self {
            start,
            end,
            callout: format!("{}", error),
            hint: error.hint(),
        }
    }
}

pub trait Highlighter {
    type W: Write;

    fn set_color(&mut self, _spec: &mut ColorSpec) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn reset(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn get_writer(&mut self) -> &mut Self::W;

    fn highlight(
        &mut self,
        tokens: Vec<Result<TokenSpan, LexerError>>,
    ) -> Result<(), std::io::Error> {
        self.highlight_errors(tokens, None)
    }

    fn highlight_runtime_error(
        &mut self,
        tokens: Vec<Result<TokenSpan, LexerError>>,
        expr_start: Location,
        expr_end: Location,
        error: Option<HighlighterError>,
    ) -> Result<(), std::io::Error> {
        let extracted = extract(tokens, expr_start, expr_end);
        self.highlight_errors(extracted, error)
    }

    fn highlight_errors(
        &mut self,
        tokens: Vec<Result<TokenSpan, LexerError>>,
        error: Option<HighlighterError>,
    ) -> Result<(), std::io::Error> {
        let mut printed_error = false;
        let mut line = 0;
        for t in tokens {
            if t.is_err() {
                break;
            } else {
                let t = t.expect("expected a legal token");
                if t.span.start().line.0 != line {
                    line = t.span.start().line.0;
                    if let Some(HighlighterError {
                        start,
                        end,
                        callout,
                        hint,
                    }) = &error
                    {
                        if start.line.0 == line - 1 {
                            printed_error = true;
                            let len = std::cmp::max((end.column.0 - start.column.0) as usize, 1);
                            let prefix = String::from(" ")
                                .repeat(start.column.0.checked_sub(1).unwrap_or(0) as usize);
                            let underline = String::from("^").repeat(len);
                            self.set_color(ColorSpec::new().set_bold(true))?;
                            write!(self.get_writer(), "      | {}", prefix)?;
                            self.set_color(
                                ColorSpec::new().set_bold(false).set_fg(Some(Color::Red)),
                            )?;
                            writeln!(self.get_writer(), "{} {}", underline, callout)?;
                            self.reset()?;
                            if let Some(hint) = hint {
                                let prefix =
                                    String::from(" ").repeat(start.column.0 as usize + len);
                                self.set_color(ColorSpec::new().set_bold(true))?;
                                write!(self.get_writer(), "      | {}", prefix)?;
                                self.set_color(
                                    ColorSpec::new().set_bold(false).set_fg(Some(Color::Yellow)),
                                )?;
                                writeln!(self.get_writer(), "NOTE: {}", hint)?;
                            }
                        }
                        self.reset()?;
                    }
                    self.set_color(ColorSpec::new().set_bold(true))?;
                    write!(self.get_writer(), "{:5} | ", line)?;
                    self.reset()?;
                }

                let x = t;
                let mut c = ColorSpec::new();
                if x.value.is_keyword() {
                    c.set_bold(true)
                        .set_intense(true)
                        .set_fg(Some(Color::Green));
                }
                if x.value.is_operator() || x.value.is_symbol() {
                    c.set_bold(true)
                        .set_intense(true)
                        .set_fg(Some(Color::White));
                }
                if x.value.is_literal() && !x.value.is_string_like() {
                    c.set_intense(true).set_fg(Some(Color::Red));
                }
                match &x.value {
                    Token::SingleLineComment(_) => {
                        c.set_intense(true).set_fg(Some(Color::Blue));
                    }
                    Token::DocComment(_) => {
                        c.set_intense(true).set_fg(Some(Color::Cyan));
                    }
                    Token::TestLiteral(_) => {
                        c.set_intense(true).set_fg(Some(Color::Magenta));
                    }

                    Token::StringLiteral(_) => {
                        c.set_intense(true).set_fg(Some(Color::Magenta));
                    }
                    Token::BadToken(_) => {
                        c.set_bold(true)
                            .set_intense(true)
                            .set_bg(Some(Color::Red))
                            .set_fg(Some(Color::White));
                    }
                    Token::Ident(_, _) => {
                        c.set_intense(true).set_fg(Some(Color::Yellow));
                    }
                    _other => (), // Just an empty spec
                }
                self.set_color(&mut c)?;

                write!(self.get_writer(), "{}", x.value)?;
                self.reset()?;
            };
        }
        if let Some(HighlighterError {
            start,
            end,
            callout,
            hint,
        }) = &error
        {
            if !printed_error || start.line.0 == line {
                while start.line.0 > line {
                    line += 1;
                    self.set_color(ColorSpec::new().set_bold(true))?;
                    writeln!(self.get_writer(), "{:5} | ", line)?;
                    self.reset()?;
                }
                printed_error = true;
                let len = std::cmp::max((end.column.0 - start.column.0) as usize, 1);
                let prefix = String::from(" ").repeat(start.column.0 as usize - 1);
                let underline = String::from("^").repeat(len);
                self.set_color(ColorSpec::new().set_bold(true))?;
                write!(self.get_writer(), "      | {}", prefix)?;
                self.set_color(ColorSpec::new().set_bold(false).set_fg(Some(Color::Red)))?;
                writeln!(self.get_writer(), "{} {}", underline, callout)?;
                self.reset()?;
                if let Some(hint) = hint {
                    let prefix = String::from(" ").repeat(start.column.0 as usize + len);
                    self.set_color(ColorSpec::new().set_bold(true))?;
                    write!(self.get_writer(), "      | {}", prefix)?;
                    self.set_color(ColorSpec::new().set_bold(false).set_fg(Some(Color::Yellow)))?;
                    writeln!(self.get_writer(), "NOTE: {}", hint)?;
                }
                self.reset()?;
            }
        }

        self.reset()?;
        self.finalize()
    }
}

#[derive(Default)]
pub struct DumbHighlighter {
    buff: Vec<u8>,
}
impl DumbHighlighter {
    pub fn new() -> Self {
        DumbHighlighter::default()
    }
}

impl Highlighter for DumbHighlighter {
    type W = Vec<u8>;
    fn get_writer(&mut self) -> &mut Self::W {
        &mut self.buff
    }
}

impl ToString for DumbHighlighter {
    fn to_string(&self) -> String {
        String::from_utf8(self.buff.clone()).expect("Highlighted source isn't a valid string")
    }
}

pub struct TermHighlighter {
    bufwtr: BufferWriter,
    buff: Buffer,
}
impl TermHighlighter {
    pub fn new() -> Self {
        let bufwtr = BufferWriter::stdout(ColorChoice::Auto);
        let buff = bufwtr.buffer();
        TermHighlighter { bufwtr, buff }
    }
}

impl Highlighter for TermHighlighter {
    type W = Buffer;
    fn set_color(&mut self, spec: &mut ColorSpec) -> Result<(), std::io::Error> {
        self.buff.set_color(spec)
    }
    fn reset(&mut self) -> Result<(), std::io::Error> {
        self.buff.reset()
    }
    fn finalize(&mut self) -> Result<(), std::io::Error> {
        self.bufwtr.print(&self.buff)
    }
    fn get_writer(&mut self) -> &mut Self::W {
        &mut self.buff
    }
}

fn extract(
    tokens: Vec<Result<TokenSpan, LexerError>>,
    start: Location,
    end: Location,
) -> Vec<Result<TokenSpan, LexerError>> {
    tokens
        .into_iter()
        .skip_while(|t| {
            if let Ok(t) = t {
                t.span.start().line < start.line
            } else {
                false
            }
        })
        .take_while(|t| {
            if let Ok(t) = t {
                t.span.end().line <= end.line
            } else {
                false
            }
        })
        .collect()
}
