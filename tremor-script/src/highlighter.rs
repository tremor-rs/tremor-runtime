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
use crate::ast::Warning;
use crate::errors::*;
use crate::lexer::{Token, TokenFuns, TokenSpan};
use crate::pos::*;
use lalrpop_util::ParseError;
use serde::{Deserialize, Serialize};
use std::io::Write;
use termcolor::{Buffer, BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

#[derive(Serialize, Deserialize, Debug)]
pub enum ErrorLevel {
    Error,
    Warning,
    Hint,
}

impl ErrorLevel {
    fn to_color(&self) -> Color {
        match self {
            ErrorLevel::Error => Color::Red,
            ErrorLevel::Warning => Color::Yellow,
            ErrorLevel::Hint => Color::Green,
        }
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub struct HighlighterError {
    pub start: Location,
    pub end: Location,
    pub callout: String,
    pub hint: Option<String>,
    pub level: ErrorLevel,
    pub token: Option<String>,
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
            level: ErrorLevel::Error,
            token: error.token(),
        }
    }
}

impl From<&Warning> for HighlighterError {
    fn from(warning: &Warning) -> Self {
        Self {
            start: warning.inner.0,
            end: warning.inner.1,
            callout: warning.msg.to_owned(),
            hint: None,
            level: ErrorLevel::Warning,
            token: None,
        }
    }
}

pub trait Highlighter {
    type W: Write;

    fn set_color(&mut self, _spec: &mut ColorSpec) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    fn reset(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    fn finalize(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    fn get_writer(&mut self) -> &mut Self::W;

    fn highlight(
        &mut self,
        tokens: Vec<Result<TokenSpan>>,
    ) -> std::result::Result<(), std::io::Error> {
        self.highlight_errors(tokens, None)
    }

    fn highlight_runtime_error(
        &mut self,
        tokens: Vec<Result<TokenSpan>>,
        expr_start: Location,
        expr_end: Location,
        error: Option<HighlighterError>,
    ) -> std::result::Result<(), std::io::Error> {
        let extracted = extract(tokens, expr_start, expr_end);
        self.highlight_errors(extracted, error)
    }

    fn highlight_errors(
        &mut self,
        tokens: Vec<Result<TokenSpan>>,
        error: Option<HighlighterError>,
    ) -> std::result::Result<(), std::io::Error> {
        let mut printed_error = false;
        let mut line = 0;
        match error {
            Some(HighlighterError {
                level: ErrorLevel::Error,
                ..
            }) => writeln!(self.get_writer(), "Error: ")?,
            Some(HighlighterError {
                level: ErrorLevel::Warning,
                ..
            }) => writeln!(self.get_writer(), "Warning: ")?,
            Some(HighlighterError {
                level: ErrorLevel::Hint,
                ..
            }) => writeln!(self.get_writer(), "Hint: ")?,
            _ => (),
        }
        for t in tokens {
            if let Ok(t) = t {
                if t.span.start().line != line {
                    line = t.span.start().line;
                    if let Some(HighlighterError {
                        start,
                        end,
                        callout,
                        hint,
                        level,
                        token,
                    }) = &error
                    {
                        if end.line == line - 1 {
                            printed_error = true;
                            let len = std::cmp::max(end.column - start.column, 1);
                            let prefix =
                                String::from(" ").repeat(start.column.checked_sub(1).unwrap_or(0));
                            let underline = String::from("^").repeat(len);

                            if let Some(token) = token {
                                write!(self.get_writer(), "{}", token)?;
                            };

                            self.set_color(ColorSpec::new().set_bold(true))?;
                            write!(self.get_writer(), "      | {}", prefix)?;
                            self.set_color(
                                ColorSpec::new()
                                    .set_bold(false)
                                    .set_fg(Some(level.to_color())),
                            )?;
                            writeln!(self.get_writer(), "{} {}", underline, callout)?;
                            self.reset()?;
                            if let Some(hint) = hint {
                                let prefix = String::from(" ").repeat(start.column + len);
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
                    Token::TestLiteral(_, _) => {
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
                match &x.value {
                    Token::HereDoc(indent, lines) => {
                        let space = String::from(" ");
                        writeln!(self.get_writer(), r#"""""#)?;
                        for l in lines {
                            line += 1;
                            self.reset()?;
                            self.set_color(ColorSpec::new().set_bold(true))?;
                            write!(self.get_writer(), "{:5} | ", line)?;
                            self.reset()?;
                            c.set_intense(true).set_fg(Some(Color::Magenta));
                            writeln!(self.get_writer(), "{}{}", space.repeat(*indent), l)?
                        }
                        line += 1;
                        self.reset()?;
                        self.set_color(ColorSpec::new().set_bold(true))?;
                        write!(self.get_writer(), "{:5} | ", line)?;
                        self.reset()?;
                        write!(self.get_writer(), r#"""""#)?;
                    }
                    Token::TestLiteral(indent, lines) => {
                        let space = String::from(" ");
                        write!(self.get_writer(), "|")?;
                        let mut first = true;
                        for l in lines {
                            if first {
                                first = false;
                            } else {
                                line += 1;
                                self.reset()?;
                                self.set_color(ColorSpec::new().set_bold(true))?;
                                write!(self.get_writer(), "\\\n{:5} | ", line)?;
                                self.reset()?;
                                c.set_intense(true).set_fg(Some(Color::Magenta));
                            }
                            write!(self.get_writer(), "{}{}", space.repeat(*indent), l)?
                        }
                        self.reset()?;
                        write!(self.get_writer(), "|")?;
                    }
                    _ => write!(self.get_writer(), "{}", x.value)?,
                }

                self.reset()?;
            };
        }
        if let Some(HighlighterError {
            start,
            end,
            callout,
            hint,
            level,
            token,
        }) = &error
        {
            if !printed_error || start.line == line {
                if end.line > line {
                    line += 1;
                    self.set_color(ColorSpec::new().set_bold(true))?;
                    writeln!(self.get_writer(), "{:5} | ", line)?;
                    self.reset()?;
                }
                printed_error = true;

                let len = if end.column > start.column {
                    end.column - start.column
                } else {
                    1
                };
                let prefix = String::from(" ").repeat(start.column - 1);
                let underline = String::from("^").repeat(len);
                if let Some(token) = token {
                    write!(self.get_writer(), "{}", token)?;
                };
                self.set_color(ColorSpec::new().set_bold(true))?;
                write!(self.get_writer(), "      | {}", prefix)?;
                self.set_color(
                    ColorSpec::new()
                        .set_bold(false)
                        .set_fg(Some(level.to_color())),
                )?;
                writeln!(self.get_writer(), "{} {}", underline, callout)?;
                self.reset()?;
                if let Some(hint) = hint {
                    let prefix = String::from(" ").repeat(start.column + len);
                    self.set_color(ColorSpec::new().set_bold(true))?;
                    write!(self.get_writer(), "      | {}", prefix)?;
                    self.set_color(ColorSpec::new().set_bold(false).set_fg(Some(Color::Yellow)))?;
                    writeln!(self.get_writer(), "NOTE: {}", hint)?;
                }
                self.reset()?;
            }
        }

        self.reset()?;
        writeln!(self.get_writer());
        self.finalize()
    }
}

/// Highlights data to allow extracting it as a plain,
/// unhighlighted string.
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
        String::from_utf8_lossy(&self.buff).to_string()
    }
}

/// Highlights data colorized and directly to the terminal
pub struct TermHighlighter {
    bufwtr: BufferWriter,
    buff: Buffer,
}

#[allow(clippy::new_without_default)]
impl TermHighlighter {
    pub fn new() -> Self {
        let bufwtr = BufferWriter::stdout(ColorChoice::Auto);
        let buff = bufwtr.buffer();
        TermHighlighter { bufwtr, buff }
    }
}

impl Highlighter for TermHighlighter {
    type W = Buffer;
    fn set_color(&mut self, spec: &mut ColorSpec) -> std::result::Result<(), std::io::Error> {
        self.buff.set_color(spec)
    }
    fn reset(&mut self) -> std::result::Result<(), std::io::Error> {
        self.buff.reset()
    }
    fn finalize(&mut self) -> std::result::Result<(), std::io::Error> {
        self.bufwtr.print(&self.buff)
    }
    fn get_writer(&mut self) -> &mut Self::W {
        &mut self.buff
    }
}

fn extract(
    tokens: Vec<Result<TokenSpan>>,
    start: Location,
    end: Location,
) -> Vec<Result<TokenSpan>> {
    tokens
        .into_iter()
        .skip_while(|t| {
            if let Ok(t) = t {
                t.span.end().line < start.line
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
