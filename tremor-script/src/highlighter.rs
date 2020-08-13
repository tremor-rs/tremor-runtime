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
use crate::ast::Warning;
use crate::errors::{CompilerError, Error as ScriptError};
use crate::lexer::{Token, TokenSpan};
use crate::pos::Location;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io::Write;
use termcolor::{Buffer, BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Error Level
pub enum ErrorLevel {
    /// Error
    Error,
    /// Warning
    Warning,
    /// Hint
    Hint,
}

impl ErrorLevel {
    fn to_color(&self) -> Color {
        match self {
            Self::Error => Color::Red,
            Self::Warning => Color::Yellow,
            Self::Hint => Color::Green,
        }
    }
}
/// Error to be highlighted
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Error {
    start: Location,
    end: Location,
    callout: String,
    hint: Option<String>,
    level: ErrorLevel,
    token: Option<String>,
}

impl Error {
    /// Get start location of the error
    pub fn start(&self) -> Location {
        self.start
    }

    /// Get end location of the error
    pub fn end(&self) -> Location {
        self.end
    }

    /// Get end location of the error
    pub fn callout(&self) -> &String {
        &self.callout
    }

    /// Get hint on how to fix the error
    pub fn hint(&self) -> Option<&String> {
        self.hint.as_ref()
    }

    /// Get the level of the error. See `ErrorLevel` for possible values.
    pub fn level(&self) -> &ErrorLevel {
        &self.level
    }

    /// Get error token
    pub fn token(&self) -> Option<&String> {
        self.token.as_ref()
    }
}

impl From<&ScriptError> for Error {
    fn from(error: &ScriptError) -> Self {
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

impl From<&CompilerError> for Error {
    fn from(error: &CompilerError) -> Self {
        let error = &error.error;
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
impl From<&Warning> for Error {
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

/// Highlighter trait for generalising over different output types
pub trait Highlighter {
    /// Writer for the highligher to write to
    type W: Write;

    /// sets the color
    fn set_color(&mut self, _spec: &mut ColorSpec) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    /// resets all formatting
    fn reset(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    /// finalises highlighting
    fn finalize(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
    /// get the raw writer
    fn get_writer(&mut self) -> &mut Self::W;

    /// highlights a token stream without line numbers
    fn highlight_no_linenos(
        &mut self,
        file: Option<&str>,
        tokens: &[TokenSpan],
    ) -> std::result::Result<(), std::io::Error> {
        self.highlight_errors2(false, file, &tokens.iter().collect::<Vec<_>>(), None)
    }

    /// highlights a token stream with line numbers
    fn highlight(
        &mut self,
        file: Option<&str>,
        tokens: &[TokenSpan],
    ) -> std::result::Result<(), std::io::Error> {
        self.highlight_errors(file, &tokens.iter().collect::<Vec<_>>(), None)?;
        self.finalize()
    }

    /// highlights a runtime error
    fn highlight_runtime_error(
        &mut self,
        file: Option<&str>,
        tokens: &[TokenSpan],
        expr_start: Location,
        expr_end: Location,
        error: Option<Error>,
    ) -> std::result::Result<(), std::io::Error> {
        let extracted = extract(tokens, expr_start, expr_end);
        self.highlight_errors(file, &extracted, error)
    }

    /// highlights compile time errors
    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    fn highlight_errors(
        &mut self,
        emit_linenos: bool,
        file: Option<&str>,
        tokens: &[&TokenSpan],
        error: Option<Error>,
    ) -> std::result::Result<(), std::io::Error> {
        let mut printed_error = false;
        let mut line = 0;
        match error {
            Some(Error {
                level: ErrorLevel::Error,
                start,
                ..
            }) => {
                if let Some(file) = file {
                    writeln!(
                        self.get_writer(),
                        "Error in {}:{}:{} ",
                        file,
                        start.line,
                        start.column
                    )?
                } else {
                    writeln!(self.get_writer(), "Error: ")?
                }
            }
            Some(Error {
                level: ErrorLevel::Warning,
                ..
            }) => writeln!(self.get_writer(), "Warning: ")?,
            Some(Error {
                level: ErrorLevel::Hint,
                ..
            }) => writeln!(self.get_writer(), "Hint: ")?,
            _ => (),
        }
        for t in tokens {
            //            if let Ok(t) = t {
            if t.span.start().line != line {
                line = t.span.start().line;
                if let Some(Error {
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
                        // FIXME This isn't perfect, there are cases in trickle where more specific
                        // hygienic errors would be preferable ( eg: for-locals integration test )
                        //
                        let delta = end.column as i64 - start.column as i64;
                        let len = usize::try_from(delta).unwrap_or(1);
                        let prefix = " ".repeat(start.column.saturating_sub(1));
                        let underline = "^".repeat(len);

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
                            let prefix = " ".repeat(start.column + len);
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
                if emit_linenos {
                    write!(self.get_writer(), "{:5} | ", line)?;
                } else {
                    write!(self.get_writer(), "      | ")?;
                }
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
                Token::LineDirective(_, _) => {
                    c.set_intense(true).set_fg(Some(Color::White));
                }
                Token::SingleLineComment(_) => {
                    c.set_intense(true).set_fg(Some(Color::Blue));
                }
                Token::DocComment(_) => {
                    c.set_intense(true).set_fg(Some(Color::Cyan));
                }
                Token::TestLiteral(_, _) | Token::StringLiteral(_) => {
                    c.set_intense(true).set_fg(Some(Color::Magenta));
                }
                Token::Bad(_) => {
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
                Token::HereDoc => {
                    // (indent, lines) => {
                    writeln!(self.get_writer(), r#"""""#)?;
                    // FIXME indentation sensing in heredoc's
                    // for l in lines {
                    //     line += 1;
                    //     self.reset()?;
                    //     self.set_color(ColorSpec::new().set_bold(true))?;
                    //     write!(self.get_writer(), "{:5} | ", line)?;
                    //     self.reset()?;
                    //     c.set_intense(true).set_fg(Some(Color::Magenta));
                    //     writeln!(self.get_writer(), "{}{}", " ".repeat(*indent), l)?
                    // }
                    line += 1;
                    self.reset()?;
                    // self.set_color(ColorSpec::new().set_bold(true))?;
                    // write!(self.get_writer(), "{:5} | ", line)?;
                    // self.reset()?;
                    // write!(self.get_writer(), r#"""""#)?;
                }
                Token::TestLiteral(indent, lines) => {
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
                        write!(self.get_writer(), "{}{}", " ".repeat(*indent), l)?
                    }
                    self.reset()?;
                    write!(self.get_writer(), "|")?;
                }
                _ => write!(self.get_writer(), "{}", x.value)?,
            }

            self.reset()?;
            //};
        }
        if let Some(Error {
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

                let len = if end.column > start.column {
                    end.column - start.column
                } else {
                    1
                };
                let prefix = " ".repeat(start.column.saturating_sub(1));
                let underline = "^".repeat(len);
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
                    let prefix = " ".repeat(start.column + len);
                    self.set_color(ColorSpec::new().set_bold(true))?;
                    write!(self.get_writer(), "      | {}", prefix)?;
                    self.set_color(ColorSpec::new().set_bold(false).set_fg(Some(Color::Yellow)))?;
                    writeln!(self.get_writer(), "NOTE: {}", hint)?;
                }
                self.reset()?;
            }
        }

        self.reset()?;
        writeln!(self.get_writer())
    }
}

/// Highlights data to allow extracting it as a plain,
/// unhighlighted string.
#[derive(Default)]
pub struct Dumb {
    buff: Vec<u8>,
}
impl Dumb {
    /// Creates a new highlighter
    pub fn new() -> Self {
        Self::default()
    }
}

impl Highlighter for Dumb {
    type W = Vec<u8>;
    fn get_writer(&mut self) -> &mut Self::W {
        &mut self.buff
    }
}

impl ToString for Dumb {
    fn to_string(&self) -> String {
        String::from_utf8_lossy(&self.buff).to_string()
    }
}

/// Highlights data colorized and directly to the terminal
pub struct Term {
    bufwtr: BufferWriter,
    buff: Buffer,
}

// This is a terminal highlighter it simply adds colors
// so we skip it in tests.

#[allow(clippy::new_without_default)]
impl Term {
    /// Creates a new highlighter
    pub fn new() -> Self {
        let bufwtr = BufferWriter::stdout(ColorChoice::Auto);
        let buff = bufwtr.buffer();
        Self { bufwtr, buff }
    }
}

impl Highlighter for Term {
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

fn extract<'input, 'tokens>(
    tokens: &'tokens [TokenSpan<'input>],
    start: Location,
    end: Location,
) -> Vec<&'tokens TokenSpan<'input>>
where
    'input: 'tokens,
{
    tokens
        .iter()
        .skip_while(|t| t.span.end().line < start.line)
        .take_while(|t| t.span.end().line <= end.line)
        .collect()
}
