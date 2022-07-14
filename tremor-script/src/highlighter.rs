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

// This is terminal related for colorful printing
// #![cfg_attr(coverage, no_coverage)]

use crate::{
    arena::Arena,
    ast::warning::{self, Warning},
    errors::{Error as ScriptError, UnfinishedToken},
    lexer::{self, Span, Token, TokenSpan},
    pos::Location,
};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use termcolor::{Buffer, BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
/// Error Level
pub enum ErrorLevel {
    /// Error
    Error,
    /// Warning
    Warning(warning::Class),
    /// Hint
    Hint,
}

impl ErrorLevel {
    fn to_color(self) -> Color {
        match self {
            Self::Error => Color::Red,
            Self::Warning(_) => Color::Yellow,
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
    token: Option<UnfinishedToken>,
}

impl Error {
    /// Get start location of the error
    #[must_use]

    pub fn start(&self) -> Location {
        self.start
    }

    /// Get end location of the error
    #[must_use]
    pub fn end(&self) -> Location {
        self.end
    }

    /// Get end location of the error
    #[must_use]
    pub fn callout(&self) -> &str {
        &self.callout
    }

    /// Get hint on how to fix the error
    #[must_use]
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    /// Get the level of the error. See `ErrorLevel` for possible values.
    #[must_use]
    pub fn level(&self) -> &ErrorLevel {
        &self.level
    }

    /// Get error token
    #[must_use]
    pub fn token(&self) -> Option<&UnfinishedToken> {
        self.token.as_ref()
    }
}

impl From<&ScriptError> for Error {
    fn from(error: &ScriptError) -> Self {
        let (start, end) = match error.context() {
            (_, Some(inner)) => (inner.start(), inner.end()),
            _ => (Location::yolo(), Location::yolo()),
        };
        Self {
            start,
            end,
            callout: format!("{error}"),
            hint: error.hint(),
            level: ErrorLevel::Error,
            token: error.token(),
        }
    }
}

impl From<&Warning> for Error {
    fn from(warning: &Warning) -> Self {
        Self {
            start: warning.inner.start(),
            end: warning.inner.end(),
            callout: warning.msg.clone(),
            hint: None,
            level: ErrorLevel::Warning(warning.class),
            token: None,
        }
    }
}

/// Highlighter trait for generalising over different output types
pub trait Highlighter {
    /// Writer for the highligher to write to
    type W: Write;

    /// Highlights a script range
    /// # Errors
    /// on io errors
    fn highlight_range(&mut self, r: Span) -> io::Result<()> {
        self.highlight_range_with_indent("", r)
    }

    /// Highlights a script range
    /// # Errors
    /// on io errors
    fn highlight_range_with_indent(&mut self, line_prefix: &str, r: Span) -> io::Result<()> {
        let aid = r.aid();
        let script = Arena::io_get(aid)?;
        let tokens: Vec<_> =
            lexer::Lexer::new(script, aid).collect::<crate::errors::Result<_>>()?;
        self.highlight(None, &tokens, line_prefix, true, Some(r))?;
        io::Result::Ok(())
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    fn format_error(&mut self, error: &crate::errors::Error) -> io::Result<()> {
        if let Some((r, aid, script)) = error
            .context()
            .0
            .and_then(|r| Some((r, r.aid(), Arena::io_get(r.aid()).ok()?)))
        {
            let tokens: Vec<_> = lexer::Lexer::new(script, aid)
                .tokenize_until_err()
                .collect();
            self.highlight_error(None, &tokens, "", true, Some(r), Some(error.into()))?;
        } else {
            write!(self.get_writer(), "Error: {error}")?;
        }
        self.finalize()
    }

    /// sets the color
    ///
    /// # Errors
    /// on io errors
    fn set_color(&mut self, _spec: &mut ColorSpec) -> io::Result<()> {
        Ok(())
    }
    /// resets all formatting
    ///
    /// # Errors
    /// on io errors
    fn reset(&mut self) -> io::Result<()> {
        Ok(())
    }
    /// finalises highlighting
    ///
    /// # Errors
    /// on io errors
    fn finalize(&mut self) -> io::Result<()> {
        Ok(())
    }
    /// get the raw writer
    fn get_writer(&mut self) -> &mut Self::W;

    /// Highlights a source
    ///
    /// # Errors
    /// on io errors
    fn highlight_str(&mut self, source: &str, ident: &str, emit_lines: bool) -> io::Result<()> {
        // TODO: do we really want to input this here?
        let (aid, source) = Arena::insert(&source)?;
        let tokens: Vec<_> = crate::lexer::Lexer::new(source, aid)
            .filter_map(Result::ok)
            .collect();
        self.highlight(Some(source), &tokens, ident, emit_lines, None)
    }

    /// highlights a token stream with line numbers
    ///
    /// # Errors
    /// on io errors
    fn highlight(
        &mut self,
        file: Option<&str>,
        tokens: &[TokenSpan],
        ident: &str,
        emit_lines: bool,
        range: Option<Span>,
    ) -> io::Result<()> {
        self.highlight_error(file, tokens, ident, emit_lines, range, None)
    }

    /// highlights compile time errors
    ///
    /// # Errors
    /// on io errors
    fn highlight_error(
        &mut self,
        file: Option<&str>,
        tokens: &[TokenSpan],
        ident: &str,
        emit_linenos: bool,
        range: Option<Span>,
        error: Option<Error>,
    ) -> io::Result<()> {
        let extracted = range.map_or_else(
            || tokens.iter().collect::<Vec<_>>(),
            |Span { start, end, .. }| extract(tokens, start, end),
        );
        self.highlight_errors_indent(ident, emit_linenos, file, &extracted, error)
    }

    /// ensure we have a newline written as last character
    ///
    /// # Errors
    /// if unable to write a newline
    ///
    fn ensure_newline(&mut self) -> std::result::Result<(), io::Error>;

    /// write line prefix and optionally line number
    ///
    /// # Errors
    /// on io errors
    #[inline]
    fn write_line_prefix(
        &mut self,
        line_prefix: &str,
        line: usize,
        emit_linenos: bool,
    ) -> io::Result<()> {
        self.set_color(ColorSpec::new().set_bold(true))?;
        if emit_linenos {
            write!(self.get_writer(), "{line_prefix}{line:5} | ")?;
        } else {
            write!(self.get_writer(), "{line_prefix}")?;
        }
        self.reset()?;
        Ok(())
    }

    /// write the actual error message below the error location
    ///
    /// # Errors
    /// on io errors
    #[inline]
    fn write_callout(
        &mut self,
        callout: &str,
        error_level: &ErrorLevel,
        start_column: usize,
        error_len: usize,
    ) -> io::Result<()> {
        let prefix = " ".repeat(start_column.saturating_sub(1));
        let underline = "^".repeat(error_len);
        self.set_color(ColorSpec::new().set_bold(true))?;
        write!(self.get_writer(), "      | {prefix}")?;
        self.set_color(
            ColorSpec::new()
                .set_bold(false)
                .set_fg(Some(error_level.to_color())),
        )?;
        writeln!(self.get_writer(), "{underline} {callout}")?;
        self.reset()?;
        Ok(())
    }

    /// write a helpful hint for users below the error message
    ///
    /// # Errors
    /// on io errors
    fn write_hint(&mut self, hint: &str, start_column: usize, error_len: usize) -> io::Result<()> {
        let prefix = " ".repeat(start_column + error_len);
        self.set_color(ColorSpec::new().set_bold(true))?;
        write!(self.get_writer(), "      | {prefix}")?;
        self.set_color(ColorSpec::new().set_bold(false).set_fg(Some(Color::Yellow)))?;
        writeln!(self.get_writer(), "NOTE: {hint}")?;

        self.reset()?;
        Ok(())
    }

    /// highlights compile time errors with indentation
    ///
    /// # Errors
    /// on io errors
    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    fn highlight_errors_indent(
        &mut self,
        line_prefix: &str,
        emit_linenos: bool,
        file: Option<&str>,
        tokens: &[&TokenSpan],
        error: Option<Error>,
    ) -> io::Result<()> {
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
                        start.line(),
                        start.column()
                    )?;
                } else {
                    writeln!(self.get_writer(), "Error: ")?;
                }
            }
            Some(Error {
                level: ErrorLevel::Warning(class),
                ..
            }) => writeln!(self.get_writer(), "Warning({class}): ")?,
            Some(Error {
                level: ErrorLevel::Hint,
                ..
            }) => writeln!(self.get_writer(), "Hint: ")?,
            _ => (),
        }

        for t in tokens {
            if t.span.start().line() != line {
                line = t.span.start().line();
                if let Some(Error {
                    start,
                    end,
                    callout,
                    hint,
                    level,
                    token,
                }) = &error
                {
                    if !printed_error && end.line() == line.saturating_sub(1) {
                        printed_error = true;
                        // TODO This isn't perfect, there are cases in trickle where more specific
                        // hygienic errors would be preferable ( eg: for-locals integration test )
                        //
                        let (start_column, len) = if start.line() == end.line() {
                            let start_column = start.column();
                            (
                                start_column,
                                end.column().saturating_sub(start_column).max(1),
                            )
                        } else {
                            // multi-line token
                            (1, end.column() - 1)
                        };

                        if let Some(token) = token {
                            let mut lines = token.value.lines();
                            if let Some(first_line) = lines.next() {
                                writeln!(self.get_writer(), "{first_line}")?;
                            }

                            while end.line() >= line {
                                if let Some(token_line) = lines.next() {
                                    line += 1;
                                    self.write_line_prefix(line_prefix, line, emit_linenos)?;
                                    writeln!(self.get_writer(), "{token_line}")?;
                                } else {
                                    break;
                                }
                            }
                            // last empty line, we print it if we didnt reach the error-end line yet
                            if end.line() > line && token.value.ends_with('\n') {
                                line += 1;
                                self.write_line_prefix(line_prefix, line, emit_linenos)?;
                                writeln!(self.get_writer())?;
                            }
                        };

                        self.ensure_newline()?;
                        self.write_callout(callout, level, start_column, len)?;
                        if let Some(hint) = hint {
                            self.write_hint(hint, start_column, len)?;
                        }
                    }
                }
                self.write_line_prefix(line_prefix, line, emit_linenos)?;
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
                Token::TestLiteral(_, _) | Token::StringLiteral(_) | Token::HereDocLiteral(_) => {
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
                Token::HereDocStart => {
                    writeln!(self.get_writer(), r#"""""#)?;
                    // TODO indentation sensing in heredoc's
                    line += 1;
                    self.reset()?;
                }
                Token::HereDocEnd => {
                    write!(self.get_writer(), r#"""""#)?;
                    // no line increment
                    self.reset()?;
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
                            write!(self.get_writer(), "\\\n{line:5} | ")?;
                            self.reset()?;
                            c.set_intense(true).set_fg(Some(Color::Magenta));
                        }
                        write!(self.get_writer(), "{}{l}", " ".repeat(*indent))?;
                    }
                    self.reset()?;
                    write!(self.get_writer(), "|")?;
                }
                _ => write!(self.get_writer(), "{}", x.value)?,
            }

            self.reset()?;
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
            if !printed_error || start.line() == line {
                let (start_column, len) = if start.line() == end.line() {
                    let start_column = start.column();
                    (
                        start_column,
                        end.column().saturating_sub(start_column).max(1),
                    )
                } else {
                    // multi-line token, use only the last lines content for addressing
                    (1, end.column().saturating_sub(1))
                };

                // write token if given
                if let Some(token) = token {
                    // handle the case where we have no tokens, thus no line prefix has been printed yet
                    // this happens if the first expression is faulty
                    if line == 0 {
                        line = token.range.start().line();
                        self.write_line_prefix(line_prefix, line, emit_linenos)?;
                    }

                    let mut lines = token.value.lines();
                    if let Some(first_line) = lines.next() {
                        writeln!(self.get_writer(), "{first_line}")?;
                    }

                    while end.line() >= line {
                        if let Some(token_line) = lines.next() {
                            line += 1;
                            self.write_line_prefix(line_prefix, line, emit_linenos)?;
                            writeln!(self.get_writer(), "{token_line}")?;
                        } else {
                            break;
                        }
                    }
                    // last empty line, we print it if we didnt reach the error-end line yet
                    if end.line() > line && token.value.ends_with('\n') {
                        line += 1;
                        self.write_line_prefix(line_prefix, line, emit_linenos)?;
                        writeln!(self.get_writer())?;
                    }
                }
                // write callout and hint
                self.ensure_newline()?;
                self.write_callout(callout, level, start_column, len)?;
                if let Some(hint) = hint {
                    self.write_hint(hint, start_column, len)?;
                }
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
    /// Takes a error and creates a highlighted version
    /// # Errors
    /// on io errors
    pub fn error_to_string(e: &crate::errors::Error) -> io::Result<String> {
        let mut h = Dumb::default();
        h.format_error(e)?;
        h.finalize()?;

        Ok(h.to_string())
    }
    /// Creates a new highlighter
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Highlighter for Dumb {
    type W = Vec<u8>;
    fn get_writer(&mut self) -> &mut Self::W {
        &mut self.buff
    }

    fn ensure_newline(&mut self) -> std::result::Result<(), io::Error> {
        match self.buff.last() {
            Some(0xa) => (), // ok
            _ => self.buff.push(0xa),
        }
        Ok(())
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
    /// Internal buffer
    pub buff: Buffer,
}

// This is a terminal highlighter it simply adds colors
// so we skip it in tests.

impl Term {
    /// create a Term Highlighter emitting to stdout
    #[must_use]
    pub fn stdout() -> Self {
        let color = if atty::is(atty::Stream::Stdout) {
            ColorChoice::Auto
        } else {
            // don't use color when stdout is not a tty
            ColorChoice::Never
        };
        let bufwtr = BufferWriter::stdout(color);
        let buff = bufwtr.buffer();
        Self { bufwtr, buff }
    }

    /// create a Term Highlighter emitting to stderr
    #[must_use]
    pub fn stderr() -> Self {
        let color = if atty::is(atty::Stream::Stderr) {
            ColorChoice::Auto
        } else {
            // don't use color when stdout is not a tty
            ColorChoice::Never
        };

        let bufwtr = BufferWriter::stderr(color);
        let buff = bufwtr.buffer();
        Self { bufwtr, buff }
    }
}

impl Default for Term {
    /// Creates a new highlighter
    fn default() -> Self {
        Term::stdout()
    }
}

impl Highlighter for Term {
    type W = Buffer;
    fn set_color(&mut self, spec: &mut ColorSpec) -> std::result::Result<(), io::Error> {
        self.buff.set_color(spec)
    }
    fn reset(&mut self) -> std::result::Result<(), io::Error> {
        self.buff.reset()
    }
    fn finalize(&mut self) -> std::result::Result<(), io::Error> {
        self.bufwtr.print(&self.buff)
    }
    fn get_writer(&mut self) -> &mut Self::W {
        &mut self.buff
    }
    fn ensure_newline(&mut self) -> std::result::Result<(), io::Error> {
        match strip_ansi_escapes::strip(self.buff.as_slice()).last() {
            Some(0xa) => Ok(()),
            _ => writeln!(self.buff),
        }
    }
}

impl ToString for Term {
    fn to_string(&self) -> String {
        String::from_utf8_lossy(self.buff.as_slice()).to_string()
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
        .skip_while(|t| t.span.end().line() < start.line())
        .take_while(|t| t.span.end().line() <= end.line())
        .collect()
}
