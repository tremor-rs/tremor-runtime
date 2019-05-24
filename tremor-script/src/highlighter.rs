use crate::lexer::{LexerError, Token, TokenFuns, TokenSpan};
use crate::pos::*;
use lalrpop_util::ParseError;
use std::io::Write;
use termcolor::{Buffer, BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

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
        self.highlight_errors(tokens, None, None)
    }

    fn highlight_parser_error(
        &mut self,
        tokens: Vec<Result<TokenSpan, LexerError>>,
        error: ParseError<Location, Token, LexerError>,
    ) -> Result<(), std::io::Error> {
        self.highlight_errors(tokens, None, None)
    }

    fn highlight_runtime_error(
        &mut self,
        tokens: Vec<Result<TokenSpan, LexerError>>,
        expr_start: Location,
        expr_end: Location,
        error: Option<(Location, Location, String)>,
    ) -> Result<(), std::io::Error> {
        let extracted = extract(tokens, expr_start, expr_end);
        self.highlight_errors(extracted, None, error)
    }

    fn highlight_errors(
        &mut self,
        tokens: Vec<Result<TokenSpan, LexerError>>,
        error: Option<ParseError<Location, Token, LexerError>>,
        runtime_error: Option<(Location, Location, String)>,
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
                    if let Some((start, end, callout)) = &runtime_error {
                        if start.line.0 == line - 1 {
                            printed_error = true;
                            let len = (end.column.0 - start.column.0) as usize;
                            let prefix = String::from(" ").repeat(start.column.0 as usize - 1);
                            let underline = String::from("^").repeat(len);
                            self.set_color(ColorSpec::new().set_bold(true))?;
                            write!(self.get_writer(), "      | {}", prefix)?;
                            self.set_color(
                                ColorSpec::new().set_bold(false).set_fg(Some(Color::Red)),
                            )?;
                            write!(self.get_writer(), "{} {}\n", underline, callout)?;
                        }
                    }
                    self.set_color(ColorSpec::new().set_bold(true))?;
                    write!(self.get_writer(), "{:5} | ", line)?;
                    self.reset()?;
                }
                /*
                if let Some((start, end, _callout)) = &runtime_error {
                    if t.span.start().absolute >= start.absolute
                        && t.span.end().absolute <= end.absolute
                    {
                        self.set_color(
                            ColorSpec::new()
                                .set_bold(true)
                                .set_intense(true)
                                .set_bg(Some(Color::Green))
                                .set_fg(Some(Color::Blue)),
                        )?;
                        in_error = true;
                    } else {
                        in_error = false;
                    }
                }
                */
                match (t, error.clone()) {
                    (ref tt, Some(ParseError::InvalidToken { location: loc }))
                        if loc == tt.span.start() =>
                    {
                        self.set_color(
                            ColorSpec::new()
                                .set_bold(true)
                                .set_intense(true)
                                .set_bg(Some(Color::Blue))
                                .set_fg(Some(Color::White)),
                        )?;
                        write!(self.get_writer(), "{}", tt.value.to_string())?;
                    }
                    (
                        ref tt,
                        Some(ParseError::UnrecognizedToken {
                            token: Some((start, Token::BadToken(ref lexeme), _end)),
                            ..
                        }),
                    ) if start == tt.span.start() => {
                        self.set_color(
                            ColorSpec::new()
                                .set_bold(true)
                                .set_intense(true)
                                .set_bg(Some(Color::Red))
                                .set_fg(Some(Color::White)),
                        )?;
                        write!(self.get_writer(), "{}", lexeme)?;
                    }
                    (
                        ref tt,
                        Some(ParseError::UnrecognizedToken {
                            token: Some((start, ref legal_token, _end)),
                            ..
                        }),
                    ) if start == tt.span.start() => {
                        self.set_color(
                            ColorSpec::new()
                                .set_bold(true)
                                .set_intense(true)
                                .set_bg(Some(Color::Green))
                                .set_fg(Some(Color::Black)),
                        )?;
                        write!(self.get_writer(), "{}", legal_token.to_string())?;
                    }
                    (ref t, Some(ParseError::ExtraToken { .. })) => {
                        self.set_color(
                            ColorSpec::new()
                                .set_bold(true)
                                .set_intense(true)
                                .set_bg(Some(Color::Magenta))
                                .set_fg(Some(Color::White)),
                        )?;
                        write!(self.get_writer(), "{}", t.value.to_string())?;
                    }
                    (
                        ref t,
                        Some(ParseError::User {
                            error: ref _lexer_error,
                        }),
                    ) => {
                        self.set_color(
                            ColorSpec::new()
                                .set_bold(true)
                                .set_intense(true)
                                .set_bg(Some(Color::Red))
                                .set_fg(Some(Color::White)),
                        )?;
                        write!(self.get_writer(), "{}", t)?;
                    }
                    (ref t, _) => {
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
                    }
                }
            };
        }
        if let Some((start, end, callout)) = &runtime_error {
            if !printed_error && start.line.0 == line {
                //write!(self.get_writer(), "\n")?;
                printed_error = true;
                let len = (end.column.0 - start.column.0) as usize;
                let prefix = String::from(" ").repeat(start.column.0 as usize - 1);
                let underline = String::from("^").repeat(len);
                self.set_color(ColorSpec::new().set_bold(true))?;
                write!(self.get_writer(), "      | {}", prefix)?;
                self.set_color(ColorSpec::new().set_bold(false).set_fg(Some(Color::Red)))?;
                write!(self.get_writer(), "{} {}\n", underline, callout)?;
            }
        }

        writeln!(&mut self.get_writer(), "\n----")?;
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
