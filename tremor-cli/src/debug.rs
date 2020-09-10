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

use crate::errors::{Error, Result};
use crate::util::*;
use clap::ArgMatches;
use lexer::Tokenizer;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use termcolor::{Color, ColorSpec};
use tremor_script::highlighter::{Dumb as TermNoHighlighter, Highlighter, Term as TermHighlighter};
use tremor_script::lexer;
use tremor_script::lexer::Token;
use tremor_script::path::load as load_module_path;
use tremor_script::pos::{Span, Spanned};
use tremor_script::query::Query;
use tremor_script::registry;
use tremor_script::registry::Registry;
use tremor_script::script::Script;

fn banner<W>(h: &mut W, section: &str, detail: &str) -> Result<()>
where
    W: Highlighter,
{
    let mut banner = ColorSpec::new();
    let mut banner = banner.set_fg(Some(Color::Green));

    h.set_color(&mut banner)?;
    let spec = format!(
        "\n\n****************\n* {} - {}\n****************\n\n",
        section, detail
    );
    write!(h.get_writer(), "{}", spec,)?;

    Ok(())
}

fn dbg_src<W>(h: &mut W, _src_file: &str, src_raw: &mut str) -> Result<()>
where
    W: Highlighter,
{
    banner(h, "Source", "Source code listing")?;
    Script::highlight_script_with(&src_raw, h)?;
    Ok(())
}

fn dbg_tokens<W>(h: &mut W, lexemes: Vec<Result<Spanned<Token>>>) -> Result<()>
where
    W: Highlighter,
{
    let mut default = ColorSpec::new();
    let mut line = ColorSpec::new();
    let mut line = line.set_fg(Some(Color::Blue));
    let mut directive = ColorSpec::new();
    let mut directive = directive.set_fg(Some(Color::White));

    for l in lexemes {
        match l {
            Ok(Spanned {
                span: Span { start, end },
                value,
            }) => match &value {
                // We ignore whitespace and newlines
                Token::Whitespace(_)
                | Token::NewLine
                // TODO Enhance to support doc/mod ( multi-line ) comments
                | Token::DocComment(_)
                | Token::ModComment(_) => (),
                Token::ConfigDirective => {
                    h.set_color(&mut line)?;
                    let line_spec = format!(
                        "{}:{} - {}:{}",
                        start.line, start.column, end.line, end.column
                    );
                    write!(h.get_writer(), "{:^16} \u{2219}    ", line_spec,)?;
                    h.set_color(&mut directive)?;
                    writeln!(h.get_writer(), " #!config ")?;
                }
                Token::LineDirective(_location, file) => {
                    h.set_color(&mut line)?;
                    let line_spec = format!(
                        "{}:{} - {}:{}",
                        start.line, start.column, end.line, end.column
                    );
                    write!(h.get_writer(), "{:^16} \u{2219}    ", line_spec,)?;
                    h.set_color(&mut directive)?;
                    writeln!(h.get_writer(), " #!line {}", file.to_string())?;
                }
                _other_token => {
                    h.set_color(&mut line)?;
                    let line_spec = format!(
                        "{}:{} - {}:{}",
                        start.line, start.column, end.line, end.column
                    );
                    write!(h.get_writer(), "{:^16} \u{2219}    ", line_spec,)?;
                    h.set_color(&mut default)?;
                    write!(
                        &mut h.get_writer(),
                        " {:<30}    \u{2219}    ",
                        format!("{:?}", value).split("(").collect::<Vec<&str>>()[0]
                    )?;
                    h.highlight_no_linenos(
                        None,
                        &[Spanned {
                            span: Span { start, end },
                            value,
                        }],
                    )?;
                }
            },
            Err(e) => println!("ERR> {}", e),
        }
    }

    Ok(())
}
fn dbg_pp<W>(h: &mut W, _src_file: &str, src_raw: &mut str) -> Result<()>
where
    W: Highlighter,
{
    banner(h, "Lexemes", "Lexical token stream after preprocessing")?;

    let lexemes: Vec<Result<Spanned<Token>>> = Tokenizer::new(&src_raw)
        .filter_map(std::result::Result::ok)
        .map(|v| Ok(v))
        .collect();

    dbg_tokens(h, lexemes)?;

    h.reset()?;

    Ok(())
}

fn dbg_lex<W>(h: &mut W, src_file: &str, src_raw: &mut str) -> Result<()>
where
    W: Highlighter,
{
    banner(h, "Lexemes", "Lexical token stream before preprocessing")?;

    let mut include_stack = lexer::IncludeStack::default();
    let cu = include_stack.push(src_raw)?;
    let mut src_raw_string = src_raw.to_string();

    let lexemes: Vec<Result<Spanned<Token>>> = lexer::Preprocessor::preprocess(
        &tremor_script::path::load(),
        &src_file,
        &mut src_raw_string,
        cu,
        &mut include_stack,
    )?
    .into_iter()
    .filter_map(std::result::Result::ok)
    .map(|v| Ok(v))
    .collect();

    dbg_tokens(h, lexemes)?;

    h.reset()?;

    Ok(())
}

fn dbg_ast<W>(h: &mut W, _should_preprocess: bool, src_file: &str, src_raw: &mut str) -> Result<()>
where
    W: Highlighter,
{
    banner(h, "AST", "Abstract Syntax Tree")?;

    let mp = load_module_path();
    let reg: Registry = registry::registry();
    match get_source_kind(src_file) {
        SourceKind::Tremor | SourceKind::Json => {
            match Script::parse(&mp, src_file, src_raw.to_string(), &reg) {
                Ok(runnable) => {
                    let ast = simd_json::to_string_pretty(&runnable.script.suffix())?;
                    println!();
                    Script::highlight_script_with(&ast, h)?;
                }
                Err(e) => {
                    if let Err(e) = Script::format_error_from_script(&src_raw, h, &e) {
                        eprintln!("Error: {}", e);
                    };
                }
            }
        }
        SourceKind::Trickle => {
            let aggr_reg = registry::aggr();
            match Query::parse(&mp, src_file, &src_raw.to_string(), vec![], &reg, &aggr_reg) {
                Ok(runnable) => {
                    let ast = simd_json::to_string_pretty(&runnable.query.suffix())?;
                    println!();
                    Script::highlight_script_with(&ast, h)?;
                }
                Err(e) => {
                    if let Err(e) = Script::format_error_from_script(&src_raw, h, &e) {
                        eprintln!("Error: {}", e);
                    };
                }
            };
        }
        _otherwise => {
            eprintln!("Unsupported");
        }
    };

    h.reset()?;

    Ok(())
}

fn dbg_tremor_source<W>(h: &mut W, matches: &ArgMatches, src: &str) -> Result<()>
where
    W: Highlighter,
{
    let mut raw = String::new();
    let input = File::open(&src);
    if let Err(e) = input {
        eprintln!("Error processing file {}: {}", &src, e);
        // ALLOW: main.rs
        std::process::exit(1);
    }
    input?.read_to_string(&mut raw)?;
    println!();
    raw.push('\n'); // Ensure last token is whitespace

    h.reset()?;
    if matches.is_present("src") {
        if let Err(e) = dbg_src(h, src, &mut raw) {
            return Err(e);
        }
    }
    h.reset()?;

    if matches.is_present("preprocess") {
        if let Err(e) = dbg_pp(h, src, &mut raw) {
            return Err(e);
        }
    }
    h.reset()?;

    if matches.is_present("lex") {
        if let Err(e) = dbg_lex(h, src, &mut raw) {
            return Err(e);
        }
    }
    h.reset()?;

    if matches.is_present("ast") {
        if let Err(e) = dbg_ast(h, !matches.is_present("no-highlight"), src, &mut raw) {
            return Err(e);
        }
    }

    Ok(())
}

fn dbg_trickle_source<W>(h: &mut W, matches: &ArgMatches, src: &str) -> Result<()>
where
    W: Highlighter,
{
    let mut raw = String::new();
    let input = File::open(&src);
    if let Err(e) = input {
        eprintln!("Error processing file {}: {}", &src, e);
        // ALLOW: main.rs
        std::process::exit(1);
    }
    input?.read_to_string(&mut raw)?;
    println!();
    raw.push('\n'); // Ensure last token is whitespace

    if matches.is_present("src") {
        if let Err(e) = dbg_src(h, src, &mut raw) {
            return Err(e);
        }
    }
    h.reset()?;

    if matches.is_present("preprocess") {
        if let Err(e) = dbg_pp(h, src, &mut raw) {
            return Err(e);
        }
    }
    h.reset()?;

    if matches.is_present("lex") {
        if let Err(e) = dbg_lex(h, src, &mut raw) {
            return Err(e);
        }
    }
    h.reset()?;

    if matches.is_present("ast") {
        if let Err(e) = dbg_ast(h, !matches.is_present("no-highlight"), src, &mut raw) {
            return Err(e);
        }
    }
    h.reset()?;

    Ok(())
}

fn run_debug<W>(script_file: &str, h: &mut W, matches: &ArgMatches) -> Result<()>
where
    W: Highlighter,
{
    match get_source_kind(&script_file) {
        SourceKind::Tremor | SourceKind::Json => dbg_tremor_source(h, matches, &script_file)?,
        SourceKind::Trickle => dbg_trickle_source(h, matches, &script_file)?,
        _otherwise => {
            eprintln!("Error: Unable to execute source: {}", &script_file);
            // ALLOW: main.rs
            std::process::exit(1);
        }
    };

    Ok(())
}

pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    let script_file = matches
        .value_of("SCRIPT")
        .ok_or_else(|| Error::from("No script file provided"))?;

    if matches.is_present("no-highlight") {
        let mut h = TermNoHighlighter::new();
        match run_debug(script_file, &mut h, matches) {
            Ok(()) => println!("{}", h.to_string()),
            Err(_e) => {}
        };
    } else {
        let mut h = TermHighlighter::new();
        match run_debug(script_file, &mut h, matches) {
            Ok(()) => {
                h.finalize()?;
            }
            Err(_e) => {}
        };
    };

    Ok(())
}
