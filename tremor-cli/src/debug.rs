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

use crate::{
    cli::{Dbg, DbgAst, DbgCommand, DbgDot, DbgOpts, DbgSrc},
    util::{get_source_kind, SourceKind},
};
use crate::{env, errors::Result};
use io::BufReader;
use lexer::Tokenizer;
use std::io::Write;
use std::io::{self, Read};
use termcolor::{Color, ColorSpec};
use tremor_common::ids::OperatorIdGen;
use tremor_script::deploy::Deploy;
use tremor_script::highlighter::{Dumb as TermNoHighlighter, Highlighter, Term as TermHighlighter};
use tremor_script::lexer::{self, Token};
use tremor_script::pos::{Span, Spanned};
use tremor_script::query::Query;
use tremor_script::script::Script;

struct DbgData {
    opts: DbgOpts,
    kind: SourceKind,
    src: String,
    raw: String,
}

impl Dbg {
    pub(crate) fn run(&self) -> Result<()> {
        let mut opts = self.opts;
        // Do not highlist or put banner when raw provided raw flag.
        opts.no_highlight = opts.no_highlight || opts.raw;
        opts.no_banner = opts.no_banner || opts.raw;

        if opts.no_highlight {
            let mut h = TermNoHighlighter::new();
            let r = match &self.command {
                DbgCommand::Dot(dot) => dot.run(&mut h, opts),
                DbgCommand::Ast(ast) => ast.run(&mut h, opts),
                DbgCommand::Lex(lex) => lex.run_lex(&mut h, opts),
                DbgCommand::Src(src) => src.run_src(&mut h, opts),
            };

            h.finalize()?;
            h.reset()?;
            println!("{}", h.to_string());
            r?;
        } else {
            let mut h = TermHighlighter::default();
            let r = match &self.command {
                DbgCommand::Dot(dot) => dot.run(&mut h, opts),
                DbgCommand::Ast(ast) => ast.run(&mut h, opts),
                DbgCommand::Lex(lex) => lex.run_lex(&mut h, opts),
                DbgCommand::Src(src) => src.run_src(&mut h, opts),
            };
            h.finalize()?;
            h.reset()?;
            r?;
        };
        Ok(())
    }
}

fn banner<W>(h: &mut W, opts: DbgOpts, section: &str, detail: &str) -> Result<()>
where
    W: Highlighter,
{
    if !opts.no_banner {
        let mut banner = ColorSpec::new();
        let mut banner = banner.set_fg(Some(Color::Green));

        h.set_color(&mut banner)?;
        let spec = format!(
            "\n\n****************\n* {} - {}\n****************\n\n",
            section, detail
        );
        write!(h.get_writer(), "{}", spec,)?;
    }
    Ok(())
}

fn preprocessed_tokens<'input>(
    _data: &DbgData,
    input: &'input mut String,
) -> Result<Vec<Spanned<'input>>> {
    let lexemes: Vec<_> = lexer::Tokenizer::new(input)
        .filter_map(std::result::Result::ok)
        .collect();
    Ok(lexemes)
}

impl DbgSrc {
    pub fn run_src<W>(&self, h: &mut W, opts: DbgOpts) -> Result<()>
    where
        W: Highlighter,
    {
        let data = load_data(&self.script, opts)?;
        banner(h, opts, "Source", "Source code listing")?;
        let mut raw_src = data.raw.clone();

        match &data.kind {
            SourceKind::Tremor | SourceKind::Json => {
                if self.preprocess {
                    let lexemes = preprocessed_tokens(&data, &mut raw_src)?;
                    h.highlight(None, &lexemes, "", !data.opts.raw, None)?;
                } else {
                    Script::highlight_script_with(&data.raw, h, !&data.opts.raw)?;
                }
            }
            SourceKind::Trickle => {
                if self.preprocess {
                    let lexemes = preprocessed_tokens(&data, &mut raw_src)?;
                    h.highlight(None, &lexemes, "", !data.opts.raw, None)?;
                } else {
                    Query::highlight_script_with(&data.raw, h, !data.opts.raw)?;
                }
            }
            SourceKind::Troy => {
                if self.preprocess {
                    let lexemes = preprocessed_tokens(&data, &mut raw_src)?;
                    h.highlight(None, &lexemes, "", !data.opts.raw, None)?;
                } else {
                    Deploy::highlight_script_with(&data.raw, h, !data.opts.raw)?;
                }
            }

            SourceKind::Unsupported(Some(t)) => error!("Unsupported: {}", t),
            SourceKind::Unsupported(None) => error!("Unsupported: no file type"),
        }

        Ok(())
    }
    fn run_lex<W>(&self, h: &mut W, opts: DbgOpts) -> Result<()>
    where
        W: Highlighter,
    {
        let data = load_data(&self.script, opts)?;
        if self.preprocess {
            banner(
                h,
                opts,
                "Lexemes",
                "Lexical token stream after preprocessing",
            )?;
            let mut raw_src = data.raw.clone();
            let lexemes = preprocessed_tokens(&data, &mut raw_src)?;
            dbg_tokens(h, lexemes)?;
        } else {
            banner(
                h,
                opts,
                "Lexemes",
                "Lexical token stream before preprocessing",
            )?;
            let lexemes: Vec<_> = Tokenizer::new(&data.raw).tokenize_until_err().collect();
            dbg_tokens(h, lexemes)?;
        }
        h.reset()?;
        Ok(())
    }
}

fn dbg_tokens<W>(h: &mut W, lexemes: Vec<Spanned>) -> Result<()>
where
    W: Highlighter,
{
    let mut default = ColorSpec::new();
    let mut line = ColorSpec::new();
    let mut line = line.set_fg(Some(Color::Blue));
    let mut directive = ColorSpec::new();
    let mut directive = directive.set_fg(Some(Color::White));

    for l in lexemes {
        let Spanned {
            span:
                Span {
                    start,
                    end,
                    pp_start,
                    pp_end,
                },
            value,
        } = l;
        match &value {
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
                        start.line(), start.column(), end.line(), end.column()
                    );
                    write!(h.get_writer(), "{:^16} \u{2219}    ", line_spec,)?;
                    h.set_color(&mut directive)?;
                    writeln!(h.get_writer(), " #!config ")?;
                }
                _other_token => {
                    h.set_color(&mut line)?;
                    let line_spec = format!(
                        "{}:{} - {}:{}",
                        start.line(), start.column(), end.line(), end.column()
                    );
                    write!(h.get_writer(), "{:^16} \u{2219}    ", line_spec,)?;
                    h.set_color(&mut default)?;
                    write!(
                        &mut h.get_writer(),
                        " {:<30}    \u{2219}    ",
                        format!("{:?}", value).split('(').collect::<Vec<&str>>()[0]
                    )?;
                    h.highlight(
                        None,
                        &[Spanned {
                            span: Span { start, end, pp_start, pp_end },
                            value,
                        }],
                        "",
                        false,
                        None
                    )?;
                }
            }
    }
    Ok(())
}

impl DbgAst {
    fn run<W>(&self, h: &mut W, opts: DbgOpts) -> Result<()>
    where
        W: Highlighter,
    {
        let data = load_data(&self.script, opts)?;
        banner(h, opts, "AST", "Abstract Syntax Tree")?;

        let env = env::setup()?;
        match data.kind {
            SourceKind::Tremor | SourceKind::Json => {
                match Script::parse(&env.module_path, &data.src, data.raw.clone(), &env.fun) {
                    Ok(runnable) => {
                        let ast = if self.exprs_only {
                            simd_json::to_string_pretty(&runnable.script.suffix().exprs)?
                        } else {
                            simd_json::to_string_pretty(&runnable.script.suffix())?
                        };
                        println!();
                        Script::highlight_script_with(&ast, h, !data.opts.raw)?;
                    }
                    Err(e) => {
                        if let Err(e) = Script::format_error_from_script(&data.raw, h, &e) {
                            eprintln!("Error: {}", e);
                        };
                    }
                }
            }
            SourceKind::Trickle => {
                match Query::parse(
                    &env.module_path,
                    &data.src,
                    &data.raw,
                    vec![],
                    &env.fun,
                    &env.aggr,
                ) {
                    Ok(runnable) => {
                        let ast = simd_json::to_string_pretty(&runnable.query.suffix())?;
                        println!();
                        Script::highlight_script_with(&ast, h, !data.opts.raw)?;
                    }
                    Err(e) => {
                        if let Err(e) = Script::format_error_from_script(&data.raw, h, &e) {
                            eprintln!("Error: {}", e);
                        };
                    }
                };
            }
            SourceKind::Troy => {
                match Deploy::parse(
                    &env.module_path,
                    &data.src,
                    &data.raw,
                    vec![],
                    &env.fun,
                    &env.aggr,
                ) {
                    Ok(runnable) => {
                        let ast = simd_json::to_string_pretty(&runnable.deploy.suffix())?;
                        println!();
                        Script::highlight_script_with(&ast, h, !data.opts.raw)?;
                    }
                    Err(e) => {
                        if let Err(e) = Script::format_error_from_script(&data.raw, h, &e) {
                            eprintln!("Error: {}", e);
                        };
                    }
                };
            }
            SourceKind::Unsupported(_) => {
                eprintln!("Unsupported");
            }
        };

        h.reset()?;

        Ok(())
    }
}

impl DbgDot {
    fn run<W>(&self, h: &mut W, opts: DbgOpts) -> Result<()>
    where
        W: Highlighter,
    {
        let data = load_data(&self.script, opts)?;

        if data.kind == SourceKind::Trickle {
            let env = env::setup()?;
            match Query::parse(
                &env.module_path,
                &data.src,
                &data.raw,
                vec![],
                &env.fun,
                &env.aggr,
            ) {
                Ok(runnable) => {
                    let mut idgen = OperatorIdGen::new();
                    let g = tremor_pipeline::query::Query(runnable).to_pipe(&mut idgen)?;

                    println!("{}", g.dot);
                }
                Err(e) => {
                    if let Err(e) = Script::format_error_from_script(&data.raw, h, &e) {
                        eprintln!("Error: {}", e);
                    };
                }
            };
        } else if data.kind == SourceKind::Troy {
            let env = env::setup()?;
            match Deploy::parse(
                &env.module_path,
                &data.src,
                &data.raw,
                vec![],
                &env.fun,
                &env.aggr,
            ) {
                Ok(runnable) => {
                    println!("{}", runnable.dot());
                }
                Err(e) => {
                    if let Err(e) = Script::format_error_from_script(&data.raw, h, &e) {
                        eprintln!("Error: {}", e);
                    };
                }
            };
        } else {
            return Err("Dot visualisation is only supported for trickle/troy files.".into());
        }
        Ok(())
    }
}

fn load_data(src: &str, opts: DbgOpts) -> Result<DbgData> {
    let mut raw = String::new();

    let mut buffer: Box<dyn Read> = match src {
        "-" => Box::new(BufReader::new(io::stdin())),
        path => Box::new(BufReader::new(crate::open_file(path, None)?)),
    };
    let kind = match src {
        "-" => SourceKind::Tremor,
        path => get_source_kind(path),
    };
    let src = match src {
        "-" => "<stdin>".to_string(),
        path => path.to_string(),
    };
    buffer.read_to_string(&mut raw)?;

    println!();
    raw.push('\n'); // Ensure last token is whitespace
    let data = DbgData {
        opts,
        kind,
        src,
        raw,
    };

    Ok(data)
}
