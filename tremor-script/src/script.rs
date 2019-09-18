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
use crate::highlighter::{DumbHighlighter, Highlighter};
pub use crate::interpreter::AggrType;
use crate::interpreter::{Cont, ExecOpts, LocalStack};
use crate::lexer::{self, TokenFuns};
use crate::parser::grammar;
use crate::pos::Range;
use crate::registry::{AggrRegistry, Context, Registry};
use crate::stry;
use serde::Serialize;
use simd_json::borrowed::Value;
use std::io::Write;
use std::sync::Arc;
use std::marker::Send;

#[derive(Debug, Serialize, PartialEq)]
pub enum Return<'event> {
    Emit {
        value: Value<'event>,
        port: Option<String>,
    },
    Drop,
    EmitEvent {
        port: Option<String>,
    },
}

impl<'run, 'event> From<Cont<'run, 'event>> for Return<'event>
where
    'event: 'run,
{
    // This clones the data since we're returning it out of the scope of the
    // esecution - we might want to investigate if we can get rid of this in some cases.
    fn from(v: Cont<'run, 'event>) -> Self {
        match v {
            Cont::Cont(value) => Return::Emit {
                value: value.into_owned(),
                port: None,
            },
            Cont::Emit(value, port) => Return::Emit { value, port },
            Cont::EmitEvent(port) => Return::EmitEvent { port },
            Cont::Drop => Return::Drop,
        }
    }
}

#[derive(Debug)] // FIXME rename ScriptRentalWrapper
pub struct Script<Ctx>
where
    Ctx: Context + Serialize + 'static,
{
    // TODO: This should probably be pulled out to allow people wrapping it themselves
    pub script: rentals::Script<Ctx>,
    pub source: String,
    pub warnings: Vec<Warning>,
    pub locals: usize,
}

#[derive(Debug)]
pub struct QueryRentalWrapper<Ctx>
where
   Ctx: Context + Serialize + 'static,
{
    pub query: Arc<rentals::Query<Ctx>>,
    pub source: String,
    pub warnings: Vec<Warning>,
    pub locals: usize,
}

#[derive(Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct StmtRentalWrapper<Ctx>
where
   Ctx: Context + Serialize + 'static,
{
    pub stmt: rentals::Stmt<Ctx>,
}

rental! {
    pub mod rentals {
        use crate::ast;
        use crate::Context;
        use std::borrow::Cow;
        use serde::Serialize;
        use std::sync::Arc;
        use std::marker::Send;

        #[rental_mut(covariant,debug)]
        pub struct Script<Ctx: Context + Clone + Serialize + 'static> {
            script: Box<String>,
            parsed: ast::Script<'script, Ctx>
        }

        #[rental_mut(covariant,debug)]
        pub struct Query<Ctx>
            where
                Ctx: Context + Serialize +'static {
            script: Box<String>,
            query: ast::Query<'script, Ctx>,
        }

        #[rental(covariant,debug)]
        pub struct Stmt<Ctx>
            where
                Ctx: Context + Serialize +'static {
            query: Arc<Query<Ctx>>,
            stmt: ast::Stmt<'query, Ctx>,
        }
    }
}

unsafe 
impl<Ctx> Send for rentals::Query<Ctx> 
where
    Ctx: Context + Serialize +'static
{
    // Nothing to do
}

impl<Ctx> PartialEq for rentals::Stmt<Ctx>
where
    Ctx: Context + Serialize +'static
{
    fn eq(&self, other: &rentals::Stmt<Ctx>) -> bool {
        self.suffix() == other.suffix()
    }
}

impl<Ctx> Eq for rentals::Stmt<Ctx>
where
    Ctx: Context + Serialize +'static
{
}

impl<Ctx> PartialOrd for rentals::Stmt<Ctx>
where
    Ctx: Context + Serialize +'static
{
    fn partial_cmp(&self, _other: &rentals::Stmt<Ctx>) -> Option<std::cmp::Ordering> {
        None // NOTE Here be dragons FIXME
    }
}

impl<Ctx> std::hash::Hash for rentals::Stmt<Ctx>
where
    Ctx: Context + Serialize +'static
{
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        // self.suffix().stmt.hash(state);
        // NOTE Heinz made me do it FIXHEINZ FIXME TODO BADGER
        // .unwrap() :)
    }}

impl<'run, 'event, 'script, Ctx> Script<Ctx>
where
    Ctx: Context + Serialize + 'static,
    'script: 'event,
    'event: 'run,
{
    pub fn parse(
        script: &'script str,
        reg: &Registry<Ctx>,
        // aggr_reg: &AggrRegistry, - we really should shadow and provide a nice hygienic error FIXME but not today
    ) -> Result<Self> {
        let mut source = script.to_string();

        let mut warnings = vec![];
        let mut locals = 0;

        // FIXME make lexer EOS tolerant to avoid this kludge
        source.push(' ');

        let script = rentals::Script::try_new(Box::new(source.clone()), |src| {
            let lexemes: Result<Vec<_>> = lexer::tokenizer(src.as_str()).collect();
            let mut filtered_tokens = Vec::new();

            for t in lexemes? {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }

            let fake_aggr_reg = AggrRegistry::default();
            let (script, local_count, ws) = grammar::ScriptParser::new()
                .parse(filtered_tokens)?
                .up_script(reg, &fake_aggr_reg)?;
            warnings = ws;
            locals = local_count;
            Ok(script)
        })
        .map_err(|e: rental::RentalError<Error, Box<String>>| e.0)?;

        Ok(Script {
            script,
            source,
            locals,
            warnings: warnings,
        })
    }

    pub fn highlight_script_with<H: Highlighter>(script: &str, h: &mut H) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        h.highlight(tokens)
    }

    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        match e.context() {
            (Some(Range(start, end)), _) => {
                h.highlight_runtime_error(tokens, start, end, Some(e.into()))
            }

            _other => {
                let _ = write!(h.get_writer(), "Error: {}", e);
                h.finalize()
            }
        }
    }

    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
            let tokens: Vec<_> = lexer::tokenizer(&self.source).collect();
            h.highlight_runtime_error(tokens, w.outer.0, w.outer.1, Some(w.into()))?;
        }
        Ok(())
    }

    #[allow(dead_code)] // NOTE: Dman dual main and lib crate ...
    pub fn format_error(&self, e: Error) -> String {
        let mut h = DumbHighlighter::default();
        if self.format_error_with(&mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }

    pub fn format_error_with<H: Highlighter>(&self, h: &mut H, e: &Error) -> std::io::Result<()> {
        Self::format_error_from_script(&self.source, h, e)
    }

    pub fn run(
        &'script self,
        context: &'run Ctx,
        aggr: AggrType,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
    ) -> Result<Return<'event>> {
        // FIXME: find a way to pre-allocate this
        let mut local = LocalStack::with_size(self.locals);

        let script = self.script.suffix();
        let mut exprs = script.exprs.iter().peekable();
        let opts = ExecOpts {
            result_needed: true,
            aggr,
        };
        while let Some(expr) = exprs.next() {
            if exprs.peek().is_none() {
                match stry!(expr.run(
                    opts.with_result(),
                    context,
                    &script.aggregates,
                    event,
                    meta,
                    &mut local,
                    &script.consts,
                )) {
                    Cont::Drop => return Ok(Return::Drop),
                    Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                    Cont::EmitEvent(port) => {
                        return Ok(Return::EmitEvent { port });
                    }
                    Cont::Cont(v) => {
                        return Ok(Return::Emit {
                            value: v.into_owned(),
                            port: None,
                        })
                    }
                }
            } else {
                match stry!(expr.run(
                    opts.without_result(),
                    context,
                    &script.aggregates,
                    event,
                    meta,
                    &mut local,
                    &script.consts,
                )) {
                    Cont::Drop => return Ok(Return::Drop),
                    Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                    Cont::EmitEvent(port) => {
                        return Ok(Return::EmitEvent { port });
                    }
                    Cont::Cont(_v) => (),
                }
            }
        }
        Ok(Return::Emit {
            value: Value::Null,
            port: None,
        })
    }
}

impl<'run, 'event, 'script, Ctx> QueryRentalWrapper<Ctx>
where
    Ctx: Context + Serialize + 'static,
    'script: 'event,
    'event: 'run,
{
    pub fn parse(script: &'script str, reg: &Registry<Ctx>, aggr_reg: &AggrRegistry) -> Result<Self> {        
        let mut source = script.to_string();

        let mut warnings = vec![];
        let mut locals = 0;

        // FIXME make lexer EOS tolerant to avoid this kludge
        source.push(' ');

        let query = rentals::Query::try_new(Box::new(source.clone()), |src| {
            let lexemes: Result<Vec<_>> = lexer::tokenizer(src.as_str()).collect();
            let mut filtered_tokens = Vec::new();

            for t in lexemes? {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }

            let (script, local_count, ws) = grammar::QueryParser::new()
                .parse(filtered_tokens)?
                .up_script(reg, aggr_reg)?;

            warnings = ws;
            locals = local_count;
            Ok(script)
        })
        .map_err(|e: rental::RentalError<Error, Box<String>>| e.0)?;

        Ok(QueryRentalWrapper {
            query: Arc::new(query),
            source,
            locals,
            warnings,
        })
    }
}

impl<'run, 'event, 'script, Ctx> StmtRentalWrapper<Ctx>
where
    Ctx: Context + Serialize + 'static,
    'script: 'event,
    'event: 'run,
{
    #[allow(dead_code)] // FIXME remove this shit
    fn with_stmt<'elide>(query: &QueryRentalWrapper<Ctx>, encumbered_stmt: crate::ast::Stmt<'elide, Ctx>) -> Self {
        StmtRentalWrapper { 
            stmt: rentals::Stmt::new(query.query.clone(), |_| {
                // NOTE We are eliding the lifetime 'elide here which is the purpose
                // of the rental and the rental wrapper, so we disabuse mem::trensmute
                // to avoid lifetime elision/mapping warnings from the rust compiler which
                // under ordinary conditions are correct, but under rental conditions are
                // exactly what we desire to avoid
                //
                // This is *safe* as the rental guarantees that Stmt and Query lifetimes
                // are compatible by definition in their rentals::{Query,Struct} co-definitions
                //
                unsafe { std::mem::transmute(encumbered_stmt) }
            })
        }
    }
}
