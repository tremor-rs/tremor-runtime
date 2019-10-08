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
use crate::interpreter::Cont;
use crate::lexer::{self, TokenFuns};
use crate::pos::Range;
use crate::prelude::*;
use rental::rental;
use serde::Serialize;
use simd_json::borrowed::Value;
use std::boxed::Box;
use std::io::Write;
use std::sync::Arc;

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
#[derive(Debug, PartialEq, PartialOrd, Eq)]
pub struct StmtRentalWrapper {
    pub stmt: Arc<rentals::Stmt>,
}
rental! {
    pub mod rentals {
        use crate::ast;
        use std::borrow::Cow;
        use serde::Serialize;
        use std::sync::Arc;

        #[rental_mut(covariant,debug)]
        pub struct Query {
            script: Box<String>,
            query: ast::Query<'script>,
        }

        #[rental(covariant,debug)]
        pub struct Stmt {
            query: Arc<super::Query>,
            stmt: ast::Stmt<'query>,
        }
    }
}

impl PartialEq for rentals::Stmt {
    fn eq(&self, other: &rentals::Stmt) -> bool {
        self.suffix() == other.suffix()
    }
}

impl Eq for rentals::Stmt {}

impl PartialOrd for rentals::Stmt {
    fn partial_cmp(&self, _other: &rentals::Stmt) -> Option<std::cmp::Ordering> {
        None // NOTE Here be dragons FIXME
    }
}
impl<'run, 'event, 'script> StmtRentalWrapper
where
    'script: 'event,
    'event: 'run,
{
    #[allow(dead_code)] // FIXME remove this shit
    fn with_stmt<'elide>(query: &Query, encumbered_stmt: crate::ast::Stmt<'elide>) -> Self {
        StmtRentalWrapper {
            stmt: Arc::new(rentals::Stmt::new(Arc::new(query.clone()), |_| {
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
            })),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Query {
    pub query: Arc<rentals::Query>,
    pub source: String,
    pub warnings: Vec<Warning>,
    pub locals: usize,
}

impl<'run, 'event, 'script> Query
where
    'script: 'event,
    'event: 'run,
{
    pub fn parse(script: &'script str, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self> {
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

            let (script, local_count, ws) = crate::parser::grammar::QueryParser::new()
                .parse(filtered_tokens)?
                .up_script(reg, aggr_reg)?;

            warnings = ws;
            locals = local_count;
            Ok(script)
        })
        .map_err(|e: rental::RentalError<Error, Box<String>>| e.0)?;

        Ok(Query {
            query: Arc::new(query),
            source,
            locals,
            warnings,
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

    #[allow(dead_code)] // NOTE: Damn dual main and lib crate ...
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
}
