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
use crate::highlighter::{Dumb as DumbHighlighter, Highlighter};
use crate::lexer::{self, TokenFuns};
use crate::pos::Range;
use crate::prelude::*;
use rental::rental;
use std::boxed::Box;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug, PartialEq, PartialOrd, Eq, Clone)]
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

#[cfg_attr(tarpaulin, skip)]
impl PartialEq for rentals::Stmt {
    fn eq(&self, other: &rentals::Stmt) -> bool {
        self.suffix() == other.suffix()
    }
}

impl Eq for rentals::Stmt {}

#[cfg_attr(tarpaulin, skip)]
impl PartialOrd for rentals::Stmt {
    fn partial_cmp(&self, _other: &rentals::Stmt) -> Option<std::cmp::Ordering> {
        None // NOTE Here be dragons FIXME
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
        source.push('\n');

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

    // Simple highlighter nothing to see here.
    #[cfg_attr(tarpaulin, skip)]
    pub fn highlight_script_with<H: Highlighter>(script: &str, h: &mut H) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');
        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        h.highlight(tokens)
    }

    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');

        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        match e.context() {
            (Some(Range(start, end)), _) => {
                h.highlight_runtime_error(tokens, start, end, Some(e.into()))?;
                h.finalize()
            }

            _other => {
                write!(h.get_writer(), "Error: {}", e)?;
                h.finalize()
            }
        }
    }

    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        let mut warnings = self.warnings.clone();
        warnings.sort();
        warnings.dedup();
        for w in &warnings {
            let tokens: Vec<_> = lexer::tokenizer(&self.source).collect();
            h.highlight_runtime_error(tokens, w.outer.0, w.outer.1, Some(w.into()))?;
        }
        h.finalize()
    }

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

#[cfg(test)]
mod test {
    use super::*;

    fn parse(query: &str) {
        let reg = crate::registry();
        let aggr_reg = crate::aggr_registry();
        if let Err(e) = Query::parse(query, &reg, &aggr_reg) {
            eprintln!("{}", e);
            assert!(false)
        } else {
            assert!(true)
        }
    }
    #[test]
    fn for_in_select() {
        parse(r#"select for event of case (a, b) => b end from in into out;"#);
    }

    #[test]
    fn script_with_args() {
        parse(
            r#"
define script test
with
  beep = "beep"
script
  { "beep": "{args.beep}" }
end;

create script beep from test;
create script boop from test
with
  beep = "boop" # override
end;

# Stream ingested data into script with default params
select event from in into beep;

# Stream ingested data into script with overridden params
select event from in into boop;

# Stream script operator synthetic events into out stream
select event from beep into out;
select event from boop into out;        
        "#,
        )
    }
}
