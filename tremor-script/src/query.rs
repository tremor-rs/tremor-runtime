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

use crate::ast::{self, Warning};
use crate::errors::*;
use crate::highlighter::{Dumb as DumbHighlighter, Highlighter};
use crate::lexer;
use crate::path::ModulePath;
use crate::pos::Range;
use crate::prelude::*;
use rental::rental;
use std::boxed::Box;
use std::io::Write;
use std::sync::Arc;

/// Rental wrapper
#[derive(Debug, PartialEq, PartialOrd, Eq, Clone)]
pub struct StmtRentalWrapper {
    /// Statement
    pub stmt: Arc<rentals::Stmt>,
}
impl StmtRentalWrapper {
    /// Gets the wrapped statement
    pub fn suffix(&self) -> &ast::Stmt {
        self.stmt.suffix()
    }
}

rental! {
    mod rentals {
        use crate::ast;
        use std::borrow::Cow;
        use serde::Serialize;
        use std::sync::Arc;

        /// rental around Query
        #[rental_mut(covariant,debug)]
        pub struct Query {
            script: Box<String>,
            query: ast::Query<'script>,
        }

        /// rental around Stmt
        #[rental(covariant,debug)]
        pub struct Stmt {
            query: Arc<super::Query>,
            stmt: ast::Stmt<'query>,
        }
    }
}

pub use rentals::Query as QueryRental;
pub use rentals::Stmt as StmtRental;

#[cfg_attr(tarpaulin, skip)]
impl PartialEq for rentals::Stmt {
    fn eq(&self, other: &Self) -> bool {
        self.suffix() == other.suffix()
    }
}

impl Eq for rentals::Stmt {}

#[cfg_attr(tarpaulin, skip)]
impl PartialOrd for rentals::Stmt {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None // NOTE Here be dragons FIXME
    }
}

/// A tremor query
#[derive(Debug, Clone)]
pub struct Query {
    /// The query
    pub query: Arc<rentals::Query>,
    /// Source of the query
    pub source: String,
    /// Warnings emitted by the script
    pub warnings: Vec<Warning>,
    /// Number of local variables (should be 0)
    pub locals: usize,
}

impl<'run, 'event, 'script> Query
where
    'script: 'event,
    'event: 'run,
{
    /// Borrows the query
    pub fn suffix(&self) -> &ast::Query {
        self.query.suffix()
    }
    /// Parses a string into a query
    pub fn parse(
        module_path: &ModulePath,
        file_name: &str,
        script: &'script str,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> Result<Self> {
        let mut source = script.to_string();

        let mut warnings = vec![];
        let mut locals = 0;

        // FIXME make lexer EOS tolerant to avoid this kludge
        source.push('\n');

        let query = rentals::Query::try_new(Box::new(source.clone()), |src: &mut String| {
            let lexemes: Vec<_> = lexer::Preprocessor::preprocess(module_path, file_name, src)?;
            let filtered_tokens = lexemes
                .into_iter()
                .filter_map(Result::ok)
                .filter(|t| !t.value.is_ignorable());

            let (script, local_count, ws) = crate::parser::g::QueryParser::new()
                .parse(filtered_tokens)?
                .up_script(reg, aggr_reg)?;

            warnings = ws;
            locals = local_count;
            Ok(script)
        })
        .map_err(|e: rental::RentalError<Error, Box<String>>| e.0)?;

        Ok(Self {
            query: Arc::new(query),
            source,
            locals,
            warnings,
        })
    }

    /// Highlights a script with a given highlighter.
    #[cfg_attr(tarpaulin, skip)]
    pub fn highlight_script_with<H: Highlighter>(script: &str, h: &mut H) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');
        let tokens: Vec<_> = lexer::Tokenizer::new(&script).collect();
        h.highlight(&tokens)
    }

    /// Preprocessesa and highlights a script with a given highlighter.
    #[cfg_attr(tarpaulin, skip)]
    pub fn highlight_preprocess_script_with<H: Highlighter>(
        file_name: &str,
        script: &'script str,
        h: &mut H,
    ) -> std::io::Result<()> {
        let mut s = script.to_string();
        let tokens: Vec<_> =
            lexer::Preprocessor::preprocess(&crate::path::load(), &file_name, &mut s)
                .expect("Did not preprocess ok");
        h.highlight(&tokens)
    }

    /// Format an error given a script source.
    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');

        let tokens: Vec<_> = lexer::Tokenizer::new(&script).collect();
        match e.context() {
            (Some(Range(start, end)), _) => {
                h.highlight_runtime_error(&tokens, start, end, Some(e.into()))?;
                h.finalize()
            }

            _other => {
                write!(h.get_writer(), "Error: {}", e)?;
                h.finalize()
            }
        }
    }

    /// Format an error given a script source.
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        let mut warnings = self.warnings.clone();
        warnings.sort();
        warnings.dedup();
        for w in &warnings {
            let tokens: Vec<_> = lexer::Tokenizer::new(&self.source).collect();
            h.highlight_runtime_error(&tokens, w.outer.0, w.outer.1, Some(w.into()))?;
        }
        h.finalize()
    }
    /// Formats an error within this script

    pub fn format_error(&self, e: &Error) -> String {
        let mut h = DumbHighlighter::default();
        if self.format_error_with(&mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }

    /// Formats an error within this script using a given highlighter
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
        let module_path = crate::path::load();
        if let Err(e) = Query::parse(&module_path, "test.trickle", query, &reg, &aggr_reg) {
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
