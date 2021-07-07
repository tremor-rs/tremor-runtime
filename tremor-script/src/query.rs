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

use crate::ast::{self, Warning};
use crate::errors::{CompilerError, Error, Result};
use crate::highlighter::{Dumb as DumbHighlighter, Highlighter};
use crate::lexer;
use crate::path::ModulePath;
use crate::prelude::*;
use std::collections::BTreeSet;
use std::io::Write;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

/// A query and it's attached source.
///
/// Implemention alalougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///

#[derive(Clone)]
pub struct SRSQuery {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ast::Query<'static>,
}

impl std::fmt::Debug for SRSQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

unsafe impl SRS for SRSQuery {
    type Structured = ast::Query<'static>;

    unsafe fn into_parts(self) -> (Vec<Arc<Pin<Vec<u8>>>>, Self::Structured) {
        (self.raw, self.structured)
    }

    fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    fn suffix(&self) -> &Self::Structured {
        &self.structured
    }
}

impl SRSQuery {
    /// Creates a new Query with a given String and
    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(mut raw: String, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut String) -> std::result::Result<ast::Query<'head>, E>,
    {
        let structured = f(&mut raw)?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured: ast::Query<'static> = unsafe { mem::transmute(structured) };
        // This is possibl as String::into_bytes just returns the `vec` of the string
        let raw = Pin::new(raw.into_bytes());
        let raw = vec![Arc::new(raw)];
        Ok(Self { raw, structured })
    }

    /// Extracts SRS  statements
    pub fn extract_stmts(&self) -> Vec<SRSStmt> {
        self.structured
            .stmts
            .iter()
            .cloned()
            .map(|structured| SRSStmt {
                raw: self.raw.clone(),
                structured,
            })
            .collect()
    }
}

/// A statement and it's attached source.
///
/// Implemention alalougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///
#[derive(Clone)]
pub struct SRSStmt {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ast::Stmt<'static>,
}

unsafe impl SRS for SRSStmt {
    type Structured = ast::Stmt<'static>;

    unsafe fn into_parts(self) -> (Vec<Arc<Pin<Vec<u8>>>>, Self::Structured) {
        (self.raw, self.structured)
    }

    fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    fn suffix(&self) -> &Self::Structured {
        &self.structured
    }
}

impl std::fmt::Debug for SRSStmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

impl PartialEq for SRSStmt {
    fn eq(&self, other: &Self) -> bool {
        self.structured == other.structured
    }
}

impl SRSStmt {
    /// Creates a new statement from another SRS
    pub fn try_new_from_srs<E, F, O: SRS>(other: &O, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head O::Structured) -> std::result::Result<ast::Stmt<'head>, E>,
    {
        let raw = other.raw().to_vec();
        let structured = f(other.suffix())?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured: ast::Stmt<'static> = unsafe { mem::transmute(structured) };

        Ok(Self { raw, structured })
    }
}

impl Eq for SRSStmt {}

impl PartialOrd for SRSStmt {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None // NOTE Here be dragons
    }
}

/// A tremor query
#[derive(Debug, Clone)]
pub struct Query {
    /// The query
    pub query: SRSQuery,
    /// Source of the query
    pub source: String,
    /// Warnings emitted by the script
    pub warnings: BTreeSet<Warning>,
    /// Number of local variables (should be 0)
    pub locals: usize,
}

impl<'run, 'event, 'script> Query
where
    'script: 'event,
    'event: 'run,
{
    /// Extracts SRS  statements
    pub fn extract_stmts(&self) -> Vec<SRSStmt> {
        self.query.extract_stmts()
    }
    /// Borrows the query
    #[must_use]
    pub fn suffix(&self) -> &ast::Query {
        self.query.suffix()
    }
    /// Parses a string into a query
    ///
    /// # Errors
    /// if the query can not be parsed
    pub fn parse(
        module_path: &ModulePath,
        file_name: &str,
        script: &'script str,
        cus: Vec<ast::CompilationUnit>,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> std::result::Result<Self, CompilerError> {
        let mut source = script.to_string();

        let mut warnings = BTreeSet::new();
        let mut locals = 0;

        // TODO make lexer EOS tolerant to avoid this kludge
        source.push('\n');

        let mut include_stack = lexer::IncludeStack::default();

        let r = |include_stack: &mut lexer::IncludeStack| -> Result<Self> {
            let query = SRSQuery::try_new::<Error, _>(source.clone(), |src: &mut String| {
                let mut helper = ast::Helper::new(reg, aggr_reg, cus);
                let cu = include_stack.push(&file_name)?;
                let lexemes: Vec<_> = lexer::Preprocessor::preprocess(
                    module_path,
                    file_name,
                    src,
                    cu,
                    include_stack,
                )?;
                let filtered_tokens = lexemes
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|t| !t.value.is_ignorable());
                let script_stage_1 = crate::parser::g::QueryParser::new().parse(filtered_tokens)?;
                let script = script_stage_1.up_script(&mut helper)?;

                std::mem::swap(&mut warnings, &mut helper.warnings);
                locals = helper.locals.len();
                Ok(script)
            })?;

            Ok(Self {
                query: query,
                source,
                locals,
                warnings,
            })
        }(&mut include_stack);
        r.map_err(|error| CompilerError {
            error,
            cus: include_stack.into_cus(),
        })
    }

    /// Highlights a script with a given highlighter.
    /// # Errors
    /// on io errors
    #[cfg(not(tarpaulin_include))]
    pub fn highlight_script_with<H: Highlighter>(
        script: &str,
        h: &mut H,
        emit_lines: bool,
    ) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');
        let tokens: Vec<_> = lexer::Tokenizer::new(&script)
            .tokenize_until_err()
            .collect();
        h.highlight(None, &tokens, "", emit_lines, None)
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');

        let tokens: Vec<_> = lexer::Tokenizer::new(&script)
            .tokenize_until_err()
            .collect();
        match e.context() {
            (Some(r), _) => {
                h.highlight_error(None, &tokens, "", true, Some(r), Some(e.into()))?;
                h.finalize()
            }

            _other => {
                write!(h.get_writer(), "Error: {}", e)?;
                h.finalize()
            }
        }
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
            let tokens: Vec<_> = lexer::Tokenizer::new(&self.source)
                .tokenize_until_err()
                .collect();
            h.highlight_error(None, &tokens, "", true, Some(w.outer), Some(w.into()))?;
        }
        h.finalize()
    }

    /// Formats an error within this script
    #[must_use]
    pub fn format_error(&self, e: &Error) -> String {
        let mut h = DumbHighlighter::default();
        if self.format_error_with(&mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }

    /// Formats an error within this script using a given highlighter
    ///
    /// # Errors
    /// on io errors
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
        let cus = vec![];
        if let Err(e) = Query::parse(&module_path, "test.trickle", query, cus, &reg, &aggr_reg) {
            eprintln!("{}", e.error());
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
