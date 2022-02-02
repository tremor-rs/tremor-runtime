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

use crate::errors::{CompilerError, Error, Result};
use crate::highlighter::{Dumb as DumbHighlighter, Highlighter};
use crate::path::ModulePath;
use crate::prelude::*;
use crate::{
    ast::{self, helper::Warning, visitors::ConstFolder, walkers::QueryWalker},
    lexer::Tokenizer,
};
use crate::{lexer, srs};
use std::collections::BTreeSet;
use std::io::Write;

/// A tremor query
#[derive(Debug, Clone)]
pub struct Query {
    /// The query
    pub query: srs::QueryInstance,
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
    #[must_use]
    pub fn extract_stmts(&self) -> Vec<srs::Stmt> {
        self.query.extract_stmts()
    }
    /// Borrows the query
    #[must_use]
    pub fn suffix(&self) -> &ast::Query {
        self.query.suffix()
    }

    /// Converts a troy embedded pipeline with resolved arguments to a runnable query
    /// # Errors
    ///   If the query fails to parse and convert correctly
    pub fn from_troy(src: &str, query: &crate::srs::QueryInstance) -> Result<Self> {
        let warnings = BTreeSet::new();
        let locals = 0;
        let query = query.clone();
        Ok(Self {
            query,
            source: src.to_string(),
            warnings,
            locals,
        })
    }

    /// Parses a string into a query supporting query arguments
    ///
    /// # Errors
    /// if the query can not be parsed
    pub fn parse(
        _module_path: &ModulePath,
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

        let target_name = std::path::Path::new(file_name)
            .file_stem()
            .ok_or(CompilerError {
                error: "Snot".into(),
                cus: Vec::new(),
            })?
            .to_string_lossy()
            .to_string();

        let r = || -> Result<Self> {
            let query = srs::QueryInstance::try_new::<Error, _>(
                &target_name,
                source.clone(),
                |src: &mut String| {
                    let mut helper = ast::Helper::new(reg, aggr_reg, cus);
                    let tokens = Tokenizer::new(src.as_str()).collect::<Result<Vec<_>>>()?;
                    let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());
                    let script_stage_1 =
                        crate::parser::g::QueryParser::new().parse(filtered_tokens)?;
                    let mut query = script_stage_1.up_script(&mut helper)?;

                    ConstFolder::new(&helper).walk_query(&mut query)?;

                    std::mem::swap(&mut warnings, &mut helper.warnings);
                    locals = helper.locals.len();
                    Ok(query)
                },
            )?;

            Ok(Self {
                query,
                source,
                warnings,
                locals,
            })
        }();
        r.map_err(|error| CompilerError {
            error,
            cus: Vec::new(),
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
        if self.format_error_with(&mut h, e).is_ok() {
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
