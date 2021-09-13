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
use crate::path::ModulePath;
use crate::prelude::*;
use crate::{lexer, srs};
use std::collections::BTreeSet;
use std::io::Write;
use tremor_value::literal;

/// A tremor deployment ( troy)
#[derive(Debug, Clone)]
pub struct Deploy {
    /// The deployment
    pub deploy: srs::Deploy,
    /// Source of the query
    pub source: String,
    /// Warnings emitted by the script
    pub warnings: BTreeSet<Warning>,
    /// Number of local variables (should be 0)
    pub locals: usize,
}

impl<'run, 'event, 'script> Deploy
where
    'script: 'event,
    'event: 'run,
{
    /// Borrows the query
    #[must_use]
    pub fn suffix(&self) -> &ast::Deploy {
        self.deploy.suffix()
    }

    /// Retrieve deployment unit
    /// # Errors
    /// If the underlying structures do not resolve to a correctly deployable unit
    pub fn as_deployment_unit(&self) -> Result<srs::UnitOfDeployment> {
        self.deploy.as_deployment_unit()
    }

    /// Provides a `GraphViz` dot representation of the deployment graph
    #[must_use]
    pub fn dot(&self) -> String {
        self.deploy.dot()
    }

    /// Parses a string into a deployment
    ///
    /// # Errors
    /// if the deployment can not be parsed
    pub fn parse(
        module_path: &ModulePath,
        file_name: &str,
        script: &'script str,
        cus: Vec<ast::CompilationUnit>,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> std::result::Result<Self, CompilerError> {
        Deploy::parse_with_args(
            module_path,
            file_name,
            script,
            cus,
            reg,
            aggr_reg,
            &literal!({}),
        )
    }

    /// Parses a string into a deployment
    ///
    /// # Errors
    /// if the deployment can not be parsed
    pub fn parse_with_args(
        module_path: &ModulePath,
        file_name: &str,
        script: &'script str,
        cus: Vec<ast::CompilationUnit>,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
        args: &Value<'_>,
    ) -> std::result::Result<Self, CompilerError> {
        let mut source = script.to_string();

        let mut warnings = BTreeSet::new();
        let mut locals = 0;

        // TODO make lexer EOS tolerant to avoid this kludge
        source.push('\n');

        let mut include_stack = lexer::IncludeStack::default();

        let r = |include_stack: &mut lexer::IncludeStack| -> Result<Self> {
            let deploy = srs::Deploy::try_new::<Error, _>(source.clone(), |src: &mut String| {
                let mut helper = ast::Helper::new(reg, aggr_reg, cus);
                helper.consts.args = args.clone_static();
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
                let script_stage_1 =
                    crate::parser::g::DeployParser::new().parse(filtered_tokens)?;
                let script = script_stage_1.up_script(&mut helper)?;

                std::mem::swap(&mut warnings, &mut helper.warnings);
                locals = helper.locals.len();
                Ok(script)
            })?;

            Ok(Self {
                deploy,
                source,
                warnings,
                locals,
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

#[cfg(test)]
mod test {
    use super::*;

    fn parse(query: &str) {
        let reg = crate::registry();
        let aggr_reg = crate::aggr_registry();
        let module_path = crate::path::load();
        let cus = vec![];
        if let Err(e) = Deploy::parse(&module_path, "test.troy", query, cus, &reg, &aggr_reg) {
            eprintln!("{}", e.error());
            assert!(false)
        } else {
            assert!(true)
        }
    }
    #[test]
    fn basic_pipeline() {
        parse(r#"define pipeline passthrough pipeline select args from in into out end;"#);
    }
}
