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
    arena::{self, Arena},
    ast::{self, helper::Warning, DeployStmt},
    errors::{Error, Result},
    highlighter::{Dumb as DumbHighlighter, Highlighter},
    lexer::{self, Tokenizer},
    prelude::*,
};
use std::collections::BTreeSet;
use std::io::Write;

/// A tremor deployment ( troy)
#[derive(Debug, Clone)]
pub struct Deploy {
    /// The deployment
    pub deploy: ast::Deploy<'static>,
    /// Source of the query
    pub aid: arena::Index,
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
    /// Retrieve deployment unit
    /// # Errors
    /// If the underlying structures do not resolve to a correctly deployable unit
    pub fn iter_flows(&self) -> impl Iterator<Item = &ast::DeployFlow<'static>> {
        self.deploy.stmts.iter().filter_map(|stmt| {
            if let DeployStmt::DeployFlowStmt(stmt) = stmt {
                Some(stmt.as_ref())
            } else {
                None
            }
        })
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
    pub fn parse<S: ToString>(script: S, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self> {
        let mut warnings = BTreeSet::new();

        let (aid, script) = Arena::insert(script)?;
        let mut helper = ast::Helper::new(reg, aggr_reg);
        //let cu = include_stack.push(&file_name)?;
        let tokens = Tokenizer::new(script, aid).collect::<Result<Vec<_>>>()?;
        let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());
        let script_stage_1 = crate::parser::g::DeployParser::new().parse(filtered_tokens)?;
        let deploy = script_stage_1.up_script(&mut helper)?;

        std::mem::swap(&mut warnings, &mut helper.warnings);
        let locals = helper.locals.len();

        Ok(Self {
            deploy,
            aid,
            warnings,
            locals,
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
        // let mut script = script.to_string();
        // script.push('\n'); FIXME
        let tokens: Vec<_> = lexer::Tokenizer::new(script, arena::Index::default())
            .tokenize_until_err()
            .collect();
        h.highlight(None, &tokens, "", emit_lines, None)
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_error_with<H: Highlighter>(h: &mut H, e: &Error) -> std::io::Result<()> {
        let aid = e.aid();

        let tokens: Vec<_> = lexer::Tokenizer::new(Arena::io_get(aid)?, aid)
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
            let tokens: Vec<_> = lexer::Tokenizer::new(Arena::io_get(self.aid)?, self.aid)
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
        if Self::format_error_with(&mut h, e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn parse(query: &str) {
        let reg = crate::registry();
        let aggr_reg = crate::aggr_registry();
        if let Err(e) = Deploy::parse(query, &reg, &aggr_reg) {
            eprintln!("{}", e);
            assert!(false, "error during parsing")
        } else {
            assert!(true)
        }
    }
    #[test]
    fn basic_pipeline() {
        parse(
            r#"define flow test flow define pipeline passthrough pipeline select args from in into out end; end;"#,
        );
    }
}
