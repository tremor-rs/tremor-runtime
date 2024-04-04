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
    ast::{self, docs::Docs, warning::Warning, DeployStmt},
    errors::Result,
    highlighter::Highlighter,
    lexer::{self, Lexer},
    prelude::*,
};
use std::collections::BTreeSet;

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
    /// Removes a deploy from the arena, freeing the memory and marking it valid for reause
    /// this function generally should not ever be used. It is a special case for the language
    /// server where we know that we really only parse the script to check for errors and
    /// warnings.
    /// That's also why it's behind a feature falg
    #[cfg(feature = "arena-delete")]
    pub unsafe fn consume_and_free(self) -> Result<()> {
        let Deploy { aid, deploy, .. } = self;
        drop(deploy);
        Arena::delte_index_this_is_really_unsafe_dont_use_it(aid)
    }

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

    /// Returns the documentation for the Deployment
    #[must_use]
    pub fn docs(&self) -> &Docs {
        &self.deploy.docs
    }

    /// Parses a string into a deployment
    ///
    /// # Errors
    /// if the deployment can not be parsed
    fn parse_(
        aid: arena::Index,
        src: &'static str,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> Result<Self> {
        let mut helper = ast::Helper::new(reg, aggr_reg);
        //let cu = include_stack.push(&file_name)?;
        let tokens = Lexer::new(src, aid).collect::<Result<Vec<_>>>()?;
        let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());
        let script_stage_1 = crate::parser::g::DeployParser::new().parse(filtered_tokens)?;
        let deploy = script_stage_1.up_script(&mut helper)?;

        // let mut warnings = BTreeSet::new();
        // std::mem::swap(&mut warnings, &mut helper.warnings);
        let locals = helper.locals.len();

        Ok(Self {
            deploy,
            aid,
            warnings: helper.warnings,
            locals,
        })
    }

    /// Parses a string into a deployment
    ///
    /// this is used in the language server to delete lements on a
    /// parsing error
    ///
    /// # Errors
    /// if the deployment can not be parsed
    #[cfg(feature = "arena-delete")]
    pub fn parse_with_aid<S>(
        src: &S,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> std::result::Result<Self, crate::errors::ErrorWithIndex>
    where
        S: ToString + ?Sized,
    {
        let (aid, src) = Arena::insert(src)?;
        Self::parse_(aid, src, reg, aggr_reg).map_err(|e| crate::errors::ErrorWithIndex(aid, e))
    }

    /// Parses a string into a deployment
    ///
    /// # Errors
    /// if the deployment can not be parsed
    pub fn parse<S>(src: &S, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self>
    where
        S: ToString + ?Sized,
    {
        let (aid, src) = Arena::insert(src)?;
        Self::parse_(aid, src, reg, aggr_reg)
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
            let tokens: Vec<_> = lexer::Lexer::new(Arena::io_get(self.aid)?, self.aid)
                .tokenize_until_err()
                .collect();
            h.highlight_error(None, &tokens, "", true, Some(w.outer), Some(w.into()))?;
        }
        h.finalize()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn parse(query: &str) {
        let reg = crate::registry();
        let aggr_reg = crate::aggr_registry();
        if let Err(e) = Deploy::parse(query, &reg, &aggr_reg) {
            eprintln!("{e}");
            panic!("error during parsing");
        }
    }
    #[test]
    fn basic_pipeline() {
        parse(
            r"define flow test flow define pipeline passthrough pipeline select args from in into out end; end;",
        );
    }
}
