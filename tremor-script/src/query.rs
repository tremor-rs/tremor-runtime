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

use crate::prelude::*;
use crate::{arena, errors::Result};
use crate::{arena::Arena, highlighter::Highlighter};
use crate::{ast::BaseExpr, lexer};
use crate::{
    ast::{self, helper::Warning, visitors::ConstFolder, walkers::QueryWalker},
    lexer::Tokenizer,
};
use std::collections::BTreeSet;

/// A tremor query
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// The query
    pub query: ast::Query<'static>,
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
    // /// FIXME
    // pub fn instance_id(&self) -> String {
    //     todo!()
    // }
    /// Converts a troy embedded pipeline with resolved arguments to a runnable query
    /// # Errors
    ///   If the query fails to parse and convert correctly
    pub fn from_query(query: &ast::Query<'static>) -> Result<Self> {
        let warnings = BTreeSet::new();
        let locals = 0;
        let query = query.clone();
        Ok(Self {
            query,
            warnings,
            locals,
        })
    }

    /// Parses a string into a query supporting query arguments
    ///
    /// # Errors
    /// if the query can not be parsed
    pub fn parse<S: ToString>(script: S, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self> {
        let mut warnings = BTreeSet::new();

        let (aid, script) = Arena::insert(script)?;
        let mut helper = ast::Helper::new(reg, aggr_reg);
        let tokens = Tokenizer::new(script, aid).collect::<Result<Vec<_>>>()?;
        let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());
        let script_stage_1 = crate::parser::g::QueryParser::new().parse(filtered_tokens)?;
        let mut query = script_stage_1.up_script(&mut helper)?;

        ConstFolder::new(&helper).walk_query(&mut query)?;

        std::mem::swap(&mut warnings, &mut helper.warnings);
        let locals = helper.locals.len();

        Ok(Self {
            query,
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
        let mut script = script.to_string();
        script.push('\n');
        let tokens: Vec<_> = lexer::Tokenizer::new(&script, arena::Index::default())
            .tokenize_until_err()
            .collect();
        h.highlight(None, &tokens, "", emit_lines, None)
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
            let tokens: Vec<_> =
                lexer::Tokenizer::new(Arena::io_get(self.query.aid())?, self.query.aid())
                    .tokenize_until_err()
                    .collect();
            h.highlight_error(None, &tokens, "", true, Some(w.outer), Some(w.into()))?;
        }
        h.finalize()
    }
}
