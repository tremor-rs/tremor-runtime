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

use crate::errors::Result;
use crate::lexer;
use crate::{arena::Arena, highlighter::Highlighter};
use crate::{ast::base_expr::Ranged, prelude::*};
use crate::{
    ast::{self, helper::Warning, visitors::ConstFolder, walkers::QueryWalker},
    lexer::Lexer,
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
    /// Converts a troy embedded pipeline with resolved arguments to a runnable query
    /// # Errors
    ///   If the query fails to parse and convert correctly
    #[must_use]
    pub fn from_query(query: ast::Query<'static>) -> Self {
        let warnings = BTreeSet::new();
        let locals = 0;
        Self {
            query,
            warnings,
            locals,
        }
    }

    /// Parses a string into a query supporting query arguments
    ///
    /// # Errors
    /// if the query can not be parsed
    pub fn parse<S>(script: &S, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self>
    where
        S: ToString + ?Sized,
    {
        let mut warnings = BTreeSet::new();

        let (aid, script) = Arena::insert(script)?;
        let mut helper = ast::Helper::new(reg, aggr_reg);
        let tokens = Lexer::new(script, aid).collect::<Result<Vec<_>>>()?;
        let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());
        let query_stage_1 = crate::parser::g::QueryParser::new().parse(filtered_tokens)?;
        let mut query = query_stage_1.up_script(&mut helper)?;
        ConstFolder::new(&helper).walk_query(&mut query)?;
        std::mem::swap(&mut warnings, &mut helper.warnings);
        let locals = helper.locals.len();

        Ok(Self {
            query,
            warnings,
            locals,
        })
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
            let tokens: Vec<_> =
                lexer::Lexer::new(Arena::io_get(self.query.aid())?, self.query.aid())
                    .tokenize_until_err()
                    .collect();
            h.highlight_error(None, &tokens, "", true, Some(w.outer), Some(w.into()))?;
        }
        h.finalize()
    }
}
