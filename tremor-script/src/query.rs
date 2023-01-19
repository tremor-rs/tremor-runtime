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

use crate::ast::optimizer::Optimizer;
use crate::lexer;
use crate::{arena, errors::Result};
use crate::{arena::Arena, highlighter::Highlighter};
use crate::{ast::base_expr::Ranged, prelude::*};
use crate::{
    ast::{self, warning::Warning},
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
    /// Arena index of the string
    pub aid: crate::arena::Index,
}

impl<'run, 'event, 'script> Query
where
    'script: 'event,
    'event: 'run,
{
    /// Removes a deploy from the arena, freeing the memory and marking it valid for reause
    /// this function generally should not ever be used.
    ///
    /// # Safety
    /// It is a special case for the language
    /// server where we know that we really only parse the script to check for errors and
    /// warnings.
    ///
    /// That's also why it's behind a feature falg
    /// # Errors
    /// if then query and it's related data is not found in the arena
    /// # Safety
    /// The function is unsafe because if the deploy is still referenced somewhere it could lead
    /// to memory unsaftey. To combat that we ensure that it is consumed when freed.

    #[cfg(feature = "arena-delete")]
    pub unsafe fn consume_and_free(self) -> Result<()> {
        let Query { aid, query, .. } = self;
        drop(query);
        Arena::delete_index_this_is_really_unsafe_dont_use_it(aid)
    }

    /// Converts a troy embedded pipeline with resolved arguments to a runnable query
    /// # Errors
    ///   If the query fails to parse and convert correctly
    #[must_use]
    pub fn from_query(query: ast::Query<'static>) -> Self {
        let warnings = BTreeSet::new();
        let aid = query.aid();
        Self {
            query,
            warnings,
            aid,
        }
    }

    /// Parses a string into a query supporting query arguments
    ///
    /// this is used in the language server to delete lements on a
    /// parsing error
    ///
    /// # Errors
    /// if the query can not be parsed
    #[cfg(feature = "arena-delete")]
    pub fn parse_with_aid<S>(
        src: &S,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> std::result::Result<Self, crate::errors::ErrorWithIndex>
    where
        S: ToString + ?Sized + std::ops::Deref<Target = str>,
    {
        let (aid, src) = Arena::insert(src)?;
        Self::parse_(aid, src, reg, aggr_reg).map_err(|e| crate::errors::ErrorWithIndex(aid, e))
    }

    /// Parses a string into a query supporting query arguments
    ///
    /// # Errors
    /// if the query can not be parsed

    pub fn parse<S>(src: &S, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self>
    where
        S: ToString + ?Sized + std::ops::Deref<Target = str>,
    {
        let (aid, src) = Arena::insert(src)?;
        Self::parse_(aid, src, reg, aggr_reg)
    }

    /// Parses a string into a query supporting query arguments
    ///
    /// # Errors
    /// if the query can not be parsed
    fn parse_(
        aid: arena::Index,
        src: &'static str,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> Result<Self> {
        let mut helper = ast::Helper::new(reg, aggr_reg);
        let tokens = Lexer::new(src, aid).collect::<Result<Vec<_>>>()?;
        let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());
        let query_stage_1 = crate::parser::g::QueryParser::new().parse(filtered_tokens)?;
        let mut query = query_stage_1.up_script(&mut helper)?;
        Optimizer::new(&helper).walk_query(&mut query)?;
        Ok(Self {
            query,
            warnings: helper.warnings,
            aid,
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
