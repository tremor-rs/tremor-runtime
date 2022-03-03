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

pub use crate::interpreter::AggrType;
use crate::{
    arena::Arena,
    ast::{
        docs::Docs,
        helper::{Warning, Warnings},
        visitors::ConstFolder,
        walkers::QueryWalker,
        Helper,
    },
    ctx::EventContext,
    errors::Result,
    highlighter::Highlighter,
    lexer::{self, Tokenizer},
    parser::g as grammar,
    registry::{Aggr as AggrRegistry, Registry},
    Value,
};
use serde::Serialize;
use std::io;

/// Return of a script execution
#[derive(Debug, Serialize, PartialEq)]
pub enum Return<'event> {
    /// This script should emit the returned
    /// value
    Emit {
        /// Value to emit
        value: Value<'event>,
        /// Port to emit to
        port: Option<String>,
    },
    /// This event should be dropped
    Drop,
    /// This script should emit the event that
    /// was passed in
    EmitEvent {
        /// Port to emit to
        port: Option<String>,
    },
}

/// A tremor script
#[derive(Debug)]
pub struct Script {
    /// Rental for the runnable script
    pub script: crate::ast::Script<'static>,
    /// Arena index of the string
    pub aid: crate::arena::Index,
    /// A set of warnings if any
    pub warnings: Warnings,
}

impl Script {
    /// Get script warnings
    pub fn warnings(&self) -> impl Iterator<Item = &Warning> {
        self.warnings.iter()
    }

    /// Parses a string and turns it into a script with the supplied parameters/arguments
    ///
    /// # Errors
    /// if the script can not be parsed
    pub fn parse<S>(script: &S, reg: &Registry) -> Result<Self>
    where
        S: ToString + ?Sized,
    {
        let mut warnings = Warnings::new();
        let (aid, script) = Arena::insert(script)?;

        let tokens = Tokenizer::new(script, aid).collect::<Result<Vec<_>>>()?;
        let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());

        let script_raw = grammar::ScriptParser::new().parse(filtered_tokens)?;
        let fake_aggr_reg = AggrRegistry::default();
        let mut helper = Helper::new(reg, &fake_aggr_reg);
        // helper.consts.args = args.clone_static();
        let mut script = script_raw.up_script(&mut helper)?;
        ConstFolder::new(&helper).walk_script(&mut script)?;
        std::mem::swap(&mut warnings, &mut helper.warnings);
        let script = script;

        Ok(Self {
            script,
            aid,
            warnings,
        })
    }

    /// Returns the documentation for the script
    #[must_use]
    pub fn docs(&self) -> &Docs {
        &self.script.docs
    }

    /// Format warnings with the given `Highligher`.
    /// # Errors
    /// on io errors
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> io::Result<()> {
        for w in self.warnings() {
            let tokens: Vec<_> = lexer::Tokenizer::new(Arena::io_get(self.aid)?, self.aid)
                .tokenize_until_err()
                .collect();
            h.highlight_error(None, &tokens, "", true, Some(w.outer), Some(w.into()))?;
        }
        h.finalize()
    }

    /// Runs an event through this script
    ///
    /// # Errors
    /// if the script fails to run for the given context, event state and metadata
    pub fn run<'run, 'event>(
        &self,
        context: &'run EventContext,
        aggr: AggrType,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
    ) -> Result<Return<'event>>
    where
        'event: 'run,
    {
        self.script.run(context, aggr, event, state, meta)
    }
}
