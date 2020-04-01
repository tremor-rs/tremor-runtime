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

use crate::ast::Warning;
use crate::ctx::EventContext;
use crate::errors::*;
use crate::highlighter::{Dumb as DumbHighlighter, Highlighter};
pub use crate::interpreter::AggrType;
use crate::interpreter::Cont;
use crate::lexer::{self};
use crate::parser::g as grammar;
use crate::pos::Range;
use crate::registry::{Aggr as AggrRegistry, Registry};
use serde::Serialize;
use simd_json::borrowed::Value;
use std::io::Write;

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

impl<'run, 'event> From<Cont<'run, 'event>> for Return<'event>
where
    'event: 'run,
{
    // TODO: This clones the data since we're returning it out of the scope of the
    // execution - we might want to investigate if we can get rid of this in some cases.
    fn from(v: Cont<'run, 'event>) -> Self {
        match v {
            Cont::Cont(value) => Return::Emit {
                value: value.into_owned(),
                port: None,
            },
            Cont::Emit(value, port) => Return::Emit { value, port },
            Cont::EmitEvent(port) => Return::EmitEvent { port },
            Cont::Drop => Return::Drop,
        }
    }
}

/// A tremor script
#[derive(Debug)] // FIXME rename ScriptRentalWrapper

pub struct Script {
    // TODO: This should probably be pulled out to allow people wrapping it themselves
    pub(crate) script: rentals::Script,
    source: String,
    warnings: Vec<Warning>,
}

impl Script {
    /// Get script warnings
    pub fn warnings(&self) -> &Vec<Warning> {
        &self.warnings
    }
}

rental! {
    mod rentals {
        use crate::ast;
        use std::borrow::Cow;
        use serde::Serialize;
        use std::sync::Arc;
        use std::marker::Send;

        #[rental_mut(covariant,debug)]
        pub(crate) struct Script{
            script: Box<String>,
            parsed: ast::Script<'script>
        }
    }
}

impl<'run, 'event, 'script> Script
where
    'script: 'event,
    'event: 'run,
{
    /// Parses a string and turns it into a script
    pub fn parse(
        script: &'script str,
        reg: &Registry,
        // aggr_reg: &AggrRegistry, - we really should shadow and provide a nice hygienic error FIXME but not today
    ) -> Result<Self> {
        let mut source = script.to_string();

        let mut warnings = vec![];

        // FIXME make lexer EOS tolerant to avoid this kludge
        source.push(' ');

        let script = rentals::Script::try_new(Box::new(source.clone()), |src| {
            let lexemes: Result<Vec<_>> = lexer::Tokenizer::new(src.as_str()).collect();
            let mut filtered_tokens = Vec::new();

            for t in lexemes? {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }

            let fake_aggr_reg = AggrRegistry::default();
            let (script, ws) = grammar::ScriptParser::new()
                .parse(filtered_tokens)?
                .up_script(reg, &fake_aggr_reg)?;
            warnings = ws;
            Ok(script)
        })
        .map_err(|e: rental::RentalError<Error, Box<String>>| e.0)?;

        Ok(Self {
            script,
            source,
            warnings,
        })
    }

    /// Highlights a script with a given highlighter.
    #[cfg_attr(tarpaulin, skip)]
    pub fn highlight_script_with<H: Highlighter>(script: &str, h: &mut H) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::Tokenizer::new(&script).collect();
        h.highlight(tokens)
    }

    /// Format an error given a script source.
    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::Tokenizer::new(&script).collect();
        if let (Some(Range(start, end)), _) = e.context() {
            h.highlight_runtime_error(tokens, start, end, Some(e.into()))?;
        } else {
            write!(h.get_writer(), "Error: {}", e)?;
        }
        h.finalize()
    }

    /// Format an error given a script source.
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        let mut warnings = self.warnings.clone();
        warnings.sort();
        warnings.dedup();
        for w in &warnings {
            let tokens: Vec<_> = lexer::Tokenizer::new(&self.source).collect();
            h.highlight_runtime_error(tokens, w.outer.0, w.outer.1, Some(w.into()))?;
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

    /// Runs an event through this script
    pub fn run(
        &'script self,
        context: &'run EventContext,
        aggr: AggrType,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
    ) -> Result<Return<'event>> {
        self.script.suffix().run(context, aggr, event, state, meta)
    }
}
