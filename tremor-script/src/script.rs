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

use crate::ast::Helper;
use crate::ast::{Docs, Warning};
use crate::ctx::EventContext;
use crate::errors::{CompilerError, Error, Result};
use crate::highlighter::{Dumb as DumbHighlighter, Highlighter};
pub use crate::interpreter::AggrType;
use crate::interpreter::Cont;
use crate::lexer::{self};
use crate::parser::g as grammar;
use crate::path::ModulePath;
use crate::pos::Range;
use crate::registry::{Aggr as AggrRegistry, Registry};
use serde::Serialize;
use simd_json::borrowed::Value;
use std::io::{self, Write};

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
#[derive(Debug)]
pub struct Script {
    // TODO: This should probably be pulled out to allow people wrapping it themselves
    /// Rental for the runnable script
    pub script: rentals::Script,
    /// Source code for this script
    pub source: String,
    /// A set of warnings if any
    pub warnings: Vec<Warning>,
}

impl Script {
    /// Get script warnings
    pub fn warnings(&self) -> &Vec<Warning> {
        &self.warnings
    }
}

rental! {
    #[allow(missing_docs)]
    /// Rental for the script/interpreter for tremor-script
    pub mod rentals {
        use crate::ast;
        use std::borrow::Cow;
        use serde::Serialize;
        use std::sync::Arc;
        use std::marker::Send;

        /// Script rental
        #[rental_mut(covariant,debug)]
        pub struct Script{
            script: Box<String>, // Fake
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
        module_path: &ModulePath,
        file_name: &str,
        script: String,
        reg: &Registry,
        // args: Option<Vec<&str>>,
        // aggr_reg: &AggrRegistry, - we really should shadow and provide a nice hygienic error FIXME but not today
    ) -> std::result::Result<Self, CompilerError> {
        let mut include_stack = lexer::IncludeStack::default();
        let r = |include_stack: &mut lexer::IncludeStack| -> Result<Self> {
            let mut warnings = vec![];

            let rented_script =
                rentals::Script::try_new(Box::new(script.clone()), |script: &mut String| {
                    let cu = include_stack.push(file_name)?;
                    let lexemes: Vec<_> = lexer::Preprocessor::preprocess(
                        module_path,
                        file_name,
                        script,
                        cu,
                        include_stack,
                    )?;
                    let filtered_tokens = lexemes
                        .into_iter()
                        .filter_map(Result::ok)
                        .filter(|t| !t.value.is_ignorable());

                    let script_raw = grammar::ScriptParser::new().parse(filtered_tokens)?;
                    let fake_aggr_reg = AggrRegistry::default();
                    let mut helper = Helper::new(&reg, &fake_aggr_reg, include_stack.cus.clone());
                    let (screw_rust, ws) = script_raw.up_script(&mut helper)?;

                    warnings = ws;
                    Ok(screw_rust)
                })
                .map_err(|e: rental::RentalError<Error, Box<String>>| e.0)?;

            Ok(Self {
                script: rented_script,
                source: script,
                warnings,
            })
        }(&mut include_stack);
        r.map_err(|error| CompilerError {
            error,
            cus: include_stack.into_cus(),
        })
    }

    /// Returns the documentation for the script
    pub fn docs(&self) -> &Docs<'_> {
        &self.script.suffix().docs
    }

    /// Highlights a script with a given highlighter.
    #[cfg_attr(tarpaulin, skip)]
    pub fn highlight_script_with<H: Highlighter>(script: &str, h: &mut H) -> io::Result<()> {
        let tokens: Vec<_> = lexer::Tokenizer::new(&script)
            .filter_map(Result::ok)
            .collect();
        h.highlight(None, &tokens)?;
        io::Result::Ok(())
    }

    /// Preprocessesa and highlights a script with a given highlighter.
    #[cfg_attr(tarpaulin, skip)]
    pub fn highlight_preprocess_script_with<H: Highlighter>(
        file_name: &str,
        script: &'script str,
        h: &mut H,
    ) -> io::Result<()> {
        let mut s = script.to_string();
        let mut include_stack = lexer::IncludeStack::default();
        let cu = include_stack.push(file_name)?;

        let tokens: Vec<_> = lexer::Preprocessor::preprocess(
            &crate::path::load(),
            &file_name,
            &mut s,
            cu,
            &mut include_stack,
        )?
        .into_iter()
        .filter_map(Result::ok)
        .collect();
        h.highlight(Some(file_name), &tokens)
    }

    /// Format an error given a script source.
    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        CompilerError { error, cus }: &CompilerError,
    ) -> io::Result<()> {
        Self::format_error_from_script_and_cus(script, h, error, cus)
    }

    /// Format an error given a script source.
    fn format_error_from_script_and_cus<H: Highlighter>(
        script: &str,
        h: &mut H,
        error: &Error,
        cus: &[lexer::CompilationUnit],
    ) -> io::Result<()> {
        if let (Some(Range(start, end)), _) = error.context() {
            let cu = error.cu();
            if cu == 0 {
                let tokens: Vec<_> = lexer::Tokenizer::new(&script)
                    .filter_map(Result::ok)
                    .collect();
                h.highlight_runtime_error(None, &tokens, start, end, Some(error.into()))?;
            } else if let Some(cu) = cus.get(cu) {
                let script = std::fs::read_to_string(cu.file_path())?;
                let tokens: Vec<_> = lexer::Tokenizer::new(&script)
                    .filter_map(Result::ok)
                    .collect();
                h.highlight_runtime_error(
                    cu.file_path().to_str(),
                    &tokens,
                    start,
                    end,
                    Some(error.into()),
                )?;
            } else {
                write!(h.get_writer(), "Error: {}", error)?;
            };
        } else {
            write!(h.get_writer(), "Error: {}", error)?;
        }
        h.finalize()
    }

    /// Format an error given a script source.
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> io::Result<()> {
        let mut warnings = self.warnings.clone();
        warnings.sort();
        warnings.dedup();
        for w in &warnings {
            let tokens: Vec<_> = lexer::Tokenizer::new(&self.source)
                .filter_map(Result::ok)
                .collect();
            h.highlight_runtime_error(None, &tokens, w.outer.0, w.outer.1, Some(w.into()))?;
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
    pub fn format_error_with<H: Highlighter>(&self, h: &mut H, e: &Error) -> io::Result<()> {
        Self::format_error_from_script_and_cus(
            &self.source,
            h,
            e,
            &self.script.suffix().node_meta.cus,
        )
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
