// Copyright 2018-2019, Wayfair GmbH
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

use crate::ast::{Helper, Warning};
use crate::errors::*;
use crate::highlighter::{DumbHighlighter, Highlighter};
use crate::interpreter::{Cont, LocalStack};
use crate::lexer::{self, TokenFuns};
use crate::parser::grammar;
use crate::pos::Range;
use crate::registry::{Context, Registry};
use crate::stry;
use halfbrown::HashMap;
use simd_json::borrowed::Value;
use std::io::Write;

#[derive(Debug, Serialize, PartialEq)]
pub enum Return<'event> {
    Emit {
        value: Value<'event>,
        port: Option<String>,
    },
    Drop,
    EmitEvent {
        port: Option<String>,
    },
}

impl<'run, 'event> From<Cont<'run, 'event>> for Return<'event>
where
    'event: 'run,
{
    // This clones the data since we're returning it out of the scope of the
    // esecution - we might want to investigate if we can get rid of this in some cases.
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

#[derive(Debug)]
pub struct Script<Ctx>
where
    Ctx: Context + 'static,
{
    // TODO: This should probably be pulled out to allow people wrapping it themsefls
    pub script: rentals::Script<Ctx>,
    pub source: String, //tokens: Vec<std::result::Result<TokenSpan<'script>, LexerError>>
    pub warnings: Vec<Warning>,
    pub locals: HashMap<String, usize>,
}

rental! {
    pub mod rentals {
        use crate::ast;
        use crate::Context;
        use std::borrow::Cow;


        #[rental_mut(covariant,debug)]
        pub struct Script<Ctx: Context + Clone + 'static> {
            script: Box<String>,
            parsed: ast::Script<'script, Ctx>
        }

    }
}

impl<'run, 'event, 'script, Ctx> Script<Ctx>
where
    Ctx: Context + 'static,
    'script: 'event,
    'event: 'run,
{
    pub fn parse(script: &'script str, reg: &Registry<Ctx>) -> Result<Self> {
        let mut source = script.to_string();
        //FIXME: There is a bug in the lexer that requires a tailing ' ' otherwise
        //       it will not recognize a singular 'keywkrd'
        //       Also: darach is a snot badger!
        source.push(' ');
        let mut helper = Helper::new(reg);
        let script = rentals::Script::try_new(Box::new(source.clone()), |src| {
            let lexemes: Result<Vec<_>> = lexer::tokenizer(src.as_str()).collect();
            let mut filtered_tokens = Vec::new();

            for t in lexemes? {
                let keep = !t.value.is_ignorable();
                if keep {
                    filtered_tokens.push(Ok(t));
                }
            }

            let script = grammar::ScriptParser::new()
                .parse(filtered_tokens)?
                .up(&mut helper)?;

            Ok(script)
        })
        .map_err(|e: rental::RentalError<Error, Box<String>>| e.0)?;

        Ok(Script {
            script,
            source,
            locals: helper.locals.clone(),
            warnings: helper.into_warnings(),
        })
    }

    /*
    pub fn format_parser_error(script: &str, e: Error) -> String {
        let mut h = DumbHighlighter::default();
        if Self::format_error_from_script(script, &mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }
     */

    pub fn highlight_script_with<H: Highlighter>(script: &str, h: &mut H) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        h.highlight(tokens)
    }

    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        match e.context() {
            (Some(Range(start, end)), _) => {
                h.highlight_runtime_error(tokens, start, end, Some(e.into()))
            }

            _other => {
                let _ = write!(h.get_writer(), "Error: {}", e);
                h.finalize()
            }
        }
    }

    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
            let tokens: Vec<_> = lexer::tokenizer(&self.source).collect();
            h.highlight_runtime_error(tokens, w.outer.0, w.outer.1, Some(w.into()))?;
        }
        Ok(())
    }

    #[allow(dead_code)] // NOTE: Dman dual main and lib crate ...
    pub fn format_error(&self, e: Error) -> String {
        let mut h = DumbHighlighter::default();
        if self.format_error_with(&mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }

    pub fn format_error_with<H: Highlighter>(&self, h: &mut H, e: &Error) -> std::io::Result<()> {
        Self::format_error_from_script(&self.source, h, e)
    }

    pub fn run(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
    ) -> Result<Return<'event>> {
        // FIXME: find a way to pre-allocate this
        let mut local = LocalStack::with_size(self.locals.len());

        let script = self.script.suffix();
        let mut exprs = script.exprs.iter().peekable();
        while let Some(expr) = exprs.next() {
            if exprs.peek().is_none() {
                match stry!(expr.run(true, context, event, meta, &mut local, &script.consts,)) {
                    Cont::Drop => return Ok(Return::Drop),
                    Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                    Cont::EmitEvent(port) => {
                        return Ok(Return::EmitEvent { port });
                    }
                    Cont::Cont(v) => {
                        return Ok(Return::Emit {
                            value: v.into_owned(),
                            port: None,
                        })
                    }
                }
            } else {
                match stry!(expr.run(false, context, event, meta, &mut local, &script.consts,)) {
                    Cont::Drop => return Ok(Return::Drop),
                    Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                    Cont::EmitEvent(port) => {
                        return Ok(Return::EmitEvent { port });
                    }
                    Cont::Cont(_v) => (),
                }
            }
        }
        Ok(Return::Emit {
            value: Value::Null,
            port: None,
        })
    }
}
