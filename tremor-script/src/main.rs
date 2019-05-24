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
#![recursion_limit = "265"]

mod ast;
mod errors;
#[allow(unused, dead_code)]
mod highlighter;
mod interpreter;
mod lexer;
#[allow(unused, dead_code)]
mod parser;
#[allow(unused, dead_code)]
mod pos;
mod registry;
mod runtime;
mod std_lib;
#[allow(unused, dead_code, clippy::transmute_ptr_to_ptr)]
mod str_suffix;
mod tilde;

use crate::ast::Script;
use crate::highlighter::{DumbHighlighter, Highlighter, TermHighlighter};
use crate::interpreter::ValueStack;
use halfbrown::hashmap;
use lalrpop_util::ParseError;
use lexer::{LexerError, Token, TokenFuns, TokenSpan};
use pos::*;
use simd_json::borrowed::Value;
use std::fs::File;
use std::io::Read;
use std::iter::FromIterator;
#[macro_use]
extern crate serde_derive;

pub use crate::registry::{registry, Context, Registry, TremorFn, TremorFnWrapper};

fn humanize(
    maybe_ast: Result<Script, ParseError<Location, Token, LexerError>>,
    tokens: Vec<Result<TokenSpan, LexerError>>,
) {
    let mut h = TermHighlighter::new();
    if let Err(err) = maybe_ast {
        println!("Parsing failed");
        h.highlight_parser_error(tokens, err)
            .expect("Failed to highliht error");
    } else {
        println!("Parsing succeeded");
        h.highlight(tokens).expect("Failed to highliht error");
    }
}

#[derive(Clone, Debug)]
struct FakeContext {}
impl Context for FakeContext {}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("ERROR: Not enough arguments supplied");
        println!("usage: {} <file.tremor>\n", args[0]);
        return;
    }

    let input = File::open(&args[1]);
    let mut raw = String::new();

    input
        .expect("bad input")
        .read_to_string(&mut raw)
        .expect("");

    let lexed_tokens = Vec::from_iter(lexer::tokenizer(&raw));

    let mut filtered_tokens = Vec::<Result<TokenSpan, LexerError>>::new();

    for t in lexed_tokens.clone() {
        let keep = !t.clone().expect("").value.is_ignorable();
        if keep {
            filtered_tokens.push(t.clone());
        }
    }
    let maybe_ast = parser::grammar::ScriptParser::new().parse(filtered_tokens.clone());
    //    humanize(maybe_ast.clone(), lexed_tokens.clone());

    match maybe_ast {
        Ok(_ast) => {
            let reg: Registry<FakeContext> = registry::registry();
            let runnable: interpreter::Script<FakeContext> =
                match interpreter::Script::parse(&raw, reg.clone()) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("{}", e);
                        panic!()
                    }
                };

            /*
            println!("Pretty ast");
            println!(
                "{}",
                serde_json::to_string_pretty(&runnable.script).expect("")
            );
            println!("----");
            */
            let mut global_map = Value::Object(interpreter::LocalMap::new());
            let mut event =
                simd_json::borrowed::Value::Object(hashmap! { "snot".into() => "bar".into() });
            let ctx = FakeContext {};
            let _expr = Value::Null;
            let stack: ValueStack = ValueStack::default();
            let expr = runnable.run(&ctx, &mut event, &mut global_map, &stack);
            humanize(Ok(_ast), lexed_tokens.clone());
            match expr {
                Ok(result) => {
                    println!("Interpreter ran ok");
                    let result = format!("{} ", serde_json::to_string_pretty(&result).expect(""));
                    let lexed_tokens = Vec::from_iter(lexer::tokenizer(&result));
                    let mut h = TermHighlighter::new();
                    h.highlight(lexed_tokens).expect("Failed to highliht error");
                }
                Err(e) => {
                    let mut h = TermHighlighter::new();
                    runnable.format_error_with(&mut  h, &e);
                    let mut h = DumbHighlighter::new();
                    runnable.format_error_with(&mut  h, &e);
                    print!("{}", h.to_string());
                },
            }
        }
        Err(e) => {
            match e {
                ParseError::UnrecognizedToken{token: Some((start, t, _end)), expected} =>
                    println!("Unrecognized token in line {:?} column {:?}. Found `{:?}` but expected one of {:?}", start.line.0, start.column.0, t, expected),
                _ => ()
            }
        }
    };
}
