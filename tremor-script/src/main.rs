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
mod datetime;
mod errors;
pub mod grok;
#[allow(unused, dead_code)]
mod highlighter;
mod influx;
pub mod interpreter;
mod lexer;
#[allow(unused, dead_code)]
mod parser;
#[allow(unused, dead_code)]
mod pos;
mod registry;
mod script;
mod std_lib;
#[allow(unused, dead_code, clippy::transmute_ptr_to_ptr)]
mod str_suffix;
mod tilde;
#[macro_use]
extern crate rental;

use crate::highlighter::{Highlighter, TermHighlighter};
use crate::script::{Return, Script};
use clap::{App, Arg};
use halfbrown::hashmap;
use simd_json::borrowed::Value;
use std::fs::File;
use std::io::Read;
use std::iter::FromIterator;

#[macro_use]
extern crate serde_derive;

pub use crate::registry::{registry, Context, Registry, TremorFn, TremorFnWrapper};

fn main() {
    let matches = App::new("tremor-script")
        .version("0.5.0")
        .about("Tremor interpreter")
        .arg(
            Arg::with_name("SCRIPT")
                .help("The script to execute")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("event")
                .short("e")
                .takes_value(true)
                .help("The event to load."),
        )
        .arg(
            Arg::with_name("string")
                .long("string")
                .takes_value(true)
                .help("A string to load."),
        )
        .arg(
            Arg::with_name("highlight-source")
                .short("s")
                .takes_value(false)
                .help("Prints the highlighted script."),
        )
        .arg(
            Arg::with_name("print-ast")
                .short("a")
                .takes_value(false)
                .help("Prints the ast highlighted."),
        )
        .arg(
            Arg::with_name("print-ast-raw")
                .short("r")
                .takes_value(false)
                .help("Prints the ast with no highlighting."),
        )
        .arg(
            Arg::with_name("print-result-raw")
                .short("x")
                .takes_value(false)
                .help("Prints the result with no highlighting."),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .takes_value(false)
                .help("Do not print the result."),
        )
        .get_matches();

    let script_file = matches.value_of("SCRIPT").expect("No script file provided");
    let input = File::open(&script_file);
    let mut raw = String::new();

    input
        .expect("bad input")
        .read_to_string(&mut raw)
        .expect("");

    let reg: Registry<()> = registry::registry();

    match Script::parse(&raw, &reg) {
        Ok(runnable) => {
            let mut h = TermHighlighter::new();
            runnable
                .format_warnings_with(&mut h)
                .expect("failed to format error");

            if matches.is_present("highlight-source") {
                println!();
                let mut h = TermHighlighter::new();
                Script::<()>::highlight_script_with(&raw, &mut h).expect("Highlighter failed");
            }
            if matches.is_present("print-ast") {
                let ast = serde_json::to_string_pretty(&runnable.script.suffix())
                    .expect("Failed to render AST");
                println!();
                let mut h = TermHighlighter::new();
                Script::<()>::highlight_script_with(&ast, &mut h).expect("Highlighter failed");
            }
            if matches.is_present("print-ast-raw") {
                let ast = serde_json::to_string_pretty(&runnable.script.suffix())
                    .expect("Failed to render AST");
                println!();
                println!("{}", ast);
            }

            if matches.is_present("highlight-source")
                || matches.is_present("print-ast")
                || matches.is_present("print-ast-raw")
            {
                std::process::exit(0);
            }

            let mut bytes = Vec::new();
            let mut event = if let Some(event_file) = matches.value_of("event") {
                let input = File::open(&event_file);
                input.expect("bad input").read_to_end(&mut bytes).expect("");
                simd_json::to_borrowed_value(&mut bytes).expect("Invalid event data")
            } else if let Some(string_file) = matches.value_of("string") {
                let input = File::open(&string_file);
                let mut raw = String::new();
                input
                    .expect("bad input")
                    .read_to_string(&mut raw)
                    .expect("");
                let raw = raw.trim_end().to_string();

                simd_json::borrowed::Value::String(raw.into())
            } else {
                simd_json::borrowed::Value::Object(hashmap! {})
            };

            let mut global_map = Value::Object(hashmap! {});
            let _expr = Value::Null;
            let expr = runnable.run(&(), &mut event, &mut global_map);
            match expr {
                // Seperate out the speical case of emiting the inbound evet,
                // this way we don't have to clone it on the way out and can
                // uswe the refference that was apssed in instead.
                Ok(Return::EmitEvent { port }) => {
                    println!("Interpreter ran ok");
                    if matches.is_present("quiet") {
                    } else if matches.is_present("print-result-raw") {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&Return::Emit { value: event, port })
                                .expect("")
                        );
                    } else {
                        let result = format!(
                            "{} ",
                            serde_json::to_string_pretty(&Return::Emit { value: event, port })
                                .expect("")
                        );
                        let lexed_tokens = Vec::from_iter(lexer::tokenizer(&result));
                        let mut h = TermHighlighter::new();
                        h.highlight(lexed_tokens).expect("Failed to highliht error");
                    }
                }
                // Handle the other success returns
                Ok(result) => {
                    println!("Interpreter ran ok");
                    if matches.is_present("quiet") {
                    } else if matches.is_present("print-result-raw") {
                        println!("{}", serde_json::to_string_pretty(&result).expect(""));
                    } else {
                        let result =
                            format!("{} ", serde_json::to_string_pretty(&result).expect(""));
                        let lexed_tokens = Vec::from_iter(lexer::tokenizer(&result));
                        let mut h = TermHighlighter::new();
                        h.highlight(lexed_tokens).expect("Failed to highliht error");
                    }
                }
                // Hande and print runtime errors.
                Err(e) => {
                    let mut h = TermHighlighter::new();
                    runnable
                        .format_error_with(&mut h, &e)
                        .expect("failed to format error");
                    std::process::exit(1);
                }
            }
        }
        // Handle and print compile time errors.
        Err(e) => {
            let mut h = TermHighlighter::new();
            if let Err(e) = Script::<()>::format_error_from_script(&raw, &mut h, &e) {
                dbg!(e);
            };
            std::process::exit(1);
        }
    };
}
