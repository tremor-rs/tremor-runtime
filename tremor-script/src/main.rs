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

// We allow dead code and unused code in the main line because
// it is just a utility
#![allow(dead_code, unused)]
#![recursion_limit = "1024"]
#![cfg_attr(
    feature = "cargo-clippy",
    deny(
        clippy::all,
        clippy::result_unwrap_used,
        clippy::option_unwrap_used,
        clippy::unnecessary_unwrap,
        clippy::pedantic
    )
)]
mod ast;
mod ctx;
mod datetime;
mod errors;
pub mod grok;
mod highlighter;
mod influx;
pub mod interpreter;
mod lexer;
mod parser;
mod pos;
mod registry;
mod script;
mod std_lib;
mod tilde;
pub mod utils;
#[macro_use]
extern crate rental;

use crate::errors::*;
use crate::highlighter::{Highlighter, Term as TermHighlighter};
use crate::script::{AggrType, Return, Script};
use clap::{App, Arg};
pub use ctx::{EventContext, EventOriginUri};
use halfbrown::hashmap;
use simd_json::borrowed::{Object, Value};
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, Read};
use std::iter::FromIterator;

#[macro_use]
extern crate serde_derive;

use crate::registry::Registry;

fn main() -> Result<()> {
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
            Arg::with_name("modules")
                .short("m")
                .takes_value(true)
                .multiple(true)
                .help("The event to load."),
        )
        .arg(
            Arg::with_name("event")
                .short("e")
                .takes_value(true)
                .multiple(true)
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
        .arg(
            Arg::with_name("replay-influx")
                .takes_value(false)
                .help("Replays a file containing influx line protocol."),
        )
        .get_matches();

    let script_file = matches
        .value_of("SCRIPT")
        .ok_or_else(|| Error::from("No script file provided"))?;

    let mut raw = String::new();
    let mut input = File::open(&script_file)?;
    input.read_to_string(&mut raw)?;

    #[allow(unused_mut)]
    let mut reg: Registry = registry::registry();

    #[cfg(feature = "fns")]
    {
        if let Some(modules) = matches.values_of("modules") {
            use std::ffi::OsStr;
            use std::path::Path;
            for module in modules {
                if let Some(name) = Path::new(module)
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .map(ToString::to_string)
                {
                    let mut code = String::new();
                    let mut input = File::open(&module)?;
                    input.read_to_string(&mut code)?;

                    reg.load_module(&name, &code)?;
                }
            }
        }
    }

    match Script::parse(&raw, &reg) {
        Ok(runnable) => {
            let mut h = TermHighlighter::new();
            runnable.format_warnings_with(&mut h)?;

            if matches.is_present("highlight-source") {
                println!();
                let mut h = TermHighlighter::new();
                Script::highlight_script_with(&raw, &mut h)?;
            }
            if matches.is_present("print-ast") {
                let ast = serde_json::to_string_pretty(&runnable.script.suffix())?;
                println!();
                let mut h = TermHighlighter::new();
                Script::highlight_script_with(&ast, &mut h)?;
            }
            if matches.is_present("print-ast-raw") {
                let ast = serde_json::to_string_pretty(&runnable.script.suffix())?;
                println!();
                println!("{}", ast);
            }

            if matches.is_present("highlight-source")
                || matches.is_present("print-ast")
                || matches.is_present("print-ast-raw")
            {
                // ALLOW: main.rs
                std::process::exit(0);
            }

            let mut inputs = Vec::new();
            let mut events = if let Some(influx_file) = matches.value_of("replay-influx") {
                let mut r = Vec::new();
                let input = File::open(&influx_file)?;
                let buff_input = BufReader::new(input);
                let lines: std::io::Result<Vec<Vec<u8>>> = buff_input
                    .lines()
                    .map(|s| s.map(String::into_bytes))
                    .collect();
                inputs = lines?;
                for i in &inputs {
                    if let Some(i) = influx::parse(std::str::from_utf8(i)?, 0)? {
                        r.push(i);
                    }
                }
                r
            } else if let Some(event_files) = matches.values_of("event") {
                let mut r = Vec::new();
                for event_file in event_files {
                    let mut bytes = Vec::new();
                    let mut input = File::open(&event_file)?;
                    input.read_to_end(&mut bytes)?;
                    inputs.push(bytes);
                }
                for i in &mut inputs {
                    r.push(simd_json::to_borrowed_value(i)?)
                }
                r
            } else if let Some(string_file) = matches.value_of("string") {
                let mut input = File::open(&string_file)?;
                let mut raw = String::new();
                input.read_to_string(&mut raw)?;
                let raw = raw.trim_end().to_string();

                vec![simd_json::borrowed::Value::String(raw.into())]
            } else {
                vec![simd_json::borrowed::Value::from(Object::new())]
            };

            let mut global_map = Value::from(Object::new());
            let mut event = events
                .pop()
                .ok_or_else(|| Error::from("At least one event needs to be specified"))?;
            for event in &mut events {
                runnable.run(
                    &EventContext {
                        at: 0, // TODO add a value here?
                        origin_uri: Some(EventOriginUri::default()),
                    },
                    AggrType::Tick,
                    event,
                    &mut global_map,
                )?;
            }
            let expr = runnable.run(
                &EventContext {
                    at: 0, // TODO add a value here?
                    origin_uri: Some(EventOriginUri::default()),
                },
                AggrType::Emit,
                &mut event,
                &mut global_map,
            );
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
                            serde_json::to_string_pretty(&Return::Emit { value: event, port })?
                        );
                    } else {
                        let result = format!(
                            "{} ",
                            serde_json::to_string_pretty(&Return::Emit { value: event, port })?
                        );
                        let lexed_tokens = Vec::from_iter(lexer::tokenizer(&result));
                        let mut h = TermHighlighter::new();
                        h.highlight(lexed_tokens)?;
                    }
                }
                // Handle the other success returns
                Ok(result) => {
                    println!("Interpreter ran ok");
                    if matches.is_present("quiet") {
                    } else if matches.is_present("print-result-raw") {
                        println!("{}", serde_json::to_string_pretty(&result)?);
                    } else {
                        let result = format!("{} ", serde_json::to_string_pretty(&result)?);
                        let lexed_tokens = Vec::from_iter(lexer::tokenizer(&result));
                        let mut h = TermHighlighter::new();
                        h.highlight(lexed_tokens)?;
                    }
                }
                // Hande and print runtime errors.
                Err(e) => {
                    let mut h = TermHighlighter::new();
                    runnable.format_error_with(&mut h, &e)?;
                    // ALLOW: main.rs
                    std::process::exit(1);
                }
            }
        }
        // Handle and print compile time errors.
        Err(e) => {
            let mut h = TermHighlighter::new();
            if let Err(e) = Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
            // ALLOW: main.rs
            std::process::exit(1);
        }
    };
    Ok(())
}
