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
#![recursion_limit = "128"]

#[macro_use]
extern crate serde_derive;

#[allow(unused, dead_code)]
mod query;

use crate::query::Query; // {Query, Return};
use clap::{App, Arg};
// use halfbrown::hashmap;
use crate::errors::*;
pub use crate::registry::{registry, Context, Registry, TremorFn, TremorFnWrapper};
use halfbrown::hashmap;
use simd_json::borrowed::Value;
use std::fs::File;
use std::io::Read;
use std::iter::FromIterator;
use tremor_script::highlighter::{Highlighter, TermHighlighter};
use tremor_script::*;

fn main() -> Result<()> {
    let matches = App::new("tremor-query")
        .version("0.6.0")
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
                .multiple(true)
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
            Arg::with_name("output")
                .long("output")
                .takes_value(true)
                .help("Output to select from."),
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
            Arg::with_name("print-pipeline-config")
                .short("c")
                .takes_value(false)
                .help("Prints the trickle script as a Pipeline configuration."),
        )
        // .arg(
        //     Arg::with_name("print-pipeline-dot")
        //         .short("d")
        //         .takes_value(false)
        //         .help("Prints the trickle script as a GraphViz dot file."),
        // )
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

    let selected_output = matches.value_of("output");

    input
        .expect("bad input")
        .read_to_string(&mut raw)
        .expect("");

    let reg: Registry<EventContext> = registry::registry();

    let aggr_reg = registry::aggr_registry();

    let runnable = match Query::parse(&raw, &reg, &aggr_reg) {
        Ok(runnable) => runnable,
        Err(e) => {
            let mut h = TermHighlighter::new();
            if let Err(e) = Script::<()>::format_error_from_script(&raw, &mut h, &e) {
                dbg!(e);
            };
            std::process::exit(1);
        }
    };
    let mut h = TermHighlighter::new();
    runnable
        .format_warnings_with(&mut h)
        .expect("failed to format error");

    if matches.is_present("highlight-source") {
        println!();
        let mut h = TermHighlighter::new();
        Query::highlight_script_with(&raw, &mut h).expect("Highlighter failed");
    }
    if matches.is_present("print-ast") {
        let ast = serde_json::to_string_pretty(&runnable.query.query.suffix())
            .expect("Failed to render AST");
        println!();
        let mut h = TermHighlighter::new();
        Query::highlight_script_with(&ast, &mut h).expect("Highlighter failed");
    }
    if matches.is_present("print-ast-raw") {
        let ast = serde_json::to_string_pretty(&runnable.query.query.suffix())
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

    let mut inputs = Vec::new();
    let mut events = if let Some(event_files) = matches.values_of("event") {
        let mut r = Vec::new();
        for event_file in event_files {
            let mut bytes = Vec::new();
            let input = File::open(&event_file);
            input?.read_to_end(&mut bytes)?;
            inputs.push(bytes);
        }
        for i in inputs.iter_mut() {
            r.push(simd_json::to_borrowed_value(i)?)
        }
        r
    } else if let Some(string_file) = matches.value_of("string") {
        let input = File::open(&string_file);
        let mut raw = String::new();
        input?.read_to_string(&mut raw)?;
        let raw = raw.trim_end().to_string();

        vec![simd_json::borrowed::Value::String(raw.into())]
    } else {
        vec![simd_json::borrowed::Value::Object(hashmap! {})]
    };

    // let mut global_map = Value::Object(hashmap! {});
    // let ctx = ();
    let _expr = Value::Null;
    if matches.is_present("print-pipeline-dot") {
        // FIXME will never fire as this ( dev-only, transient ) facility has been removed
        //                let expr = runnable.to_pipe(&ctx, &mut global_map);
        //                println!("{}", &expr.expect("").0.to_dot());
    } else if matches.is_present("print-pipeline-config") {
        let expr = runnable.to_config()?; // (&ctx, &mut global_map)?;
        println!("{}", serde_json::to_string_pretty(&expr)?);
    } else {
        let mut execable = runnable.to_pipe()?; // (&ctx, &mut global_map)?;

        // FIXME todo exercise graph with event / MRP
        //                dbg!(&execable);
        let mut continuation: tremor_pipeline::Returns = vec![];

        let mut i = 0;
        loop {
            for event in &events {
                let value = LineValue::new(Box::new(vec![]), |data| unsafe {
                    std::mem::transmute(event.clone())
                });
                continuation.clear();
                let _ = execable.enqueue(
                    "in",
                    tremor_pipeline::Event {
                        id: i,
                        ingest_ns: i * 1_000_000_000,
                        is_batch: false,
                        kind: None,
                        meta: tremor_pipeline::MetaMap::new(),
                        value: value.clone(),
                    },
                    &mut continuation,
                );

                for (output, event) in continuation.drain(..) {
                    let event = event.value.suffix();
                    if matches.is_present("quiet") {
                    } else if matches.is_present("print-result-raw") {
                        println!("{}", serde_json::to_string_pretty(event).expect(""));
                    } else {
                        if selected_output.is_none() || selected_output == Some(&output) {
                            println!("{}>>", output);
                            let result =
                                format!("{} ", serde_json::to_string_pretty(event).expect(""));
                            let lexed_tokens = Vec::from_iter(lexer::tokenizer(&result));
                            let mut h = TermHighlighter::new();
                            h.highlight(lexed_tokens)?;
                        }
                    }
                }
            }
            i += 1;
            std::thread::sleep_ms(1000);
        }
    };

    Ok(())
}
