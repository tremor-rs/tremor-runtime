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

// This isn't a external crate so we don't worry about docs
// #![deny(missing_docs)]
#![forbid(warnings)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate)]

use crate::query::Query; // {Query, Return};
use chrono::{Timelike, Utc};
use clap::{App, Arg};
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use tremor_pipeline::errors::{Error, ErrorKind, Result};
use tremor_pipeline::{Event, Ids};
use tremor_script::highlighter::{Highlighter, Term as TermHighlighter};
use tremor_script::path::load as load_module_path;
use tremor_script::{
    lexer, query,
    registry::{self, Registry},
    LineValue, Object, Script, ValueAndMeta,
};

#[allow(clippy::cast_sign_loss)]
pub fn nanotime() -> u64 {
    let now = Utc::now();
    let seconds: u64 = now.timestamp() as u64;
    let nanoseconds: u64 = u64::from(now.nanosecond());

    (seconds * 1_000_000_000) + nanoseconds
}

#[allow(clippy::too_many_lines)]
fn main() -> Result<()> {
    let matches = App::new("tremor-query")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
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
            Arg::with_name("replay-influx")
                .long("replay-influx")
                .takes_value(true)
                .help("Replays a file containing influx line protocol."),
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
            Arg::with_name("highlight-preprocess-source")
                .short("p")
                .takes_value(false)
                .help("Prints the highlighted preprocessed script."),
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

    let script_file = matches
        .value_of("SCRIPT")
        .ok_or_else(|| Error::from("No script file provided"))?;

    let mut input = File::open(&script_file)?;
    let mut raw = String::new();

    let selected_output = matches.value_of("output");

    input.read_to_string(&mut raw)?;

    let reg: Registry = registry::registry();

    let aggr_reg = registry::aggr();

    let module_path = load_module_path();
    let runnable = match Query::parse(&module_path, script_file, &raw, vec![], &reg, &aggr_reg) {
        Ok(runnable) => runnable,
        Err(e) => {
            let mut h = TermHighlighter::new();
            if let Err(e) = Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
            // ALLOW: main.rs
            std::process::exit(1);
        }
    };
    let mut h = TermHighlighter::new();
    runnable.format_warnings_with(&mut h)?;
    let runnable = tremor_pipeline::query::Query(runnable);
    if matches.is_present("highlight-source") {
        println!();
        let mut h = TermHighlighter::new();
        Query::highlight_script_with(&raw, &mut h)?;
    }
    if matches.is_present("highlight-preprocess-source") {
        println!();
        if matches.is_present("print-results-raw") {
        } else {
            let mut h = TermHighlighter::new();
            Query::highlight_preprocess_script_with(script_file, &raw, &mut h)?;
        }
    }

    if matches.is_present("print-ast") {
        let ast = simd_json::to_string_pretty(&runnable.0.suffix())?;
        println!();
        let mut h = TermHighlighter::new();
        Query::highlight_script_with(&ast, &mut h)?;
    }
    if matches.is_present("print-ast-raw") {
        let ast = simd_json::to_string_pretty(&runnable.0.suffix())?;
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
    let events = if let Some(influx_file) = matches.value_of("replay-influx") {
        eprint!("Preloading messages ...");
        let mut r = Vec::new();
        let input = File::open(&influx_file)?;
        let buff_input = BufReader::new(input);
        let lines: std::io::Result<Vec<Vec<u8>>> = buff_input
            .lines()
            .map(|s| s.map(String::into_bytes))
            .collect();
        inputs = lines?;
        for i in &inputs {
            let s = std::str::from_utf8(i)?;
            if let Some(i) = tremor_influx::decode(s, 0)
                .map_err(|e| ErrorKind::InvalidInfluxData(s.to_string(), e))?
            {
                r.push(i);
            }
        }
        eprintln!("loaded {} messages", r.len());
        r
    } else if let Some(event_files) = matches.values_of("event") {
        let mut r = Vec::new();
        for event_file in event_files {
            let mut bytes = Vec::new();
            let input = File::open(&event_file);
            input?.read_to_end(&mut bytes)?;
            inputs.push(bytes);
        }
        for i in &mut inputs {
            r.push(simd_json::to_borrowed_value(i)?)
        }
        r
    } else if let Some(string_file) = matches.value_of("string") {
        let input = File::open(&string_file);
        let mut raw = String::new();
        input?.read_to_string(&mut raw)?;
        let raw = raw.trim_end().to_string();

        vec![simd_json::borrowed::Value::from(raw)]
    } else {
        vec![simd_json::borrowed::Value::from(Object::default())]
    };

    let mut uid = 0;
    let mut execable = runnable.to_pipe(&mut uid)?; // (&ctx, &mut global_map)?;

    // FIXME todo exercise graph with event / MRP
    let mut continuation: tremor_pipeline::Returns = vec![];

    let mut id = 0;
    loop {
        for event in &events {
            let value = LineValue::new(vec![], |_| unsafe {
                std::mem::transmute(ValueAndMeta::from(event.clone()))
            });
            continuation.clear();
            let ingest_ns = nanotime();
            execable.enqueue(
                "in",
                Event {
                    id: Ids::new(0, id),
                    ingest_ns,
                    data: value.clone(),
                    ..Event::default()
                },
                &mut continuation,
            )?;

            for (output, event) in continuation.drain(..) {
                let event = event.data.suffix().value();
                if matches.is_present("quiet") {
                } else if matches.is_present("print-result-raw") {
                    println!("{}", simd_json::to_string_pretty(event)?);
                } else if selected_output.is_none() || selected_output == Some(&output) {
                    println!("{}>>", output);
                    let result = format!("{} ", simd_json::to_string_pretty(event)?);
                    let lexed_tokens: Vec<_> = lexer::Tokenizer::new(&result)
                        .filter_map(std::result::Result::ok)
                        .collect();
                    let mut h = TermHighlighter::new();
                    h.highlight(Some(script_file), &lexed_tokens)?;
                }
            }
        }
        id += 1;
        if matches.value_of("replay-influx").is_none() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        } else {
            break;
        }
    }

    Ok(())
}
