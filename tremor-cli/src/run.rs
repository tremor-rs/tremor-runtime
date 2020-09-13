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

use crate::errors::{Error, Result};
use crate::util::{get_source_kind, highlight, nanotime, slurp_string, SourceKind};
use clap::ArgMatches;
use simd_json::borrowed::Value;
use simd_json::prelude::*;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter, Read, Write};
use tremor_runtime::codec::Codec;
use tremor_runtime::postprocessor::Postprocessor;
use tremor_runtime::preprocessor::Preprocessor;
use tremor_script::ctx::{EventContext, EventOriginUri};
use tremor_script::highlighter::{Highlighter, Term as TermHighlighter};
use tremor_script::path::load as load_module_path;
use tremor_script::query::Query;
use tremor_script::registry;
use tremor_script::registry::Registry;
use tremor_script::script::{AggrType, Return, Script};
use tremor_script::LineValue;
use tremor_script::ValueAndMeta;

struct Ingress {
    is_interactive: bool,
    is_pretty: bool,
    buf: [u8; 4096],
    buffer: Box<dyn BufRead>,
    preprocessor: Box<dyn Preprocessor>,
    codec: Box<dyn Codec>,
}

type IngressHandler<T> =
    dyn Fn(&mut T, &mut u64, &mut Egress, u64, simd_json::BorrowedValue) -> Result<()>;

impl Ingress {
    fn from_args(matches: &ArgMatches) -> Result<Self> {
        let codec_pre = matches.value_of("PREPROCESSOR").unwrap_or("lines");
        let codec_decoder = matches.value_of("DECODER").unwrap_or("json");
        let is_interactive = matches.is_present("interactive");
        let is_pretty = matches.is_present("pretty");

        let buffer: Box<dyn BufRead> = match matches.value_of("INFILE") {
            None | Some("-") => Box::new(BufReader::new(io::stdin())),
            Some(data) => Box::new(BufReader::new(crate::open_file(data, None)?)),
        };

        let codec = tremor_runtime::codec::lookup(codec_decoder);
        if let Err(_e) = codec {
            eprintln!("Error Codec {} not found error.", codec_decoder);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let codec = codec?;
        let preprocessor = tremor_runtime::preprocessor::lookup(codec_pre);
        if let Err(_e) = preprocessor {
            eprintln!("Error Preprocessor {} not found error.", codec_pre);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let preprocessor = preprocessor?;

        Ok(Self {
            is_interactive,
            is_pretty,
            buf: [0_u8; 4096],
            preprocessor,
            codec,
            buffer,
        })
    }

    fn process<T>(
        &mut self,
        runnable: &mut T,
        mut id: u64,
        mut egress: &mut Egress,
        handler: &IngressHandler<T>,
    ) -> Result<()> {
        loop {
            match self.buffer.read(&mut self.buf) {
                Ok(0) => {
                    // ALLOW: main.rs
                    return Ok(());
                }
                Ok(n) => {
                    let mut at = nanotime();
                    let x = self.preprocessor.process(&mut at, &self.buf[0..n])?;
                    for data in x {
                        let event = match self.codec.decode(data, at) {
                            Ok(Some(data)) => data,
                            Ok(None) => continue,
                            Err(e) => return Err(e.into()),
                        };
                        let event = event.suffix().value().clone();

                        if self.is_interactive {
                            eprintln!(
                                "ingress> [codec: {}], [preprocessor: {}]",
                                self.codec.name(),
                                self.preprocessor.name()
                            );
                            highlight(self.is_pretty, &event)?;
                        }
                        handler(runnable, &mut id, &mut egress, at, event)?;
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}

struct Egress {
    is_interactive: bool,
    is_pretty: bool,
    buffer: Box<dyn Write>,
    codec: Box<dyn Codec>,
    postprocessor: Box<dyn Postprocessor>,
}

impl Egress {
    fn from_args(matches: &ArgMatches) -> Result<Self> {
        let codec_post = matches.value_of("POSTPROCESSOR").unwrap_or("lines");
        let codec_encoder = matches.value_of("ENCODER").unwrap_or("json");
        let is_interactive = matches.is_present("interactive");
        let is_pretty = matches.is_present("pretty");

        let buffer: Box<dyn Write> = match matches.value_of("OUTFILE") {
            None | Some("-") => Box::new(BufWriter::new(io::stdout())),
            Some(data) => Box::new(BufWriter::new(File::create(data)?)),
        };

        let codec = tremor_runtime::codec::lookup(codec_encoder);
        if let Err(_e) = codec {
            eprintln!("Error Codec {} not found error.", codec_encoder);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let codec = codec?;

        let postprocessor = tremor_runtime::postprocessor::lookup(codec_post);
        if let Err(_e) = postprocessor {
            eprintln!("Error Postprocessor {} not found error.", codec_post);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let postprocessor = postprocessor?;

        Ok(Self {
            is_interactive,
            is_pretty,
            buffer,
            postprocessor,
            codec,
        })
    }

    fn process(&mut self, _src: &str, event: &Value, ret: Result<Return>) -> Result<()> {
        match ret {
            Ok(Return::Drop) => Ok(()),
            Ok(Return::Emit { value, port }) => {
                match port.unwrap_or_else(|| String::from("out")).as_str() {
                    "error" | "stderr" => {
                        self.buffer
                            .write_all(format!("{}\n", value.encode()).as_bytes())?;
                        self.buffer.flush()?;
                    }
                    _ => {
                        if self.is_interactive {
                            eprintln!(
                                "egress> [codec: {}], [postprocessor: {}]",
                                self.codec.name(),
                                self.postprocessor.name()
                            );
                            highlight(self.is_pretty, &value)?;
                        }

                        let encoded = self.codec.encode(&value);

                        let ppd = self
                            .postprocessor
                            .process(nanotime(), nanotime(), &encoded?);
                        for packet in ppd? {
                            self.buffer.write_all(&packet)?;
                            self.buffer.flush()?;
                        }
                    }
                };
                self.buffer.flush()?;
                Ok(())
            }
            Ok(Return::EmitEvent { port }) => {
                match port.unwrap_or_else(|| String::from("out")).as_str() {
                    "error" | "stderr" => {
                        eprintln!("{}", event.encode());
                    }
                    _ => {
                        self.buffer
                            .write_all(format!("{}\n", event.encode()).as_bytes())?;
                        self.buffer.flush()?;
                    }
                };
                Ok(())
            }
            Err(e) => {
                eprintln!("error processing event: {}", e);
                Err(e)
            }
        }
    }
}

fn run_tremor_source(matches: &ArgMatches, src: String) -> Result<()> {
    let raw = slurp_string(&src);
    if let Err(e) = raw {
        eprintln!("Error processing file {}: {}", &src, e);
        // ALLOW: main.rs
        std::process::exit(1);
    }
    let raw = raw?;

    let reg: Registry = registry::registry();
    let mp = load_module_path();
    let mut h = TermHighlighter::new();

    match Script::parse(&mp, &src, raw.clone(), &reg) {
        Ok(mut script) => {
            script.format_warnings_with(&mut h)?;

            let mut ingress = Ingress::from_args(&matches)?;
            let mut egress = Egress::from_args(&matches)?;
            let id = 0_u64;

            ingress.process(
                &mut script,
                id,
                &mut egress,
                &move |runnable, _id, egress, at, event| {
                    let mut global_map = Value::object();
                    let mut state = Value::null();
                    let mut event = event.clone_static();
                    match runnable.run(
                        &EventContext::new(at, Some(EventOriginUri::default())),
                        AggrType::Tick,
                        &mut event,
                        &mut state,
                        &mut global_map,
                    ) {
                        Ok(r) => egress.process(&src, &event, Ok(r)),
                        Err(e) => egress.process(&src, &event, Err(e.into())),
                    }?;
                    Ok(())
                },
            )?;

            Ok(())
        }
        Err(e) => {
            if let Err(e) = Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
            Err(e.into())
        }
    }
}

fn run_trickle_source(matches: &ArgMatches, src: &str) -> Result<()> {
    let raw = slurp_string(&src);
    if let Err(e) = raw {
        eprintln!("Error processing file {}: {}", &src, e);
        // ALLOW: main.rs
        std::process::exit(1);
    }
    let raw = raw?;

    let reg: Registry = registry::registry();
    let aggr = registry::aggr();
    let mp = load_module_path();
    let mut h = TermHighlighter::new();

    let runnable = match Query::parse(&mp, &src, &raw, vec![], &reg, &aggr) {
        Ok(runnable) => runnable,
        Err(e) => {
            if let Err(e) = Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
            // ALLOW: main.rs
            std::process::exit(1);
        }
    };

    runnable.format_warnings_with(&mut h)?;

    let mut ingress = Ingress::from_args(&matches)?;
    let mut egress = Egress::from_args(&matches)?;

    let runnable = tremor_pipeline::query::Query(runnable);
    let mut pipeline = runnable.to_pipe()?;
    let id = 0_u64;

    ingress.process(
        &mut pipeline,
        id,
        &mut egress,
        &move |runnable, id, egress, at, event| {
            let value = LineValue::new(vec![], |_| unsafe {
                std::mem::transmute(ValueAndMeta::from(event.clone()))
            });

            let mut continuation = vec![];

            runnable.enqueue(
                "in",
                tremor_pipeline::Event {
                    id: *id,
                    ingest_ns: at,
                    origin_uri: None,
                    is_batch: false,
                    kind: None,
                    data: value.clone(),
                },
                &mut continuation,
            )?;
            *id += 1;

            for (port, rvalue) in continuation.drain(..) {
                egress.process(
                    &simd_json::to_string_pretty(&value.suffix().value())?,
                    &event,
                    Ok(Return::Emit {
                        value: rvalue.data.suffix().value().clone_static(),
                        port: Some(port.to_string()),
                    }),
                )?;
            }

            Ok(())
        },
    )?;

    h.finalize()?;

    Ok(())
}

fn run_pipeline_source(matches: &ArgMatches, src: &str) -> Result<()> {
    let config: tremor_runtime::config::Config = serde_yaml::from_str(&slurp_string(&src)?)?;
    let runtime = tremor_runtime::incarnate(config)?;
    let pipeline = &runtime.pipes[0];
    let mut pipeline = pipeline.to_executable_graph(tremor_pipeline::buildin_ops)?;

    let mut ingress = Ingress::from_args(&matches)?;
    let mut egress = Egress::from_args(&matches)?;
    let id = 0_u64;

    ingress.process(
        &mut pipeline,
        id,
        &mut egress,
        &move |runnable, id, egress, at, event| {
            let value = LineValue::new(vec![], |_| unsafe {
                std::mem::transmute(ValueAndMeta::from(event.clone()))
            });

            let mut continuation = vec![];

            runnable.enqueue(
                "in",
                tremor_pipeline::Event {
                    id: *id,
                    ingest_ns: at,
                    origin_uri: None,
                    is_batch: false,
                    kind: None,
                    data: value.clone(),
                },
                &mut continuation,
            )?;
            *id += 1;

            for (port, rvalue) in continuation.drain(..) {
                egress.process(
                    &simd_json::to_string_pretty(&value.suffix().value())?,
                    &event,
                    Ok(Return::Emit {
                        value: rvalue.data.suffix().value().clone_static(),
                        port: Some(port.to_string()),
                    }),
                )?;
            }

            Ok(())
        },
    )?;

    Ok(())
}

pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    let script_file = matches
        .value_of("SCRIPT")
        .ok_or_else(|| Error::from("No script file provided"))?;
    let script_file = script_file.to_string();
    match get_source_kind(&script_file) {
        SourceKind::Tremor | SourceKind::Json => run_tremor_source(&matches, script_file),
        SourceKind::Trickle => run_trickle_source(&matches, &script_file),
        SourceKind::Pipeline => run_pipeline_source(&matches, &script_file),
        SourceKind::Unsupported => {
            eprintln!("Error: Unable to execute source: {}", &script_file);
            // ALLOW: main.rs
            std::process::exit(1);
        }
    }
}
