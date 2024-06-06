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

use crate::cli::Run;
use crate::env;
use crate::errors::Result;
use crate::util::{get_source_kind, highlight, slurp_string, SourceKind};
use futures::executor::block_on;
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter};
use tremor_codec::Codec;
use tremor_common::{
    file,
    ids::OperatorIdGen,
    ports::{Port, IN},
    time::nanotime,
};
use tremor_interceptor::{postprocessor, preprocessor};
use tremor_runtime::system::{World, WorldConfig};
use tremor_script::{
    arena::Arena,
    highlighter::{Error as HighlighterError, Highlighter, Term as TermHighlighter},
    lexer::Lexer,
    prelude::*,
    query::Query,
    script::Script,
};
use tremor_system::event::{Event, EventId};

struct Ingress {
    is_interactive: bool,
    is_pretty: bool,
    buf: [u8; 4096],
    buffer: Box<dyn BufRead>,
    preprocessor: Box<dyn preprocessor::Preprocessor>,
    codec: Box<dyn Codec>,
}

type IngressHandler<T> =
    dyn Fn(&mut T, &mut u64, &mut Egress, &mut Value<'static>, u64, Value) -> Result<()>;

impl Ingress {
    fn from_args(cmd: &Run) -> Result<Self> {
        let buffer: Box<dyn BufRead> = if cmd.infile == "-" {
            Box::new(BufReader::new(io::stdin()))
        } else {
            Box::new(BufReader::new(crate::open_file(&cmd.infile, None)?))
        };

        let codec = tremor_codec::resolve(&tremor_codec::Config::from(&cmd.decoder));
        if let Err(_e) = codec {
            eprintln!("Error Codec {} not found error.", cmd.decoder);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let codec = codec?;
        let preprocessor = preprocessor::lookup(&cmd.preprocessor);
        if let Err(_e) = preprocessor {
            eprintln!("Error Preprocessor {} not found error.", cmd.preprocessor);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let preprocessor = preprocessor?;

        Ok(Self {
            is_interactive: cmd.interactive,
            is_pretty: cmd.pretty,
            buf: [0_u8; 4096],
            preprocessor,
            codec,
            buffer,
        })
    }

    async fn process<T>(
        &mut self,
        runnable: &mut T,
        mut id: u64,
        egress: &mut Egress,
        handler: &IngressHandler<T>,
    ) -> Result<()> {
        let mut state: Value<'static> = Value::null();
        loop {
            match self.buffer.read(&mut self.buf) {
                Ok(0) => {
                    // ALLOW: main.rs
                    return Ok(());
                }
                Ok(n) => {
                    let mut at = nanotime();
                    // We access the entire read buffer the len is provided by read
                    let x = self.preprocessor.process(
                        &mut at,
                        unsafe { self.buf.get_unchecked(0..n) },
                        Value::object(),
                    )?;
                    for (mut data, meta) in x {
                        let event = match self.codec.decode(data.as_mut_slice(), at, meta).await {
                            Ok(Some((data, _))) => data,
                            Ok(None) => continue,
                            Err(e) => return Err(e.into()),
                        };

                        if self.is_interactive {
                            eprintln!(
                                "ingress> [codec: {}], [preprocessor: {}]",
                                self.codec.name(),
                                self.preprocessor.name()
                            );
                            highlight(self.is_pretty, &event)?;
                        }
                        handler(runnable, &mut id, egress, &mut state, at, event)?;
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
    postprocessor: Box<dyn postprocessor::Postprocessor>,
}

impl Egress {
    fn from_args(cmd: &Run) -> Result<Self> {
        let buffer: Box<dyn Write> = if cmd.outfile == "-" {
            Box::new(BufWriter::new(io::stdout()))
        } else {
            Box::new(BufWriter::new(file::create(&cmd.outfile)?))
        };

        let codec = tremor_codec::resolve(&tremor_codec::Config::from(&cmd.encoder));
        if let Err(_e) = codec {
            eprintln!("Error Codec {} not found error.", cmd.encoder);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let codec = codec?;

        let postprocessor = postprocessor::lookup(&cmd.postprocessor);
        if let Err(_e) = postprocessor {
            eprintln!("Error Postprocessor {} not found error.", cmd.postprocessor);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let postprocessor = postprocessor?;

        Ok(Self {
            is_interactive: cmd.interactive,
            is_pretty: cmd.pretty,
            buffer,
            codec,
            postprocessor,
        })
    }

    async fn process<'v>(&mut self, event: &Value<'v>, ret: Return<'v>) -> Result<()> {
        match ret {
            Return::Drop => Ok(()),
            Return::Emit { value, port } => {
                if port == Some(Port::Err) {
                    self.buffer
                        .write_all(format!("{}\n", value.encode()).as_bytes())?;
                    self.buffer.flush()?;
                } else {
                    if self.is_interactive {
                        eprintln!(
                            "egress> [codec: {}], [postprocessor: {}]",
                            self.codec.name(),
                            self.postprocessor.name()
                        );
                        highlight(self.is_pretty, &value)?;
                    }

                    let encoded = self.codec.encode(&value, &Value::const_null()).await;

                    let ppd = self
                        .postprocessor
                        .process(nanotime(), nanotime(), &encoded?);
                    for packet in ppd? {
                        self.buffer.write_all(&packet)?;
                        self.buffer.flush()?;
                    }
                };
                self.buffer.flush()?;
                Ok(())
            }
            Return::EmitEvent { port } => {
                if port == Some(Port::Err) {
                    eprintln!("{}", event.encode());
                } else {
                    self.buffer
                        .write_all(format!("{}\n", event.encode()).as_bytes())?;
                    self.buffer.flush()?;
                };
                Ok(())
            }
        }
    }
}

impl Run {
    async fn run_tremor_source(&self) -> Result<()> {
        let raw = slurp_string(&self.script);
        if let Err(e) = raw {
            eprintln!("Error processing file {}: {e}", self.script);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let raw = raw?;
        let (aid, raw) = Arena::insert(&raw)?;

        let env = env::setup()?;

        let mut h = TermHighlighter::stderr();
        match Script::parse(raw, &env.fun) {
            Ok(mut script) => {
                script.format_warnings_with(&mut h)?;

                let mut ingress = Ingress::from_args(self)?;
                let mut egress = Egress::from_args(self)?;
                let id = 0_u64;
                let src = self.script.clone();
                ingress
                    .process(
                        &mut script,
                        id,
                        &mut egress,
                        &move |runnable, _id, egress, state, at, event| {
                            let mut global_map = Value::object();
                            let mut event = event.clone_static();
                            match runnable.run(
                                &EventContext::new(at, None),
                                AggrType::Tick,
                                &mut event,
                                state,
                                &mut global_map,
                            ) {
                                Ok(r) => block_on(egress.process(&event, r)),
                                Err(e) => {
                                    if let (Some(r), _) = e.context() {
                                        let mut inner = TermHighlighter::stderr();
                                        let tokens: Vec<_> =
                                            Lexer::new(raw, aid).tokenize_until_err().collect();

                                        if let Err(highlight_error) = inner.highlight_error(
                                            Some(&src),
                                            &tokens,
                                            "",
                                            true,
                                            Some(r),
                                            Some(HighlighterError::from(&e)),
                                        ) {
                                            eprintln!(
                                            "Error during error highlighting: {highlight_error}",
                                        );
                                            Err(highlight_error.into())
                                        } else {
                                            inner.finalize()?;
                                            Ok(()) // error has already been displayed
                                        }
                                    } else {
                                        eprintln!("Error processing event: {e}");
                                        Err(e.into())
                                    }
                                }
                            }
                        },
                    )
                    .await?;

                Ok(())
            }
            Err(e) => {
                if let Err(e) = h.format_error(&e) {
                    eprintln!("Error: {e}");
                };

                // ALLOW: main.rs
                std::process::exit(1);
            }
        }
    }

    async fn run_trickle_source(&self) -> Result<()> {
        let raw = slurp_string(&self.script);
        if let Err(e) = raw {
            eprintln!("Error processing file {}: {e}", self.script);
            // ALLOW: main.rs
            std::process::exit(1);
        }
        let raw = raw?;
        let env = env::setup()?;
        let mut h = TermHighlighter::stderr();

        let runnable = match Query::parse(&raw, &env.fun, &env.aggr) {
            Ok(runnable) => runnable,
            Err(e) => {
                if let Err(e) = h.format_error(&e) {
                    eprintln!("Error: {e}");
                };
                // ALLOW: main.rs
                std::process::exit(1);
            }
        };
        self.run_trickle_query(runnable, self.script.to_string(), &mut h)
            .await
    }

    async fn run_trickle_query(
        &self,
        runnable: Query,
        file: String,
        h: &mut TermHighlighter,
    ) -> Result<()> {
        runnable.format_warnings_with(h)?;

        let mut ingress = Ingress::from_args(self)?;
        let mut egress = Egress::from_args(self)?;

        let runnable = tremor_pipeline::query::Query(runnable);
        let mut idgen = OperatorIdGen::new();
        let mut pipeline = runnable.to_executable_graph(&mut idgen)?;
        let id = 0_u64;

        ingress
            .process(
                &mut pipeline,
                id,
                &mut egress,
                &move |runnable, id, egress, _state, at, event| {
                    let value =
                        EventPayload::new(vec![], |_| ValueAndMeta::from(event.clone_static()));

                    let mut continuation = vec![];

                    if let Err(e) = runnable.enqueue(
                        IN,
                        Event {
                            id: EventId::from_id(0, 0, *id),
                            data: value.clone(),
                            ingest_ns: at,
                            ..Event::default()
                        },
                        &mut continuation,
                    ) {
                        match e.0 {
                            tremor_pipeline::errors::ErrorKind::Script(script_kind) => {
                                let script_error: tremor_script::errors::Error = script_kind.into();
                                if let (Some(r), _) = script_error.context() {
                                    let mut inner = TermHighlighter::stderr();
                                    let aid = script_error.aid();
                                    let input = Arena::io_get(aid)?;

                                    let tokens: Vec<_> =
                                        Lexer::new(input, aid).tokenize_until_err().collect();

                                    if let Err(highlight_error) = inner.highlight_error(
                                        Some(&file),
                                        &tokens,
                                        "",
                                        true,
                                        Some(r),
                                        Some(HighlighterError::from(&script_error)),
                                    ) {
                                        eprintln!(
                                            "Error during error highlighting: {highlight_error}"
                                        );
                                        return Err(highlight_error.into());
                                    }
                                    inner.finalize()?;
                                    return Ok(());
                                }
                            }
                            _ => {
                                return Err(e.into());
                            }
                        }
                    }
                    *id += 1;

                    for (port, rvalue) in continuation.drain(..) {
                        block_on(egress.process(
                            &event,
                            Return::Emit {
                                value: rvalue.data.suffix().value().clone_static(),
                                port: Some(port),
                            },
                        ))?;
                    }

                    Ok(())
                },
            )
            .await?;

        h.finalize()?;

        Ok(())
    }

    async fn run_troy_source(&self) -> Result<()> {
        let config = WorldConfig {
            debug_connectors: true,
        };
        let (world, handle) = World::start(config).await?;
        tremor_runtime::load_troy_file(&world, &self.script).await?;
        handle.await??;
        Ok(())
    }

    pub(crate) async fn run(&self) -> Result<()> {
        match get_source_kind(&self.script) {
            SourceKind::Troy => self.run_troy_source().await,
            SourceKind::Trickle => self.run_trickle_source().await,
            SourceKind::Tremor => self.run_tremor_source().await,
            SourceKind::Archive | SourceKind::Json | SourceKind::Unsupported(_) => {
                Err(format!("Error: Unable to execute source: {}", &self.script).into())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use tokio::time::timeout;

    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn run_troy_source() -> Result<()> {
        let r = Run {
            script: "tests/fixtures/exit.troy".into(),
            interactive: false,
            pretty: false,
            encoder: "json".into(),
            decoder: "json".into(),
            infile: "-".into(),
            outfile: "-".into(),
            preprocessor: String::new(),
            postprocessor: String::new(),
            port: None,
        };
        timeout(Duration::from_secs(1), r.run()).await?
    }
}
