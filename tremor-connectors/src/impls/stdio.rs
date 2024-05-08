// Copyright 2021, The Tremor Team
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

#![allow(clippy::doc_markdown)]
//! The stdio connector integrates with standard input, output and error
//! streams on the host operating system. These facilities are useful for
//! integrating with UNIX pipelines or processing with command line tools
//! such as `jq` during development.
//!
//! The connector redirects `stdin`, `stdout` and `stderr` from the host operating
//! system through the connected pipelines in tremor flow definitions accordingly
//!
//! ## Configuration
//!
//! ```tremor
//! define connector console from stdio;
//!
//! create connector console from console;
//!
//! connect /connector/console to /pipeline/stdin;
//!
//! connect /pipeline/stdout to /connector/console;
//!
//! connect /pipeline/stderr to /connector/console/err;
//! ```
//!
//! ## NOTE
//!
//! When using the `tremor` command line interface stdard input and output are
//! often connected with line delimited JSON codecs as a convenience for rapid
//! applicaiton development.
//!
//! :::note
//! These defaults can be overridden in most of the builtin cli tools.
//!
//! Consult the builtin help via the `--help` or `-h` flags for each
//! sub command to find out more
//! :::

use crate::{sink::prelude::*, source::prelude::*};
use lazy_static::lazy_static;
use tokio::{
    io::{stderr, stdin, stdout, AsyncReadExt, AsyncWriteExt, Stderr, Stdout},
    sync::broadcast::{channel as broadcast, error::RecvError, Receiver},
};
use tremor_common::ports::{Port, IN};
use tremor_system::event::DEFAULT_STREAM_ID;
const INPUT_SIZE_BYTES: usize = 8192;

lazy_static! {
    pub(crate) static ref STDIN: Receiver<Vec<u8>> = {
        // This gets initialized only once - the first time a stdio connector
        // is created, after that we simply clone the channel.
        let (tx, rx) = broadcast(qsize());
        // We user overflow so that non collected messages can be removed
        // is this what we want? for STDIO it should be good enough
        tokio::task::spawn(async move {
            let mut stream = stdin();
            let mut buffer = [0_u8; INPUT_SIZE_BYTES];
            while let Ok(len) = stream.read(&mut buffer).await {
                if len == 0 {
                    info!("STDIN done reading.");
                    break;
                    // ALLOW: we get len from read
                } else if let Err(e) = tx.send(buffer[0..len].to_vec()) {
                    error!("STDIN error: {}", e);
                    break;
                }
            }
        });
        rx
    };
}

/// connector handling 1 std stream (stdout, stderr or stdin)
pub(crate) struct StdStreamConnector {}

/// Builder for the stdio connector
#[derive(Debug, Default)]
pub struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "stdio".into()
    }
    async fn build(
        &self,
        _id: &alias::Connector,
        _raw_config: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        Ok(Box::new(StdStreamConnector {}))
    }
}

/// stdstream source (stdin)
pub(crate) struct StdStreamSource {
    stdin: Option<Receiver<Vec<u8>>>,
    origin_uri: EventOriginUri,
    done: bool,
}

impl StdStreamSource {
    fn new() -> Self {
        Self {
            stdin: None,
            origin_uri: EventOriginUri {
                scheme: "tremor-stdin".to_string(),
                host: crate::utils::hostname(),
                port: None,
                path: vec![],
            },
            done: false,
        }
    }
}

#[async_trait::async_trait()]
impl Source for StdStreamSource {
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        let reply = if self.done {
            SourceReply::Finished
        } else {
            let stdin = self.stdin.get_or_insert_with(|| STDIN.resubscribe());
            loop {
                match stdin.recv().await {
                    Ok(data) => {
                        break SourceReply::Data {
                            origin_uri: self.origin_uri.clone(),
                            data,
                            meta: None,
                            stream: Some(DEFAULT_STREAM_ID),
                            port: None,
                            codec_overwrite: None,
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue, // retry, this is expected
                    Err(RecvError::Closed) => {
                        // receive error from broadcast channel
                        // either the stream is done (in case of a pipe)
                        // or everything is very broken. Either way, ending the stream seems appropriate.
                        self.done = true;
                        break SourceReply::EndStream {
                            origin_uri: self.origin_uri.clone(),
                            stream: DEFAULT_STREAM_ID,
                            meta: None,
                        };
                    }
                }
            }
        };
        Ok(reply)
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        // if we would put true here, the runtime would pull until we are done/empty or finished.
        // no such signal will arrive, so we instead say we are done immediately
        false
    }
}

/// stdstream sink
pub(crate) struct StdStreamSink {
    stderr: Stderr,
    stdout: Stdout,
}

impl StdStreamConnector {
    const IN_PORTS: [Port<'static>; 3] = [IN, Port::const_str("stdout"), Port::const_str("stderr")];
    const REF_IN_PORTS: &'static [Port<'static>; 3] = &Self::IN_PORTS;
}

#[async_trait::async_trait()]
impl Sink for StdStreamSink {
    async fn on_event(
        &mut self,
        input: &str,
        event: tremor_system::event::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        for (value, meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, meta, event.ingest_ns).await?;
            for chunk in data {
                match input {
                    "in" | "stdout" => self.stdout.write_all(&chunk).await?,
                    "stderr" => self.stderr.write_all(&chunk).await?,
                    _ => {
                        return Err(crate::Error::InvalidPort(
                            ctx.alias().clone(),
                            input.to_string().into(),
                        )
                        .into())
                    }
                }
            }
        }
        self.stdout.flush().await?;
        self.stderr.flush().await?;
        Ok(SinkReply::ACK)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

#[async_trait::async_trait()]
impl Connector for StdStreamConnector {
    fn input_ports(&self) -> &[Port<'static>] {
        Self::REF_IN_PORTS
    }

    /// create sink if we have a stdout or stderr stream
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = StdStreamSink {
            stdout: stdout(),
            stderr: stderr(),
        };
        Ok(Some(builder.spawn(sink, ctx)))
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let source = StdStreamSource::new();
        Ok(Some(builder.spawn(source, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_consts() {
        let source = StdStreamSource {
            stdin: None,
            origin_uri: EventOriginUri::default(),
            done: false,
        };
        assert!(!source.asynchronous());
        assert!(!source.is_transactional());
    }
    #[test]
    fn sink_consts() {
        let sink = StdStreamSink {
            stdout: stdout(),
            stderr: stderr(),
        };
        assert!(sink.auto_ack());
    }
    #[test]
    fn connector_consts() {
        let connector = StdStreamConnector {};
        assert_eq!(connector.codec_requirements(), CodecReq::Required);
        assert_eq!(connector.input_ports(), ["in", "stdout", "stderr"]);
    }
}
