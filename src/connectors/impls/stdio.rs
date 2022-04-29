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
use crate::connectors::prelude::*;
use crate::utils::hostname;
use async_broadcast::{broadcast, Receiver};
use async_std::io::{stderr, stdin, stdout, ReadExt, Stderr, Stdout};
use beef::Cow;
use futures::AsyncWriteExt;

use tremor_pipeline::{EventOriginUri, DEFAULT_STREAM_ID};

const INPUT_SIZE_BYTES: usize = 8192;

lazy_static! {
    pub(crate) static ref STDIN: Receiver<Vec<u8>> = {
        // This gets initialized only once - the first time a stdio connector
        // is created, after that we simply clone the channel.
        let (mut tx, rx) = broadcast(crate::QSIZE.load(Ordering::Relaxed));
        // We user overflow so that non collected messages can be removed
        // is this what we want? for STDIO it should be good enough
        tx.set_overflow(true);
        async_std::task::spawn(async move {
            let mut stream = stdin();
            let mut buffer = [0_u8; INPUT_SIZE_BYTES];
            while let Ok(len) = stream.read(&mut buffer).await {
                if len == 0 {
                    error!("STDIN empty?!?");
                    break;
                    // ALLOW: we get len from read
                } else if let Err(e) = tx.broadcast(buffer[0..len].to_vec()).await {
                    error!("STDIN error: {}", e);
                    break;
                }
            }
        });
        rx
    };
}

/// connector handling 1 std stream (stdout, stderr or stdin)
pub struct StdStreamConnector {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "stdio".into()
    }
    async fn build(&self, _id: &str, _raw_config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        Ok(Box::new(StdStreamConnector {}))
    }
}

/// stdstream source (stdin)
pub struct StdStreamSource {
    stdin: Option<Receiver<Vec<u8>>>,
    origin_uri: EventOriginUri,
}

impl StdStreamSource {
    fn new() -> Self {
        Self {
            stdin: None,
            origin_uri: EventOriginUri {
                scheme: "tremor-stdin".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
        }
    }
}

#[async_trait::async_trait()]
impl Source for StdStreamSource {
    #[allow(clippy::option_if_let_else)]
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let stdin = if let Some(stdin) = &mut self.stdin {
            stdin
        } else {
            self.stdin.insert(STDIN.clone())
        };
        let data = stdin
            .recv()
            .await
            .map_err(|e| Error::from(format!("recv error: {e}")))?;
        Ok(SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            data,
            meta: None,
            stream: Some(DEFAULT_STREAM_ID),
            port: None,
            codec_overwrite: None,
        })
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

/// stdstream sink
pub struct StdStreamSink {
    stderr: Stderr,
    stdout: Stdout,
}

impl StdStreamConnector {
    const IN_PORTS: [Cow<'static, str>; 3] =
        [IN, Cow::const_str("stdout"), Cow::const_str("stderr")];
    const REF_IN_PORTS: &'static [Cow<'static, str>; 3] = &Self::IN_PORTS;
}

#[async_trait::async_trait()]
impl Sink for StdStreamSink {
    async fn on_event(
        &mut self,
        input: &str,
        event: tremor_pipeline::Event,
        _ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (value, _meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, event.ingest_ns)?;
            for chunk in data {
                match input {
                    "in" | "stdout" => self.stdout.write_all(&chunk).await?,
                    "stderr" => self.stderr.write_all(&chunk).await?,
                    _ => {
                        return Err(
                            "{} is not a valid port, use one of `in`, `stdout` or `stderr`".into(),
                        )
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
    fn input_ports(&self) -> &[Cow<'static, str>] {
        Self::REF_IN_PORTS
    }

    /// create sink if we have a stdout or stderr stream
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = StdStreamSink {
            stdout: stdout(),
            stderr: stderr(),
        };
        let addr = builder.spawn(sink, sink_context)?;
        Ok(Some(addr))
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = StdStreamSource::new();
        builder.spawn(source, source_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}
