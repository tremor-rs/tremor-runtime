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
use async_std::io::{stderr, stdin, stdout, ReadExt, Stderr, Stdin, Stdout, Write};
use futures::AsyncWriteExt;
use tremor_pipeline::{EventOriginUri, DEFAULT_STREAM_ID};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum StdStream {
    Stdout,
    Stderr,
    Stdin,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    stream: StdStream,
    #[serde(default = "Default::default")]
    prefix: String,
    /// print non-string payloads as raw bytes, not in debug formatting
    #[serde(default = "Default::default")]
    raw: bool,
}

impl ConfigImpl for Config {}

/// connector handling 1 std stream (stdout, stderr or stdin)
pub struct StdStreamConnector {
    stream: StdStream,
    prefix: String,
    raw: bool,
}

pub(crate) struct Builder {}
impl ConnectorBuilder for Builder {
    fn from_config(
        &self,
        _id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw) = raw_config {
            let config = Config::new(raw)?;
            Ok(Box::new(StdStreamConnector {
                stream: config.stream,
                prefix: config.prefix,
                raw: config.raw,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("std_stream")).into())
        }
    }
}

/// stdstream source (stdin)
pub struct StdStreamSource {
    stream: Stdin,
    buffer: Vec<u8>,
    origin_uri: EventOriginUri,
}

impl StdStreamSource {
    const INPUT_SIZE_BYTES: usize = 8192;

    fn new() -> Self {
        Self {
            stream: stdin(),
            buffer: vec![0; Self::INPUT_SIZE_BYTES],
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
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let len = self.stream.read(&mut self.buffer).await?;
        if len == 0 {
            // reached the end of stdin
            // FIXME: initiate state change to stop this source
            Ok(SourceReply::Empty(1000))
        } else {
            Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                // ALLOW: len cannot be > INPUT_SIZE_BYTES
                data: self.buffer[0..len].to_vec(),
                meta: None,
                stream: DEFAULT_STREAM_ID,
            })
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }
}

/// stdstream sink
pub struct StdStreamSink<T>
where
    T: Write + std::marker::Unpin + Send,
{
    stream: T,
    prefix: String,
    raw: bool,
}

#[async_trait::async_trait()]
impl<T> Sink for StdStreamSink<T>
where
    T: Write + std::marker::Unpin + Send,
{
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        _ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> ResultVec {
        for (value, _meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, event.ingest_ns)?;
            for chunk in data {
                self.stream.write_all(self.prefix.as_bytes()).await?;
                if self.raw {
                    self.stream.write_all(&chunk).await?;
                } else if let Ok(s) = std::str::from_utf8(&chunk) {
                    self.stream.write_all(s.as_bytes()).await?;
                } else {
                    self.stream
                        .write_all(format!("{:?}", &chunk).as_bytes())
                        .await?;
                }
                self.stream.write_all(b"\n").await?;
            }
        }
        self.stream.flush().await?;
        Ok(vec![])
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

#[async_trait::async_trait()]
impl Connector for StdStreamConnector {
    async fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        _notifier: ConnectionLostNotifier,
    ) -> Result<bool> {
        Ok(true)
    }

    /// create sink if we have a stdout or stderr stream
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let addr = match self.stream {
            StdStream::Stdout => {
                let sink: StdStreamSink<Stdout> = StdStreamSink {
                    stream: stdout(),
                    prefix: self.prefix.clone(),
                    raw: self.raw,
                };
                builder.spawn(sink, sink_context)?
            }
            StdStream::Stderr => {
                let sink: StdStreamSink<Stderr> = StdStreamSink {
                    stream: stderr(),
                    prefix: self.prefix.clone(),
                    raw: self.raw,
                };
                builder.spawn(sink, sink_context)?
            }
            StdStream::Stdin => return Ok(None),
        };
        Ok(Some(addr))
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        Ok(if let StdStream::Stdin = self.stream {
            let source = StdStreamSource::new();
            let addr = builder.spawn(source, source_context)?;
            Some(addr)
        } else {
            None
        })
    }

    async fn on_start(&mut self, _ctx: &ConnectorContext) -> Result<ConnectorState> {
        Ok(ConnectorState::Running)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
