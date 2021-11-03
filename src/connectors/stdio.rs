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
use async_broadcast::{broadcast, Receiver, TryRecvError};
use async_std::io::{stderr, stdin, stdout, ReadExt, Stderr, Stdout, Write};
use futures::AsyncWriteExt;

use tremor_pipeline::{EventOriginUri, DEFAULT_STREAM_ID};

const INPUT_SIZE_BYTES: usize = 8192;

// FIXME: consider stderr being a port or metadata instead of a own version of the connector

lazy_static! {
    pub(crate) static ref STDIN: Receiver<Vec<u8>> = {
        // This gets initialized only once - the first time a stdio connector
        // is created, after that we simply clone the channel.
        let (mut tx, rx) = broadcast(crate::QSIZE.load(Ordering::Relaxed));
        // We user overflow so that non collected messages can be removed
        // FIXME: is this what we want? for STDIO it should be good enough
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

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum StdStream {
    Stdout,
    Stderr,
    None,
}
impl Default for StdStream {
    fn default() -> Self {
        Self::Stdout
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "Default::default")]
    output: StdStream, //FIXME should std_Stream be stdio + err depending on port?
}

impl ConfigImpl for Config {}

/// connector handling 1 std stream (stdout, stderr or stdin)
pub struct StdStreamConnector {
    stream: StdStream,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        _id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw) = raw_config {
            let config = Config::new(raw)?;
            Ok(Box::new(StdStreamConnector {
                stream: config.output,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("std_stream")).into())
        }
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
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let stdin = if let Some(stdin) = &mut self.stdin {
            stdin
        } else {
            self.stdin.insert(STDIN.clone())
        };
        match stdin.try_recv() {
            Ok(data) => Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                // ALLOW: len cannot be > INPUT_SIZE_BYTES
                data,
                meta: None,
                stream: DEFAULT_STREAM_ID,
                port: None,
            }),
            Err(TryRecvError::Closed) => Err(TryRecvError::Closed.into()),
            Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
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
    ) -> Result<SinkReply> {
        for (value, _meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, event.ingest_ns)?;
            for chunk in data {
                self.stream.write_all(&chunk).await?;
            }
        }
        self.stream.flush().await?;
        Ok(SinkReply::ACK)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

#[async_trait::async_trait()]
impl Connector for StdStreamConnector {
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
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
                let sink: StdStreamSink<Stdout> = StdStreamSink { stream: stdout() };
                builder.spawn(sink, sink_context)?
            }
            StdStream::Stderr => {
                let sink: StdStreamSink<Stderr> = StdStreamSink { stream: stderr() };
                builder.spawn(sink, sink_context)?
            }
            StdStream::None => return Ok(None),
        };
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

    fn default_codec(&self) -> &str {
        "json"
    }
}
