// Copyright 2022, The Tremor Team
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
use crate::errors::{Kind as ErrorKind, Result};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::os::unix::net::UnixStream;
use async_std::path::PathBuf;
use futures::AsyncWriteExt;

use super::UnixSocketReader;

const URL_SCHEME: &str = "tremor-unix-socket-client";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    path: String,
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "unix_socket_client".into()
    }
    async fn build_cfg(
        &self,
        _: &Alias,
        _: &ConnectorConfig,
        conf: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(conf)?;
        let (source_tx, source_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        Ok(Box::new(Client {
            config,
            source_tx,
            source_rx,
        }))
    }
}

pub struct Client {
    config: Config,
    source_tx: Sender<SourceReply>,
    source_rx: Receiver<SourceReply>,
}

#[async_trait::async_trait()]
impl Connector for Client {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        // this source is wired up to the ending channel that is forwarding data received from the TCP (or TLS) connection
        let source = ChannelSource::from_channel(self.source_tx.clone(), self.source_rx.clone());
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = UnixSocketSink::new(self.config.clone(), self.source_tx.clone());
        builder.spawn(sink, sink_context).map(Some)
    }
}

struct UnixSocketSink {
    config: Config,
    source_runtime: ChannelSourceRuntime,
    stream: Option<UnixStream>,
}

impl UnixSocketSink {
    fn new(config: Config, source_tx: Sender<SourceReply>) -> Self {
        let source_runtime = ChannelSourceRuntime::new(source_tx);
        Self {
            config,
            source_runtime,
            stream: None,
        }
    }

    async fn write(&mut self, data: Vec<Vec<u8>>) -> Result<()> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::from(ErrorKind::NoSocket))?;
        for chunk in data {
            let slice: &[u8] = chunk.as_slice();
            stream.write_all(slice).await?;
        }
        // TODO: necessary?
        stream.flush().await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.take() {
            stream.shutdown(std::net::Shutdown::Write)?;
        }
        Ok(())
    }
}

#[async_trait::async_trait()]
impl Sink for UnixSocketSink {
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let path = PathBuf::from(&self.config.path);
        if !path.exists().await {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("{} not found or not accessible.", path.display()),
            )
            .into());
        }
        let stream = UnixStream::connect(&path).await?;
        let origin_uri = EventOriginUri {
            scheme: URL_SCHEME.to_string(),
            host: hostname(),
            port: None,
            path: vec![path.display().to_string()],
        };
        let meta = ctx.meta(literal!({
            "peer": path.display().to_string()
        }));
        self.stream = Some(stream.clone());
        let reader = UnixSocketReader::new(
            stream,
            vec![0; self.config.buf_size],
            ctx.alias().to_string(),
            origin_uri,
            meta,
        );
        self.source_runtime
            .register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let ingest_ns = event.ingest_ns;
        for value in event.value_iter() {
            let data = serializer.serialize(value, ingest_ns)?;
            if let Err(e) = self.write(data).await {
                error!("{ctx} Error sending data: {e}. Initiating Reconnect...");
                self.stream = None;
                ctx.notifier().connection_lost().await?;
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }

    /// when writing is done
    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        // ignore error here
        if let Err(e) = self.close().await {
            error!("{ctx} Failed stopping: {e}..");
        }
        Ok(())
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
