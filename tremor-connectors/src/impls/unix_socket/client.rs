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

use super::UnixSocketReader;
use crate::{prelude::*, utils::socket};
use std::{path::PathBuf, sync::Arc};
use tokio::{
    io::{split, AsyncWriteExt, WriteHalf},
    net::UnixStream,
};

const URL_SCHEME: &str = "tremor-unix-socket-client";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    path: String,
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl tremor_config::Impl for Config {}

/// Builder for the unix socket client connector
#[derive(Debug, Default)]
pub struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "unix_socket_client".into()
    }
    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        conf: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(conf)?;
        let (source_tx, source_rx) = bounded(qsize());
        Ok(Box::new(Client {
            config,
            source_tx,
            source_rx: Some(source_rx),
        }))
    }
}

pub(crate) struct Client {
    config: Config,
    source_tx: Sender<SourceReply>,
    source_rx: Option<Receiver<SourceReply>>,
}

#[async_trait::async_trait()]
impl Connector for Client {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        // this source is wired up to the ending channel that is forwarding data received from the TCP (or TLS) connection
        let source = ChannelSource::from_channel(
            self.source_tx.clone(),
            self.source_rx
                .take()
                .ok_or(GenericImplementationError::AlreadyConnected)?,
            Arc::default(), // we don't need to know if the source is connected. Worst case if nothing is connected is that the receiving task is blocked.
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = UnixSocketSink::new(self.config.clone(), self.source_tx.clone());
        Ok(Some(builder.spawn(sink, ctx)))
    }
}

struct UnixSocketSink {
    config: Config,
    source_runtime: ChannelSourceRuntime,
    writer: Option<WriteHalf<UnixStream>>,
}

impl UnixSocketSink {
    fn new(config: Config, source_tx: Sender<SourceReply>) -> Self {
        let source_runtime = ChannelSourceRuntime::new(source_tx);
        Self {
            config,
            source_runtime,
            writer: None,
        }
    }

    async fn write(&mut self, data: Vec<Vec<u8>>) -> anyhow::Result<()> {
        let stream = self.writer.as_mut().ok_or(socket::Error::NoSocket)?;
        for chunk in data {
            let slice: &[u8] = chunk.as_slice();
            stream.write_all(slice).await?;
        }
        // TODO: necessary?
        stream.flush().await?;
        Ok(())
    }
}

#[async_trait::async_trait()]
impl Sink for UnixSocketSink {
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let path = PathBuf::from(&self.config.path);
        if !path.exists() {
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
        let (reader, writer) = split(stream);
        self.writer = Some(writer);
        let reader = UnixSocketReader::new(
            reader,
            self.config.buf_size,
            ctx.alias().to_string(),
            origin_uri,
            meta,
            None,
        );
        self.source_runtime
            .register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);
        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_system::event::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        let ingest_ns = event.ingest_ns;
        for (value, meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, meta, ingest_ns).await?;
            if let Err(e) = self.write(data).await {
                error!("{ctx} Error sending data: {e}. Initiating Reconnect...");
                self.writer = None;
                ctx.notifier().connection_lost().await?;
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
