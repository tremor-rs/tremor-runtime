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

//! Unix socket server
//!
//! Identifies client connections by their stream id, a u64.
//! There is no other data we can associate to a connection.
//!
//! When we have metadata like on an event we receive via the sink part of this connector:
//!
//! ```json
//! {
//!     "unix_socket_server": {
//!         "peer": 123
//!     }
//! }
//! ```
//!
//! We try to route the event to the connection with `stream_id` `123`.
use super::{UnixSocketReader, UnixSocketWriter};
use crate::{
    sink::{
        channel_sink::{ChannelSink, ChannelSinkMsg, ChannelSinkRuntime},
        prelude::*,
    },
    source::{channel_source::ChannelSourceRuntime, prelude::*},
    spawn_task, StreamIdGen, ACCEPT_TIMEOUT,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    io::split,
    net::UnixListener,
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
    time::timeout,
};
use tremor_value::prelude::*;

const URL_SCHEME: &str = "tremor-unix-socket-server";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub path: String,
    pub permissions: Option<String>,
    /// receive buffer size
    #[serde(default = "crate::utils::default_buf_size")]
    buf_size: usize,
}

impl tremor_config::Impl for Config {}

//struct ConnectionMeta {}

/// Builder for the unix socket server connector
#[derive(Debug, Default)]
pub struct Builder {}
#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "unix_socket_server".into()
    }

    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let (sink_tx, sink_rx) = channel(qsize());
        Ok(Box::new(UnixSocketServer {
            config,
            sink_tx,
            sink_rx: Some(sink_rx),
            sink_is_connected: Arc::default(),
        }))
    }
}

/// just a `stream_id`
#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy, Ord, PartialOrd)]
pub(super) struct ConnectionMeta(u64);

///
/// Expect connection meta as:
///
/// ```json
/// {
///     "unix_socket_server": {
///         "peer": 123
///     }
/// }
fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMeta> {
    meta.get_u64("peer").map(ConnectionMeta)
}

struct UnixSocketServer {
    config: Config,
    sink_tx: Sender<ChannelSinkMsg<ConnectionMeta>>,
    sink_rx: Option<Receiver<ChannelSinkMsg<ConnectionMeta>>>,
    sink_is_connected: Arc<AtomicBool>,
}

#[async_trait::async_trait()]
impl Connector for UnixSocketServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let sink_runtime = ChannelSinkRuntime::new(self.sink_tx.clone());
        let source = UnixSocketSource::new(
            self.config.clone(),
            sink_runtime,
            self.sink_is_connected.clone(),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = ChannelSink::from_channel_no_meta(
            resolve_connection_meta,
            builder.reply_tx(),
            self.sink_tx.clone(),
            self.sink_rx
                .take()
                .ok_or(GenericImplementationError::AlreadyConnected)?,
            self.sink_is_connected.clone(),
        );
        Ok(Some(builder.spawn(sink, ctx)))
    }
}

struct UnixSocketSource {
    config: Config,
    listener_task: Option<JoinHandle<()>>,
    connection_rx: Receiver<SourceReply>,
    runtime: ChannelSourceRuntime,
    sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
    sink_is_connected: Arc<AtomicBool>,
}

impl UnixSocketSource {
    fn new(
        config: Config,
        sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
        sink_is_connected: Arc<AtomicBool>,
    ) -> Self {
        let (tx, rx) = channel(qsize());
        let runtime = ChannelSourceRuntime::new(tx);
        Self {
            config,
            listener_task: None,
            connection_rx: rx,
            runtime,
            sink_runtime,
            sink_is_connected,
        }
    }
}

#[async_trait::async_trait()]
impl Source for UnixSocketSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        if let Some(listener_task) = self.listener_task.take() {
            listener_task.abort();
        }
        let path = PathBuf::from(&self.config.path);
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }
        let listener = UnixListener::bind(&path)?;
        if let Some(mode_description) = self.config.permissions.as_ref() {
            let mut mode = file_mode::Mode::empty();
            mode.set_str_umask(mode_description, 0)?;
            mode.set_mode_path(&path)?;
        }
        let buf_size = self.config.buf_size;
        let ctx = ctx.clone();
        let runtime = self.runtime.clone();
        let sink_runtime = self.sink_runtime.clone();
        let sink_is_connected = self.sink_is_connected.clone();

        self.listener_task = Some(spawn_task(ctx.clone(), async move {
            let mut stream_id_gen = StreamIdGen::default();
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: crate::utils::hostname(),
                port: None,
                path: vec![path.display().to_string()],
            };
            while ctx.quiescence_beacon().continue_reading().await {
                match timeout(ACCEPT_TIMEOUT, listener.accept()).await {
                    Ok(Ok((stream, _peer_addr))) => {
                        let stream_id: u64 = stream_id_gen.next_stream_id();
                        let connection_meta = ConnectionMeta(stream_id);

                        /*
                            {
                                "unix_socket_server": {
                                    "peer": 123
                                }
                            }
                        */
                        let meta = ctx.meta(literal!({
                            "path": path.display().to_string(),
                            "peer": stream_id
                        }));

                        let (read_half, write_half) = split(stream);
                        // only register a writer, if any pipeline is connected to the sink
                        // otherwise we have a dead sink not consuming from channels and possibly dead-locking the source or other tasks
                        // complex shit...
                        let reader_runtime = if sink_is_connected.load(Ordering::Acquire) {
                            sink_runtime
                                .register_stream_writer(
                                    stream_id,
                                    Some(connection_meta),
                                    &ctx,
                                    UnixSocketWriter::new(write_half),
                                )
                                .await;
                            Some(sink_runtime.clone())
                        } else {
                            info!(
                                "{ctx} Nothing connected to IN port, not starting a stream writer."
                            );
                            None
                        };

                        let reader = UnixSocketReader::new(
                            read_half,
                            buf_size,
                            ctx.alias().to_string(),
                            origin_uri.clone(),
                            meta,
                            reader_runtime,
                        );
                        runtime.register_stream_reader(stream_id, &ctx, reader);
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => continue,
                };
            }
            Ok(())
        }));
        Ok(true)
    }
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        Ok(self
            .connection_rx
            .recv()
            .await
            .ok_or(GenericImplementationError::ChannelEmpty)?)
    }

    async fn on_stop(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        if let Some(listener_task) = self.listener_task.take() {
            // stop acceptin' new connections
            listener_task.abort();
        }
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
