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
use crate::{connectors::prelude::*, errors::already_created_error};
use crate::{
    connectors::sink::channel_sink::{ChannelSinkMsg, NoMeta},
    errors::empty_error,
};
use dashmap::DashMap;
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::{io::split, net::UnixListener, task::JoinHandle, time::timeout};
use tremor_value::structurize;

const URL_SCHEME: &str = "tremor-unix-socket-server";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub permissions: Option<String>,
    /// receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}
#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "unix_socket_server".into()
    }

    async fn build_cfg(
        &self,
        _: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let (sink_tx, sink_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));

        let (tx, rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let runtime = ChannelSourceRuntime::new(tx);

        Ok(Box::new(UnixSocketServer {
            config,
            sink_tx: sink_tx.clone(),
            sink_rx: Some(sink_rx),
            connector_rx: Some(rx),
            sink_is_connected: Arc::default(),
            listeners: Arc::new(DashMap::new()),
            sink_runtime: ChannelSinkRuntime::new(sink_tx),
            runtime,
        }))
    }
}

#[derive(FileIo, SocketClient, QueueSubscriber, DatabaseWriter)]
struct UnixSocketServerSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMeta> + Send + Sync + 'static,
{
    inner: ChannelSink<ConnectionMeta, T, NoMeta>,
    listeners: Arc<DashMap<String, JoinHandle<()>>>,
    sink_is_connected: Arc<AtomicBool>,
    sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
    runtime: ChannelSourceRuntime,
    config: Config,
}

#[async_trait::async_trait()]
impl<T> Sink for UnixSocketServerSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMeta> + Send + Sync + 'static,
{
    async fn on_event(
        &mut self,
        input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        self.inner
            .on_event(input, event, ctx, serializer, start)
            .await
    }

    async fn on_signal(
        &mut self,
        signal: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
        self.inner.on_signal(signal, ctx, serializer).await
    }

    async fn metrics(&mut self, timestamp: u64, ctx: &SinkContext) -> Vec<EventPayload> {
        self.inner.metrics(timestamp, ctx).await
    }

    async fn on_start(&mut self, ctx: &SinkContext) -> Result<()> {
        self.inner.on_start(ctx).await
    }

    async fn connect(&mut self, ctx: &SinkContext, attempt: &Attempt) -> Result<bool> {
        Sink::connect(&mut self.inner, ctx, attempt).await
    }

    async fn on_pause(&mut self, ctx: &SinkContext) -> Result<()> {
        self.inner.on_pause(ctx).await
    }

    async fn on_resume(&mut self, ctx: &SinkContext) -> Result<()> {
        self.inner.on_resume(ctx).await
    }

    async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        self.inner.on_stop(ctx).await
    }

    async fn on_connection_lost(&mut self, ctx: &SinkContext) -> Result<()> {
        self.inner.on_connection_lost(ctx).await
    }

    async fn on_connection_established(&mut self, ctx: &SinkContext) -> Result<()> {
        self.inner.on_connection_established(ctx).await
    }

    fn auto_ack(&self) -> bool {
        self.inner.auto_ack()
    }

    fn asynchronous(&self) -> bool {
        self.inner.asynchronous()
    }
}

#[async_trait::async_trait()]
impl<T> SocketServer for UnixSocketServerSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMeta> + Send + Sync + 'static,
{
    async fn listen(&mut self, address: &str, handle: &str, ctx: &SinkContext) -> Result<()> {
        let path = PathBuf::from(address);
        if path.exists() {
            async_std::fs::remove_file(&path).await?;
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
        let handle = handle.to_string();

        self.listeners.insert(
            handle.to_string(),
            spawn_task(ctx.clone(), async move {
                let mut stream_id_gen = StreamIdGen::default();
                let origin_uri = EventOriginUri {
                    scheme: URL_SCHEME.to_string(),
                    host: hostname(),
                    port: None,
                    path: vec![path.display().to_string()],
                };
                while ctx.quiescence_beacon().continue_reading().await {
                    match timeout(ACCEPT_TIMEOUT, listener.accept()).await {
                        Ok(Ok((stream, _peer_addr))) => {
                            let stream_id: u64 = stream_id_gen.next_stream_id();
                            let connection_meta = ConnectionMeta {
                                stream_id,
                                handle: handle.clone(),
                            };

                            /*
                                {
                                    "unix_socket_server": {
                                        "peer": 123
                                    }
                                }
                            */
                            let meta = ctx.meta(literal!({
                                "path": path.display().to_string(),
                                "peer": stream_id,
                                "handle": handle.clone(),
                            }));

                            // only register a writer, if any pipeline is connected to the sink
                            // otherwise we have a dead sink not consuming from channels and possibly dead-locking the source or other tasks
                            // complex shit...
                            let (read, write) = split(stream);
                            let reader_runtime = if sink_is_connected.load(Ordering::Acquire) {
                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta),
                                        &ctx,
                                        UnixSocketWriter::new(write),
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
                                read,
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
            }),
        );

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        if let Some((_, listener)) = self.listeners.remove(handle) {
            listener.abort();
        }

        Ok(())
    }
}

/// just a `stream_id`
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub(super) struct ConnectionMeta {
    handle: String,
    stream_id: u64,
}

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
    let peer = meta.get("peer").as_u64();
    let handle = meta.get("handle").as_str().map(ToString::to_string);

    if let (Some(peer), Some(handle)) = (peer, handle) {
        Some(ConnectionMeta {
            handle,
            stream_id: peer,
        })
    } else {
        None
    }
}

struct UnixSocketServer {
    config: Config,
    sink_tx: Sender<ChannelSinkMsg<ConnectionMeta>>,
    sink_rx: Option<Receiver<ChannelSinkMsg<ConnectionMeta>>>,
    connector_rx: Option<Receiver<SourceReply>>,
    sink_is_connected: Arc<AtomicBool>,
    listeners: Arc<DashMap<String, JoinHandle<()>>>,
    sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
    runtime: ChannelSourceRuntime,
}

#[async_trait::async_trait()]
impl Connector for UnixSocketServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = UnixSocketSource::new(
            self.connector_rx.take().ok_or_else(already_created_error)?,
            self.listeners.clone(),
        );
        Ok(Some(builder.spawn(source, source_context)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let inner = ChannelSink::from_channel_no_meta(
            resolve_connection_meta,
            builder.reply_tx(),
            self.sink_tx.clone(),
            self.sink_rx.take().ok_or_else(already_created_error)?,
            self.sink_is_connected.clone(),
        );
        let sink = UnixSocketServerSink {
            inner,
            listeners: self.listeners.clone(),
            sink_is_connected: self.sink_is_connected.clone(),
            sink_runtime: self.sink_runtime.clone(),
            runtime: self.runtime.clone(),
            config: self.config.clone(),
        };

        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn validate(&self, config: &ConnectorConfig) -> Result<()> {
        for command in &config.initial_commands {
            match structurize::<Command>(command.clone())? {
                Command::SocketServer(_) => {}
                _ => {
                    return Err(ErrorKind::NotImplemented(
                        "File IO commands are not supported for Unix Socket connectors."
                            .to_string(),
                    )
                    .into())
                }
            }
        }

        Ok(())
    }
}

struct UnixSocketSource {
    listeners: Arc<DashMap<String, JoinHandle<()>>>,
    connection_rx: Receiver<SourceReply>,
}

impl UnixSocketSource {
    fn new(
        connection_rx: Receiver<SourceReply>,
        listeners: Arc<DashMap<String, JoinHandle<()>>>,
    ) -> Self {
        Self {
            listeners,
            connection_rx,
        }
    }
}

#[async_trait::async_trait()]
impl Source for UnixSocketSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        for mut e in self.listeners.iter_mut() {
            e.value_mut().abort();
        }
        self.listeners.clear();

        Ok(true)
    }
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        self.connection_rx.recv().await.ok_or_else(empty_error)
    }

    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        for mut e in self.listeners.iter_mut() {
            e.value_mut().abort();
        }
        self.listeners.clear();

        Ok(())
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
