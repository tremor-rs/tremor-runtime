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
use super::{TcpDefaults, TcpReader, TcpWriter};
use crate::connectors::utils::{ConnectionMeta, ConnectionMetaWithHandle};
use crate::connectors::{
    prelude::*,
    sink::channel_sink::ChannelSinkMsg,
    utils::{socket::TcpSocketOptions, tls::TLSServerConfig},
};
use crate::{connectors::sink::channel_sink::NoMeta, errors::already_created_error};
use crate::{connectors::utils::socket::tcp_server_socket, errors::empty_error};
use dashmap::DashMap;
use rustls::ServerConfig;
use simd_json::ValueAccess;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::{io::split, task::JoinHandle, time::timeout};
use tokio_rustls::TlsAcceptor;
use tremor_value::structurize;

const URL_SCHEME: &str = "tremor-tcp-server";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    tls: Option<TLSServerConfig>,
    // TCP: receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,

    /// it is an `i32` because the underlying api also accepts an i32
    #[serde(default = "default_backlog")]
    backlog: i32,

    #[serde(default)]
    socket_options: TcpSocketOptions,
}

impl ConfigImpl for Config {}

#[allow(clippy::module_name_repetitions)]
pub(crate) struct TcpServer {
    sink_tx: Sender<ChannelSinkMsg<ConnectionMetaWithHandle>>,
    sink_rx: Option<Receiver<ChannelSinkMsg<ConnectionMetaWithHandle>>>,
    /// marker that the sink is connected
    sink_is_connected: Arc<AtomicBool>,
    runtime: ChannelSourceRuntime,
    sink_runtime: ChannelSinkRuntime<ConnectionMetaWithHandle>,
    connection_rx: Option<Receiver<SourceReply>>,
    tls_server_config: Option<ServerConfig>,
    buf_size: usize,
    backlog: i32,
    socket_options: TcpSocketOptions,
    accept_tasks: Arc<DashMap<String, JoinHandle<()>>>,
}

impl TcpServer {
    pub fn new(config: &Config, tls_server_config: Option<ServerConfig>) -> Self {
        let (sink_tx, sink_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));

        let (tx, rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let runtime = ChannelSourceRuntime::new(tx);
        let sink_runtime = ChannelSinkRuntime::new(sink_tx.clone());

        TcpServer {
            sink_tx,
            sink_rx: Some(sink_rx),
            sink_is_connected: Arc::default(),
            runtime,
            sink_runtime,
            connection_rx: Some(rx),
            tls_server_config,
            buf_size: config.buf_size,
            backlog: config.backlog,
            socket_options: config.socket_options.clone(),
            accept_tasks: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "tcp_server".into()
    }
    async fn build_cfg(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> crate::errors::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let tls_server_config = config
            .tls
            .as_ref()
            .map(TLSServerConfig::to_server_config)
            .transpose()?;

        Ok(Box::new(TcpServer::new(&config, tls_server_config)))
    }
}

fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMetaWithHandle> {
    let handle = meta.get("handle").as_str().map(ToString::to_string);
    let peer_host = meta
        .get("peer")
        .get("host")
        .as_str()
        .map(ToString::to_string);
    let peer_port = meta.get("peer").get("port").as_u16();

    if let (Some(handle), Some(peer_host), Some(peer_port)) = (handle, peer_host, peer_port) {
        Some(ConnectionMetaWithHandle {
            handle,
            meta: ConnectionMeta {
                host: peer_host,
                port: peer_port,
            },
        })
    } else {
        None
    }
}

#[derive(FileIo, SocketClient, QueueSubscriber, DatabaseWriter)]
struct TcpSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMetaWithHandle> + Send + Sync + 'static,
{
    inner: ChannelSink<ConnectionMetaWithHandle, T, NoMeta>,
    accept_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    runtime: ChannelSourceRuntime,
    sink_runtime: ChannelSinkRuntime<ConnectionMetaWithHandle>,
    buf_size: usize,
    sink_is_connected: Arc<AtomicBool>,
    tls_server_config: Option<ServerConfig>,
    backlog: i32,
    socket_options: TcpSocketOptions,
}

#[async_trait::async_trait]
impl<T> Sink for TcpSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMetaWithHandle> + Send + Sync + 'static,
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

#[async_trait::async_trait]
impl<T> SocketServer for TcpSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMetaWithHandle> + Send + Sync + 'static,
{
    #[allow(clippy::too_many_lines)]
    async fn listen(&mut self, address: &str, handle: &str, ctx: &SinkContext) -> Result<()> {
        let url = Url::<TcpDefaults>::parse(address)?;
        let path = vec![url.port_or_dflt().to_string()];

        let sink_is_connected = self.sink_is_connected.clone();
        let runtime = self.runtime.clone();
        let sink_runtime = self.sink_runtime.clone();
        let tls_server_config = self.tls_server_config.clone();
        let accept_ctx = ctx.clone();
        let ctx = ctx.clone();
        let buf_size = self.buf_size;
        let backlog = self.backlog;
        let socket_options = self.socket_options.clone();
        let handle_meta = handle.to_string();

        let join_handle = spawn_task(ctx.clone(), async move {
            let mut stream_id_gen = StreamIdGen::default();

            let listener = tcp_server_socket(&url, backlog, &socket_options).await?;

            while ctx.quiescence_beacon().continue_reading().await {
                match timeout(ACCEPT_TIMEOUT, listener.accept()).await {
                    Ok(Ok((stream, peer_addr))) => {
                        debug!("{accept_ctx} new connection from {peer_addr}");
                        let stream_id: u64 = stream_id_gen.next_stream_id();
                        let connection_meta = ConnectionMetaWithHandle {
                            handle: handle_meta.clone(),
                            meta: ConnectionMeta {
                                host: peer_addr.ip().to_string(),
                                port: peer_addr.port(),
                            },
                        };
                        // Async<T> allows us to read in one thread and write in another concurrently - see its documentation
                        // So we don't need no BiLock like we would when using `.split()`
                        let origin_uri = EventOriginUri {
                            scheme: URL_SCHEME.to_string(),
                            host: peer_addr.ip().to_string(),
                            port: Some(peer_addr.port()),
                            path: path.clone(), // captures server port
                        };

                        let tls_acceptor: Option<TlsAcceptor> = tls_server_config
                            .clone()
                            .map(|sc| TlsAcceptor::from(Arc::new(sc)));
                        if let Some(acceptor) = tls_acceptor {
                            let tls_stream = acceptor.accept(stream).await?;
                            let (tls_read_stream, tls_write_sink) = split(tls_stream);
                            let meta = ctx.meta(literal!({
                                "tls": true,
                                "peer": {
                                    "host": peer_addr.ip().to_string(),
                                    "port": peer_addr.port()
                                }
                            }));

                            // we only register a writer when we actually have something connected to the sink
                            // the connected sink will not be driven by the sink task anyways (no calls to on_event/on_signal)
                            let reader_runtime = if sink_is_connected.load(Ordering::Acquire) {
                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta.clone()),
                                        &ctx,
                                        TcpWriter::tls_server(tls_write_sink),
                                    )
                                    .await;
                                Some(sink_runtime.clone())
                            } else {
                                debug!("{ctx} Sink not connected, not offering writing to TCP connections.");
                                None
                            };
                            let tls_reader = TcpReader::tls_server(
                                tls_read_stream,
                                vec![0; buf_size],
                                ctx.alias().clone(),
                                origin_uri.clone(),
                                meta,
                                reader_runtime,
                            );

                            runtime.register_stream_reader(stream_id, &ctx, tls_reader);
                        } else {
                            let meta = ctx.meta(literal!({
                                "tls": false,
                                "peer": {
                                    "host": peer_addr.ip().to_string(),
                                    "port": peer_addr.port()
                                }
                            }));

                            // we only register a writer when we actually have something connected to the sink
                            // the connected sink will not be driven by the sink task anyways (no calls to on_event/on_signal)
                            let (read, write) = split(stream);
                            let reader_runtime = if sink_is_connected.load(Ordering::Acquire) {
                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta.clone()),
                                        &ctx,
                                        TcpWriter::new(write),
                                    )
                                    .await;
                                Some(sink_runtime.clone())
                            } else {
                                debug!("{ctx} Sink not connected, not offering writing to TCP connections.");
                                None
                            };
                            let tcp_reader = TcpReader::new(
                                read,
                                vec![0; buf_size],
                                ctx.alias().clone(),
                                origin_uri.clone(),
                                meta,
                                reader_runtime,
                            );

                            runtime.register_stream_reader(stream_id, &ctx, tcp_reader);
                        }
                    }
                    Ok(Err(e)) => {
                        error!("{ctx} Error Accepting: {e}");
                        return Err(e.into());
                    }
                    Err(_) => continue, // timeout accepting
                };
            }
            debug!("{ctx} stopped accepting connections.");
            Ok(())
        });

        self.accept_tasks.insert(handle.into(), join_handle);

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        if let Some((_, task)) = self.accept_tasks.remove(handle) {
            task.abort();
        }
        Ok(())
    }
}

#[async_trait::async_trait()]
impl Connector for TcpServer {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = TcpServerSource::new(
            self.connection_rx
                .take()
                .ok_or_else(already_created_error)?,
            self.accept_tasks.clone(),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        // we use this constructor as we need the sink channel already when creating the source
        let sink_inner = ChannelSink::from_channel_no_meta(
            resolve_connection_meta,
            builder.reply_tx(),
            self.sink_tx.clone(),
            self.sink_rx.take().ok_or_else(already_created_error)?,
            self.sink_is_connected.clone(),
        );

        let sink = TcpSink {
            inner: sink_inner,
            accept_tasks: self.accept_tasks.clone(),
            runtime: self.runtime.clone(),
            sink_runtime: self.sink_runtime.clone(),
            buf_size: self.buf_size,
            sink_is_connected: self.sink_is_connected.clone(),
            tls_server_config: self.tls_server_config.clone(),
            backlog: self.backlog,
            socket_options: self.socket_options.clone(),
        };
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    fn validate(&self, config: &ConnectorConfig) -> Result<()> {
        for command in &config.initial_commands {
            match structurize::<Command>(command.clone())? {
                Command::SocketServer(_) => {}
                _ => {
                    return Err(ErrorKind::NotImplemented(
                        "Only SocketServer commands are supported for TCP connectors.".to_string(),
                    )
                    .into())
                }
            }
        }
        Ok(())
    }
}

struct TcpServerSource {
    accept_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    connection_rx: Receiver<SourceReply>,
}

impl TcpServerSource {
    fn new(
        connection_rx: Receiver<SourceReply>,
        accept_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    ) -> Self {
        Self {
            accept_tasks,
            connection_rx,
        }
    }
}
#[async_trait::async_trait()]
impl Source for TcpServerSource {
    #[allow(clippy::too_many_lines)]
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        // cancel last accept task if necessary, this will drop the previous listener
        for mut e in self.accept_tasks.iter_mut() {
            // stop acceptin' new connections
            e.value_mut().abort();
        }
        self.accept_tasks.clear();
        Ok(true)
    }

    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        self.connection_rx.recv().await.ok_or_else(empty_error)
    }

    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        for mut e in self.accept_tasks.iter_mut() {
            // stop acceptin' new connections
            e.value_mut().abort();
        }
        self.accept_tasks.clear();
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
