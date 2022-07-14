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
use crate::{
    channel::{bounded, Receiver, Sender},
    errors::{already_created_error, empty_error},
};
use crate::{
    connectors::{
        prelude::*,
        sink::channel_sink::ChannelSinkMsg,
        utils::{
            socket::{tcp_server_socket, TcpSocketOptions},
            tls::TLSServerConfig,
            ConnectionMeta,
        },
    },
    errors::err_connector_def,
};
use rustls::ServerConfig;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::{task::JoinHandle, time::timeout};
use tokio_rustls::TlsAcceptor;

const URL_SCHEME: &str = "tremor-tcp-server";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    url: Url<TcpDefaults>,
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

impl tremor_config::Impl for Config {}

#[allow(clippy::module_name_repetitions)]
pub(crate) struct TcpServer {
    config: Config,
    tls_server_config: Option<ServerConfig>,
    sink_tx: Sender<ChannelSinkMsg<ConnectionMeta>>,
    sink_rx: Option<Receiver<ChannelSinkMsg<ConnectionMeta>>>,
    /// marker that the sink is connected
    sink_is_connected: Arc<AtomicBool>,
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
        id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
    ) -> crate::errors::Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        if config.url.port().is_none() {
            return Err(err_connector_def(id, "Missing port for TCP server"));
        }
        let tls_server_config = if let Some(tls_config) = config.tls.as_ref() {
            Some(tls_config.to_server_config()?)
        } else {
            None
        };
        let (sink_tx, sink_rx) = bounded(qsize());
        Ok(Box::new(TcpServer {
            config,
            tls_server_config,
            sink_tx,
            sink_rx: Some(sink_rx),
            sink_is_connected: Arc::default(),
        }))
    }
}

fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMeta> {
    let peer = meta.get("peer");
    peer.get_u16("port")
        .zip(peer.get_str("host"))
        .map(|(port, host)| -> ConnectionMeta {
            ConnectionMeta {
                host: host.to_string(),
                port,
            }
        })
}

#[async_trait::async_trait()]
impl Connector for TcpServer {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let sink_runtime = ChannelSinkRuntime::new(self.sink_tx.clone());
        let source = TcpServerSource::new(
            self.config.clone(),
            self.tls_server_config.clone(),
            sink_runtime,
            self.sink_is_connected.clone(),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        // we use this constructor as we need the sink channel already when creating the source
        let sink = ChannelSink::from_channel_no_meta(
            resolve_connection_meta,
            builder.reply_tx(),
            self.sink_tx.clone(),
            self.sink_rx.take().ok_or_else(already_created_error)?,
            self.sink_is_connected.clone(),
        );
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct TcpServerSource {
    config: Config,
    tls_server_config: Option<ServerConfig>,
    accept_task: Option<JoinHandle<()>>,
    connection_rx: Receiver<SourceReply>,
    runtime: ChannelSourceRuntime,
    sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
    sink_is_connected: Arc<AtomicBool>,
}

impl TcpServerSource {
    fn new(
        config: Config,
        tls_server_config: Option<ServerConfig>,
        sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
        sink_is_connected: Arc<AtomicBool>,
    ) -> Self {
        let (tx, rx) = bounded(qsize());
        let runtime = ChannelSourceRuntime::new(tx);
        Self {
            config,
            tls_server_config,
            accept_task: None,
            connection_rx: rx,
            runtime,
            sink_runtime,
            sink_is_connected,
        }
    }
}
#[async_trait::async_trait()]
impl Source for TcpServerSource {
    #[allow(clippy::too_many_lines)]
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        let path = vec![self.config.url.port_or_dflt().to_string()];
        let accept_ctx = ctx.clone();
        let buf_size = self.config.buf_size;

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(previous_handle) = self.accept_task.take() {
            previous_handle.abort();
        }

        let listener = tcp_server_socket(
            &self.config.url,
            self.config.backlog,
            &self.config.socket_options,
        )
        .await?;
        info!("{ctx} Listening on {}", listener.local_addr()?);

        let ctx = ctx.clone();
        let tls_server_config = self.tls_server_config.clone();

        let runtime = self.runtime.clone();
        let sink_runtime = self.sink_runtime.clone();
        let sink_is_connected = self.sink_is_connected.clone();
        // accept task
        self.accept_task = Some(spawn_task(ctx.clone(), async move {
            let mut stream_id_gen = StreamIdGen::default();

            while ctx.quiescence_beacon().continue_reading().await {
                match timeout(ACCEPT_TIMEOUT, listener.accept()).await {
                    Ok(Ok((stream, peer_addr))) => {
                        debug!("{accept_ctx} new connection from {peer_addr}");
                        let stream_id: u64 = stream_id_gen.next_stream_id();
                        let connection_meta: ConnectionMeta = peer_addr.into();
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
                            let (tls_read_stream, tls_write_sink) = tokio::io::split(tls_stream);
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
                                ctx.alias.clone(),
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

                            let (read_stream, write_stream) = tokio::io::split(stream);
                            // we only register a writer when we actually have something connected to the sink
                            // the connected sink will not be driven by the sink task anyways (no calls to on_event/on_signal)
                            let reader_runtime = if sink_is_connected.load(Ordering::Acquire) {
                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta.clone()),
                                        &ctx,
                                        TcpWriter::new(write_stream),
                                    )
                                    .await;
                                Some(sink_runtime.clone())
                            } else {
                                debug!("{ctx} Sink not connected, not offering writing to TCP connections.");
                                None
                            };
                            let tcp_reader = TcpReader::new(
                                read_stream,
                                vec![0; buf_size],
                                ctx.alias.clone(),
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
            debug!("{accept_ctx} stopped accepting connections.");
            Ok(())
        }));

        Ok(true)
    }

    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        Ok(self.connection_rx.recv().await.ok_or_else(empty_error)?)
    }

    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(accept_task) = self.accept_task.take() {
            // stop acceptin' new connections
            accept_task.abort();
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
