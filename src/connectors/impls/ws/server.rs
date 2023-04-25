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

use super::{WsReader, WsWriter};
use crate::connectors::impls::ws::WsDefaults;
use crate::connectors::sink::channel_sink::WithMeta;
use crate::connectors::utils::ConnectionMetaWithHandle;
use crate::connectors::{
    prelude::*,
    utils::{
        socket::{tcp_server_socket, TcpSocketOptions},
        tls::TLSServerConfig,
        ConnectionMeta,
    },
};
use dashmap::DashMap;
use futures::StreamExt;
use rustls::ServerConfig;
use simd_json::ValueAccess;
use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::{task::JoinHandle, time::timeout};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_async;

const URL_SCHEME: &str = "tremor-ws-server";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default)]
    socket_options: TcpSocketOptions,
    /// it is an `i32` because the underlying api also accepts an i32
    #[serde(default = "default_backlog")]
    backlog: i32,
    tls: Option<TLSServerConfig>,
}

impl ConfigImpl for Config {}

#[allow(clippy::module_name_repetitions)]
pub(crate) struct WsServer {
    config: Config,
    accept_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMetaWithHandle>>,
    source_runtime: Option<ChannelSourceRuntime>,
    tls_server_config: Option<ServerConfig>,
    /// marker that the sink is actually connected to some pipeline
    sink_is_connected: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "ws_server".into()
    }
    async fn build_cfg(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> crate::errors::Result<Box<dyn Connector>> {
        let config = Config::new(raw_config)?;

        let tls_server_config = config
            .tls
            .as_ref()
            .map(TLSServerConfig::to_server_config)
            .transpose()?;

        Ok(Box::new(WsServer {
            config,
            accept_tasks: Arc::new(DashMap::new()),
            sink_runtime: None, // replaced in create_sink()
            source_runtime: None,
            tls_server_config,
            sink_is_connected: Arc::default(),
        }))
    }
}

fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMetaWithHandle> {
    let peer = meta.get("peer");
    peer.get_u16("port")
        .zip(peer.get_str("host"))
        .zip(meta.get_str("handle"))
        .map(|((port, host), handle)| -> ConnectionMetaWithHandle {
            ConnectionMetaWithHandle {
                meta: ConnectionMeta {
                    host: host.to_string(),
                    port,
                },
                handle: handle.to_string(),
            }
        })
}

#[derive(FileIo, SocketClient, QueueSubscriber, DatabaseWriter)]
struct WsServerSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMetaWithHandle> + Send + Sync + 'static,
{
    inner: ChannelSink<ConnectionMetaWithHandle, T, WithMeta>,
    accept_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    sink_is_connected: Arc<AtomicBool>,
    tls_server_config: Option<ServerConfig>,
    config: Config,
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMetaWithHandle>>,
    source_runtime: Option<ChannelSourceRuntime>,
}

#[async_trait::async_trait()]
impl<T> SocketServer for WsServerSink<T>
where
    T: Fn(&Value) -> Option<ConnectionMetaWithHandle> + Send + Sync + 'static,
{
    #[allow(clippy::too_many_lines)]
    async fn listen(&mut self, address: &str, handle: &str, ctx: &SinkContext) -> Result<()> {
        let mut url = Url::<WsDefaults>::parse(address)?;
        // TODO: this can be simplified as the connect can be moved into the source
        let path = vec![url.port_or_dflt().to_string()];

        let source_runtime = self
            .source_runtime
            .clone()
            .ok_or("Source runtime not initialized")?;
        let sink_runtime = self
            .sink_runtime
            .clone()
            .ok_or("sink runtime not initialized")?;

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some((_, previous_handle)) = self.accept_tasks.remove(handle) {
            previous_handle.abort();
        }

        // TODO: allow for other sockets
        if url.port().is_none() {
            let port = if url.scheme() == "wss" { 443 } else { 80 };
            url.set_port(Some(port)).map_err(|_| "bad port")?;
        }
        let listener =
            tcp_server_socket(&url, self.config.backlog, &self.config.socket_options).await?;

        let ctx = ctx.clone();
        let tls_server_config = self.tls_server_config.clone();
        let sink_is_connected = self.sink_is_connected.clone();
        let handle_clone = handle.to_string();

        // accept task
        let accept_task = spawn_task(ctx.clone(), async move {
            let mut stream_id_gen = StreamIdGen::default();
            while ctx.quiescence_beacon().continue_reading().await {
                match timeout(ACCEPT_TIMEOUT, listener.accept()).await {
                    Ok(Ok((tcp_stream, peer_addr))) => {
                        let stream_id: u64 = stream_id_gen.next_stream_id();
                        let connection_meta: ConnectionMetaWithHandle = ConnectionMetaWithHandle {
                            meta: peer_addr.into(),
                            handle: handle_clone.clone(),
                        };

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
                            let meta = ctx.meta(WsServer::meta(peer_addr, &handle_clone, true));
                            // TODO: this should live in its own task, as it requires rome roundtrips :()
                            let tls_stream = acceptor.accept(tcp_stream).await?;
                            let ws_stream = accept_async(tls_stream).await?;
                            debug!("{ctx} new connection from {peer_addr}");

                            let (ws_write, ws_read) = ws_stream.split();

                            let reader_runtime = if sink_is_connected.load(Ordering::Acquire) {
                                let ws_writer = WsWriter::new_tls_server(ws_write);
                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta.clone()),
                                        &ctx,
                                        ws_writer,
                                    )
                                    .await;
                                Some(sink_runtime.clone())
                            } else {
                                None
                            };

                            let ws_reader = WsReader::new(
                                ws_read,
                                reader_runtime,
                                origin_uri.clone(),
                                meta,
                                ctx.clone(),
                            );
                            source_runtime.register_stream_reader(stream_id, &ctx, ws_reader);
                        } else {
                            let ws_stream = match accept_async(tcp_stream).await {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("{ctx} Websocket connection error: {e}");
                                    continue;
                                }
                            };
                            debug!("{ctx} new connection from {peer_addr}",);

                            let (ws_write, ws_read) = ws_stream.split();

                            let meta = ctx.meta(WsServer::meta(peer_addr, &handle_clone, false));

                            let reader_runtime = if sink_is_connected.load(Ordering::Acquire) {
                                let ws_writer = WsWriter::new(ws_write);

                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta.clone()),
                                        &ctx,
                                        ws_writer,
                                    )
                                    .await;
                                Some(sink_runtime.clone())
                            } else {
                                None
                            };

                            let ws_reader = WsReader::new(
                                ws_read,
                                reader_runtime,
                                origin_uri.clone(),
                                meta,
                                ctx.clone(),
                            );
                            source_runtime.register_stream_reader(stream_id, &ctx, ws_reader);
                        }
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => continue,
                };
            }
            Ok(())
        });

        self.accept_tasks.insert(handle.to_string(), accept_task);

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        if let Some((_, accept_task)) = self.accept_tasks.remove(handle) {
            accept_task.abort();
        }

        Ok(())
    }
}

#[async_trait::async_trait()]
impl<T> Sink for WsServerSink<T>
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

impl WsServer {
    fn meta(peer: SocketAddr, handle: &str, has_tls: bool) -> Value<'static> {
        let peer_ip = peer.ip().to_string();
        let peer_port = peer.port();

        literal!({
            "tls": has_tls,
            "peer": {
                "host": peer_ip,
                "port": peer_port
            },
            "handle": handle.to_string()
        })
    }
}

#[async_trait::async_trait()]
impl Connector for WsServer {
    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        for mut e in self.accept_tasks.iter_mut() {
            // stop acceptin' new connections
            e.value_mut().abort();
        }
        self.accept_tasks.clear();
        Ok(())
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = ChannelSource::new(
            Arc::default(), // we don't need to know if the source is connected. Worst case if nothing is connected is that the receiving task is blocked.
        );
        self.source_runtime = Some(source.runtime());

        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ChannelSink::new_with_meta(
            resolve_connection_meta,
            builder.reply_tx(),
            self.sink_is_connected.clone(),
        );

        self.sink_runtime = Some(sink.runtime());
        let addr = builder.spawn(
            WsServerSink {
                inner: sink,
                accept_tasks: self.accept_tasks.clone(),
                sink_is_connected: self.sink_is_connected.clone(),
                tls_server_config: self.tls_server_config.clone(),
                config: self.config.clone(),
                sink_runtime: self.sink_runtime.clone(),
                source_runtime: self.source_runtime.clone(),
            },
            ctx,
        );
        Ok(Some(addr))
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
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
                        "Only SocketServer commands are supported for the WebSocket connector."
                            .to_string(),
                    )
                    .into())
                }
            }
        }
        Ok(())
    }
}
