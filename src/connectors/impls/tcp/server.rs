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
    connectors::{
        prelude::*,
        sink::channel_sink::ChannelSinkMsg,
        utils::{
            tls::{load_server_config, TLSServerConfig},
            ConnectionMeta,
        },
    },
    errors::err_conector_def,
};
use async_std::{
    channel::{bounded, Receiver, Sender},
    net::TcpListener,
    prelude::*,
    task::JoinHandle,
};
use async_tls::TlsAcceptor;
use futures::io::AsyncReadExt;
use rustls::ServerConfig;
use simd_json::ValueAccess;
use std::sync::Arc;

use crate::system::World;
use abi_stable::{
    export_root_module,
    prefix_type::PrefixTypeTrait,
    rstr, rvec, sabi_extern_fn,
    std_types::{
        ROption::{self, RSome},
        RResult::{RErr, ROk},
        RStr, RString,
    },
    type_level::downcasting::TD_Opaque,
};
use async_ffi::{BorrowingFfiFuture, FfiFuture, FutureExt as _};
use std::future;
use tremor_common::{pdk::RError, ttry};

const URL_SCHEME: &str = "tremor-tcp-server";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    url: Url<TcpDefaults>,
    tls: Option<TLSServerConfig>,
    // TCP: receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

#[allow(clippy::module_name_repetitions)]
pub struct TcpServer {
    config: Config,
    tls_server_config: Option<ServerConfig>,
    sink_tx: Sender<ChannelSinkMsg<ConnectionMeta>>,
    sink_rx: Receiver<ChannelSinkMsg<ConnectionMeta>>,
}

/// Note that since it's a built-in plugin, `#[export_root_module]` can't be
/// used or it would conflict with other plugins.
pub fn instantiate_root_module() -> ConnectorPluginRef {
    ConnectorPlugin {
        connector_type,
        from_config,
    }
    .leak_into_prefix()
}

#[sabi_extern_fn]
pub fn connector_type() -> ConnectorType {
    "tcp_server".into()
}

#[sabi_extern_fn]
pub fn from_config<'a>(
    id: RStr<'a>,
    _: &ConnectorConfig,
    config: &'a Value,
    _: ROption<World>,
) -> BorrowingFfiFuture<'a, RResult<BoxedRawConnector>> {
    async move {
        let config = ttry!(Config::new(config));
        if config.url.port().is_none() {
            return RErr(err_conector_def(id.as_str(), "Missing port for TCP server").into());
        }
        let tls_server_config = if let Some(tls_config) = config.tls.as_ref() {
            Some(ttry!(load_server_config(tls_config)))
        } else {
            None
        };
        let (sink_tx, sink_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        ROk(BoxedRawConnector::from_value(
            TcpServer {
                config,
                tls_server_config,
                sink_tx,
                sink_rx,
            },
            TD_Opaque,
        ))
    }
    .into_ffi()
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

impl RawConnector for TcpServer {
    fn create_source(
        &mut self,
        _source_context: SourceContext,
        _qsize: usize,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        let sink_runtime = ChannelSinkRuntime::new(self.sink_tx.clone());
        let source = TcpServerSource::new(
            self.config.clone(),
            self.tls_server_config.clone(),
            sink_runtime,
        );
        let source = BoxedRawSource::from_value(source, TD_Opaque);
        future::ready(ROk(RSome(source))).into_ffi()
    }

    fn create_sink<'a>(
        &'a mut self,
        _sink_context: SinkContext,
        _qsize: usize,
        // TODO: this is not quite the same as in main
        reply_tx: BoxedContraflowSender,
    ) -> BorrowingFfiFuture<'a, RResult<ROption<BoxedRawSink>>> {
        // we use this constructor as we need the sink channel already when creating the source
        let sink = ChannelSink::from_channel_no_meta(
            resolve_connection_meta,
            reply_tx,
            self.sink_tx.clone(),
            self.sink_rx.clone(),
        );
        let sink = BoxedRawSink::from_value(sink, TD_Opaque);
        future::ready(ROk(RSome(sink))).into_ffi()
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
}

impl TcpServerSource {
    fn new(
        config: Config,
        tls_server_config: Option<ServerConfig>,
        sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
    ) -> Self {
        let (tx, rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let runtime = ChannelSourceRuntime::new(tx);
        Self {
            config,
            tls_server_config,
            accept_task: None,
            connection_rx: rx,
            runtime,
            sink_runtime,
        }
    }
}
impl RawSource for TcpServerSource {
    #[allow(clippy::too_many_lines)]
    fn connect<'a>(
        &'a mut self,
        ctx: &'a SourceContext,
        _attempt: &'a Attempt,
    ) -> BorrowingFfiFuture<'a, RResult<bool>> {
        async move {
            let path = rvec![RString::from(self.config.url.port_or_dflt().to_string())];
            let accept_ctx = ctx.clone();
            let buf_size = self.config.buf_size;

            // cancel last accept task if necessary, this will drop the previous listener
            if let Some(previous_handle) = self.accept_task.take() {
                previous_handle.cancel().await;
            }

            let host = self.config.url.host_or_local();
            let port = self.config.url.port_or_dflt();

            let listener = ttry!(TcpListener::bind((host, port)).await.map_err(Error::from));

            let ctx = ctx.clone();
            let tls_server_config = self.tls_server_config.clone();

            let runtime = self.runtime.clone();
            let sink_runtime = self.sink_runtime.clone();
            // accept task
            self.accept_task = Some(spawn_task(ctx.clone(), async move {
                let mut stream_id_gen = StreamIdGen::default();

                while ctx.quiescence_beacon().continue_reading().await {
                    match listener.accept().timeout(ACCEPT_TIMEOUT).await {
                        Ok(Ok((stream, peer_addr))) => {
                            debug!("{accept_ctx} new connection from {peer_addr}");
                            let stream_id: u64 = stream_id_gen.next_stream_id();
                            let connection_meta: ConnectionMeta = peer_addr.into();
                            // Async<T> allows us to read in one thread and write in another concurrently - see its documentation
                            // So we don't need no BiLock like we would when using `.split()`
                            let origin_uri = EventOriginUri {
                                scheme: RString::from(URL_SCHEME),
                                host: RString::from(peer_addr.ip().to_string()),
                                port: RSome(peer_addr.port()),
                                path: path.clone(), // captures server port
                            };

                            let tls_acceptor: Option<TlsAcceptor> = tls_server_config
                                .clone()
                                .map(|sc| TlsAcceptor::from(Arc::new(sc)));
                            if let Some(acceptor) = tls_acceptor {
                                let tls_stream = acceptor.accept(stream.clone()).await?;
                                let (tls_read_stream, tls_write_sink) = tls_stream.split();
                                let meta = ctx.meta(literal!({
                                    "tls": true,
                                    "peer": {
                                        "host": peer_addr.ip().to_string(),
                                        "port": peer_addr.port()
                                    }
                                }));
                                let tls_reader = TcpReader::tls_server(
                                    tls_read_stream,
                                    stream.clone(),
                                    vec![0; buf_size],
                                    ctx.alias.clone().into(),
                                    origin_uri.clone(),
                                    meta,
                                );

                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta.clone()),
                                        &ctx,
                                        TcpWriter::tls_server(tls_write_sink, stream),
                                    )
                                    .await;

                                runtime.register_stream_reader(stream_id, &ctx, tls_reader);
                            } else {
                                let meta = ctx.meta(literal!({
                                    "tls": false,
                                    "peer": {
                                        "host": peer_addr.ip().to_string(),
                                        "port": peer_addr.port()
                                    }
                                }));
                                let tcp_reader = TcpReader::new(
                                    stream.clone(),
                                    vec![0; buf_size],
                                    ctx.alias.clone().into(),
                                    origin_uri.clone(),
                                    meta,
                                );

                                sink_runtime
                                    .register_stream_writer(
                                        stream_id,
                                        Some(connection_meta.clone()),
                                        &ctx,
                                        TcpWriter::new(stream),
                                    )
                                    .await;

                                runtime.register_stream_reader(stream_id, &ctx, tcp_reader);
                            }
                        }
                        Ok(Err(e)) => return Err(e.into()),
                        Err(_) => continue, // timeout accepting
                    };
                }
                debug!("{accept_ctx} stopped accepting connections.");
                Ok::<(), Error>(())
            }));

            ROk(true)
        }
        .into_ffi()
    }

    fn pull_data<'a>(
        &'a mut self,
        _pull_id: &'a mut u64,
        _ctx: &'a SourceContext,
    ) -> BorrowingFfiFuture<'a, RResult<SourceReply>> {
        async move { ROk(ttry!(self.connection_rx.recv().await.map_err(Error::from))) }.into_ffi()
    }

    fn on_stop(&mut self, _ctx: &SourceContext) -> BorrowingFfiFuture<'_, RResult<()>> {
        async move {
            if let Some(accept_task) = self.accept_task.take() {
                // stop acceptin' new connections
                accept_task.cancel().await;
            }
            ROk(())
        }
        .into_ffi()
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
