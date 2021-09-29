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
use crate::connectors::tcp::{TcpReader, TcpWriter};
use crate::connectors::tls::{load_server_config, TLSServerConfig};
use async_std::net::TcpListener;
use async_std::task::{self, JoinHandle};
use async_tls::TlsAcceptor;
use futures::io::AsyncReadExt;
use rustls::ServerConfig;
use simd_json::ValueAccess;
use std::net::SocketAddr;
use std::sync::Arc;
use tremor_pipeline::EventOriginUri;
use tremor_value::prelude::*;

const URL_SCHEME: &str = "tremor-tcp-server";

#[derive(Deserialize, Debug)]
pub struct Config {
    // kept as a str, so it is re-resolved upon each connect
    host: String,
    port: u16,
    // TCP: receive buffer size
    tls: Option<TLSServerConfig>,
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ConnectionMeta {
    host: String,
    port: u16,
}

impl From<SocketAddr> for ConnectionMeta {
    fn from(sa: SocketAddr) -> Self {
        Self {
            host: sa.ip().to_string(),
            port: sa.port(),
        }
    }
}

pub struct TcpServer {
    url: TremorUrl,
    config: Config,
    accept_task: Option<JoinHandle<Result<()>>>,
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
    source_runtime: Option<ChannelSourceRuntime>,
    tls_server_config: Option<ServerConfig>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> crate::errors::Result<Box<dyn Connector>> {
        if let Some(raw_config) = raw_config {
            let config = Config::new(raw_config)?;

            let tls_server_config = if let Some(tls_config) = config.tls.as_ref() {
                Some(load_server_config(tls_config)?)
            } else {
                None
            };
            Ok(Box::new(TcpServer {
                url: id.clone(),
                config,
                accept_task: None,  // not yet started
                sink_runtime: None, // replaced in create_sink()
                source_runtime: None,
                tls_server_config,
            }))
        } else {
            Err(crate::errors::ErrorKind::MissingConfiguration(String::from("TcpServer")).into())
        }
    }
}

fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMeta> {
    meta.get_u16("port")
        .zip(meta.get_str("host"))
        .map(|(port, host)| -> ConnectionMeta {
            ConnectionMeta {
                host: host.to_string(),
                port,
            }
        })
}

#[async_trait::async_trait()]
impl Connector for TcpServer {
    async fn on_stop(&mut self, _ctx: &ConnectorContext) {
        if let Some(accept_task) = self.accept_task.take() {
            // stop acceptin' new connections
            accept_task.cancel().await;
        }
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = ChannelSource::new(ctx.clone(), builder.qsize());
        self.source_runtime = Some(source.runtime());
        let addr = builder.spawn(source, ctx)?;

        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ChannelSink::new(builder.qsize(), resolve_connection_meta, builder.reply_tx());
        self.sink_runtime = Some(sink.runtime());
        let addr = builder.spawn(sink, ctx)?;
        Ok(Some(addr))
    }

    #[allow(clippy::too_many_lines)]
    async fn connect(&mut self, ctx: &ConnectorContext) -> Result<bool> {
        let path = vec![self.config.port.to_string()];
        let accept_url = self.url.clone();

        let source_runtime = self
            .source_runtime
            .clone()
            .ok_or("Source runtime not initialized")?;
        let sink_runtime = self
            .sink_runtime
            .clone()
            .ok_or("sink runtime not initialized")?;
        let buf_size = self.config.buf_size;

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(previous_handle) = self.accept_task.take() {
            previous_handle.cancel().await;
        }

        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;

        let ctx = ctx.clone();
        let tls_server_config = self.tls_server_config.clone();

        // accept task
        self.accept_task = Some(task::spawn(async move {
            let mut stream_id_gen = StreamIdGen::default();
            while let (true, Ok((stream, peer_addr))) = (
                ctx.quiescence_beacon.continue_reading().await,
                listener.accept().await,
            ) {
                trace!(
                    "[Connector::{}] new connection from {}",
                    &accept_url,
                    peer_addr
                );
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
                match tls_acceptor {
                    Some(acceptor) => {
                        let tls_stream = acceptor.accept(stream.clone()).await?; // TODO: this should live in its own task, as it requires rome roundtrips :()
                        let (tls_read_stream, tls_write_sink) = tls_stream.split();
                        let meta = literal!({
                            "tls": true,
                            "peer": {
                                "host": peer_addr.ip().to_string(),
                                "port": peer_addr.port()
                            }
                        });
                        let tls_reader = TcpReader::tls_server(
                            tls_read_stream,
                            stream.clone(),
                            vec![0; buf_size],
                            ctx.url.clone(),
                            origin_uri.clone(),
                            meta,
                        );
                        source_runtime.register_stream_reader(stream_id, &ctx, tls_reader);

                        sink_runtime.register_stream_writer(
                            stream_id,
                            Some(connection_meta.clone()),
                            &ctx,
                            TcpWriter::tls_server(tls_write_sink, stream),
                        );
                    }
                    None => {
                        let meta = literal!({
                            "tls": false,
                            "peer": {
                                "host": peer_addr.ip().to_string(),
                                "port": peer_addr.port()
                            }
                        });
                        let tcp_reader = TcpReader::new(
                            stream.clone(),
                            vec![0; buf_size],
                            ctx.url.clone(),
                            origin_uri.clone(),
                            meta,
                        );
                        source_runtime.register_stream_reader(stream_id, &ctx, tcp_reader);

                        sink_runtime.register_stream_writer(
                            stream_id,
                            Some(connection_meta.clone()),
                            &ctx,
                            TcpWriter::new(stream),
                        );
                    }
                }
            }

            // notify connector task about disconnect
            // of the listening socket
            ctx.notifier.notify().await?;
            Ok(())
        }));

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
