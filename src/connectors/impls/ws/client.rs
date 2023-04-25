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

//! WS Client connector - maintains a connection to the configured upstream host
#![allow(clippy::module_name_repetitions)]

use super::{WsReader, WsWriter};
use crate::connectors::sink::channel_sink::{ChannelSinkMsg, WithMeta};
use crate::connectors::{
    prelude::*,
    utils::{
        socket::{tcp_client_socket, TcpSocketOptions},
        tls::TLSClientConfig,
    },
};
use crate::{connectors::impls::ws::WsDefaults, errors::already_created_error};
use either::Either;
use futures::StreamExt;
use rustls::ServerName;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;
use tokio_tungstenite::client_async;
use tremor_value::structurize;

const URL_SCHEME: &str = "tremor-ws-client";

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default)]
    socket_options: TcpSocketOptions,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,
    default_handle: Option<String>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "ws_client".into()
    }
    async fn build_cfg(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;

        let (tx, rx) = bounded(qsize());

        Ok(Box::new(WsClient {
            config,
            source_runtime: None,
            tx,
            rx: Some(rx),
        }))
    }
}

#[derive(FileIo, SocketServer, QueueSubscriber, DatabaseWriter)]
struct WsClientSink<T>
where
    T: Fn(&Value) -> Option<String> + Send + Sync + 'static,
{
    inner: ChannelSink<String, T, WithMeta>,
    sink_runtime: ChannelSinkRuntime<String>,
    source_runtime: Option<ChannelSourceRuntime>,
    config: Config,
    stream_id_generator: StreamIdGen,
    writer_handles: HashMap<String, JoinHandle<Result<()>>>,
}

#[async_trait::async_trait]
impl<T> SocketClient for WsClientSink<T>
where
    T: Fn(&Value) -> Option<String> + Send + Sync + 'static,
{
    async fn connect_socket(
        &mut self,
        address: &str,
        handle: &str,
        ctx: &SinkContext,
    ) -> Result<()> {
        let url = Url::<WsDefaults>::parse(address)?;
        let host = url
            .host()
            .ok_or_else(|| Error::from("missing host"))?
            .to_string();
        // TODO: do we really need to make the port required when we have a default defined on the URL?
        if url.port().is_none() {
            return Err(Error::from("missing port"));
        };

        let source_runtime = self
            .source_runtime
            .as_mut()
            .ok_or_else(|| Error::from("source runtime not set"))?;

        let tls_config = match self.config.tls.as_ref() {
            Some(Either::Right(true)) => {
                // default config
                Some((TLSClientConfig::default().to_client_connector()?, host))
            }
            Some(Either::Left(tls_config)) => Some((
                tls_config.to_client_connector()?,
                tls_config.domain.clone().unwrap_or(host),
            )),
            Some(Either::Right(false)) | None => None,
        };

        let tcp_stream = tcp_client_socket(&url, &self.config.socket_options).await?;
        let (local_addr, peer_addr) = (tcp_stream.local_addr()?, tcp_stream.peer_addr()?);
        let stream_id = self.stream_id_generator.next_stream_id();

        if let Some((tls_connector, tls_domain)) = tls_config {
            // TLS
            // wrap it into arcmutex, because we need to clone it in order to close it properly
            let server_name = ServerName::try_from(tls_domain.as_str())?;
            let tls_stream = tls_connector.connect(server_name, tcp_stream).await?;
            let (ws_stream, _http_response) = client_async(url.as_str(), tls_stream).await?;
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: local_addr.ip().to_string(),
                port: Some(local_addr.port()),
                path: vec![local_addr.port().to_string()], // local port
            };
            let (writer, reader) = ws_stream.split();
            let meta = ctx.meta(WsClient::meta(peer_addr, true));
            let ws_writer = WsWriter::new_tls_client(writer);

            let writer_handle = self
                .sink_runtime
                .register_stream_writer(stream_id, Some(handle.to_string()), ctx, ws_writer)
                .await;

            self.writer_handles
                .insert(handle.to_string(), writer_handle);

            let ws_reader = WsReader::new(
                reader,
                Some(self.sink_runtime.clone()),
                origin_uri,
                meta,
                ctx.clone(),
            );
            source_runtime.register_stream_reader(stream_id, ctx, ws_reader);
        } else {
            // No TLS
            let (ws_stream, _http_response) = client_async(url.as_str(), tcp_stream).await?;
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: local_addr.ip().to_string(),
                port: Some(local_addr.port()),
                path: vec![local_addr.port().to_string()], // local port
            };
            let (writer, reader) = ws_stream.split();
            let meta = ctx.meta(WsClient::meta(peer_addr, false));

            let ws_writer = WsWriter::new_tungstenite_client(writer);

            let writer_handle = self
                .sink_runtime
                .register_stream_writer(stream_id, Some(handle.to_string()), ctx, ws_writer)
                .await;

            self.writer_handles
                .insert(handle.to_string(), writer_handle);

            let ws_reader = WsReader::new(
                reader,
                Some(self.sink_runtime.clone()),
                origin_uri,
                meta,
                ctx.clone(),
            );
            source_runtime.register_stream_reader(stream_id, ctx, ws_reader);
        }

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        if let Some(handle) = self.writer_handles.remove(handle) {
            handle.abort();
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Sink for WsClientSink<T>
where
    T: Fn(&Value) -> Option<String> + Send + Sync + 'static,
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

struct WsClient {
    config: Config,
    source_runtime: Option<ChannelSourceRuntime>,
    tx: Sender<ChannelSinkMsg<String>>,
    rx: Option<Receiver<ChannelSinkMsg<String>>>,
}

impl WsClient {
    fn meta(peer: SocketAddr, has_tls: bool) -> Value<'static> {
        let peer_ip = peer.ip().to_string();
        let peer_port = peer.port();

        literal!({
            "tls": has_tls,
            "peer": {
                "host": peer_ip,
                "port": peer_port
            }
        })
    }
}

#[async_trait::async_trait]
impl Connector for WsClient {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = ChannelSource::new(Arc::new(AtomicBool::new(true)));
        self.source_runtime = Some(source.runtime());
        let addr = builder.spawn(source, source_context);
        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let default_handle = self.config.default_handle.clone();
        let sink = ChannelSink::new(
            move |x| resolve_metadata(x, default_handle.clone()),
            builder.reply_tx(),
            self.tx.clone(),
            self.rx.take().ok_or_else(already_created_error)?,
            Arc::new(AtomicBool::new(true)),
        );

        let runtime = sink.runtime();
        let sink = WsClientSink {
            inner: sink,
            sink_runtime: runtime,
            source_runtime: self.source_runtime.clone(),
            config: self.config.clone(),
            stream_id_generator: StreamIdGen::default(),
            writer_handles: HashMap::new(),
        };

        Ok(Some(builder.spawn(sink, sink_context)))
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    fn validate(&self, config: &ConnectorConfig) -> Result<()> {
        for cmd in &config.initial_commands {
            match structurize::<Command>(cmd.clone())? {
                Command::SocketClient(_) => {}
                _ => {
                    return Err(ErrorKind::NotImplemented(
                        "only socket client commands are supported".into(),
                    )
                    .into())
                }
            }
        }

        Ok(())
    }
}

fn resolve_metadata(meta: &Value, default_handle: Option<String>) -> Option<String> {
    meta.get("handle")
        .as_str()
        .map(ToString::to_string)
        .or(default_handle)
}
