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
use crate::connectors::sink::{AsyncSinkReply, ChannelSink, ChannelSinkMsg};
use crate::connectors::source::ChannelSource;
pub use crate::errors::{Error, ErrorKind, Result};
use async_std::channel::{bounded, Sender, TrySendError};
use async_std::net::TcpListener;
use async_std::task::{self, JoinHandle};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use simd_json::ValueAccess;
use std::net::SocketAddr;
use tremor_common::stry;
use tremor_common::time::nanotime;
use tremor_pipeline::EventOriginUri;
use tremor_value::{literal, Value};

use super::reconnect::ConnectionLostNotifier;
use super::sink::{SinkAddr, SinkManagerBuilder};
use super::source::{SourceAddr, SourceManagerBuilder};

const URL_SCHEME: &str = "tremor-tcp";

#[derive(Deserialize, Debug)]
pub struct Config {
    // kept as a str, so it is re-resolved upon each connect
    host: String,
    port: u16,
    // TCP: receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

fn default_buf_size() -> usize {
    8192
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
    sink_channel: Sender<ChannelSinkMsg<ConnectionMeta>>,
    source_channel: SourceReplySender,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}
impl ConnectorBuilder for Builder {
    fn from_config(
        &self,
        id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> crate::errors::Result<Box<dyn Connector>> {
        if let Some(raw_config) = raw_config {
            let config = Config::new(raw_config)?;
            let (dummy_tx, _dummy_rx) = bounded(1);
            let (dummy_tx2, _dummy_rx) = bounded(1);
            Ok(Box::new(TcpServer {
                url: id.clone(),
                config,
                accept_task: None,        // not yet started
                sink_channel: dummy_tx2,  // replaced in create_sink()
                source_channel: dummy_tx, // replaced in create_source()
            }))
        } else {
            Err(crate::errors::ErrorKind::MissingConfiguration(String::from("TcpServer")).into())
        }
    }
}

/// send a message to a channel
///   - first try a non-blocking send
///   - if the queue is full, we log, use the blocking version and await on it
/// dont care about send errors, just log an error
async fn send<T>(url: &TremorUrl, msg: T, tx: &Sender<T>) -> Result<()> {
    let res = match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(m)) => {
            // TODO: emit a metric about this instead of logging
            debug!("[Connector::{}] Send queue full.", &url);
            tx.send(m).await.map_err(Error::from)
        }
        Err(e) => Err(Error::from(e)),
    };

    if let Err(e) = &res {
        error!("[Connector::{}] Error sending message: {}", url, e);
    }
    res
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
        let source = ChannelSource::new(builder.qsize());
        self.source_channel = source.sender();
        let addr = builder.spawn(source, ctx)?;

        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ChannelSink::new(builder.qsize(), resolve_connection_meta, builder.reply_tx());
        self.sink_channel = sink.sender();
        let addr = builder.spawn(sink, ctx)?;
        Ok(Some(addr))
    }

    #[allow(clippy::too_many_lines)]
    async fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        notifier: ConnectionLostNotifier,
        quiescence: &QuiescenceBeacon,
    ) -> Result<bool> {
        let path = vec![self.config.port.to_string()];
        let accept_url = self.url.clone();

        let source_tx = self.source_channel.clone();
        let sink_tx = self.sink_channel.clone();
        let buf_size = self.config.buf_size;

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(previous_handle) = self.accept_task.take() {
            previous_handle.cancel().await;
        }

        // for checking if we should stop reading / writing - getting notified by the engine
        let quiescence = quiescence.clone();

        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        // accept task
        self.accept_task = Some(task::spawn(async move {
            let mut stream_id_gen = StreamIdGen::default();
            while let (true, Ok((stream, peer_addr))) =
                (quiescence.continue_reading(), listener.accept().await)
            {
                trace!(
                    "[Connector::{}] new connection from {}",
                    &accept_url,
                    peer_addr
                );
                let stream_id: u64 = stream_id_gen.next_stream_id();
                let connection_meta = peer_addr.into();
                // Async<T> allows us to read in one thread and write in another concurrently - see its documentation
                // So we don't need no BiLock like we would when using `.split()`
                let mut source_stream = stream.clone();
                let mut sink_stream = stream;
                let sc_stream = source_tx.clone();
                let stream_url = accept_url.clone();
                let sink_url = accept_url.clone();
                let origin_uri = EventOriginUri {
                    scheme: URL_SCHEME.to_string(),
                    host: peer_addr.ip().to_string(),
                    port: Some(peer_addr.port()),
                    path: path.clone(), // captures server port
                };

                // TODO: issue metrics on send time, to detect queues running full
                stry!(send(&accept_url, SourceReply::StartStream(stream_id), &source_tx).await);

                let read_quiescence = quiescence.clone();
                // spawn source stream task
                task::spawn(async move {
                    let mut buffer = vec![0; buf_size];
                    while let (true, Ok(bytes_read)) = (
                        read_quiescence.continue_reading(),
                        source_stream.read(&mut buffer).await,
                    ) {
                        if bytes_read == 0 {
                            // EOF
                            trace!("[Connector::{}] EOF", &stream_url);
                            break;
                        }
                        trace!("[Connector::{}] read {} bytes", &stream_url, bytes_read);
                        // TODO: meta needs to be wrapped in <RESOURCE_TYPE>.<ARTEFACT> by the source manager
                        // this is only the connector specific part, without the path mentioned above
                        let meta = literal!({
                            "peer": {
                                "host": peer_addr.ip().to_string(),
                                "port": peer_addr.port()
                            }
                        });
                        let sc_data = SourceReply::Data {
                            origin_uri: origin_uri.clone(),
                            stream: stream_id,
                            meta: Some(meta),
                            // ALLOW: we know bytes_read is smaller than or equal buf_size
                            data: buffer[0..bytes_read].to_vec(),
                        };
                        stry!(send(&stream_url, sc_data, &sc_stream).await);
                    }
                    if let Err(e) = source_stream.shutdown(std::net::Shutdown::Read) {
                        error!(
                            "[Connector::{}] Error shutting down reading half of stream {}: {}",
                            &stream_url, stream_id, e
                        );
                    }
                    stry!(send(&stream_url, SourceReply::EndStream(stream_id), &sc_stream).await);
                    Ok(())
                });

                let (stream_tx, stream_rx) = bounded::<SinkData>(QSIZE.load(Ordering::Relaxed));
                // spawn sink stream task
                let stream_sink_tx = sink_tx.clone();
                let write_quiescence = quiescence.clone();
                task::spawn(async move {
                    // receive loop from channel sink
                    while let (
                        true,
                        Ok(SinkData {
                            data,
                            contraflow,
                            start,
                        }),
                    ) = (write_quiescence.continue_writing(), stream_rx.recv().await)
                    {
                        let mut failed = false;
                        for chunk in data {
                            let slice: &[u8] = &chunk;
                            if let Err(e) = sink_stream.write_all(slice).await {
                                failed = true;
                                error!("[Connector::{}] Error writing TCP data: {}", &sink_url, e);
                                break;
                            }
                        }
                        // send asyn contraflow insights if requested (only if event.transactional)
                        if let Some((cf_data, sender)) = contraflow {
                            let reply = if failed {
                                AsyncSinkReply::Fail(cf_data)
                            } else {
                                AsyncSinkReply::Ack(cf_data, nanotime() - start)
                            };
                            if let Err(e) = sender.send(reply).await {
                                error!(
                                    "[Connector::{}] Error sending async sink reply: {}",
                                    &sink_url, e
                                );
                            }
                        }
                        if failed {
                            break;
                        }
                    }
                    if let Err(e) = sink_stream.shutdown(std::net::Shutdown::Write) {
                        error!(
                            "[Connector::{}] Error shutting down read half of stream {}: {}",
                            &sink_url, stream_id, e
                        );
                    }
                    stream_sink_tx
                        .send(ChannelSinkMsg::RemoveStream(stream_id))
                        .await?;
                    Result::Ok(())
                });
                sink_tx
                    .send(ChannelSinkMsg::NewStream {
                        stream_id,
                        meta: Some(connection_meta),
                        sender: stream_tx,
                    })
                    .await?;
            }

            // notify connector task about disconnect
            // of the listening socket
            notifier.notify().await?;
            Ok(())
        }));

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
