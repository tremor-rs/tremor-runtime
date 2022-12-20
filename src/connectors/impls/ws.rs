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

pub(crate) mod client;
pub(crate) mod server;

use crate::connectors::prelude::*;
use futures::prelude::*;
use futures::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub(crate) struct WsDefaults;
impl Defaults for WsDefaults {
    const SCHEME: &'static str = "ws";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 80;
}

struct WsReader<Stream, Ctx, Runtime>
where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin,
    Ctx: Context + Send,
    Runtime: SinkRuntime,
{
    stream: SplitStream<WebSocketStream<Stream>>,
    // we keep this around for closing the writing part if the reader is done
    sink_runtime: Option<Runtime>,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
    ctx: Ctx,
}

impl<Stream, Ctx, Runtime> WsReader<Stream, Ctx, Runtime>
where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin,
    Ctx: Context + Send + Sync,
    Runtime: SinkRuntime,
{
    fn new(
        stream: SplitStream<WebSocketStream<Stream>>,
        sink_runtime: Option<Runtime>,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
        ctx: Ctx,
    ) -> Self {
        Self {
            stream,
            sink_runtime,
            origin_uri,
            meta,
            ctx,
        }
    }
}

#[async_trait::async_trait]
impl<Stream, Ctx, Runtime> StreamReader for WsReader<Stream, Ctx, Runtime>
where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin,
    Ctx: Context + Send + Sync,
    Runtime: SinkRuntime,
{
    async fn quiesce(&mut self, stream: u64) -> Option<SourceReply> {
        Some(SourceReply::EndStream {
            origin_uri: self.origin_uri.clone(),
            stream,
            meta: Some(self.meta.clone()),
        })
    }
    async fn read(&mut self, stream: u64) -> Result<SourceReply> {
        let mut is_binary = false;
        match self.stream.next().await {
            Some(Ok(message)) => {
                let data = match message {
                    Message::Text(text) => text.into_bytes(),
                    Message::Binary(binary) => {
                        is_binary = true;
                        binary
                    }
                    Message::Close(_) => {
                        // read from the stream once again to drive the closing handshake
                        let after_close = self.stream.next().await;
                        debug_assert!(
                            after_close.is_none(),
                            "WS reader not behaving as expected after receiving a close message"
                        );
                        return Ok(SourceReply::EndStream {
                            origin_uri: self.origin_uri.clone(),
                            stream,
                            meta: Some(self.meta.clone()),
                        });
                    }
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                        // ignore those, but don't let the source wait
                        return self.read(stream).await;
                    }
                };
                let mut meta = self.meta.clone();
                if is_binary {
                    meta.insert("binary", Value::const_true())?;
                };
                Ok(SourceReply::Data {
                    origin_uri: self.origin_uri.clone(),
                    stream: Some(stream),
                    meta: Some(meta),
                    data,
                    port: None,
                    codec_overwrite: None,
                })
            }
            Some(Err(_)) | None => Ok(SourceReply::EndStream {
                origin_uri: self.origin_uri.clone(),
                stream,
                meta: Some(self.meta.clone()),
            }),
        }
    }

    async fn on_done(&mut self, stream: u64) -> StreamDone {
        // make the writer stop, otherwise the underlying socket will never be closed
        if let Some(sink_runtime) = self.sink_runtime.as_mut() {
            self.ctx.swallow_err(
                sink_runtime.unregister_stream_writer(stream).await,
                "Error unregistering stream",
            );
        }
        StreamDone::StreamClosed
    }
}

struct WsWriter<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Sync,
{
    sink: SplitSink<WebSocketStream<S>, Message>,
}

impl WsWriter<TcpStream> {
    fn new(sink: SplitSink<WebSocketStream<TcpStream>, Message>) -> Self {
        Self { sink }
    }
}

impl WsWriter<TlsStream<TcpStream>> {
    fn new_tls_server(sink: SplitSink<WebSocketStream<TlsStream<TcpStream>>, Message>) -> Self {
        Self { sink }
    }
}

impl WsWriter<TcpStream> {
    fn new_tungstenite_client(sink: SplitSink<WebSocketStream<TcpStream>, Message>) -> Self {
        Self { sink }
    }
}

impl WsWriter<tokio_rustls::client::TlsStream<TcpStream>> {
    fn new_tls_client(
        sink: SplitSink<WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>, Message>,
    ) -> Self {
        Self { sink }
    }
}

#[async_trait::async_trait]
impl<S> StreamWriter for WsWriter<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Sync + Send + Unpin,
{
    async fn write(&mut self, data: Vec<Vec<u8>>, meta: Option<SinkMeta>) -> Result<()> {
        for chunk in data {
            if let Some(meta) = &meta {
                // If metadata is set, check for a binary framing flag
                if let Some(true) = meta.get_bool("binary") {
                    let message = Message::Binary(chunk);
                    self.sink.send(message).await?;
                } else {
                    let message = std::str::from_utf8(&chunk)?;
                    let message = Message::Text(message.to_string());
                    self.sink.send(message).await?;
                }
            } else {
                // No metadata, default to text ws framing
                let message = std::str::from_utf8(&chunk)?;
                let message = Message::Text(message.to_string());
                self.sink.send(message).await?;
            };
        }
        Ok(())
    }
    async fn on_done(&mut self, _stream: u64) -> Result<StreamDone> {
        self.sink.close().await?;
        Ok(StreamDone::StreamClosed)
    }
}
