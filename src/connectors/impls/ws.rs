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
use async_tungstenite::tungstenite::Message;
use async_tungstenite::WebSocketStream;
use futures::prelude::*;
use futures::stream::{SplitSink, SplitStream};
use simd_json::StaticNode;

pub(crate) struct WsDefaults;
impl Defaults for WsDefaults {
    const SCHEME: &'static str = "ws";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 80;
}

struct WsReader<Stream, Ctx, Runtime>
where
    Stream: futures::AsyncRead + futures::AsyncWrite + Send + Sync + Unpin,
    Ctx: Context + Send,
    Runtime: SinkRuntime,
{
    stream: SplitStream<WebSocketStream<Stream>>,
    // we keep this around for closing the writing part if the reader is done
    sink_runtime: Runtime,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
    ctx: Ctx,
}

impl<Stream, Ctx, Runtime> WsReader<Stream, Ctx, Runtime>
where
    Stream: futures::AsyncRead + futures::AsyncWrite + Send + Sync + Unpin,
    Ctx: Context + Send + Sync,
    Runtime: SinkRuntime,
{
    fn new(
        stream: SplitStream<WebSocketStream<Stream>>,
        sink_runtime: Runtime,
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
    Stream: futures::AsyncRead + futures::AsyncWrite + Send + Sync + Unpin,
    Ctx: Context + Send + Sync,
    Runtime: SinkRuntime,
{
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
                        return Ok(SourceReply::Empty(0));
                    }
                };
                let mut meta = self.meta.clone();
                if is_binary {
                    meta.insert("binary", Value::Static(StaticNode::Bool(true)))?;
                };
                Ok(SourceReply::Data {
                    origin_uri: self.origin_uri.clone(),
                    stream,
                    meta: Some(meta),
                    data,
                    port: None,
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
        self.ctx.log_err(
            self.sink_runtime.unregister_stream_writer(stream).await,
            "Error unregistering stream",
        );
        StreamDone::StreamClosed
    }
}

struct WsWriter<S>
where
    S: async_std::io::Read + async_std::io::Write + std::marker::Unpin + std::marker::Sync,
{
    sink: SplitSink<WebSocketStream<S>, Message>,
}

impl WsWriter<async_std::net::TcpStream> {
    fn new(sink: SplitSink<WebSocketStream<async_std::net::TcpStream>, Message>) -> Self {
        Self { sink }
    }
}

impl WsWriter<async_tls::server::TlsStream<async_std::net::TcpStream>> {
    fn new_tls_server(
        sink: SplitSink<
            WebSocketStream<async_tls::server::TlsStream<async_std::net::TcpStream>>,
            Message,
        >,
    ) -> Self {
        Self { sink }
    }
}

impl WsWriter<async_std::net::TcpStream> {
    fn new_tungstenite_client(
        sink: SplitSink<WebSocketStream<async_std::net::TcpStream>, Message>,
    ) -> Self {
        Self { sink }
    }
}

impl WsWriter<async_tls::client::TlsStream<async_std::net::TcpStream>> {
    fn new_tls_client(
        sink: SplitSink<
            WebSocketStream<async_tls::client::TlsStream<async_std::net::TcpStream>>,
            Message,
        >,
    ) -> Self {
        Self { sink }
    }
}

#[async_trait::async_trait]
impl<S> StreamWriter for WsWriter<S>
where
    S: futures::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Sync
        + std::marker::Send
        + futures::io::AsyncRead,
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
