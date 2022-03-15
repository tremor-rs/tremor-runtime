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
use async_tungstenite::tungstenite::Error;
use async_tungstenite::tungstenite::Message;
use async_tungstenite::WebSocketStream;
use futures::prelude::*;
use futures::stream::SplitSink;
use simd_json::StaticNode;

struct WsReader<S>
where
    S: std::marker::Unpin
        + std::marker::Sync
        + std::marker::Send
        + futures::Stream<Item = std::result::Result<Message, Error>>,
{
    wrapped_stream: S,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
}

impl<S> WsReader<S>
where
    S: std::marker::Unpin
        + std::marker::Sync
        + std::marker::Send
        + futures::Stream<Item = std::result::Result<Message, Error>>,
{
    fn new(stream: S, origin_uri: EventOriginUri, meta: Value<'static>) -> Self {
        Self {
            wrapped_stream: stream,
            origin_uri,
            meta,
        }
    }
}

#[async_trait::async_trait]
impl<S> StreamReader for WsReader<S>
where
    S: std::marker::Unpin
        + std::marker::Sync
        + std::marker::Send
        + futures::Stream<Item = std::result::Result<Message, Error>>,
{
    async fn read(&mut self, stream: u64) -> Result<SourceReply> {
        let mut is_binary = false;
        match self.wrapped_stream.next().await {
            Some(Ok(message)) => {
                let data = match message {
                    Message::Text(text) => text.into_bytes(),
                    Message::Binary(binary) => {
                        is_binary = true;
                        binary
                    }
                    Message::Close(_) => {
                        return Ok(SourceReply::EndStream {
                            origin_uri: self.origin_uri.clone(),
                            stream,
                            meta: Some(self.meta.clone()),
                        });
                    }
                    _ => todo!(),
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

    async fn on_done(&mut self, _stream: u64) -> StreamDone {
        // NOTE We depend on the StreamWriter to cascade an actual close
        StreamDone::StreamClosed
    }
}

struct WsWriter<S>
where
    S: futures::AsyncRead + futures::AsyncWrite + std::marker::Unpin + std::marker::Sync,
{
    wrapped_stream: SplitSink<WebSocketStream<S>, Message>,
}

impl WsWriter<async_std::net::TcpStream> {
    fn new(stream: SplitSink<WebSocketStream<async_std::net::TcpStream>, Message>) -> Self {
        Self {
            wrapped_stream: stream,
        }
    }
}

impl WsWriter<async_tls::server::TlsStream<async_std::net::TcpStream>> {
    fn new_tls_server(
        stream: SplitSink<
            WebSocketStream<async_tls::server::TlsStream<async_std::net::TcpStream>>,
            Message,
        >,
    ) -> Self {
        Self {
            wrapped_stream: stream,
        }
    }
}

impl
    WsWriter<
        async_tungstenite::stream::Stream<
            async_std::net::TcpStream,
            async_tls::client::TlsStream<async_std::net::TcpStream>,
        >,
    >
{
    fn new_tungstenite_client(
        stream: SplitSink<
            WebSocketStream<
                async_tungstenite::stream::Stream<
                    async_std::net::TcpStream,
                    async_tls::client::TlsStream<async_std::net::TcpStream>,
                >,
            >,
            Message,
        >,
    ) -> Self {
        Self {
            wrapped_stream: stream,
        }
    }
}

impl WsWriter<async_tls::client::TlsStream<async_std::net::TcpStream>> {
    fn new_tls_client(
        stream: SplitSink<
            WebSocketStream<async_tls::client::TlsStream<async_std::net::TcpStream>>,
            Message,
        >,
    ) -> Self {
        Self {
            wrapped_stream: stream,
        }
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
                    self.wrapped_stream.send(message).await?;
                } else {
                    let message = std::str::from_utf8(&chunk)?;
                    let message = Message::Text(message.to_string());
                    self.wrapped_stream.send(message).await?;
                }
            } else {
                // No metadata, default to text ws framing
                let message = std::str::from_utf8(&chunk)?;
                let message = Message::Text(message.to_string());
                self.wrapped_stream.send(message).await?;
            };
        }
        Ok(())
    }
    async fn on_done(&mut self, _stream: u64) -> Result<StreamDone> {
        self.wrapped_stream.close().await?;
        Ok(StreamDone::StreamClosed)
    }
}
