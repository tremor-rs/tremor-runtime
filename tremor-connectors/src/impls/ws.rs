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

//! The [`ws_server`](#ws_server) and [`ws_client`](#ws_client) connectors provide support for the `WebSocket` protocol specification.
//!
//! Tremor can expose a client or server connection.
//!
//! Text and binary frames can be used.
//!
//! ## `ws_server`
//!
//! This connector is a websocket server. It opens a TCP listening socket, and for each incoming connection it initiates the Websocket handshake. Then websocket frames can flow
//! and are processed with the given `preprocessors` and `codec` and sent to the `out` port of the connector.
//!
//! Each incoming connection creates a new stream of events. Events from a websocket connection bear the following metadata record at `$ws_server`:
//!
//! ```js
//! {
//!   "tls": true, // whether or not TLS is configured
//!   "peer": {
//!     "host": "127.0.0.1", // ip of the connection peer
//!     "port": 12345        // port of the connection peer
//!   }
//! }
//! ```
//!
//! When a connection is established and events are received, it is possible to send events to any open connection. In order to achieve this, a pipeline needs to be connected to the `in` port of this connector and send events to it. There are multiple ways to target a certain connection with a specific event:
//!
//! * Send the event you just received from the `ws_server` right back to it. It will be able to track the the event to its websocket connection. You can even do this with an aggregate event coming from a select with a window. If an event is the result of events from multiple websocket connections, it will send the event back down to each websocket connection.
//! * Attach the same metadata you receive on the connection under `$ws_server` to the event you want to send to that connection.
//!
//! ### Configuration
//!
//! | Option           | Description                                                                                                 | Type             | Required | Default value                                                                |
//! |------------------|-------------------------------------------------------------------------------------------------------------|------------------|----------|------------------------------------------------------------------------------|
//! | `url`            | The host and port as url to listen on.                                                                      | string           | yes      |                                                                              |
//! | `tls`            | Optional Transport Level Security configuration. See [TLS configuration](./index.md#server). | record           | no       | No TLS configured.                                                           |
//! | `backlog`        | The maximum size of the queue of pending connections not yet `accept`ed.                                    | positive integer | no       | 128                                                                          |
//! | `socket_options` | See [TCP socket options](./index.md#tcp-socket-options).                                     | record           | no       | See [TCP socket options defaults](./index#tcp-socket-options) |
//!
//! ### Examples
//!
//! An annotated example of a plain WS cient configuration leveraging defaults:
//!
//! ```tremor title="config.troy"
//! define connector in from ws_server
//! with
//!   preprocessors = [
//!     {
//!       "name": "separate",
//!       "config": {
//!         "buffered": false
//!       }
//!     }
//!   ],
//!   codec = "json",
//!   config = {
//!     "url": "127.0.0.1:4242",
//!   }
//! end;
//! ```
//!
//! An annotated example of a secure WS server configuration:
//!
//! ```tremor title="config.troy"
//! define connector ws_server from ws_server
//! with
//!   preprocessors = ["separate"],
//!   codec = "json",
//!   config = {
//!     "url": "0.0.0.0:65535",
//!     "tls": {
//!       # Security certificate for this service endpoint
//!       "cert": "./before/localhost.cert",
//!       # Security key
//!       "key": "./before/localhost.key",
//!     }
//!   }
//! end;
//! ```
//!
//! ## `ws_client`
//!
//! This connector is a websocket client, that establishes one connection to the host and port configured in `url`. Events sent to the `in` port of this connector will be processed by the configured `codec` and `postprocessors` and turned into a text or binary frame, depending on the events boolean metadata value `$ws_server.binary`. If you want to sent a binary frame, you need to set:
//!
//! ```tremor
//! let $ws_server["binary"] = true;
//! ```
//!
//! If nothing is provided a text frame is sent.
//!
//! Data received on the open connection is processed frame by frame by the configured `preprocessors` and `codec` and sent as event via the `out` port of the connector. Each event contains a metadata record of the following form via `$ws_server`:
//!
//! ```js
//! {
//!   "tls": false, // whether or not tls is enabled on the connection
//!   "peer": {
//!     "host": "192.168.0.1", // ip of the connection peer
//!     "port": 56431          // port of the connection peer
//!   }
//! }
//! ```
//!
//! ### Configuration
//!
//! | Option           | Description                                                                                                 | Type              | Required | Default value                                                                |
//! |------------------|-------------------------------------------------------------------------------------------------------------|-------------------|----------|------------------------------------------------------------------------------|
//! | `url`            | The URL to connect to in order to initiate the websocket connection.                                        | string            | yes      |                                                                              |
//! | `tls`            | Optional Transport Level Security configuration. See [TLS configuration](./index.md#client). | record or boolean | no       | No TLS configured.                                                           |
//! | `socket_options` | See [TCP socket options](./index.md#tcp-socket-options).                                     | record            | no       | See [TCP socket options defaults](./index#tcp-socket-options) |
//!
//! ### Examples
//!
//! An annotated example of a non-tls plain WS cient configuration leveraging defaults:
//!
//! ```tremor title="config.troy"
//! define my_wsc out from ws_client
//! with
//!   postprocessors = ["separate"],
//!   codec = "json",
//!   config = {
//!     # Connect to port 4242 on the loopback device
//!     "url": "ws://127.0.0.1:4242/"
//!
//!     # Optional Transport Level Security configuration
//!     # "tls" = { ... }
//!
//!     # Optional tuning of the Nagle algorithm ( default: true )
//!     # - By default no delay is preferred
//!     # "no_delay" = false
//!   }
//! end;
//! ```
//!
//! An annotated example of a secure WS client configuration with
//! reconnection quality of service configured:
//!
//! ```tremor title="config.troy"
//! define connector ws_client from ws_client
//! with
//!   postprocessors = ["separate"],
//!   codec = "json",
//!   config = {
//!     # Listen on all interfaces on TCP port 65535
//!     "url": "wss://0.0.0.0:65535",
//!
//!     # Prefer delay and enable the TCP Nagle algorithm
//!     "no_delay": false,
//!
//!     # Enable SSL/TLS
//!     "tls": {
//!       # CA certificate
//!       "cafile": "./before/localhost.cert",
//!       # Domain
//!       "domain": "localhost",
//!     }
//!   },
//!   # Reconnect starting at half a second, backoff by doubling, maximum of 3 tries before circuit breaking
//!   reconnect = {
//!     "retry": {
//!       "interval_ms": 500,
//!       "growth_rate": 2,
//!       "max_retries": 3,
//!     }
//!   }
//! end;
//! ```

pub(crate) mod client;
pub(crate) mod server;

use crate::prelude::*;
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
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Sync + Send,
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
    async fn write(&mut self, data: Vec<Vec<u8>>, meta: Option<&Value>) -> Result<()> {
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
