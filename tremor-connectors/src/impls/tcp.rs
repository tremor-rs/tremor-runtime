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

//! The [`tcp_server`](#tcp_server) and [`tcp_client`](#tcp_client) connectors allow TCP-based clients and servers to be integrated with tremor.
//!
//! ## `tcp_server`
//!
//! This connector listens for incoming TCP connections on the host and port configured in `url`. Each connection starts its own stream of events. Each TCP packet is received into a local buffer of `buf_size` bytes, which should be equal or bigger than the maximum expected packet size. Each packet is processed by the configured `preprocessors` and `codec`.
//!
//! Each event will contain information about the TCP connection it comes from in a metadata record accessible via `$tcp_server`. The record is of the following form:
//!
//! ```js
//! {
//!   "tls": true, // boolean, indicating if this connection has tls configured or not
//!   "peer": {
//!     "host": "127.0.0.1", // ip of the connection peer
//!     "port": 443          // port of the connection peer
//!   }
//! }
//! ```
//!
//! When a connection is established and events are received, it is possible to send events to any open connection. In order to achieve this, a pipeline needs to be connected to the `in` port of this connector and send events to it. There are multiple ways to target a certain connection with a specific event:
//!
//! * Send the event you just received from the `tcp_server` right back to it. It will be able to track the the event to its TCP connection. You can even do this with an aggregate event coming from a select with a window. If an event is the result of events from multiple TCP connections, it will send the event back down to each TCP connection.
//! * Attach the same metadata you receive on the connection under `$tcp_server` to the event you want to send to that connection.
//!
//! ### Configuration
//!
//! | Option           | Description                                                                                                 | Type             | Required | Default value                                                                |
//! |------------------|-------------------------------------------------------------------------------------------------------------|------------------|----------|------------------------------------------------------------------------------|
//! | `url`            | Host and port to listen on.                                                                                 | string           | yes      |                                                                              |
//! | `tls`            | Optional Transport Level Security configuration. See [TLS configuration](./index.md#server). | record           | no       | No TLS configured.                                                           |
//! | `buf_size`       | TCP receive buffer size. This should be greater than or equal to the expected maximum packet size.          | positive integer | no       | 8192                                                                         |
//! | `backlog`        | The maximum size of the queue of pending connections not yet `accept`ed.                                    | positive integer | no       | 128                                                                          |
//! | `socket_options` | See [TCP socket options](./index.md#tcp-socket-options).                                     | record           | no       | See [TCP socket options defaults](./index#tcp-socket-options) |
//!
//! Example:
//!
//! ```tremor title="config.troy"
//! define connector `tcp-in` from tcp_server
//! with
//!   codec = "string",
//!   config = {
//!     "url": "localhost:4242",
//!     "socket_options": {
//!       # tweaking the Nagle alhorithm, true by default
//!       "TCP_NODELAY": false,
//!       # enable multiple tcp servers sharing the same port
//!       "SO_REUSEPORT": true
//!     },
//!     # Optional Transport Level Security configuration
//!     # "tls" = { ... }
//!
//!     # Data buffer size ( default: 8K, limits maximum message size )
//!     # "buf_size" = "16K",
//!   }
//! end;
//! ```
//!
//!
//! ## `tcp_client`
//!
//! This connector establishes and maintains one TCP connection to the host and port configured in `url`.
//! Every event this connector receives via its `in` port will be sent to that connection.
//! It will emit events for the data it receives on that connection on the `out` port of this connector. Emitted events contain information about the TCP connection they originate from in the metadata available via `$tcp_client`. It is a record of the following form:
//!
//! ```js
//! {
//!   "tls": false, // whether or not TLS is configured on the connection
//!   "peer": {
//!     "host": "::1", // ip address of the connection peer
//!     "port": 12345  // port of the connection peer
//!   }
//! }
//! ```
//!
//! ### Configuration
//!
//! | Option           | Description                                                                                                 | Type              | Required | Default value                                                                |
//! |------------------|-------------------------------------------------------------------------------------------------------------|-------------------|----------|------------------------------------------------------------------------------|
//! | `url`            | Host and port to connect to.                                                                                | string            | yes      |                                                                              |
//! | `tls`            | Optional Transport Level Security configuration. See [TLS configuration](./index.md#client). | record or boolean | no       | No TLS configured.                                                           |
//! | `buf_size`       | TCP receive buffer size. This should be greater than or equal to the expected maximum packet size.          | positive integer  | no       | 8192                                                                         |
//! | `socket_options` | See [TCP socket options](./index.md#tcp-socket-options).                                     | record            | no       | See [TCP socket options defaults](./index#tcp-socket-options) |
//!
//!
//! Example:
//!
//! ```tremor title="config.troy"
//! define connector `tcp-out` from tcp_client
//! with
//!   codec = "yaml",
//!   postprocessors = ["base64"],
//!   config = {
//!     "url": "localhost:4242",
//!
//!     # Optional Transport Level Security configuration
//!     # "tls" = { ... },
//!
//!     # Data buffer size ( default: 8K, limits maximum message size )
//!     # "buf_size" = "16_000",
//!   }
//! end;
//! ```

/// TCP client Connector
pub mod client;
/// TCP server Connector
pub mod server;

use crate::{
    log_error,
    sink::{channel_sink::ChannelSinkRuntime, SinkRuntime, StreamWriter},
    source::{SourceReply, StreamReader},
    utils::ConnectionMeta,
    StreamDone,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tremor_common::{alias, url::Defaults};
use tremor_script::EventOriginUri;
use tremor_value::Value;

pub(crate) struct TcpDefaults;
impl Defaults for TcpDefaults {
    const SCHEME: &'static str = "tcp";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 4242;
}

struct TcpReader<S>
where
    S: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    wrapped_stream: S,
    buffer: Vec<u8>,
    alias: alias::Connector,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
    // notify the writer when the connection is done,
    // otherwise the socket will never close
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
}

impl TcpReader<ReadHalf<TcpStream>> {
    fn new(
        wrapped_stream: ReadHalf<TcpStream>,
        buffer: Vec<u8>,
        alias: alias::Connector,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
        sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
    ) -> Self {
        Self {
            wrapped_stream,
            buffer,
            alias,
            origin_uri,
            meta,
            sink_runtime,
        }
    }
}

impl TcpReader<ReadHalf<tokio_rustls::server::TlsStream<TcpStream>>> {
    fn tls_server(
        stream: ReadHalf<tokio_rustls::server::TlsStream<TcpStream>>,
        buffer: Vec<u8>,
        alias: alias::Connector,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
        sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
    ) -> Self {
        Self {
            wrapped_stream: stream,
            buffer,
            alias,
            origin_uri,
            meta,
            sink_runtime,
        }
    }
}

impl TcpReader<ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>> {
    fn tls_client(
        stream: ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>,
        buffer: Vec<u8>,
        alias: alias::Connector,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
    ) -> Self {
        Self {
            wrapped_stream: stream,
            buffer,
            alias,
            origin_uri,
            meta,
            sink_runtime: None,
        }
    }
}

#[async_trait::async_trait]
impl<S> StreamReader for TcpReader<S>
where
    S: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    async fn quiesce(&mut self, stream: u64) -> Option<SourceReply> {
        Some(SourceReply::EndStream {
            origin_uri: self.origin_uri.clone(),
            stream,
            meta: Some(self.meta.clone()),
        })
    }
    async fn read(&mut self, stream: u64) -> anyhow::Result<SourceReply> {
        let bytes_read = self.wrapped_stream.read(&mut self.buffer).await?;
        if bytes_read == 0 {
            // EOF
            trace!("[Connector::{}] Stream {stream} EOF", &self.alias);
            return Ok(SourceReply::EndStream {
                origin_uri: self.origin_uri.clone(),
                meta: Some(self.meta.clone()),
                stream,
            });
        }
        debug!("[Connector::{}] Read {} bytes", &self.alias, bytes_read);

        Ok(SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            stream: Some(stream),
            meta: Some(self.meta.clone()),
            // ALLOW: we know bytes_read is smaller than or equal buf_size
            data: self.buffer[0..bytes_read].to_vec(),
            port: None,
            codec_overwrite: None,
        })
    }

    async fn on_done(&mut self, stream: u64) -> StreamDone {
        // THIS IS SHUTDOWN!
        // we do this in the connector
        // if let Err(e) = self.underlying_stream.shutdown(std::net::Shutdown::Read) {
        //     warn!(
        //         "[Connector::{}] Error shutting down reading half of stream {}: {}",
        //         &self.alias, stream, e
        //     );
        // }
        // notify the writer that we are closed, otherwise the socket will never be correctly closed on our side
        if let Some(sink_runtime) = self.sink_runtime.as_mut() {
            log_error!(
                sink_runtime.unregister_stream_writer(stream).await,
                "Error notifying the tcp_writer to close the socket: {e}"
            );
        }
        StreamDone::StreamClosed
    }
}

struct TcpWriter<S>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    wrapped_stream: S,
}

impl TcpWriter<WriteHalf<TcpStream>> {
    fn new(wrapped_stream: WriteHalf<TcpStream>) -> Self {
        Self { wrapped_stream }
    }
}
impl TcpWriter<WriteHalf<tokio_rustls::server::TlsStream<TcpStream>>> {
    fn tls_server(tls_stream: WriteHalf<tokio_rustls::server::TlsStream<TcpStream>>) -> Self {
        Self {
            wrapped_stream: tls_stream,
        }
    }
}

#[async_trait::async_trait]
impl<S> StreamWriter for TcpWriter<S>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    async fn write(&mut self, data: Vec<Vec<u8>>, _meta: Option<&Value>) -> anyhow::Result<()> {
        for chunk in data {
            let slice: &[u8] = &chunk;
            self.wrapped_stream.write_all(slice).await?;
        }
        Ok(())
    }
    async fn on_done(&mut self, _stream: u64) -> anyhow::Result<StreamDone> {
        Ok(StreamDone::StreamClosed)
    }
}
