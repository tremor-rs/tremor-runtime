// Copyright 2022, The Tremor Team
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

//! The [`unix_socket_server`](#unix_socket_server) and [`unix_socket_client`](#unix_socket_client) connectors allow clients and servers based on UNIX domain sockets to be integrated with tremor.
//!
//! ## `unix_socket_client`
//!
//! This connector establishes a connection to the unix domain socket at the given `path`, which must exist for this connector to connect successfully. Every event it receives via its `in` port is processed with the configured codec and postprocessors sent down that socket. Every data received from that socket will be processed with the configured preprocessors and codec and finally emitted as event to its `out` port. Emitted events will contain information about the socket they come from in their metadata via `$unix_socket_client`. The metadata has the following form:
//!
//! ```js
//! {
//!   "peer": "/tmp/my_sock" // path of the socket
//! }
//! ```
//!
//! | Option     | Description                                                                                    | Type             | Required | Default value |
//! |------------|------------------------------------------------------------------------------------------------|------------------|----------|---------------|
//! | `path`     | Path to an existing unix domain socket.                                                        | string           | yes      |               |
//! | `buf_size` | Size of the receive buffer in bytes. Determining the maximum packet size that can be received. | positive integer | no       | 8192          |
//!
//! ### Configuration
//!
//! Example:
//!
//! ```tremor title="config.troy"
//! define connector client from unix_socket_client
//! with
//!   postprocessors = ["separate"],
//!   preprocessors = ["separate"],
//!   codec = "json",
//!   config = {
//!     # required - Path to socket file for this client
//!     "path": "/tmp/unix-socket.sock",
//!     # required - Size of buffer for data IO
//!     "buf_size": 1024,
//!   },
//!   # Configure basic reconnection QoS for clients - max 3 retries starting with 100ms reconnect attempt interval
//!   reconnect = {
//!     "retry": {
//!       "interval_ms": 100,
//!       "growth_rate": 2,
//!       "max_retries": 3,
//!     }
//!   }
//! end;
//! ```
//!
//! ## `unix_socket_server`
//!
//! This connector is creating a unix domain socket at the configured `path` and listens for incoming connections on it.
//! Each connection starts its own stream of events. Each packet is received into a local buffer of `buf_size` bytes, which should be equal or bigger than the maximum expected packet size. Each packet is processed by the configured `preprocessors` and `codec`.
//!
//! Each event will contain information about the unix domain socket connection it comes from in a metadata record accessible via `$unix_socket_server`. The record is of the following form:
//!
//! ```js
//! {
//!   "path": "/tmp/sock", // path of the socket
//!   "peer": 123 // some opaque number identifying the connection
//! }
//! ```
//!
//! When a connection is established and events are received, it is possible to send events to any open connection. In order to achieve this, a pipeline needs to be connected to the `in` port of this connector and send events to it. There are multiple ways to target a certain connection with a specific event:
//!
//! * Send the event you just received from the `unix_socket_server` right back to it. It will be able to track the the event to its socket connection. You can even do this with an aggregate event coming from a select with a window. If an event is the result of events from multiple socket connections, it will send the event back down to each connection.
//! * Attach the same metadata you receive on the connection under `$unix_socket_server` to the event you want to send to that connection.
//!
//! ### Configuration
//!
//! | Option        | Description                                                                                             | Type             | Required | Default value |
//! |---------------|---------------------------------------------------------------------------------------------------------|------------------|----------|---------------|
//! | `path`        | Path to the socket. Will be created if it doesn't exits. Will recreate `path` as a socket if it exists. | string           | yes      |               |
//! | `permissions` | Permissing for `path`.                                                                                  | string           | no       |               |
//! | `buf_size`    | Size of the receive buffer in bytes. Determining the maximum packet size that can be received.          | positive integer | no       | 8192          |
//!
//! Example:
//!
//! ```tremor title="config.troy"
//! define connector server from unix_socket_server
//! with
//!   # Server produces/consumes line delimited JSON data
//!   preprocessors = ["separate"],
//!   postprocessors = ["separate"],
//!   codec = "json",
//!
//!   # UNIX socket specific configuration
//!   config = {
//!     # required - Path to socket file for this server
//!     "path": "/tmp/unix-socket.sock",
//!     # optional permissions for the socket
//!     "permissions": "=600",
//!     # optional - Use a 1K buffer - this should be tuned based on data value space requirements
//!     "buf_size": 1024,
//!   }
//! end;
//! ```

use crate::prelude::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::UnixStream,
};
use tremor_value::Value;

/// unix domain socket server
pub(crate) mod server;

/// unix domain socket client
pub(crate) mod client;

struct UnixSocketReader {
    stream: ReadHalf<UnixStream>,
    buffer: Vec<u8>,
    alias: String,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
    sink_runtime: Option<ChannelSinkRuntime<server::ConnectionMeta>>,
}

impl UnixSocketReader {
    fn new(
        stream: ReadHalf<UnixStream>,
        buffer_size: usize,
        alias: String,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
        sink_runtime: Option<ChannelSinkRuntime<server::ConnectionMeta>>,
    ) -> Self {
        Self {
            stream,
            buffer: vec![0; buffer_size],
            alias,
            origin_uri,
            meta,
            sink_runtime,
        }
    }
}

#[async_trait::async_trait()]
impl StreamReader for UnixSocketReader {
    async fn quiesce(&mut self, stream: u64) -> Option<SourceReply> {
        Some(SourceReply::EndStream {
            origin_uri: self.origin_uri.clone(),
            stream,
            meta: Some(self.meta.clone()),
        })
    }
    async fn read(&mut self, stream: u64) -> anyhow::Result<SourceReply> {
        let bytes_read = self.stream.read(&mut self.buffer).await?;
        if bytes_read == 0 {
            // EOF
            trace!("[Connector::{}] Stream {stream} EOF", &self.alias);
            return Ok(SourceReply::EndStream {
                origin_uri: self.origin_uri.clone(),
                meta: Some(self.meta.clone()),
                stream,
            });
        }
        // ALLOW: we know bytes_read is smaller than or equal buf_size
        let data = self.buffer[0..bytes_read].to_vec();
        debug!("[Connector::{}] Read {bytes_read} bytes", &self.alias);
        debug!(
            "[Connector::{}] {}",
            &self.alias,
            String::from_utf8_lossy(&data)
        );

        Ok(SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            stream: Some(stream),
            meta: Some(self.meta.clone()),
            data,
            port: None,
            codec_overwrite: None,
        })
    }

    async fn on_done(&mut self, stream: u64) -> StreamDone {
        if let Some(sink_runtime) = self.sink_runtime.as_mut() {
            if let Err(e) = sink_runtime.unregister_stream_writer(stream).await {
                warn!("[Connector::{}] Error notifying the unix_socket_server sink to close stream {stream}: {e}", &self.alias);
            }
        }
        StreamDone::StreamClosed
    }
}

struct UnixSocketWriter {
    stream: WriteHalf<UnixStream>,
}

impl UnixSocketWriter {
    fn new(stream: WriteHalf<UnixStream>) -> Self {
        Self { stream }
    }
}

#[async_trait::async_trait()]
impl StreamWriter for UnixSocketWriter {
    async fn write(&mut self, data: Vec<Vec<u8>>, _meta: Option<&Value>) -> anyhow::Result<()> {
        for chunk in data {
            let slice: &[u8] = &chunk;
            debug!(
                "[UNIX SOCKET WRITER] WRITING: {}",
                String::from_utf8_lossy(slice)
            );
            self.stream.write_all(slice).await?;
        }
        // TODO: necessary?
        self.stream.flush().await?;
        Ok(())
    }
    async fn on_done(&mut self, _stream: u64) -> anyhow::Result<StreamDone> {
        self.stream.shutdown().await?;
        Ok(StreamDone::StreamClosed)
    }
}
