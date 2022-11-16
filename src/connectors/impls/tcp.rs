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

use crate::{
    connectors::{prelude::*, utils::ConnectionMeta},
    log_error,
};
use async_std::net::TcpStream;
use futures::{
    io::{ReadHalf, WriteHalf},
    AsyncReadExt, AsyncWriteExt,
};

pub(crate) struct TcpDefaults;
impl Defaults for TcpDefaults {
    const SCHEME: &'static str = "tcp";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 4242;
}

/// Default TCP backlog size
///
/// Value taken from the Rust std library
const DEFAULT_BACKLOG: i32 = 128;

/// Default TCP backlog size
fn default_backlog() -> i32 {
    DEFAULT_BACKLOG
}

struct TcpReader<S>
where
    S: futures::io::AsyncRead + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    wrapped_stream: S,
    underlying_stream: TcpStream,
    buffer: Vec<u8>,
    alias: Alias,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
    // notify the writer when the connection is done,
    // otherwise the socket will never close
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
}

impl TcpReader<TcpStream> {
    fn new(
        stream: TcpStream,
        buffer: Vec<u8>,
        alias: Alias,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
        sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
    ) -> Self {
        Self {
            wrapped_stream: stream.clone(),
            underlying_stream: stream,
            buffer,
            alias,
            origin_uri,
            meta,
            sink_runtime,
        }
    }
}

impl TcpReader<ReadHalf<async_tls::server::TlsStream<TcpStream>>> {
    fn tls_server(
        stream: ReadHalf<async_tls::server::TlsStream<TcpStream>>,
        underlying_stream: TcpStream,
        buffer: Vec<u8>,
        alias: Alias,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
        sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
    ) -> Self {
        Self {
            wrapped_stream: stream,
            underlying_stream,
            buffer,
            alias,
            origin_uri,
            meta,
            sink_runtime,
        }
    }
}

impl TcpReader<ReadHalf<async_tls::client::TlsStream<TcpStream>>> {
    fn tls_client(
        stream: ReadHalf<async_tls::client::TlsStream<TcpStream>>,
        underlying_stream: TcpStream,
        buffer: Vec<u8>,
        alias: Alias,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
    ) -> Self {
        Self {
            wrapped_stream: stream,
            underlying_stream,
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
    S: futures::io::AsyncRead + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    async fn quiesce(&mut self, stream: u64) -> Option<SourceReply> {
        Some(SourceReply::EndStream {
            origin_uri: self.origin_uri.clone(),
            stream,
            meta: Some(self.meta.clone()),
        })
    }
    async fn read(&mut self, stream: u64) -> Result<SourceReply> {
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
        if let Err(e) = self.underlying_stream.shutdown(std::net::Shutdown::Read) {
            warn!(
                "[Connector::{}] Error shutting down reading half of stream {}: {}",
                &self.alias, stream, e
            );
        }
        // notify the writer that we are closed, otherwise the socket will never be correctly closed on our side
        if let Some(sink_runtime) = self.sink_runtime.as_ref() {
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
    S: futures::io::AsyncWrite + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    wrapped_stream: S,
    underlying_stream: TcpStream,
}

impl TcpWriter<TcpStream> {
    fn new(stream: TcpStream) -> Self {
        Self {
            wrapped_stream: stream.clone(),
            underlying_stream: stream,
        }
    }
}
impl TcpWriter<WriteHalf<async_tls::server::TlsStream<TcpStream>>> {
    fn tls_server(
        tls_stream: WriteHalf<async_tls::server::TlsStream<TcpStream>>,
        underlying_stream: TcpStream,
    ) -> Self {
        Self {
            wrapped_stream: tls_stream,
            underlying_stream,
        }
    }
}

#[async_trait::async_trait]
impl<S> StreamWriter for TcpWriter<S>
where
    S: futures::io::AsyncWrite + std::marker::Unpin + std::marker::Sync + std::marker::Send,
{
    async fn write(&mut self, data: Vec<Vec<u8>>, _meta: Option<SinkMeta>) -> Result<()> {
        for chunk in data {
            let slice: &[u8] = &chunk;
            self.wrapped_stream.write_all(slice).await?;
        }
        Ok(())
    }
    async fn on_done(&mut self, _stream: u64) -> Result<StreamDone> {
        self.underlying_stream.shutdown(std::net::Shutdown::Write)?;
        Ok(StreamDone::StreamClosed)
    }
}
