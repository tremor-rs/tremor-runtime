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

use crate::connectors::prelude::*;
use async_std::os::unix::net::UnixStream;
use futures::{AsyncReadExt, AsyncWriteExt};
use tremor_pipeline::EventOriginUri;
use tremor_value::Value;

/// unix domain socket server
pub(crate) mod server;

/// unix domain socket client
pub(crate) mod client;

struct UnixSocketReader {
    stream: UnixStream,
    buffer: Vec<u8>,
    alias: String,
    origin_uri: EventOriginUri,
    meta: Value<'static>,
}

impl UnixSocketReader {
    fn new(
        stream: UnixStream,
        buffer: Vec<u8>,
        alias: String,
        origin_uri: EventOriginUri,
        meta: Value<'static>,
    ) -> Self {
        Self {
            stream,
            buffer,
            alias,
            origin_uri,
            meta,
        }
    }
}

#[async_trait::async_trait()]
impl StreamReader for UnixSocketReader {
    async fn read(&mut self, stream: u64) -> Result<SourceReply> {
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
        let data = self.buffer[0..bytes_read].to_vec();
        debug!("[Connector::{}] Read {bytes_read} bytes", &self.alias);
        debug!(
            "[Connector::{}] {}",
            &self.alias,
            String::from_utf8_lossy(&data)
        );

        Ok(SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            stream,
            meta: Some(self.meta.clone()),
            // ALLOW: we know bytes_read is smaller than or equal buf_size
            data: self.buffer[0..bytes_read].to_vec(),
            port: None,
        })
    }

    async fn on_done(&mut self, stream: u64) -> StreamDone {
        // THIS IS SHUTDOWN!
        if let Err(e) = self.stream.shutdown(std::net::Shutdown::Read) {
            warn!(
                "[Connector::{}] Error shutting down reading half of stream {stream}: {e}",
                &self.alias
            );
        }
        StreamDone::StreamClosed
    }
}

struct UnixSocketWriter {
    stream: UnixStream,
}

impl UnixSocketWriter {
    fn new(stream: UnixStream) -> Self {
        Self { stream }
    }
}

#[async_trait::async_trait()]
impl StreamWriter for UnixSocketWriter {
    async fn write(&mut self, data: Vec<Vec<u8>>, _meta: Option<SinkMeta>) -> Result<()> {
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
    async fn on_done(&mut self, _stream: u64) -> Result<StreamDone> {
        self.stream.shutdown(std::net::Shutdown::Write)?;
        Ok(StreamDone::StreamClosed)
    }
}
