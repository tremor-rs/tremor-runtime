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

use async_std::channel::Receiver;
use futures::ready;
use http_types::Method;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use smol::future::FutureExt;
use std::fmt;
use std::io::{Cursor, Read};
use std::str::FromStr;
use std::task::Poll;

// We use surf for http clients
pub use surf::{
    Request as SurfRequest, RequestBuilder as SurfRequestBuilder, Response as SurfResponse,
};

// We use tide for http servers
pub use tide::{Request as TideRequest, Response as TideResponse, Result as TideResult};

struct MethodStrVisitor;

impl<'de> Visitor<'de> for MethodStrVisitor {
    type Value = SerdeMethod;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an HTTP method string.")
    }

    fn visit_str<E>(self, v: &str) -> core::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Method::from_str(v)
            .map(SerdeMethod)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SerdeMethod(pub(crate) Method);

impl<'de> Deserialize<'de> for SerdeMethod {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(MethodStrVisitor)
    }
}

/// A Thingy that implements `AsyncBufRead`, so it can be turned into a tide Body
/// and it can be asynchronously fed with chunks from a channel, so we don't need to buffer all the chunks in memory
pub(crate) struct StreamingBodyReader {
    current: Cursor<Vec<u8>>,
    chunk_rx: Receiver<Vec<u8>>,
}

trait HasCurrentCursor {
    fn current(&self) -> &Cursor<Vec<u8>>;

    fn current_empty(&self) -> bool {
        self.current().position() >= self.current().get_ref().len() as u64
    }

    #[allow(clippy::cast_possible_truncation)]
    fn current_slice(&self) -> &[u8] {
        let cur_ref = self.current().get_ref();
        let len = self.current().position().min(cur_ref.len() as u64);
        // ALLOW: position is always set from usize
        &cur_ref[(len as usize)..]
    }
}

impl StreamingBodyReader {
    /// Constructor
    pub(crate) fn new(chunk_rx: Receiver<Vec<u8>>) -> Self {
        Self {
            chunk_rx,
            current: Cursor::new(vec![]),
        }
    }
}

impl HasCurrentCursor for StreamingBodyReader {
    fn current(&self) -> &Cursor<Vec<u8>> {
        &self.current
    }
}

impl async_std::io::Read for StreamingBodyReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.current_empty() {
            // cursor is empty
            match ready!(this.chunk_rx.recv().poll(cx)) {
                Ok(chunk) => {
                    this.current = Cursor::new(chunk);
                    Poll::Ready(this.current.read(buf))
                }
                Err(_e) => {
                    // channel closed, lets signal EOF
                    Poll::Ready(Ok(0))
                }
            }
        } else {
            Poll::Ready(this.current.read(buf))
        }
    }
}

impl async_std::io::BufRead for StreamingBodyReader {
    fn poll_fill_buf(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        let this = self.get_mut();
        if this.current_empty() {
            match ready!(this.chunk_rx.recv().poll(cx)) {
                Ok(chunk) => {
                    this.current = Cursor::new(chunk);
                    Poll::Ready(Ok(this.current_slice()))
                }
                Err(_e) => {
                    // channel closed, lets signal EOF
                    Poll::Ready(Ok(&[]))
                }
            }
        } else {
            Poll::Ready(Ok(this.current_slice()))
        }
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        let current_pos = this.current.position();
        this.current.set_position(current_pos + (amt as u64));
    }
}

pub(crate) struct FixedBodyReader {
    len: usize,
    data: Vec<Vec<u8>>,
    current: Cursor<Vec<u8>>,
}

impl FixedBodyReader {
    pub(crate) fn new(mut data: Vec<Vec<u8>>) -> Self {
        if data.is_empty() {
            Self {
                len: 0,
                data,
                current: Cursor::new(vec![]),
            }
        } else {
            let len = data.iter().map(Vec::len).sum();
            let current = data.remove(0);
            Self {
                len,
                data,
                current: Cursor::new(current),
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl HasCurrentCursor for FixedBodyReader {
    fn current(&self) -> &Cursor<Vec<u8>> {
        &self.current
    }
}

impl async_std::io::Read for FixedBodyReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.current_empty() && !this.data.is_empty() {
            let c = this.data.remove(0);
            this.current = Cursor::new(c);
        }
        Poll::Ready(this.current.read(buf))
    }
}

impl async_std::io::BufRead for FixedBodyReader {
    fn poll_fill_buf(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        let this = self.get_mut();
        if this.current_empty() && !this.data.is_empty() {
            let c = this.data.remove(0);
            this.current = Cursor::new(c);
        }
        Poll::Ready(Ok(this.current_slice()))
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        let current_pos = this.current.position();
        this.current.set_position(current_pos + (amt as u64));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Result;
    use async_std::channel::unbounded;
    use async_std::io::prelude::BufReadExt;
    use futures::AsyncReadExt;

    #[async_std::test]
    async fn streaming_body_reader() -> Result<()> {
        let (tx, rx) = unbounded();
        let mut reader = StreamingBodyReader::new(rx);

        let handle = async_std::task::spawn::<_, Result<Vec<String>>>(async move {
            let mut lines = vec![];
            let mut line = String::new();
            let mut bytes_read = reader.read_line(&mut line).await?;
            while bytes_read > 0 {
                line.truncate(bytes_read);
                lines.push(std::mem::take(&mut line));
                bytes_read = reader.read_line(&mut line).await?;
            }
            Ok(lines)
        });
        tx.send("ABC".as_bytes().to_vec()).await?;
        tx.send("\nDEF\nGHIIII".as_bytes().to_vec()).await?;
        tx.close();
        let lines = handle.await?;
        assert_eq!(3, lines.len());
        assert_eq!("ABC\n".to_string(), lines[0]);
        assert_eq!("DEF\n".to_string(), lines[1]);
        assert_eq!("GHIIII".to_string(), lines[2]);

        Ok(())
    }

    #[async_std::test]
    async fn streaming_body_reader_empty() -> Result<()> {
        let (tx, rx) = unbounded();
        let mut reader = StreamingBodyReader::new(rx);
        tx.close();
        let mut buf = Vec::new();
        let bytes_read = reader.read_until(b'\n', &mut buf).await?;
        assert_eq!(0, bytes_read);
        assert_eq!(0, buf.len());
        Ok(())
    }

    #[async_std::test]
    async fn fixed_body_reader() -> Result<()> {
        let mut reader = FixedBodyReader::new(vec![b"snot".to_vec(), b"badger".to_vec()]);
        assert_eq!(10, reader.len());
        let mut buf = vec![0; 100];
        let bytes_read = reader.read(&mut buf).await?;
        assert_eq!(4, bytes_read);
        assert_eq!(b"snot", &buf[..bytes_read]);

        let bytes_read = reader.read(&mut buf).await?;
        assert_eq!(6, bytes_read);
        assert_eq!(b"badger", &buf[..bytes_read]);

        assert_eq!(0, reader.read(&mut buf).await?);

        Ok(())
    }

    #[async_std::test]
    async fn fixed_body_reader_empty() -> Result<()> {
        let mut reader = FixedBodyReader::new(vec![]);

        let mut buf = vec![0; 100];
        assert_eq!(0, reader.read(&mut buf).await?);

        let mut reader = FixedBodyReader::new(vec![b"".to_vec()]);
        assert_eq!(0, reader.read(&mut buf).await?);

        Ok(())
    }
}
