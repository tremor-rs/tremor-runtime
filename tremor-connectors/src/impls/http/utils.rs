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

use either::Either;
use futures::Stream;
use hyper::body::Bytes;
use std::pin::Pin;
use std::task::Poll;

/// A body of an HTTP request or response.
pub struct Body {
    pub(crate) data: Pin<Box<dyn Stream<Item = Result<Vec<u8>, std::io::Error>> + Send>>,
}
unsafe impl Send for Body {}
unsafe impl Sync for Body {}

impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Body").finish()
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::empty()
    }
}

impl Body {
    #[must_use]
    /// Create a new body
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Box::pin(futures::stream::once(async move { Ok(data) })),
        }
    }

    #[must_use]
    /// Create an empty body
    pub fn empty() -> Self {
        Self {
            data: Box::pin(futures::stream::empty()),
        }
    }

    pub(crate) fn wrap_stream(
        stream: impl Stream<Item = Result<Vec<u8>, std::io::Error>> + Send + 'static,
    ) -> Self {
        Self {
            data: Box::pin(stream),
        }
    }

    // #[cfg(test)]
    /// Create a new body from a string.
    /// This is a test-only function.
    /// It is not recommended to use this function in production code.
    /// Use `Body::new` instead.
    /// # Errors
    /// Returns an error if the string cannot be converted to bytes.
    pub async fn to_bytes(mut self) -> Result<Vec<u8>, std::io::Error> {
        use futures::StreamExt;
        let mut bytes_vec = Vec::new();

        let mut data = self.data.as_mut();
        // While there are items in the stream, append them to the bytes_vec.
        while let Some(chunk) = data.next().await {
            match chunk {
                Ok(bytes) => {
                    bytes_vec.extend_from_slice(&bytes);
                }
                Err(e) => return Err(e), // Return early with the error.
            }
        }

        Ok(bytes_vec)
    }
}

impl http_body::Body for Body {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        this.data
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map(|data| http_body::Frame::data(data.into()))))
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(transparent)]
pub(crate) struct Header(
    #[serde(with = "either::serde_untagged")] pub(crate) Either<Vec<String>, String>,
);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RequestId(u64);

impl RequestId {
    pub(crate) fn new(id: u64) -> Self {
        Self(id)
    }
    pub(crate) fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
