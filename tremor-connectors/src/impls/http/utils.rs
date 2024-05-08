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
use http_body_util::Full;
use hyper::body::Bytes;

/// We re-use Full from `http_body_util` crate which is a wrapper around
/// `hyper::body::Bytes` and implements `http_body::Body` trait. This
/// avoids introducing our own Body type
pub type Body = Full<Bytes>;

/// An empty body
#[must_use]
pub fn empty_body() -> Body {
    Full::new(Bytes::new())
}

/// Create a new body from bytes
#[must_use]
pub fn body_from_bytes(bytes: Vec<u8>) -> Body {
    Full::new(Bytes::from(bytes))
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
