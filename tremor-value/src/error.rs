// Copyright 2020-2021, The Tremor Team
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

/// Result type
pub type Result<T> = std::result::Result<T, Error>;
use std::fmt;

use fmt::Display;
#[derive(Debug)]
/// A Error
pub enum Error {
    /// A map was expected but some other value was found
    ExpectedMap,
    /// A generic serde error
    Serde(String),
    /// A SIMD Json error
    SimdJson(simd_json::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ExpectedMap => write!(f, "Expected a struct, but did not find out"),
            Error::Serde(s) => f.write_str(&s),
            Error::SimdJson(e) => write!(f, "SIMD JSON error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(not(tarpaulin_include))] // this is a simple error
impl serde_ext::de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Serde(msg.to_string())
    }
}

#[cfg(not(tarpaulin_include))] // this is a simple error
impl serde_ext::ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Serde(msg.to_string())
    }
}
