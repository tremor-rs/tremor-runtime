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

/// Metrics facilities
pub(crate) mod metrics;

/// Quiescence support facilities
pub(crate) mod quiescence;

/// Reconnection facilities
pub(crate) mod reconnect;

/// Transport Level Security facilities
pub(crate) mod tls;

/// MIME encoding utilities
pub(crate) mod mime;

/// Protocol Buffer utilities
pub(crate) mod pb;

pub(crate) mod url {

    use crate::errors::Result;
    use serde::{Deserialize, Serialize};

    /// Endpoint URL
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Url(url::Url);

    impl std::fmt::Display for Url {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Default for Url {
        fn default() -> Self {
            // ALLOW: this is a known safe url, we have a test for it
            Self(url::Url::parse("http://localhost").unwrap())
        }
    }

    impl Url {
        pub fn parse(input: &str) -> Result<Self> {
            Ok(Self(url::Url::parse(input)?))
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;
        #[test]
        fn default() {
            Url::default();
        }
    }
}
