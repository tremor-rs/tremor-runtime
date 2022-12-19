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

pub(crate) mod consumer;
pub(crate) mod producer;
use crate::connectors::prelude::*;

#[allow(clippy::unwrap_used)]
fn default_endpoint() -> Url<HttpsDefaults> {
    // ALLOW: this URL is hardcoded, so the only reason for parse failing would be if it was changed
    Url::parse("https://pubsub.googleapis.com").unwrap()
}
fn default_connect_timeout() -> u64 {
    1_000_000_000u64 // 1 second
}
fn default_request_timeout() -> u64 {
    10_000_000_000u64 // 10 seconds
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    pub fn default_endpoint_does_not_panic() {
        // This test will fail if this panics (it should never)
        default_endpoint();
    }

    #[test]
    pub fn default_connect_timeout_is_1s() {
        let actual = Duration::from_nanos(default_connect_timeout());

        assert_eq!(actual, Duration::from_secs(1));
    }
}
