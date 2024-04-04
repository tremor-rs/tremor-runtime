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

use std::net::SocketAddr;

/// Metrics facilities
pub mod metrics;
/// MIME encoding utilities
#[cfg(feature = "mime")]
pub(crate) mod mime;
/// Some common things for object storage connectors like gcs and s3
#[cfg(any(feature = "aws", feature = "gcp"))]
pub mod object_storage;
/// Protocol Buffer utilities
#[cfg(feature = "protobuf")]
pub mod pb;
/// Quiescence support facilities
pub mod quiescence;
/// Reconnection facilities
pub mod reconnect;
/// Socket utilities
#[cfg(feature = "socket")]
pub mod socket;
/// Transport Level Security facilities
#[cfg(feature = "tls")]
pub mod tls;

/// Integration test utils
#[cfg(any(test, feature = "integration-utils"))]
pub mod integration;

/// google  utilities
#[cfg(feature = "gcp")]
pub(crate) mod google;
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ConnectionMeta {
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl From<SocketAddr> for ConnectionMeta {
    fn from(sa: SocketAddr) -> Self {
        Self {
            host: sa.ip().to_string(),
            port: sa.port(),
        }
    }
}

/// Fetches the current task id or not - helper for unstable tokio
#[must_use]
pub fn task_id() -> String {
    // tokio::task::try_id().map_or_else(|| String::from("<no-task>"), |i| i.to_string())
    String::from("<no-task>")
}

/// Fetches a hostname with `tremor-host.local` being the default
#[must_use]
pub fn hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|hostname| hostname.into_string().ok())
        .unwrap_or_else(|| "tremor_host.local".to_string())
}
