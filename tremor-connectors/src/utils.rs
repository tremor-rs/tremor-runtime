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
pub(crate) mod metrics;
/// MIME encoding utilities
pub(crate) mod mime;
/// Some common things for object storage connectors like gcs and s3
pub(crate) mod object_storage;
/// Protocol Buffer utilities
pub(crate) mod pb;
/// Quiescence support facilities
pub(crate) mod quiescence;
/// Reconnection facilities
pub(crate) mod reconnect;
/// Socket utilities
pub(crate) mod socket;
/// Transport Level Security facilities
pub mod tls;

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

#[must_use]
pub(crate) fn task_id() -> String {
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
