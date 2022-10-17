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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

/// Metrics facilities
pub(crate) mod metrics;
/// MIME encoding utilities
pub(crate) mod mime;
/// Protocol Buffer utilities
pub(crate) mod pb;
/// Quiescence support facilities
pub(crate) mod quiescence;
/// Reconnection facilities
pub(crate) mod reconnect;
/// Socket utilities
pub(crate) mod socket;
/// Transport Level Security facilities
pub(crate) mod tls;
/// URL untils
pub(crate) mod url;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct ConnectionMeta {
    pub(crate) host: String,
    pub(crate) port: u16,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub(crate) struct ConnectionMetaWithHandle {
    pub(crate) handle: String,
    pub(crate) meta: ConnectionMeta,
}

impl From<SocketAddr> for ConnectionMeta {
    fn from(sa: SocketAddr) -> Self {
        Self {
            host: sa.ip().to_string(),
            port: sa.port(),
        }
    }
}

/// Keeps track of process env manipulations and restores previous values upon drop
#[allow(unused)]
pub(crate) struct EnvHelper {
    restore: HashMap<String, String>,
    remove: HashSet<String>,
}

impl EnvHelper {
    #[allow(unused)]
    pub(crate) fn new() -> Self {
        Self {
            restore: HashMap::new(),
            remove: HashSet::new(),
        }
    }

    #[allow(unused)]
    pub(crate) fn set_var(&mut self, key: &str, value: &str) {
        if let Ok(old_value) = std::env::var(key) {
            self.restore.insert(key.to_string(), old_value);
        } else {
            self.remove.insert(key.to_string());
        }
        std::env::set_var(key, value);
    }

    #[allow(unused)]
    pub(crate) fn remove_var(&mut self, key: &str) {
        if let Ok(old_value) = std::env::var(key) {
            self.restore.insert(key.to_string(), old_value);
        }
        std::env::remove_var(key);
    }
}

impl Drop for EnvHelper {
    fn drop(&mut self) {
        for (k, v) in &self.restore {
            std::env::set_var(k, v);
        }
        for k in &self.remove {
            std::env::remove_var(k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EnvHelper;

    #[test]
    fn env_helper() {
        let mut env_helper = EnvHelper::new();
        env_helper.set_var("snot", "badger");
        env_helper.remove_var("HOME");
        assert_eq!(Some("badger".to_string()), std::env::var("snot").ok());

        env_helper.set_var("snot", "meh");

        assert_eq!(Some("meh".to_string()), std::env::var("snot").ok());

        assert!(std::env::var("HOME").is_err());
        drop(env_helper);

        // restored/removed
        assert!(std::env::var("snot").is_err());
        assert!(std::env::var("HOME").is_ok());
    }
}
