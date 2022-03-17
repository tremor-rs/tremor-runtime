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

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
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

pub(crate) mod url {

    use crate::errors::Result;
    use serde::{Deserialize, Serialize};
    use std::marker::PhantomData;

    pub(crate) trait Defaults {
        /// Default url
        const DEFAULT: &'static str;
        const PORT: u16;
    }

    // Default HTTP
    pub(crate) struct HttpDefaults;
    impl Defaults for HttpDefaults {
        const DEFAULT: &'static str = "http://localhost";
        const PORT: u16 = 80;
    }
    pub(crate) struct HttpsDefaults;
    impl Defaults for HttpsDefaults {
        const DEFAULT: &'static str = "https://localhost";
        const PORT: u16 = 443;
    }
    /// Endpoint URL
    #[derive(Serialize)]
    pub(crate) struct Url<D: Defaults = HttpDefaults> {
        url: url::Url,
        #[serde(skip)]
        _marker: PhantomData<D>,
    }

    // We have a custom deserializer since we want to not have it nested as\
    // ```json
    // {"url": "http..."}
    // ```
    impl<'de, Dflt: Defaults> Deserialize<'de> for Url<Dflt> {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Ok(Self {
                url: url::Url::deserialize(deserializer)?,
                _marker: PhantomData::default(),
            })
        }
    }

    impl<D: Defaults, T> AsRef<T> for Url<D>
    where
        url::Url: AsRef<T>,
    {
        fn as_ref(&self) -> &T {
            self.url.as_ref()
        }
    }

    impl<D: Defaults, T> PartialEq<T> for Url<D>
    where
        url::Url: PartialEq<T>,
    {
        fn eq(&self, other: &T) -> bool {
            self.url == *other
        }
    }

    impl<D: Defaults> std::ops::Deref for Url<D> {
        type Target = url::Url;

        fn deref(&self) -> &Self::Target {
            &self.url
        }
    }

    impl<D: Defaults> Clone for Url<D> {
        fn clone(&self) -> Self {
            Self {
                url: self.url.clone(),
                _marker: PhantomData::default(),
            }
        }
    }

    impl<D: Defaults> std::fmt::Debug for Url<D> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Url").field("url", &self.url).finish()
        }
    }

    impl std::fmt::Display for Url {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.url)
        }
    }

    impl<D: Defaults> Default for Url<D> {
        fn default() -> Self {
            Self {
                // ALLOW: this is a known safe url, we have a test for it
                url: url::Url::parse(D::DEFAULT).unwrap(),
                _marker: PhantomData::default(),
            }
        }
    }

    impl<D: Defaults> Url<D> {
        pub(crate) fn parse(input: &str) -> Result<Self> {
            Ok(Self {
                url: url::Url::parse(input)?,
                ..Self::default()
            })
        }
        pub(crate) fn port_or_dflt(&self) -> u16 {
            self.url.port().unwrap_or(D::PORT)
        }
        pub(crate) fn host_or_local(&self) -> &str {
            self.url.host_str().unwrap_or("localhost")
        }

        pub(crate) fn url(&self) -> &url::Url {
            &self.url
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;
        #[test]
        fn default() {
            Url::<HttpDefaults>::default();
            Url::<HttpsDefaults>::default();
        }
    }
}
