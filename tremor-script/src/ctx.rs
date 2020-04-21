// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a cstd::result::Result::Err(*right_val)::Result::Err(*right_val)License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::*;
use std::default;
use std::fmt;
use url::Url;

/// Event origin URI
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventOriginUri {
    /// schema part
    pub scheme: String,
    /// host part
    pub host: String,
    /// port part
    pub port: Option<u16>,
    /// path part
    pub path: Vec<String>,
    // implement query params if we find a good usecase for it
    //pub query: Hashmap<String, String>
}

impl EventOriginUri {
    /// parses a string into a URI
    pub fn parse(url: &str) -> Result<Self> {
        match Url::parse(url) {
            Ok(r) => {
                let host = r
                    .host_str()
                    // TODO add an error kind here
                    .ok_or_else(|| Error::from("EventOriginUri Parse Error: Missing host"))?;
                Ok(Self {
                    scheme: r.scheme().to_string(),
                    host: host.to_string(),
                    port: r.port(),
                    path: r
                        .path_segments()
                        .map_or_else(Vec::new, |segs| segs.map(String::from).collect()),
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    /// return the schema
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// return the host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// return the port
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// return the path
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Format as host and port
    pub fn host_port(&self) -> String {
        if let Some(port) = self.port() {
            format!("{}:{}", self.host(), port)
        } else {
            self.host().to_string()
        }
    }
}

impl fmt::Display for EventOriginUri {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.host)?;
        if let Some(port) = self.port {
            write!(f, ":{}", port)?;
        }
        let maybe_sep = if self.path.is_empty() { "" } else { "/" };
        write!(f, "{}{}", maybe_sep, self.path.join("/"))
    }
}

impl default::Default for EventOriginUri {
    fn default() -> Self {
        Self {
            scheme: "tremor-script".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: Vec::new(),
        }
    }
}

// TODO check if we need all of these derives here still

/// Context in that an event is executed
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct EventContext {
    at: u64,
    /// URI of the origin
    pub origin_uri: Option<EventOriginUri>,
}

impl EventContext {
    /// Creates a new context
    pub fn new(ingest_ns: u64, origin_uri: Option<EventOriginUri>) -> Self {
        Self {
            at: ingest_ns,
            origin_uri,
        }
    }

    /// returns the events `ingest_ns`
    pub fn ingest_ns(&self) -> u64 {
        self.at
    }

    /// returns the events origin uri
    pub fn origin_uri(&self) -> &Option<EventOriginUri> {
        &self.origin_uri
    }
}

#[cfg(test)]
mod tests {
    use super::{EventContext, EventOriginUri};

    #[test]
    fn valid_event_origin_uris() {
        // Base-line: scheme + hostname
        let eouri = EventOriginUri::parse("protocol://the.host.name").expect("Valid URI");
        assert_eq!(eouri.scheme(), "protocol");
        assert_eq!(eouri.host(), "the.host.name");
        assert_eq!(eouri.port(), None);
        assert_eq!(eouri.host_port().as_str(), "the.host.name");
        assert!(eouri.path().is_empty());
        assert_eq!(eouri.to_string(), "protocol://the.host.name");

        // IPv4 host
        let eouri = EventOriginUri::parse("protocol://192.168.1.1").expect("Valid URI");
        assert_eq!(eouri.scheme(), "protocol");
        assert_eq!(eouri.host(), "192.168.1.1");
        assert_eq!(eouri.port(), None);
        assert_eq!(eouri.host_port().as_str(), "192.168.1.1");
        assert!(eouri.path().is_empty());
        assert_eq!(eouri.to_string(), "protocol://192.168.1.1");

        // With port
        let eouri = EventOriginUri::parse("protocol://the.host.name:8080").expect("Valid URI");
        assert_eq!(eouri.scheme(), "protocol");
        assert_eq!(eouri.host(), "the.host.name");
        assert_eq!(eouri.port(), Some(8080));
        assert_eq!(eouri.host_port().as_str(), "the.host.name:8080");
        assert!(eouri.path().is_empty());
        assert_eq!(eouri.to_string(), "protocol://the.host.name:8080");

        // With terminating slash
        let eouri = EventOriginUri::parse("protocol://the.host.name/").expect("Valid URI");
        assert_eq!(eouri.scheme(), "protocol");
        assert_eq!(eouri.host(), "the.host.name");
        assert_eq!(eouri.port(), None);
        assert_eq!(eouri.host_port().as_str(), "the.host.name");
        assert_eq!(eouri.path(), &[""]);
        assert_eq!(eouri.to_string(), "protocol://the.host.name/");

        // With path
        let eouri = EventOriginUri::parse("protocol://the.host.name/some/path/segments")
            .expect("Valid URI");
        assert_eq!(eouri.scheme(), "protocol");
        assert_eq!(eouri.host(), "the.host.name");
        assert_eq!(eouri.port(), None);
        assert_eq!(eouri.host_port().as_str(), "the.host.name");
        assert_eq!(eouri.path(), &["some", "path", "segments"]);
        assert_eq!(
            eouri.to_string(),
            "protocol://the.host.name/some/path/segments"
        );

        // With path with terminating slash
        let eouri = EventOriginUri::parse("protocol://the.host.name/some/path/segments/")
            .expect("Valid URI");
        assert_eq!(eouri.scheme(), "protocol");
        assert_eq!(eouri.host(), "the.host.name");
        assert_eq!(eouri.port(), None);
        assert_eq!(eouri.host_port().as_str(), "the.host.name");
        assert_eq!(eouri.path(), &["some", "path", "segments", ""]);
        assert_eq!(
            eouri.to_string(),
            "protocol://the.host.name/some/path/segments/"
        );

        // Non-ASCII characters in host
        let eouri = EventOriginUri::parse("protocol://host.names.are.🔥").expect("Valid URI");
        assert_eq!(eouri.scheme(), "protocol");
        assert_eq!(eouri.host(), "host.names.are.%F0%9F%94%A5"); // 🔥 gets percent-encoded by `url`
        assert_eq!(eouri.port(), None);
        assert_eq!(eouri.host_port().as_str(), "host.names.are.%F0%9F%94%A5");
        assert!(eouri.path().is_empty());
        assert_eq!(eouri.to_string(), "protocol://host.names.are.%F0%9F%94%A5");

        // From Default
        let eouri = EventOriginUri::default();
        assert_eq!(eouri.scheme(), "tremor-script");
        assert_eq!(eouri.host(), "localhost");
        assert_eq!(eouri.port(), None);
        assert_eq!(eouri.host_port().as_str(), "localhost");
        assert!(eouri.path().is_empty());
        assert_eq!(eouri.to_string(), "tremor-script://localhost");
    }

    #[test]
    fn invalid_event_origin_uris() {
        // Wrong protocol/host-separator: extra slash
        let err = EventOriginUri::parse("protocol:///the.host.name").expect_err("Invalid URI");
        assert_eq!(
            err.description(),
            "EventOriginUri Parse Error: Missing host"
        );

        // Wrong protocol/host-separator: missing slash
        let err = EventOriginUri::parse("protocol:/the.host.name").expect_err("Invalid URI");
        assert_eq!(
            err.description(),
            "EventOriginUri Parse Error: Missing host"
        );

        // Port number out of range
        let err = EventOriginUri::parse("protocol://the.host.name:66000").expect_err("Invalid URI");
        assert_eq!(err.description(), "Url Parse Error: invalid port number");

        // Space inside the host name
        let err = EventOriginUri::parse("protocol://oops.a space").expect_err("Invalid URI");
        assert_eq!(
            err.description(),
            "Url Parse Error: invalid domain character"
        );
    }

    #[test]
    fn event_context() {
        // From Default
        let ctx = EventContext::default();
        assert_eq!(ctx.ingest_ns(), 0);
        assert_eq!(ctx.origin_uri(), &None, "Default has no origin URI");
    }
}
