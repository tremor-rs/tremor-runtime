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
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

// TODO add tests for this struct
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
                    path: r.path().split('/').map(String::from).collect(),
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
    pub fn port(&self) -> &Option<u16> {
        &self.port
    }

    /// return the path
    pub fn path(&self) -> &Vec<String> {
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
        let r = write!(f, "{}://{}", self.scheme, self.host);
        if let Some(port) = self.port {
            write!(f, ":{}", port)?;
        }
        write!(f, "/{}", self.path.join("/"))?;
        r
    }
}

impl default::Default for EventOriginUri {
    fn default() -> Self {
        Self {
            scheme: "tremor-script".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: vec![String::default()],
        }
    }
}

// TODO check if we need all of these derives here still

/// Context in that an event is executed
#[derive(Debug, Default, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize)]
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
