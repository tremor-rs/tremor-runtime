// Copyright 2018-2019, Wayfair GmbH
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

// TODO add Copy here?
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventOriginUri {
    pub scheme: String,
    pub host: String,
    pub port: Option<u16>,
    pub path: Vec<String>,
    // implement query params if we find a good usecase for it
    //pub query: Hashmap<String, String>
}

// TODO add tests for this struct
impl EventOriginUri {
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

    pub fn scheme(&self) -> &str {
        &self.scheme
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn port(&self) -> &Option<u16> {
        &self.port
    }
    pub fn path(&self) -> &Vec<String> {
        &self.path
    }

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
#[derive(Debug, Default, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize)]
pub struct EventContext {
    pub at: u64,
    // TODO make this non-optional?
    pub origin_uri: Option<EventOriginUri>,
}

impl EventContext {
    pub fn new(ingest_ns: u64, origin_uri: Option<EventOriginUri>) -> Self {
        Self {
            at: ingest_ns,
            origin_uri,
        }
    }

    pub fn ingest_ns(&self) -> u64 {
        self.at
    }

    pub fn origin_uri(&self) -> &Option<EventOriginUri> {
        &self.origin_uri
    }
}
