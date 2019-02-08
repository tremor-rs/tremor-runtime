// Copyright 2018-2019, Wayfair GmbH
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

use crate::dflt;
use serde_yaml;
use serde_yaml::Mapping;
use std::collections::HashMap;
use url;

pub type ID = String;
pub type Port = TremorURL;
pub type InputPort = String;
pub type OutputPort = String;

pub type RampSetVec = Vec<RampSet>;
pub type PipelineVec = Vec<Pipeline>;
pub type BindingVec = Vec<Binding>;
pub type LinkMap = HashMap<Port, Port>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "dflt::d_vec")]
    pub ramps: Vec<RampSet>,
    #[serde(default = "dflt::d_vec")]
    pub bindings: Vec<Binding>,
    #[serde(default = "dflt::d_vec")]
    pub pipelines: Vec<Pipeline>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ramp {
    #[serde(rename = "type")]
    pub binding_type: String,
    pub id: ID,
    pub config: Mapping,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RampSet {
    pub id: ID,
    pub description: String,
    pub onramps: Vec<Ramp>,
    pub offramps: Vec<Ramp>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Binding {
    pub id: ID,
    #[serde(default = "dflt::d_empty")]
    pub description: String,
    pub links: LinkMap,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Interfaces {
    #[serde(default = "dflt::d_vec")]
    pub inputs: Vec<InputPort>,
    #[serde(default = "dflt::d_vec")]
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: ID,
    #[serde(rename = "type")]
    pub node_type: String,
    pub config: Option<Mapping>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: ID,
    pub interface: Interfaces,
    #[serde(default = "dflt::d_vec")]
    pub nodes: Vec<Node>,
    #[serde(default = "dflt::d_hashmap")]
    pub links: HashMap<Port, Vec<Port>>,
    #[serde(default = "dflt::d_hashmap")]
    pub imports: HashMap<InputPort, Port>,
    #[serde(default = "dflt::d_hashmap")]
    pub exports: HashMap<OutputPort, Vec<Port>>,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct TremorURL {
    pub url: url::Url,
    pub relative: bool,
    pub original: String,
}

impl TremorURL {
    pub fn parse(url: &str) -> Result<Self, url::ParseError> {
        let mut r = Self::parse_url(url, false)?;
        r.original = String::from(url);
        Ok(r)
    }
    fn parse_url(url: &str, relative: bool) -> Result<Self, url::ParseError> {
        match url::Url::parse(url) {
            Ok(r) => Ok(Self {
                url: r,
                relative,
                original: "".into(),
            }),
            Err(url::ParseError::RelativeUrlWithoutBase) => match url.get(0..1) {
                Some("/") => Self::parse_url(&format!("tremor://{}", url), false),
                _ => Self::parse_url(&format!("tremor:///{}", url), true),
            },
            Err(e) => Err(e),
        }
    }
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }
    pub fn username(&self) -> &str {
        self.url.username()
    }
    pub fn password(&self) -> Option<&str> {
        self.url.password()
    }
    pub fn host_str(&self) -> Option<&str> {
        self.url.host_str()
    }
    pub fn port(&self) -> Option<u16> {
        self.url.port()
    }
    pub fn path(&self) -> &str {
        self.url.path()
    }
    pub fn relative(&self) -> bool {
        self.relative
    }
}

use serde::{Deserialize, Deserializer, Serialize, Serializer};

impl<'de> Deserialize<'de> for TremorURL {
    fn deserialize<D>(deserializer: D) -> Result<TremorURL, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        TremorURL::parse(&s).map_err(|err| Error::custom(err.to_string()))
    }
}

impl Serialize for TremorURL {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.original)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_yaml;
    use std::fs::File;
    use std::io::BufReader;
    #[test]
    fn load() {
        let file = File::open("tests/config.yaml").expect("could not open file");
        let buffered_reader = BufReader::new(file);
        let c: Config = serde_yaml::from_reader(buffered_reader).unwrap();
        println!("c: {:?}", c);
        assert_eq!(&c.ramps[0].id, "logging");
        assert_eq!(&c.pipelines[0].id, "main");
    }

    #[test]
    fn url() {
        let tremor_url =
            TremorURL::parse("tremor://127.0.0.1:1234/pipelines/main/in?format=json").unwrap();

        assert_eq!(tremor_url.relative(), false);
        assert_eq!(tremor_url.scheme(), "tremor");
        assert_eq!(tremor_url.username(), "");
        assert_eq!(tremor_url.password(), None);
        assert_eq!(tremor_url.host_str(), Some("127.0.0.1"));
        assert_eq!(tremor_url.port(), Some(1234));
        assert_eq!(tremor_url.path(), "/pipelines/main/in");
    }

    #[test]
    fn short_url() {
        let tremor_url = TremorURL::parse("/pipelines/main/in").unwrap();

        assert_eq!(tremor_url.relative(), false);
        assert_eq!(tremor_url.scheme(), "tremor");
        assert_eq!(tremor_url.username(), "");
        assert_eq!(tremor_url.password(), None);
        assert_eq!(tremor_url.host_str(), Some(""));
        assert_eq!(tremor_url.port(), None);
        assert_eq!(tremor_url.path(), "/pipelines/main/in");
    }

    #[test]
    fn relative_short_url() {
        let tremor_url = TremorURL::parse("pipelines/main/in").unwrap();

        assert_eq!(tremor_url.relative(), true);
        assert_eq!(tremor_url.scheme(), "tremor");
        assert_eq!(tremor_url.username(), "");
        assert_eq!(tremor_url.password(), None);
        assert_eq!(tremor_url.host_str(), Some(""));
        assert_eq!(tremor_url.port(), None);
        assert_eq!(tremor_url.path(), "/pipelines/main/in");
    }

}
