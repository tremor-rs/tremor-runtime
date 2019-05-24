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

//use crate::url::TremorURL;
use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_yaml;

pub type ID = String;
pub type PipelineInputPort = String;
pub type PipelineOutputPort = String;
pub type PipelineVec = Vec<Pipeline>;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct InputPort {
    pub id: String,
    pub port: String,
    had_port: bool,
}

impl<'de> Deserialize<'de> for InputPort {
    fn deserialize<D>(deserializer: D) -> Result<InputPort, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let v: Vec<&str> = s.split('/').collect();
        match v.as_slice() {
            [id, port] => Ok(InputPort {
                id: id.to_string(),
                port: port.to_string(),
                had_port: true,
            }),
            [id] => Ok(InputPort {
                id: id.to_string(),
                port: "in".to_string(),
                had_port: false,
            }),
            _ => Err(Error::custom(
                "Bad port syntax, needs to be <id>/<port> or <id> (where port becomes 'in')",
            )),
        }
    }
}

impl Serialize for InputPort {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.had_port {
            serializer.serialize_str(&format!("{}/{}", self.id, self.port))
        } else {
            serializer.serialize_str(&self.id)
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct OutputPort {
    pub id: String,
    pub port: String,
    had_port: bool,
}

impl<'de> Deserialize<'de> for OutputPort {
    fn deserialize<D>(deserializer: D) -> Result<OutputPort, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let v: Vec<&str> = s.split('/').collect();
        match v.as_slice() {
            [id, port] => Ok(OutputPort {
                id: id.to_string(),
                port: port.to_string(),
                had_port: true,
            }),
            [id] => Ok(OutputPort {
                id: id.to_string(),
                port: "out".to_string(),
                had_port: false,
            }),
            _ => Err(Error::custom(
                "Bad port syntax, needs to be <id>/<port> or <id> (where port becomes 'out')",
            )),
        }
    }
}

impl Serialize for OutputPort {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.had_port {
            serializer.serialize_str(&format!("{}/{}", self.id, self.port))
        } else {
            serializer.serialize_str(&self.id)
        }
    }
}

pub fn dflt<T: Default>() -> T {
    T::default()
}

pub type LinkMap = IndexMap<String, String>;
pub type ConfigMap = Option<serde_yaml::Value>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interfaces {
    #[serde(default = "dflt")]
    pub inputs: Vec<PipelineInputPort>,
    #[serde(default = "dflt")]
    pub outputs: Vec<PipelineOutputPort>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub id: ID,
    #[serde(default = "dflt")]
    pub description: String,
    #[serde(rename = "op")]
    pub node_type: String,
    pub config: ConfigMap,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    pub id: ID,
    pub interface: Interfaces,
    #[serde(default = "dflt")]
    pub description: String,
    #[serde(default = "dflt")]
    pub nodes: Vec<Node>,
    #[serde(default = "dflt")]
    pub links: IndexMap<OutputPort, Vec<InputPort>>,
    // #[serde(default = "d_idxmap")]
    // pub imports: IndexMap<InputPort, String>,
    // #[serde(default = "d_idxmap")]
    // pub exports: IndexMap<OutputPort, Vec<String>>,
}

#[cfg(test)]
mod test {
    // TOOD
}
