// Copyright 2018-2020, Wayfair GmbH
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

use crate::common_cow;
use crate::op::prelude::{IN, OUT};
use indexmap::IndexMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;

pub(crate) type ID = String;
pub(crate) type PipelineInputPort = String;
pub(crate) type PipelineOutputPort = String;
//pub(crate) type PipelineVec = Vec<Pipeline>;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct InputPort {
    pub id: Cow<'static, str>,
    pub port: Cow<'static, str>,
    pub had_port: bool,
}

impl<'de> Deserialize<'de> for InputPort {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let v: Vec<&str> = s.split('/').collect();
        match v.as_slice() {
            [id, port] => Ok(Self {
                id: common_cow(id),
                port: common_cow(port),
                had_port: true,
            }),
            [id] => Ok(Self {
                id: common_cow(id),
                port: IN,
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
pub(crate) struct OutputPort {
    pub id: Cow<'static, str>,
    pub port: Cow<'static, str>,
    pub had_port: bool,
}

impl<'de> Deserialize<'de> for OutputPort {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let v: Vec<&str> = s.split('/').collect();
        match v.as_slice() {
            [id, port] => Ok(Self {
                id: common_cow(id),
                port: common_cow(port),
                had_port: true,
            }),
            [id] => Ok(Self {
                id: common_cow(id),
                port: OUT,
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

pub(crate) fn dflt<T: Default>() -> T {
    T::default()
}

// pub(crate) type LinkMap = IndexMap<String, String>;
#[allow(clippy::module_name_repetitions)]
/// A configuration map
pub type ConfigMap = Option<serde_yaml::Value>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Interfaces {
    #[serde(default = "dflt")]
    pub inputs: Vec<PipelineInputPort>,
    #[serde(default = "dflt")]
    pub outputs: Vec<PipelineOutputPort>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Node {
    pub id: ID,
    #[serde(default = "dflt")]
    pub description: String,
    #[serde(rename = "op")]
    pub node_type: String,
    pub config: ConfigMap,
}

/// The configuration for a pipeline
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    /// ID of the pipeline
    pub id: ID,
    pub(crate) interface: Interfaces,
    #[serde(default = "dflt")]
    pub(crate) description: String,
    #[serde(default = "dflt")]
    pub(crate) nodes: Vec<Node>,
    #[serde(default = "dflt")]
    pub(crate) links: IndexMap<OutputPort, Vec<InputPort>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
}

#[cfg(test)]
mod test {
    // TOOD
}
