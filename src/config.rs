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

use crate::url::TremorURL;
use hashbrown::HashMap;
use tremor_pipeline::config as dynaconfig;

pub(crate) type ID = String;
pub(crate) type OnRampVec = Vec<OnRamp>;
pub(crate) type OffRampVec = Vec<OffRamp>;
pub(crate) type PipelineVec = Vec<dynaconfig::Pipeline>;
pub(crate) type BindingVec = Vec<Binding>;
pub(crate) type BindingMap = HashMap<TremorURL, Vec<TremorURL>>;
pub(crate) type MappingMap = HashMap<TremorURL, HashMap<String, String>>;

/// A full tremopr config
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "Default::default")]
    pub(crate) onramp: OnRampVec,
    #[serde(default = "Default::default")]
    pub(crate) offramp: OffRampVec,
    #[serde(default = "Default::default")]
    pub(crate) binding: Vec<Binding>,
    #[serde(default = "Default::default")]
    pub(crate) pipeline: PipelineVec,
    #[serde(default = "Default::default")]
    pub(crate) mapping: MappingMap,
}

/// Configuration for an onramp
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnRamp {
    /// ID of the onramp
    pub id: ID,
    #[serde(rename = "type")]
    pub(crate) binding_type: String,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) preprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: dynaconfig::ConfigMap,
}

/// Configuration of an offramp
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OffRamp {
    /// ID of the offramp
    pub id: ID,
    #[serde(rename = "type")]
    pub(crate) binding_type: String,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) postprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: dynaconfig::ConfigMap,
}

/// Configuration for a Binding
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// ID of the binding
    pub id: ID,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    pub(crate) links: BindingMap, // is this right? this should be url to url?
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_yaml;
    use std::fs::File;
    use std::io::BufReader;
    fn slurp(file: &str) -> Config {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).expect("could parse file")
    }

    #[test]
    fn load() {
        let c = slurp("tests/configs/config.yaml");
        assert_eq!(&c.offramp[0].id, "blackhole");
        assert_eq!(&c.pipeline[0].id, "main");
    }
}
