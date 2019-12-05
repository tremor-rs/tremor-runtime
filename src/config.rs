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

use crate::dflt::dflt;
use crate::url::TremorURL;
use hashbrown::HashMap;
use tremor_pipeline::config as dynaconfig;

pub type ID = String;

pub type OnRampVec = Vec<OnRamp>;
pub type OffRampVec = Vec<OffRamp>;
pub type PipelineVec = Vec<dynaconfig::Pipeline>;
pub type BindingVec = Vec<Binding>;
pub type BindingMap = HashMap<TremorURL, Vec<TremorURL>>;
pub type MappingMap = HashMap<TremorURL, HashMap<String, String>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "dflt")]
    pub onramp: OnRampVec,
    #[serde(default = "dflt")]
    pub offramp: OffRampVec,
    #[serde(default = "dflt")]
    pub binding: Vec<Binding>,
    #[serde(default = "dflt")]
    pub pipeline: PipelineVec,
    #[serde(default = "dflt")]
    pub mapping: MappingMap,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnRamp {
    #[serde(rename = "type")]
    pub binding_type: String,
    pub id: ID,
    #[serde(default = "dflt")]
    pub description: String,
    #[serde(default = "dflt", skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(default = "dflt", skip_serializing_if = "Option::is_none")]
    pub preprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: dynaconfig::ConfigMap,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OffRamp {
    #[serde(rename = "type")]
    pub binding_type: String,
    pub id: ID,
    #[serde(default = "dflt")]
    pub description: String,
    #[serde(default = "dflt", skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(default = "dflt", skip_serializing_if = "Option::is_none")]
    pub postprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: dynaconfig::ConfigMap,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub id: ID,
    #[serde(default = "dflt")]
    pub description: String,
    pub links: BindingMap, // is this right? this should be url to url?
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
