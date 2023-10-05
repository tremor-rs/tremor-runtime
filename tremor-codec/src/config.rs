// Copyright 2020-2021, The Tremor Team
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

use crate::errors::Result;
use serde::Deserialize;
use tremor_value::prelude::*;

#[derive(Clone, Debug, Default)]
#[allow(clippy::module_name_repetitions)]
pub struct NameWithConfig {
    pub name: String,
    pub config: Option<Value<'static>>,
}

impl<'v> serde::Deserialize<'v> for NameWithConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'v>,
    {
        // This makes little sense for some reason having three tuple variants is required
        // where two with optional config should be enough.
        // They are in the tests but here they are not and we couldn't figure out why :.(
        #[derive(Deserialize, Debug)]
        #[serde(bound(deserialize = "'de: 'v, 'v: 'de"), untagged)]
        enum Variants<'v> {
            // json: "json"
            Name(String),
            // json: { "name": "json", "config": { ... } }
            NameAndConfig { name: String, config: Value<'v> },
            // json: { "name": "json" }
            NameAndNoConfig { name: String },
        }

        let var = Variants::deserialize(deserializer)?;

        match var {
            Variants::NameAndConfig { name, config } => Ok(NameWithConfig {
                name,
                config: Some(config.into_static()),
            }),
            Variants::NameAndNoConfig { name } | Variants::Name(name) => {
                Ok(NameWithConfig { name, config: None })
            }
        }
    }
}

impl<'v> TryFrom<&Value<'v>> for NameWithConfig {
    type Error = crate::errors::Error;

    fn try_from(value: &Value) -> Result<Self> {
        if let Some(name) = value.as_str() {
            Ok(Self::from(name))
        } else if let Some(name) = value.get_str("name") {
            Ok(Self {
                name: name.to_string(),
                config: value.get("config").map(Value::clone_static),
            })
        } else {
            Err(format!("Invalid codec: {value}").into())
        }
    }
}

impl From<&str> for NameWithConfig {
    fn from(name: &str) -> Self {
        Self {
            name: name.to_string(),
            config: None,
        }
    }
}
impl From<&String> for NameWithConfig {
    fn from(name: &String) -> Self {
        name.clone().into()
    }
}
impl From<String> for NameWithConfig {
    fn from(name: String) -> Self {
        Self { name, config: None }
    }
}

/// A Codec
pub type Codec = NameWithConfig;
/// A Preprocessor
pub type Preprocessor = NameWithConfig;
/// A Postprocessor
pub type Postprocessor = NameWithConfig;

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    #[test]
    fn name_with_config() {
        let v = literal!({"name": "json", "config": {"mode": "sorted"}});
        let nac = NameWithConfig::deserialize(v).expect("could structurize two element struct");
        assert_eq!(nac.name, "json");
        assert!(nac.config.as_object().is_some());
        let v = literal!({"name": "yaml"});
        let nac = NameWithConfig::deserialize(v).expect("could structurize one element struct");
        assert_eq!(nac.name, "yaml");
        assert_eq!(nac.config, None);
        let v = literal!("name");
        let nac = NameWithConfig::deserialize(v).expect("could structurize string");
        assert_eq!(nac.name, "name");
        assert_eq!(nac.config, None);
    }

    #[test]
    fn name_with_config_in_a_hatemap() {
        let codec = "json";
        let data = literal!( {
            "application/json": {"name": "json", "config": {"mode": "sorted"}},
            "application/yaml": {"name": "yaml"},
            "*/*": codec,
        });
        let nac = HashMap::<String, NameWithConfig>::deserialize(data)
            .expect("could structurize two element struct");

        assert_eq!(nac.len(), 3);
    }

    #[test]
    fn name_with_config_in_a_hatemap_in_struct() {
        #[derive(Deserialize, Debug, Clone)]
        #[serde(deny_unknown_fields)]
        struct Config {
            mime_mapping: Option<HashMap<String, NameWithConfig>>,
        }
        let codec = "json";
        let data = literal!({ "mime_mapping": {
            "application/json": {"name": "json", "config": {"mode": "sorted"}},
            "application/yaml": {"name": "yaml"},
            "*/*": codec,
        }});
        let nac = Config::deserialize(data).expect("could structurize two element struct");

        assert_eq!(nac.mime_mapping.map(|h| h.len()).unwrap_or_default(), 3);
    }
}
