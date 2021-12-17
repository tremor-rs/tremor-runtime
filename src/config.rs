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

use crate::connectors::ConnectorType;
use crate::Result;
use halfbrown::HashMap;
use simd_json::ValueType;
use tremor_pipeline::FN_REGISTRY;
use tremor_script::{
    ast::{ConnectStmt, Helper},
    srs::ConnectorDefinition,
};
use tremor_value::prelude::*;

pub(crate) type Id = String;

/// possible reconnect strategies for controlling if and how to reconnect
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum Reconnect {
    /// do not reconnect
    None,
    // TODO: RandomizedBackoff
    /// custom reconnect strategy
    Custom {
        /// start interval to wait after a failing connect attempt
        interval_ms: u64,
        /// growth rate for consecutive connect attempts, will be added to interval_ms
        #[serde(default = "default_growth_rate")]
        growth_rate: f64,
        //TODO: randomized: bool
        /// maximum number of retries to execute
        #[serde(default = "Default::default")]
        max_retries: Option<u64>,
    },
}

fn default_growth_rate() -> f64 {
    1.2
}

impl Default for Reconnect {
    fn default() -> Self {
        Self::None
    }
}

/* TODO: currently this is implemented differently in every connector

/// how a connector behaves upon Pause or CB trigger events
/// w.r.t maintaining its connection to the outside world (e.g. TCP connection, database connection)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PauseBehaviour {
    /// close the connection
    Close,
    /// does not support Pause and throws an error if it is attempted
    Error,
    /// keep the connection open, this will possibly fill OS buffers and lead to sneaky errors once they run full
    KeepOpen,
}

impl Default for PauseBehaviour {
    fn default() -> Self {
        Self::KeepOpen
    }
}
*/

/// Codec name and configuration
#[derive(Clone, Debug, Default)]
pub struct NameWithConfig {
    pub(crate) name: String,
    pub(crate) config: Option<Value<'static>>,
}

impl NameWithConfig {
    fn from_value(value: &Value) -> Result<Self> {
        if let Some(name) = value.as_str() {
            Ok(Self::from(name))
        } else if let Some(name) = value.get_str("name") {
            Ok(Self {
                name: name.to_string(),
                config: value.get("config").map(Value::clone_static),
            })
        } else {
            Err(format!("Invalid codec: {}", value).into())
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

/// A Codec
pub type Codec = NameWithConfig;
/// A Preprocessor
pub(crate) type Preprocessor = NameWithConfig;
/// A Postprocessor
pub(crate) type Postprocessor = NameWithConfig;

/// Connector configuration - only the parts applicable to all connectors
/// Specific parts are catched in the `config` map.
#[derive(Clone, Debug, Default)]
pub struct Connector {
    /// connector identifier
    pub id: Id,
    pub(crate) connector_type: ConnectorType,

    pub(crate) codec: Option<Codec>,

    pub(crate) config: tremor_pipeline::ConfigMap,

    /// mapping from mime-type to codec used to handle requests/responses
    /// with this mime-type
    ///
    /// e.g.:
    ///       codec_map:
    ///         "application/json": "json"
    ///         "text/plain": "string"
    ///
    /// A default builtin codec mapping is defined
    /// for msgpack, json, yaml and plaintext codecs with the common mime-types
    pub(crate) codec_map: Option<HashMap<String, Codec>>,

    // TODO: interceptors or configurable processors
    pub(crate) preprocessors: Option<Vec<Preprocessor>>,
    pub(crate) postprocessors: Option<Vec<Postprocessor>>,

    pub(crate) reconnect: Reconnect,

    //pub(crate) on_pause: PauseBehaviour,
    pub(crate) metrics_interval_s: Option<u64>,
}

impl Connector {
    /// Spawns a connector from a declaration
    pub fn from_decl(decl: &ConnectorDefinition) -> crate::Result<Connector> {
        let aggr_reg = tremor_script::registry::aggr();
        let reg = &*FN_REGISTRY.lock()?;
        let mut helper = Helper::new(reg, &aggr_reg, vec![]);
        let params = decl.params.clone();

        let defn = params.generate_config(&mut helper)?;

        Connector::from_defn(decl.instance_id.clone(), decl.kind.clone().into(), defn)
    }
    /// Creates a connector from it's definition (aka config + settings)
    pub fn from_defn(
        id: String,
        connector_type: ConnectorType,
        defn: Value<'static>,
    ) -> crate::Result<Connector> {
        let config = defn.get("config").cloned();

        fn validate_type(v: &Value, k: &str, t: ValueType) -> Result<()> {
            if v.get(k).is_some() && v.get(k).map(Value::value_type) != Some(t) {
                return Err(format!(
                    "Expect type {:?} for key {} but got {:?}",
                    t,
                    k,
                    v.get(k).map(Value::value_type).unwrap_or(ValueType::Null)
                )
                .into());
            }
            Ok(())
        }

        // TODO: can we get hygenic errors here?
        validate_type(&defn, "codec_map", ValueType::Object)?;
        validate_type(&defn, "preprocessors", ValueType::Array)?;
        validate_type(&defn, "postprocessors", ValueType::Array)?;
        validate_type(&defn, "metrics_interval_s", ValueType::U64)?;

        Ok(Connector {
            id,
            connector_type,
            config: config,
            codec_map: defn
                .get_object("codec_map")
                .map(|o| {
                    o.iter()
                        .map(|(k, v)| Ok((k.to_string(), Codec::from_value(v)?)))
                        .collect::<Result<HashMap<_, _>>>()
                })
                .transpose()?,
            preprocessors: defn
                .get_array("preprocessors")
                .map(|o| {
                    o.iter()
                        .map(Preprocessor::from_value)
                        .collect::<Result<_>>()
                })
                .transpose()?,
            postprocessors: defn
                .get_array("postprocessors")
                .map(|o| {
                    o.iter()
                        .map(Preprocessor::from_value)
                        .collect::<Result<_>>()
                })
                .transpose()?,
            reconnect: defn
                .get("reconnect")
                .cloned()
                .map(tremor_value::structurize)
                .transpose()?
                .unwrap_or_default(),
            metrics_interval_s: defn.get_u64("metrics_interval_s"),
            codec: defn.get("codec").map(Codec::from_value).transpose()?,
        })
    }
}

/// Configuration for a Binding
#[derive(Clone, Debug)]
pub struct Binding {
    /// ID of the binding
    pub id: Id,
    /// Description
    pub description: String,
    /// Binding map
    pub links: Vec<ConnectStmt>, // is this right? this should be url to url?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Result;

    #[test]
    fn test_reconnect_serde() -> Result<()> {
        assert_eq!(
            "---\n\
            none\n",
            serde_yaml::to_string(&Reconnect::None)?
        );
        let none_strategy = r#"
        none
        "#;
        let reconnect = serde_yaml::from_str::<Reconnect>(none_strategy)?;
        assert!(matches!(reconnect, Reconnect::None));
        let custom = r#"
        custom:
          interval_ms: 123
          growth_rate: 1.234567
        "#;
        let reconnect = serde_yaml::from_str::<Reconnect>(custom)?;
        assert!(matches!(
            reconnect,
            Reconnect::Custom {
                interval_ms: 123,
                growth_rate: _,
                max_retries: None
            }
        ));
        Ok(())
    }
}
