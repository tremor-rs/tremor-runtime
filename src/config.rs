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

use crate::connectors::prelude::*;
use simd_json::ValueType;
use tremor_script::{
    ast::deploy::ConnectorDefinition,
    ast::{self, Helper},
    FN_REGISTRY,
};
use tremor_value::prelude::*;

use abi_stable::{
    std_types::{
        ROption::{self, RNone},
        RString, RVec,
    },
    StableAbi,
};

pub(crate) type Id = String;

/// Reconnect strategies for controlling if and how to reconnect
#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize, StableAbi)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Reconnect {
    /// No reconnection
    None,
    /// Configurable retries
    Retry {
        /// start interval to wait after a failing connect attempt
        interval_ms: u64,
        /// growth rate for consecutive connect attempts, will be added to interval_ms
        #[serde(default = "default_growth_rate")]
        growth_rate: f64,
        /// maximum number of retries to execute
        max_retries: ROption<u64>,
        /// Randomize the growth rate
        #[serde(default = "default_true")]
        randomized: bool,
    },
}

fn default_growth_rate() -> f64 {
    1.5
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
#[repr(C)]
#[derive(Clone, Debug, Default, StableAbi)]
#[allow(clippy::module_name_repetitions)]
pub struct NameWithConfig {
    pub(crate) name: RString,
    pub(crate) config: ROption<Value<'static>>,
}

impl<'v> TryFrom<&Value<'v>> for NameWithConfig {
    type Error = crate::errors::Error;

    fn try_from(value: &Value) -> Result<Self> {
        if let Some(name) = value.as_str() {
            Ok(Self::from(name))
        } else if let Some(name) = value.get_str("name") {
            Ok(Self {
                name: RString::from(name),
                config: value.get("config").map(Value::clone_static).into(),
            })
        } else {
            Err(format!("Invalid codec: {}", value).into())
        }
    }
}

impl From<&str> for NameWithConfig {
    fn from(name: &str) -> Self {
        Self {
            name: RString::from(name),
            config: RNone,
        }
    }
}
impl From<&String> for NameWithConfig {
    fn from(name: &String) -> Self {
        Self {
            name: name.clone().into(),
            config: RNone,
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
#[repr(C)]
#[derive(Clone, Debug, Default, StableAbi)]
pub(crate) struct Connector {
    /// Connector type
    pub connector_type: ConnectorType,

    /// Codec in force for connector
    pub codec: ROption<Codec>,

    /// Configuration map
    pub config: tremor_pipeline::ConfigMap,

    // TODO: interceptors or configurable processors
    /// Preprocessor chain configuration
    pub preprocessors: ROption<RVec<Preprocessor>>,

    // TODO: interceptors or configurable processors
    /// Postprocessor chain configuration
    pub postprocessors: ROption<RVec<Postprocessor>>,

    pub(crate) reconnect: Reconnect,

    //pub(crate) on_pause: PauseBehaviour,
    pub(crate) metrics_interval_s: ROption<u64>,
}

impl Connector {
    /// Spawns a connector from a definition
    pub(crate) fn from_defn(defn: &ast::ConnectorDefinition<'static>) -> crate::Result<Self> {
        let aggr_reg = tremor_script::registry::aggr();
        let reg = &*FN_REGISTRY.read()?;

        let mut helper = Helper::new(reg, &aggr_reg);
        let params = defn.params.clone();

        let conf = params.generate_config(&mut helper)?;

        Self::from_config(defn.id.as_str(), defn.builtin_kind.clone().into(), &conf)
    }
    /// Creates a connector from it's definition (aka config + settings)
    #[allow(clippy::too_many_lines)]
    pub(crate) fn from_config(
        connector_id: &str,
        connector_type: ConnectorType,
        connector_config: &Value<'static>,
    ) -> crate::Result<Self> {
        fn validate_type(v: &Value, k: &str, t: ValueType, connector_id: &str) -> Result<()> {
            if v.get(k).is_some() && v.get(k).map(Value::value_type) != Some(t) {
                return Err(ErrorKind::InvalidConnectorDefinition(
                    connector_id.to_string(),
                    format!(
                        "Expected type {t:?} for key {k} but got {:?}",
                        v.get(k).map_or(ValueType::Null, Value::value_type)
                    ),
                )
                .into());
            }
            Ok(())
        }
        let config = connector_config.get(ConnectorDefinition::CONFIG).cloned();

        // TODO: can we get hygenic errors here?

        validate_type(
            connector_config,
            ConnectorDefinition::CODEC,
            ValueType::String,
            connector_id,
        )?;
        validate_type(
            connector_config,
            ConnectorDefinition::CONFIG,
            ValueType::Object,
            connector_id,
        )?;
        validate_type(
            connector_config,
            ConnectorDefinition::RECONNECT,
            ValueType::Object,
            connector_id,
        )?;
        validate_type(
            connector_config,
            ConnectorDefinition::PREPROCESSORS,
            ValueType::Array,
            connector_id,
        )?;
        validate_type(
            connector_config,
            ConnectorDefinition::POSTPROCESSORS,
            ValueType::Array,
            connector_id,
        )?;
        validate_type(
            connector_config,
            ConnectorDefinition::METRICS_INTERVAL_S,
            ValueType::U64,
            connector_id,
        )
        .or_else(|_| {
            validate_type(
                connector_config,
                ConnectorDefinition::METRICS_INTERVAL_S,
                ValueType::I64,
                connector_id,
            )
        })?;

        Ok(Self {
            connector_type,
            config: config.into(),
            preprocessors: ROption::from(
                connector_config
                    .get_array(ConnectorDefinition::PREPROCESSORS)
                    .map(|o| o.iter().map(Preprocessor::try_from).collect::<Result<_>>())
                    .transpose()?,
            ),
            postprocessors: ROption::from(
                connector_config
                    .get_array(ConnectorDefinition::POSTPROCESSORS)
                    .map(|o| o.iter().map(Preprocessor::try_from).collect::<Result<_>>())
                    .transpose()?,
            ),
            reconnect: connector_config
                .get(ConnectorDefinition::RECONNECT)
                .cloned()
                .map(tremor_value::structurize)
                .transpose()?
                .unwrap_or_default(),
            metrics_interval_s: ROption::from(
                connector_config.get_u64(ConnectorDefinition::METRICS_INTERVAL_S),
            ),
            codec: ROption::from(
                connector_config
                    .get(ConnectorDefinition::CODEC)
                    .map(Codec::try_from)
                    .transpose()?,
            ),
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
    pub links: Vec<ast::ConnectStmt>, // is this right? this should be url to url?
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
        let retry = r#"
        retry:
          interval_ms: 123
          growth_rate: 1.234567
        "#;
        let reconnect = serde_yaml::from_str::<Reconnect>(retry)?;
        assert!(matches!(
            reconnect,
            Reconnect::Retry {
                interval_ms: 123,
                growth_rate: _,
                max_retries: None,
                randomized: true
            }
        ));
        Ok(())
    }

    #[test]
    fn test_config_builtin_preproc_with_config() -> Result<()> {
        let c = Connector::from_config(
            "my_otel_client",
            ConnectorType::from("otel_client".to_string()),
            &literal!({
                "preprocessors": [ {"name": "snot", "config": { "separator": "\n" }}],
            }),
        )?;
        let pp = c.preprocessors;
        assert!(pp.is_some());

        if let Some(pp) = pp {
            assert_eq!("snot", pp[0].name);
            assert!(pp[0].config.is_some());
            if let Some(config) = &pp[0].config {
                assert_eq!("\n", config.get("separator").unwrap().to_string());
            }
        }
        Ok(())
    }

    #[test]
    fn test_connector_config_wrong_config() -> Result<()> {
        let config = literal!({
            "preprocessors": [],
            "postprocessors": [],
            "codec": "string",
            "reconnect": {},
            "metrics_interval_s": "wrong_type"
        });
        let id = "my_id";
        let res = Connector::from_config(id, "fancy_schmancy".into(), &config);
        assert!(res.is_err());
        assert_eq!(String::from("Invalid Definition for connector \"my_id\": Expected type I64 for key metrics_interval_s but got String"), res.err().unwrap().to_string());
        Ok(())
    }
}
