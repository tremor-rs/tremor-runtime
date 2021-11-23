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
use either::Either;
use hashbrown::HashMap;
use tremor_common::url::TremorUrl;

pub(crate) type Id = String;
pub(crate) type ConnectorVec = Vec<Connector>;
pub(crate) type BindingVec = Vec<Binding>;
pub(crate) type BindingMap = HashMap<TremorUrl, Vec<TremorUrl>>;
pub(crate) type MappingMap = HashMap<TremorUrl, HashMap<String, String>>;

/// A full tremor config
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "Default::default")]
    pub(crate) connector: ConnectorVec,
    #[serde(default = "Default::default")]
    pub(crate) binding: Vec<Binding>,
    #[serde(default = "Default::default")]
    pub(crate) mapping: MappingMap,
}

/// possible reconnect strategies for controlling if and how to reconnect
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Reconnect {
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
        #[serde(default = "default_max_retries")]
        max_retries: Option<u64>,
    },
}

fn default_growth_rate() -> f64 {
    1.2
}

fn default_max_retries() -> Option<u64> {
    Some(3)
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Codec {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,
}

impl From<&str> for Codec {
    fn from(name: &str) -> Self {
        Self {
            name: name.to_string(),
            config: None,
        }
    }
}

/// Pre- or Postprocessor name and config
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Processor {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,
}

impl From<&str> for Processor {
    fn from(name: &str) -> Self {
        Self {
            name: name.to_string(),
            config: None,
        }
    }
}

impl From<&ProcessorOrName> for Processor {
    fn from(p: &ProcessorOrName) -> Self {
        match &p.inner {
            Either::Left(name) => name.as_str().into(),
            Either::Right(config) => config.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ProcessorOrName {
    #[serde(with = "either::serde_untagged")]
    inner: Either<String, Processor>,
}

pub(crate) type Preprocessor = Processor;
pub(crate) type Postprocessor = Processor;

/// Connector configuration - only the parts applicable to all connectors
/// Specific parts are catched in the `config` map.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connector {
    /// connector identifier
    pub id: Id,
    #[serde(rename = "type")]
    pub(crate) binding_type: ConnectorType,

    #[serde(default = "Default::default")]
    pub(crate) description: String,

    #[serde(
        with = "either::serde_untagged_optional",
        skip_serializing_if = "Option::is_none",
        default = "Default::default"
    )]
    pub(crate) codec: Option<Either<String, Codec>>,

    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec_map: Option<halfbrown::HashMap<String, Either<String, Codec>>>,

    // TODO: interceptors or configurable processors
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) preprocessors: Option<Vec<ProcessorOrName>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) postprocessors: Option<Vec<ProcessorOrName>>,

    #[serde(default)]
    pub(crate) reconnect: Reconnect,

    //#[serde(default)]
    //pub(crate) on_pause: PauseBehaviour,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
}

/// Configuration for a Binding
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// ID of the binding
    pub id: Id,
    #[serde(default = "Default::default")]
    /// Description
    pub description: String,
    /// Binding map
    pub links: BindingMap, // is this right? this should be url to url?
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
