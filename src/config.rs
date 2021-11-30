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
use tremor_pipeline::FN_REGISTRY;
use tremor_script::{
    ast::{ConnectStmt, Helper},
    srs::ConnectorDecl,
};

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
#[derive(Clone, Debug)]
pub struct Codec {
    pub(crate) name: String,
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
#[derive(Clone, Debug)]
pub struct Processor {
    pub(crate) name: String,
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

#[derive(Clone, Debug)]
pub(crate) struct ProcessorOrName {
    inner: Either<String, Processor>,
}

pub(crate) type Preprocessor = Processor;
pub(crate) type Postprocessor = Processor;

/// Connector configuration - only the parts applicable to all connectors
/// Specific parts are catched in the `config` map.
#[derive(Clone, Debug, Default)]
pub struct Connector {
    /// connector identifier
    pub id: Id,
    pub(crate) binding_type: ConnectorType,

    pub(crate) description: String,

    pub(crate) codec: Option<Either<String, Codec>>,

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
    pub(crate) codec_map: Option<halfbrown::HashMap<String, Either<String, Codec>>>,

    // TODO: interceptors or configurable processors
    pub(crate) preprocessors: Option<Vec<ProcessorOrName>>,
    pub(crate) postprocessors: Option<Vec<ProcessorOrName>>,

    pub(crate) reconnect: Reconnect,

    //pub(crate) on_pause: PauseBehaviour,
    pub(crate) metrics_interval_s: Option<u64>,
}

impl Connector {
    /// FIXME
    pub fn from_decl(decl: &ConnectorDecl) -> crate::Result<Connector> {
        // FIXME: make this proper
        let aggr_reg = tremor_script::registry::aggr();
        let reg = &*FN_REGISTRY.lock()?;
        let mut helper = Helper::new(reg, &aggr_reg, vec![]);
        let params = decl.params.clone();

        let config = params.generate_config(&mut helper)?;
        let config = config
            .into_iter()
            .collect::<tremor_value::Value>()
            .into_static();

        Ok(Connector {
            id: decl.instance_id.clone(),
            binding_type: decl.kind.clone().into(),
            description: "FIXME: placeholder description".to_string(),
            config: Some(config),
            codec_map: None,
            preprocessors: None,
            postprocessors: None,
            reconnect: Reconnect::None,
            metrics_interval_s: None,
            codec: None,
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
