// Copyright 2020, The Tremor Team
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

use crate::errors::{ErrorKind, Result};
use crate::url::TremorURL;
use hashbrown::HashMap;

pub(crate) type ID = String;
pub(crate) type OnRampVec = Vec<OnRamp>;
pub(crate) type OffRampVec = Vec<OffRamp>;
pub(crate) type BindingVec = Vec<Binding>;
pub(crate) type BindingMap = HashMap<TremorURL, Vec<TremorURL>>;
pub(crate) type MappingMap = HashMap<TremorURL, HashMap<String, String>>;

/// A full tremopr config
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "Default::default")]
    pub(crate) onramp: OnRampVec,
    #[serde(default = "Default::default")]
    pub(crate) offramp: OffRampVec,
    #[serde(default = "Default::default")]
    pub(crate) binding: BindingVec,
    #[serde(default = "Default::default")]
    pub(crate) mapping: MappingMap,
}

/// Configuration for an onramp
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OnRamp {
    /// ID of the onramp
    pub id: ID,
    #[serde(
        rename = "type",
        default = "Default::default",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) binding_type: Option<String>,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    /// whether to enable linked transport
    #[serde(rename = "linked", default = "Default::default")]
    pub(crate) is_linked: bool,

    /// Reference to a peer offramp
    /// A `Peer` to an onramp is an offramp that is capable of being referenced here
    /// and being configured as onramp for events originating from it (e.g. responses from an upstream server)
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) peer: Option<String>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
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
    pub(crate) codec_map: Option<halfbrown::HashMap<String, String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) preprocessors: Option<Vec<String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) postprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,
}

impl OnRamp {
    /// validate the given onramp config, consuming it and only return it if it is valid
    ///
    /// # Errors
    ///
    /// * if we have an invalid onramp config
    pub fn validate(self) -> Result<Self> {
        match (&self.binding_type, &self.peer) {
            (Some(_), Some(_)) => {
                return Err(ErrorKind::InvalidConfig(format!(
                    "Invalid onramp '{}': Provide either `type` or `peer`, not both",
                    self.id
                ))
                .into());
            }
            (None, None) => {
                return Err(ErrorKind::InvalidConfig(format!(
                    "Invalid onramp '{}': Provide one of `type` or `peer`",
                    self.id
                ))
                .into());
            }
            (None, Some(_)) if self.config.is_some() => {
                return Err(ErrorKind::InvalidConfig(format!(
                    "Invalid onramp '{}': `config` section inside `peer` onramp is not allowed",
                    self.id
                ))
                .into());
            }
            _ => (),
        }
        Ok(self)
    }
}

/// Configuration of an offramp
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OffRamp {
    /// ID of the offramp
    pub id: ID,
    #[serde(
        rename = "type",
        default = "Default::default",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) binding_type: Option<String>,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    /// whether to enable linked transport
    #[serde(rename = "linked", default = "Default::default")]
    // TODO validate that this is turned on only for supported offramps (rest, ws)
    pub(crate) is_linked: bool,
    /// A peer to an offramp is an onramp that can be referenced as an onramp
    /// An offramp configured with a peer will configure how events flowing back into this onramp
    /// will be handled.
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) peer: Option<String>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
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
    pub(crate) codec_map: Option<halfbrown::HashMap<String, String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) preprocessors: Option<Vec<String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) postprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,
}

impl OffRamp {
    /// validate the given offramp config, consuming it and only return it if it is valid
    ///
    /// # Errors
    /// * if the offramp config is invalid
    pub fn validate(self) -> Result<Self> {
        match (&self.binding_type, &self.peer) {
            (Some(_), Some(_)) => {
                return Err(ErrorKind::InvalidConfig(format!(
                    "Invalid offramp '{}': Provide either `type` or `peer`, not both",
                    self.id
                ))
                .into());
            }
            (None, None) => {
                return Err(ErrorKind::InvalidConfig(format!(
                    "Invalid offramp '{}': Provide one of `type` or `peer`",
                    self.id
                ))
                .into());
            }
            (None, Some(_)) if self.config.is_some() => {
                return Err(ErrorKind::InvalidConfig(format!(
                    "Invalid offramp '{}': `config` section inside `peer` onramp is not allowed",
                    self.id
                ))
                .into());
            }
            _ => (),
        }
        Ok(self)
    }
}

/// Configuration for a Binding
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// ID of the binding
    pub id: ID,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    pub(crate) links: BindingMap, // is this right? this should be url to url?
}
