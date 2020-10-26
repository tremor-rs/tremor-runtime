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

//! Tremor runtime

#![forbid(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rental;

#[macro_use]
pub(crate) mod macros;
pub(crate) mod async_sink;
/// Tremor codecs
pub mod codec;
/// Tremor runtime configuration
pub mod config;
/// Tremor runtime errors
pub mod errors;
/// Tremor function library
pub mod functions;
pub(crate) mod lifecycle;
/// Runtime metrics helper
pub mod metrics;
pub(crate) mod offramp;
pub(crate) mod onramp;
pub(crate) mod permge;
pub(crate) mod pipeline;
/// Onramp Preprocessors
pub mod postprocessor;
/// Offramp Postprocessors
pub mod preprocessor;
pub(crate) mod ramp;
/// Tremor registry
pub mod registry;
/// The tremor repository
pub mod repository;
pub(crate) mod sink;
pub(crate) mod source;
/// Tremor runtime system
pub mod system;
/// Tremor URI
pub mod url;
/// Utility functions
pub mod utils;
/// Tremor runtime version tools
pub mod version;

use crate::errors::Result;

pub(crate) type OnRampVec = Vec<OnRamp>;
pub(crate) type OffRampVec = Vec<OffRamp>;
pub(crate) type BindingVec = config::BindingVec;
pub(crate) type MappingMap = config::MappingMap;

pub(crate) use crate::config::Binding;
//pub(crate) use crate::config::Config;
pub(crate) use crate::config::OffRamp;
pub(crate) use crate::config::OnRamp;
pub(crate) use serde_yaml::Value as OpConfig;
pub(crate) use tremor_pipeline::Event;

/// Default Q Size
pub const QSIZE: usize = 128;

/// In incarnated config
#[derive(Debug, PartialEq)]
pub struct IncarnatedConfig {
    /// Onramps
    pub onramps: OnRampVec,
    /// Offramps
    pub offramps: OffRampVec,
    /// Bindings
    pub bindings: BindingVec,
    /// Mappings
    pub mappings: MappingMap,
}

/// Incarnates a configuration into it's runnable state
///
/// # Errors
///  * if the pipeline can not be incarnated
pub fn incarnate(config: config::Config) -> Result<IncarnatedConfig> {
    let onramps = incarnate_onramps(config.onramp.clone())?;
    let offramps = incarnate_offramps(config.offramp.clone())?;
    let bindings = incarnate_links(&config.binding);
    Ok(IncarnatedConfig {
        onramps,
        offramps,
        bindings,
        mappings: config.mapping,
    })
}

fn incarnate_onramps(config: config::OnRampVec) -> Result<OnRampVec> {
    let len = config.len();
    config
        .into_iter()
        .try_fold(Vec::with_capacity(len), |mut acc, o| {
            acc.push(o.validate()?);
            Ok(acc)
        })
}

fn incarnate_offramps(config: config::OffRampVec) -> Result<OffRampVec> {
    let len = config.len();
    config
        .into_iter()
        .try_fold(Vec::with_capacity(len), |mut acc, o| {
            acc.push(o.validate()?);
            Ok(acc)
        })
}

fn incarnate_links(config: &[Binding]) -> BindingVec {
    config.to_owned()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use serde_yaml;
    use std::io::BufReader;
    use tremor_common::file as cfile;

    fn slurp(file: &str) -> config::Config {
        let file = cfile::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).expect("Failed to read config.")
    }

    #[test]
    fn load_simple_deploys() {
        let config = slurp("tests/configs/deploy.simple.yaml");
        println!("{:?}", config);
        let runtime = incarnate(config).expect("Failed to incarnate config");
        assert_eq!(1, runtime.onramps.len());
        assert_eq!(1, runtime.offramps.len());
        assert_eq!(0, runtime.bindings.len());
    }

    #[test]
    fn load_passthrough_stream() {
        let config = slurp("tests/configs/ut.passthrough.yaml");
        println!("{:?}", &config);
        let runtime = incarnate(config).expect("Failed to incarnate config");
        assert_eq!(1, runtime.onramps.len());
        assert_eq!(1, runtime.offramps.len());
        assert_eq!(2, runtime.bindings[0].links.len());
    }

    #[test]
    fn incarnate_invalid_configs() {
        let mut configs = halfbrown::HashMap::new();
        configs.insert("tests/configs/invalid_onramp_no_type_no_peer.yaml",  "Invalid Configuration: Invalid onramp 'invalid-onramp': Provide one of `type` or `peer`.");
        configs.insert("tests/configs/invalid_onramp_peer_and_config.yaml",  "Invalid Configuration: Invalid onramp 'invalid-onramp': `config` section inside `peer` onramp is not allowed.");
        configs.insert("tests/configs/invalid_onramp_type_and_peer.yaml",    "Invalid Configuration: Invalid onramp 'invalid-onramp': Provide either `type` or `peer`, not both.");
        configs.insert("tests/configs/invalid_offramp_no_type_no_peer.yaml",  "Invalid Configuration: Invalid offramp 'invalid-offramp': Provide one of `type` or `peer`.");
        configs.insert("tests/configs/invalid_offramp_peer_and_config.yaml",  "Invalid Configuration: Invalid offramp 'invalid-offramp': `config` section inside `peer` onramp is not allowed.");
        configs.insert("tests/configs/invalid_offramp_type_and_peer.yaml",    "Invalid Configuration: Invalid offramp 'invalid-offramp': Provide either `type` or `peer`, not both.");
        for (config_path, errmsg) in configs {
            let config = slurp(config_path);
            assert_eq!(
                errmsg,
                incarnate(config)
                    .expect_err(&format!("No error for {}.", config_path))
                    .to_string(),
                "Invalid Error Message for config in {}",
                config_path
            );
        }
    }
}
