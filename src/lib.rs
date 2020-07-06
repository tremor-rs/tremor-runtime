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

//! Tremor runtime

#![forbid(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate, clippy::missing_errors_doc)]

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
pub(crate) mod dflt;
/// Tremor runtime errors
pub mod errors;
/// Tremor function library
pub mod functions;
#[cfg(feature = "gcp")]
pub(crate) mod google;
pub(crate) mod lifecycle;
/// Runtime metrics helper
pub mod metrics;
pub(crate) mod offramp;
pub(crate) mod onramp;
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
pub(crate) type PipelineVec = Vec<tremor_pipeline::Pipeline>;
pub(crate) type MappingMap = config::MappingMap;

pub(crate) use crate::config::Binding;
//pub(crate) use crate::config::Config;
pub(crate) use crate::config::OffRamp;
pub(crate) use crate::config::OnRamp;
pub(crate) use serde_yaml::Value as OpConfig;
pub(crate) use tremor_pipeline::Event;

/// In incarnated config
#[derive(Debug)]
pub struct IncarnatedConfig {
    /// Onramps
    pub onramps: OnRampVec,
    /// Offramps
    pub offramps: OffRampVec,
    /// Bindings
    pub bindings: BindingVec,
    /// Pipelines
    pub pipes: PipelineVec,
    /// Mappings
    pub mappings: MappingMap,
}

/// Incarnates a configuration into it's runnable state
pub fn incarnate(config: config::Config) -> Result<IncarnatedConfig> {
    let onramps = incarnate_onramps(config.onramp.clone());
    let offramps = incarnate_offramps(config.offramp.clone());
    let bindings = incarnate_links(&config.binding);
    let pipes = incarnate_pipes(config.pipeline)?;
    Ok(IncarnatedConfig {
        onramps,
        offramps,
        bindings,
        pipes,
        mappings: config.mapping,
    })
}

fn incarnate_onramps(config: config::OnRampVec) -> OnRampVec {
    config.into_iter().map(|d| d).collect()
}

fn incarnate_offramps(config: config::OffRampVec) -> OffRampVec {
    config.into_iter().map(|d| d).collect()
}

fn incarnate_links(config: &[Binding]) -> BindingVec {
    config.to_owned()
}

fn incarnate_pipes(config: config::PipelineVec) -> Result<PipelineVec> {
    config
        .into_iter()
        .map(|d| Ok(tremor_pipeline::build_pipeline(d)?))
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use serde_yaml;
    use std::fs::File;
    use std::io::BufReader;

    fn slurp(file: &str) -> config::Config {
        let file = File::open(file).expect("could not open file");
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
        assert_eq!(0, runtime.pipes.len());
    }

    #[test]
    fn load_simple_pipes() {
        let config = slurp("tests/configs/pipe.simple.yaml");
        println!("{:?}", &config);
        let runtime = incarnate(config).expect("Failed to incarnate config");
        assert_eq!(0, runtime.onramps.len());
        assert_eq!(0, runtime.offramps.len());
        assert_eq!(0, runtime.bindings.len());
        assert_eq!(1, runtime.pipes.len());
    }

    #[test]
    fn load_passthrough_stream() {
        let config = slurp("tests/configs/ut.passthrough.yaml");
        println!("{:?}", &config);
        let runtime = incarnate(config).expect("Failed to incarnate config");
        assert_eq!(1, runtime.onramps.len());
        assert_eq!(1, runtime.offramps.len());
        assert_eq!(2, runtime.bindings[0].links.len());
        assert_eq!(1, runtime.pipes.len());
    }

    #[test]
    fn load_passthrough_op() {
        let config = slurp("tests/configs/ut.single-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_branch_op() {
        let config = slurp("tests/configs/ut.branch-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine_op() {
        let config = slurp("tests/configs/ut.combine-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine2_op() {
        let config = slurp("tests/configs/ut.combine2-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine3_op() {
        let config = slurp("tests/configs/ut.combine3-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine4_op_cycle_error() {
        let config = slurp("tests/configs/ut.combine4-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_err());
    }

    #[test]
    fn pipeline_to_runner() {}
}
