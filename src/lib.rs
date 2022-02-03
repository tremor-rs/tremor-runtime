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

//! Tremor runtime

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
// TODO this is needed due to a false positive in clippy
// https://github.com/rust-lang/rust/issues/83125
// we will need this in 1.53.1
#![allow(proc_macro_back_compat)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

#[cfg(test)]
extern crate test_case;

#[macro_use]
pub(crate) mod macros;
/// Tremor codecs
pub mod codec;
/// Tremor runtime configuration
pub mod config;
/// Tremor runtime errors
pub mod errors;
/// Tremor function library
pub mod functions;

pub(crate) mod permge;

/// pipelines
pub mod pipeline;
/// Onramp Preprocessors
pub mod postprocessor;
/// Offramp Postprocessors
pub mod preprocessor;

/// Tremor connector extensions
pub mod connectors;
/// Tremor runtime system
pub mod system;
/// Utility functions
pub mod utils;
/// Tremor runtime version tools
pub mod version;

/// Instance management
pub mod instance;

/// Metrics instance name
pub static mut INSTANCE: &str = "tremor";

use std::sync::atomic::AtomicUsize;

use crate::errors::{Error, Result};

pub(crate) use crate::config::Connector;
use system::World;
pub use tremor_pipeline::Event;
use tremor_script::Script;
use tremor_script::FN_REGISTRY;
use tremor_script::{
    deploy::Deploy, highlighter::Dumb as ToStringHighlighter, highlighter::Term as TermHighlighter,
};

/// Operator Config
pub type OpConfig = tremor_value::Value<'static>;

lazy_static! {
    /// Default Q Size
    pub static ref QSIZE: AtomicUsize = AtomicUsize::new(128);
}

/// Loads a config yaml file
/// # Errors
/// Fails if the file can not be loaded
pub async fn load_troy_file(world: &World, file_name: &str) -> Result<usize> {
    // use std::ffi::OsStr;
    use std::io::Read;
    info!("Loading troy from {}", file_name);

    let mut file = tremor_common::file::open(&file_name)?;
    let mut src = String::new();

    file.read_to_string(&mut src)
        .map_err(|e| Error::from(format!("Could not open file {} => {}", file_name, e)))?;
    let aggr_reg = tremor_script::registry::aggr();
    let module_path = tremor_script::path::load();

    let deployable = Deploy::parse(
        &module_path,
        file_name,
        &src,
        vec![],
        &*FN_REGISTRY.read()?,
        &aggr_reg,
    );
    let deployable = match deployable {
        Ok(deployable) => deployable,
        Err(e) => {
            let mut h = TermHighlighter::stderr();
            if let Err(e) = Script::format_error_from_script(&src, &mut h, &e) {
                eprintln!("Error: {}", e);
            };

            return Err(format!("failed to load troy file: {}", file_name).into());
        }
    };

    let unit = deployable.deploy.as_flows()?;

    for flow in &unit.instances {
        world.start_deploy(&src, flow).await?;
    }
    Ok(unit.instances.len())
}
