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
/// Tremor registry
pub mod registry;
/// The tremor repository
pub mod repository;
/// Tremor runtime system
pub mod system;
/// Tremor URI
pub mod url;
/// Utility functions
pub mod utils;
/// Tremor runtime version tools
pub mod version;

/// Bindings
pub mod binding;
/// Tremor connector extensions
pub mod connectors;

/// Metrics instance name
pub static mut INSTANCE: &str = "tremor";

use std::sync::atomic::AtomicUsize;
use std::{io::BufReader, path::Path};

use crate::errors::{Error, Result};

pub(crate) use crate::config::{Binding, Connector};
use crate::repository::BindingArtefact;
use crate::url::TremorUrl;
pub use serde_yaml::Value as OpConfig;
use system::World;
pub use tremor_pipeline::Event;
use tremor_pipeline::{query::Query, FN_REGISTRY};
use tremor_script::highlighter::Term as TermHighlighter;
use tremor_script::Script;
use tremor_value::literal;

lazy_static! {
    /// Default Q Size
    pub static ref QSIZE: AtomicUsize = AtomicUsize::new(128);
}
/// Loads a tremor query file
/// # Errors
/// Fails if the file can not be loaded
pub async fn load_query_file(world: &World, file_name: &str) -> Result<usize> {
    use std::ffi::OsStr;
    use std::io::Read;
    info!("Loading configuration from {}", file_name);
    let file_id = Path::new(file_name)
        .file_stem()
        .unwrap_or_else(|| OsStr::new(file_name))
        .to_string_lossy();
    let mut file = tremor_common::file::open(&file_name)?;
    let mut raw = String::new();

    file.read_to_string(&mut raw)
        .map_err(|e| Error::from(format!("Could not open file {} => {}", file_name, e)))?;

    // TODO: Should ideally be const
    let aggr_reg = tremor_script::registry::aggr();
    let module_path = tremor_script::path::load();
    let query = Query::parse_with_args(
        &module_path,
        &raw,
        file_name,
        vec![],
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
        &literal!({}), // TODO add support for runtime args once troy+connectors branches have merged
    );
    let query = match query {
        Ok(query) => query,
        Err(e) => {
            let mut h = TermHighlighter::stderr();
            if let Err(e) = Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };

            return Err(format!("failed to load trickle script: {}", file_name).into());
        }
    };
    let id = query.id().unwrap_or(&file_id);

    let id = TremorUrl::parse(&format!("/pipeline/{}", id))?;
    info!("Loading {} from file {}.", id, file_name);
    world.repo.publish_pipeline(&id, false, query).await?;

    Ok(1)
}

/// Loads a config yaml file
/// # Errors
/// Fails if the file can not be loaded
pub async fn load_cfg_file(world: &World, file_name: &str) -> Result<usize> {
    info!(
        "Loading configuration from {}",
        tremor_common::file::canonicalize(std::path::Path::new(file_name))?.display()
    );
    let mut count = 0;
    let file = tremor_common::file::open(file_name)?;
    let buffered_reader = BufReader::new(file);
    let config: config::Config = serde_yaml::from_reader(buffered_reader)?;

    for c in config.connector {
        let id = TremorUrl::from_connector_id(&c.id)?;
        info!("Loading {} from file {}.", id, file_name);
        world.repo.publish_connector(&id, false, c).await?;
        count += 1;
    }
    for binding in config.binding {
        let id = TremorUrl::from_binding_id(&binding.id)?;
        info!("Loading {} from file {}.", id, file_name);
        world
            .repo
            .publish_binding(&id, false, BindingArtefact::new(binding, None))
            .await?;
        count += 1;
    }
    for (binding, mapping) in config.mapping {
        world.link_binding(&binding, mapping).await?;
        world.reg.start_binding(&binding).await?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod test {
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
        assert_eq!(1, config.connector.len());
        assert_eq!(0, config.binding.len());
    }

    #[test]
    fn load_passthrough_stream() {
        let config = slurp("tests/configs/ut.passthrough.yaml");
        assert_eq!(2, config.binding[0].links.len());
    }
}
