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
/// Tremor registry
pub mod registry;
/// The tremor repository
pub mod repository;
/// Tremor runtime system
pub mod system;
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

use std::path::Path;
use std::sync::atomic::AtomicUsize;

use crate::errors::{Error, Result};

pub(crate) use crate::config::{Binding, Connector};
use crate::repository::BindingArtefact;
use system::World;
use tremor_common::url::TremorUrl;
pub use tremor_pipeline::Event;
use tremor_pipeline::{query::Query, FN_REGISTRY};
use tremor_script::Script;
use tremor_script::{deploy::Deploy, highlighter::Term as TermHighlighter, srs};

/// Operator Config
pub type OpConfig = tremor_value::Value<'static>;

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
    let query = Query::parse(
        &module_path,
        &raw,
        file_name,
        vec![],
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
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
pub async fn load_troy_file(world: &World, file_name: &str) -> Result<usize> {
    use std::ffi::OsStr;
    use std::io::Read;
    info!("Loading troy from {}", file_name);

    let file_id = Path::new(file_name)
        .file_stem()
        .unwrap_or_else(|| OsStr::new(file_name))
        .to_string_lossy();
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
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
    );
    let deployable = match deployable {
        Ok(deployable) => deployable,
        Err(e) => {
            let mut h = TermHighlighter::stderr();
            if let Err(e) = Script::format_error_from_script(&src, &mut h, &e) {
                eprintln!("Error: {}", e);
            };

            return Err(format!("failed to load trickle script: {}", file_name).into());
        }
    };

    let unit = deployable.deploy.as_flows()?;

    for (binding_instance, flow) in &unit.instances {
        let binding_url = TremorUrl::from_binding_instance(&file_id, binding_instance);

        let flow_decls = &flow.decl;

        for connector in &flow.decl.connectors {
            handle_troy_connector(&world, connector).await?;
        }
        for pipeline in &flow.decl.pipelines {
            handle_troy_pipeline(&world, &src, pipeline).await?;
        }

        let binding = BindingArtefact {
            binding: Binding {
                id: binding_instance.to_string(),
                description: "Troy managed binding".to_string(),
                links: flow_decls.links.clone(),
            },
            mapping: None,
        };
        world
            .repo
            .publish_binding(&binding_url, false, binding)
            .await?;
        let kv = hashbrown::HashMap::new();
        world.launch_binding(&binding_url, kv).await?;
    }
    Ok(unit.instances.len())
}

async fn handle_troy_connector(world: &World, decl: &srs::ConnectorDecl) -> Result<()> {
    let url = TremorUrl::from_connector_instance(decl.artefact_id.id(), &decl.instance_id);
    let connector = Connector::from_decl(decl)?;
    world.repo.publish_connector(&url, false, connector).await?;
    Ok(())
}

async fn handle_troy_pipeline(world: &World, src: &str, atom: &srs::Query) -> Result<()> {
    let url = TremorUrl::parse(&format!(
        "/pipeline/{}/{}",
        atom.node_id.clone(),
        atom.alias
    ))?;
    world
        .repo
        .publish_pipeline(
            &url,
            false,
            tremor_pipeline::query::Query(tremor_script::Query::from_troy(src, atom)?),
        )
        .await?;

    Ok(())
}
