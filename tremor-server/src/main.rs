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

#![forbid(warnings)]
// This isn't a external crate so we don't worry about docs
// #![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate)]

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate log;

mod args;

use crate::errors::*;
use crate::system::World;
use crate::url::TremorURL;
use async_std::task;
use env_logger;
use serde_yaml;
use std::fs::File;
use std::io::BufReader;
use std::mem;
use std::path::Path;
use tremor_api as api;
use tremor_pipeline::query::Query;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime::repository::{BindingArtefact, PipelineArtefact};
use tremor_runtime::{self, config, errors, functions, metrics, system, url, version};

#[cfg_attr(tarpaulin, skip)]
async fn load_file(world: &World, file_name: &str) -> Result<usize> {
    info!("Loading configuration from {}", file_name);
    let mut count = 0;
    let file = File::open(file_name)
        .map_err(|e| Error::from(format!("Could not open file {} => {}", file_name, e)))?;
    let buffered_reader = BufReader::new(file);
    let config: config::Config = serde_yaml::from_reader(buffered_reader)?;
    let config = tremor_runtime::incarnate(config)?;

    for o in config.offramps {
        let id = TremorURL::parse(&format!("/offramp/{}", o.id))?;
        info!("Loading {} from file.", id);
        world.repo.publish_offramp(&id, false, o).await?;
        count += 1;
    }
    for pipeline in config.pipes {
        let id = TremorURL::parse(&format!("/pipeline/{}", pipeline.id))?;
        warn!("The pipeline {} is defined in the YAML file {}, this functionality is deprecated please migrate to trickle pipelines.", id, file_name);
        world
            .repo
            .publish_pipeline(&id, false, PipelineArtefact::Pipeline(Box::new(pipeline)))
            .await?;
        count += 1;
    }
    for o in config.onramps {
        let id = TremorURL::parse(&format!("/onramp/{}", o.id))?;
        info!("Loading {} from file.", id);
        world.repo.publish_onramp(&id, false, o).await?;
        count += 1;
    }
    for binding in config.bindings {
        let id = TremorURL::parse(&format!("/binding/{}", binding.id))?;
        info!("Loading {} from file.", id);
        world
            .repo
            .publish_binding(
                &id,
                false,
                BindingArtefact {
                    binding,
                    mapping: None,
                },
            )
            .await?;
        count += 1;
    }
    for (binding, mapping) in config.mappings {
        world.link_binding(&binding, mapping).await?;
        count += 1;
    }
    Ok(count)
}

#[cfg_attr(tarpaulin, skip)]
async fn load_query_file(world: &World, file_name: &str) -> Result<usize> {
    use std::ffi::OsStr;
    use std::io::Read;
    info!("Loading configuration from {}", file_name);
    let path = Path::new(file_name);
    let id = path
        .file_stem()
        .unwrap_or_else(|| OsStr::new(file_name))
        .to_string_lossy();
    let mut file = File::open(path)
        .map_err(|e| Error::from(format!("Could not open file {} => {}", file_name, e)))?;
    let mut raw = String::new();

    file.read_to_string(&mut raw)
        .map_err(|e| Error::from(format!("Could not open file {} => {}", file_name, e)))?;

    // FIXME: We should have them constanted
    let aggr_reg = tremor_script::registry::aggr();
    let module_path = tremor_script::path::load_module_path();
    let query = Query::parse(
        &module_path,
        &raw,
        file_name.to_string(),
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
    )?;

    let id = TremorURL::parse(&format!("/pipeline/{}", id))?;
    info!("Loading {} from file.", id);
    world
        .repo
        .publish_pipeline(&id, false, PipelineArtefact::Query(query))
        .await?;

    Ok(1)
}

// fn fix_tide(r: api::Result<tide::Response>) -> tide::Result<tide::Response> {
//     match r {
//         Ok(r) => Ok(r),
//         Err(e) => Ok(e.into()),
//     }
// }
fn fix_tide(r: api::Result<tide::Response>) -> tide::Response {
    match r {
        Ok(r) => r,
        Err(e) => e.into(),
    }
}

#[cfg_attr(tarpaulin, skip)]
#[allow(clippy::too_many_lines)]
async fn run_dun() -> Result<()> {
    functions::load()?;

    let matches = args::parse().get_matches();

    // Logging

    if let Some(logger_config) = matches.value_of("logger") {
        log4rs::init_file(logger_config, log4rs::file::Deserializers::default())?;
    } else {
        env_logger::init();
    }
    version::log();

    unsafe {
        // We know that instance will only get set once at
        // the very beginning nothing can access it yet,
        // this makes it allowable to use unsafe here.
        let s = matches
            .value_of("instance")
            .ok_or_else(|| Error::from("instance argument missing"))?;
        let forget_s = mem::transmute(&s as &str);
        // This means we're going to LEAK this memory, however
        // it is fine since as we do actually need it for the
        // rest of the program execution.
        metrics::INSTANCE = forget_s;
    }

    let storage_directory = matches
        .value_of("storage-directory")
        .map(std::string::ToString::to_string);
    // TODO: Allow configuring this for offramps and pipelines
    let (world, handle) = World::start(64, storage_directory).await?;

    // We load queries first since those are only pipelines.
    if let Some(query_files) = matches.values_of("query") {
        for query_file in query_files {
            load_query_file(&world, query_file).await?;
        }
    }

    if let Some(config_files) = matches.values_of("config") {
        for config_file in config_files {
            load_file(&world, config_file).await?;
        }
    }

    let host = matches
        .value_of("host")
        .ok_or_else(|| Error::from("host argument missing"))?;

    let mut app = tide::Server::with_state(api::State {
        world: world.clone(),
    });

    app.at("/version")
        .get(|r| async { fix_tide(api::version::get(r).await) });
    app.at("/binding")
        .get(|r| async { fix_tide(api::binding::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::binding::publish_artefact(r).await) });
    app.at("/binding/{aid}")
        .get(|r| async { fix_tide(api::binding::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::binding::unpublish_artefact(r).await) });
    app.at("/binding/{aid}/{sid}")
        .get(|r| async { fix_tide(api::binding::get_servant(r).await) })
        .post(|r| async { fix_tide(api::binding::link_servant(r).await) })
        .delete(|r| async { fix_tide(api::binding::unlink_servant(r).await) });
    app.at("/pipeline")
        .get(|r| async { fix_tide(api::pipeline::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::pipeline::publish_artefact(r).await) });
    app.at("/pipeline/{aid}")
        .get(|r| async { fix_tide(api::pipeline::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::pipeline::unpublish_artefact(r).await) });
    app.at("/onramp")
        .get(|r| async { fix_tide(api::onramp::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::onramp::publish_artefact(r).await) });
    app.at("/onramp/{aid}")
        .get(|r| async { fix_tide(api::onramp::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::onramp::unpublish_artefact(r).await) });
    app.at("/offramp")
        .get(|r| async { fix_tide(api::offramp::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::offramp::publish_artefact(r).await) });
    app.at("/offramp/{aid}")
        .get(|r| async { fix_tide(api::offramp::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::offramp::unpublish_artefact(r).await) });

    if !matches.is_present("no-api") {
        eprintln!("Listening at: http://{}", host);
        info!("Listening at: http://{}", host);

        if let Err(e) = app.listen(&host).await {
            error!("API Error: {}", e);
        }
        warn!("API stopped");
        world.stop().await;
    }

    handle.await;
    warn!("World stopped");
    Ok(())
}

#[cfg_attr(tarpaulin, skip)]
fn main() {
    version::print();
    if let Err(ref e) = task::block_on(run_dun()) {
        error!("error: {}", e);
        eprintln!("error: {}", e);
        for e in e.iter().skip(1) {
            error!("error: {}", e);
            eprintln!("error: {}", e);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            error!("backtrace: {:?}", backtrace);
            eprintln!("backtrace: {:?}", backtrace);
        }

        // ALLOW: main.rs
        ::std::process::exit(1);
    }
}
