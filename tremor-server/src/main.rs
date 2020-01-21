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
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate)]

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/*
use jemallocator;
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
*/

#[macro_use]
extern crate log;

mod args;

use crate::errors::*;
use crate::system::World;
use crate::url::TremorURL;
use actix_cors::Cors;
use actix_files as fs;
use env_logger;
use serde_yaml;
use tremor_api;
use tremor_pipeline::query::Query;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::config;
use tremor_runtime::errors;
use tremor_runtime::functions;
use tremor_runtime::metrics;
use tremor_runtime::repository::{BindingArtefact, PipelineArtefact};
use tremor_runtime::system;
use tremor_runtime::url;
use tremor_runtime::version;

use actix_web::{
    //error,
    middleware,
    web,
    App,
    HttpServer,
};
use std::fs::File;
use std::io::BufReader;
use std::mem;
use std::path::Path;

#[cfg_attr(tarpaulin, skip)]
fn load_file(world: &World, file_name: &str) -> Result<usize> {
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
        world.repo.publish_offramp(&id, false, o)?;
        count += 1;
    }
    for pipeline in config.pipes {
        let id = TremorURL::parse(&format!("/pipeline/{}", pipeline.id))?;
        warn!("The pipeline {} is defined in the YAML file {}, this functionality is deprecated please migrate to trickle pipelines.", id, file_name);
        world
            .repo
            .publish_pipeline(&id, false, PipelineArtefact::Pipeline(Box::new(pipeline)))?;
        count += 1;
    }
    for o in config.onramps {
        let id = TremorURL::parse(&format!("/onramp/{}", o.id))?;
        info!("Loading {} from file.", id);
        world.repo.publish_onramp(&id, false, o)?;
        count += 1;
    }
    for binding in config.bindings {
        let id = TremorURL::parse(&format!("/binding/{}", binding.id))?;
        info!("Loading {} from file.", id);
        world.repo.publish_binding(
            &id,
            false,
            BindingArtefact {
                binding,
                mapping: None,
            },
        )?;
        count += 1;
    }
    for (binding, mapping) in config.mappings {
        world.link_binding(&binding, mapping)?;
        count += 1;
    }
    Ok(count)
}

#[cfg_attr(tarpaulin, skip)]
fn load_query_file(world: &World, file_name: &str) -> Result<usize> {
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

    let query = Query::parse(&raw, &*FN_REGISTRY.lock()?, &aggr_reg)?;

    let id = TremorURL::parse(&format!("/pipeline/{}", id))?;
    info!("Loading {} from file.", id);
    world
        .repo
        .publish_pipeline(&id, false, PipelineArtefact::Query(query))?;

    Ok(1)
}

#[cfg_attr(tarpaulin, skip)]
#[allow(clippy::too_many_lines)]
fn run_dun() -> Result<()> {
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
    let (world, handle) = World::start(50, storage_directory)?;

    // We load queries first since those are only pipelines.
    if let Some(query_files) = matches.values_of("query") {
        for query_file in query_files {
            load_query_file(&world, query_file)?;
        }
    }

    if let Some(config_files) = matches.values_of("config") {
        for config_file in config_files {
            load_file(&world, config_file)?;
        }
    }

    let host = matches
        .value_of("host")
        .ok_or_else(|| Error::from("host argument missing"))?;

    let w = world.clone();
    let s = HttpServer::new(move || {
        App::new()
            .data(tremor_api::State { world: w.clone() })
            .wrap(middleware::Logger::default())
            .wrap(Cors::new())
            .service(
                web::resource("/binding")
                    .route(web::get().to(tremor_api::binding::list_artefact))
                    .route(web::post().to(tremor_api::binding::publish_artefact)),
            )
            .service(
                web::resource("/binding/{aid}")
                    .route(web::get().to(tremor_api::binding::get_artefact))
                    .route(web::delete().to(tremor_api::binding::unpublish_artefact)),
            )
            .service(
                web::resource("/binding/{aid}/{sid}")
                    .route(web::get().to(tremor_api::binding::get_servant))
                    .route(web::post().to(tremor_api::binding::link_servant))
                    .route(web::delete().to(tremor_api::binding::unlink_servant)),
            )
            .service(
                web::resource("/pipeline")
                    .route(web::get().to(tremor_api::pipeline::list_artefact))
                    .route(web::post().to(tremor_api::pipeline::publish_artefact)),
            )
            .service(
                web::resource("/pipeline/{aid}")
                    .route(web::get().to(tremor_api::pipeline::get_artefact))
                    .route(web::delete().to(tremor_api::pipeline::unpublish_artefact)),
            )
            .service(
                web::resource("/onramp")
                    .route(web::get().to(tremor_api::onramp::list_artefact))
                    .route(web::post().to(tremor_api::onramp::publish_artefact)),
            )
            .service(
                web::resource("/onramp/{aid}")
                    .route(web::get().to(tremor_api::onramp::get_artefact))
                    .route(web::delete().to(tremor_api::onramp::unpublish_artefact)),
            )
            .service(
                web::resource("/offramp")
                    .route(web::get().to(tremor_api::offramp::list_artefact))
                    .route(web::post().to(tremor_api::offramp::publish_artefact)),
            )
            .service(
                web::resource("/offramp/{aid}")
                    .route(web::get().to(tremor_api::offramp::get_artefact))
                    .route(web::delete().to(tremor_api::offramp::unpublish_artefact)),
            )
            .service(web::resource("/version").route(web::get().to(tremor_api::version::get)))
            .service(fs::Files::new("/api-docs", "static").index_file("index.html"))
    });
    eprintln!("Listening at: http://{}", host);
    info!("Listening at: http://{}", host);

    if !matches.is_present("no-api") {
        s.bind(&host)
            .map_err(|e| Error::from(format!("Can not bind to {}", e)))?
            .run()?;
        warn!("API stopped");
        world.stop();
    }
    if let Err(e) = handle.join() {
        error!("Tremor terminated badly: {:?}", e);
    }
    warn!("World stopped");
    Ok(())
}

#[cfg_attr(tarpaulin, skip)]
fn main() {
    version::print();
    if let Err(ref e) = run_dun() {
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
