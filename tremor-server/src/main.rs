// Copyright 2018-2019, Wayfair GmbH
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

#![warn(unused_extern_crates)]
#![recursion_limit = "1024"]
#![cfg_attr(feature = "cargo-clippy", deny(clippy::all))]

#[cfg(feature = "jemallocator")]
use jemallocator;
#[cfg(feature = "jemallocator")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate log;

mod args;

use crate::system::World;
use env_logger;
use serde_yaml;
use tremor_api;
use tremor_runtime;
use tremor_runtime::config;
use tremor_runtime::errors;
use tremor_runtime::functions;
use tremor_runtime::metrics;
use tremor_runtime::repository::{BindingArtefact, PipelineArtefact};
use tremor_runtime::system;
use tremor_runtime::url;
use tremor_runtime::version;

use crate::errors::*;
use crate::url::TremorURL;
use actix_web::middleware::cors::Cors;
use actix_web::{
    fs,
    http,
    //error,
    middleware,
    server,
    App,
};
use std::fs::File;
use std::io::BufReader;
use std::mem;

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
        world.repo.publish_offramp(id, false, o)?;
        count += 1;
    }
    for pipeline in config.pipes {
        let id = TremorURL::parse(&format!("/pipeline/{}", pipeline.id))?;
        info!("Loading {} from file.", id);
        world
            .repo
            .publish_pipeline(id, false, PipelineArtefact { pipeline })?;
        count += 1;
    }
    for o in config.onramps {
        let id = TremorURL::parse(&format!("/onramp/{}", o.id))?;
        info!("Loading {} from file.", id);
        world.repo.publish_onramp(id, false, o)?;
        count += 1;
    }
    for binding in config.bindings {
        let id = TremorURL::parse(&format!("/binding/{}", binding.id))?;
        info!("Loading {} from file.", id);
        world.repo.publish_binding(
            id,
            false,
            BindingArtefact {
                binding,
                mapping: None,
            },
        )?;
        count += 1;
    }
    for (binding, mapping) in config.mappings {
        world.link_binding(binding, mapping)?;
        count += 1;
    }
    Ok(count)
}
fn run_dun() -> Result<()> {
    functions::load()?;

    let matches = args::parse().get_matches();

    // Logging

    if let Some(logger_config) = matches.value_of("logger") {
        log4rs::init_file(logger_config, Default::default())?;
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

    if let Some(config_files) = matches.values_of("config") {
        for config_file in config_files {
            load_file(&world, config_file)?;
        }
    }

    let host = matches
        .value_of("host")
        .ok_or_else(|| Error::from("host argument missing"))?;
;

    let w = world.clone();
    let s = server::new(move || {
        App::with_state(tremor_api::State { world: w.clone() })
            .configure(|app| {
                Cors::for_app(app)
                    .resource("/binding", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::binding::list_artefact);
                        r.method(http::Method::POST)
                            .with(tremor_api::binding::publish_artefact);
                    })
                    .resource("/binding/{aid}", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::binding::get_artefact);
                        r.method(http::Method::DELETE)
                            .with(tremor_api::binding::unpublish_artefact);
                    })
                    .resource("/binding/{aid}/{sid}", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::binding::get_servant);
                        r.method(http::Method::POST)
                            .with(tremor_api::binding::link_servant);
                        r.method(http::Method::DELETE)
                            .with(tremor_api::binding::unlink_servant);
                    })
                    .resource("/pipeline", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::pipeline::list_artefact);
                        r.method(http::Method::POST)
                            .with(tremor_api::pipeline::publish_artefact);
                    })
                    .resource("/pipeline/{aid}", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::pipeline::get_artefact);
                        r.method(http::Method::DELETE)
                            .with(tremor_api::pipeline::unpublish_artefact);
                    })
                    .resource("/onramp", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::onramp::list_artefact);
                        r.method(http::Method::POST)
                            .with(tremor_api::onramp::publish_artefact);
                    })
                    .resource("/onramp/{aid}", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::onramp::get_artefact);
                        r.method(http::Method::DELETE)
                            .with(tremor_api::onramp::unpublish_artefact);
                    })
                    .resource("/offramp", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::offramp::list_artefact);
                        r.method(http::Method::POST)
                            .with(tremor_api::offramp::publish_artefact);
                    })
                    .resource("/offramp/{aid}", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::offramp::get_artefact);
                        r.method(http::Method::DELETE)
                            .with(tremor_api::offramp::unpublish_artefact);
                    })
                    .resource("/version", |r| {
                        r.method(http::Method::GET)
                            .with(tremor_api::version::get_version);
                    })
                    .register()
            })
            .handler(
                "/api-docs",
                fs::StaticFiles::new("static")
                    .expect("Missing static directoy")
                    .index_file("index.html"),
            )
            .middleware(middleware::Logger::default())
    });
    eprintln!("Listening at: http://{}", host);
    info!("Listening at: http://{}", host);

    s.bind(&host)
        .map_err(|e| Error::from(format!("Can not bind to {}", e)))?
        .run();
    warn!("API stopped");
    world.stop();
    let _ = handle.join();
    warn!("World stopped");
    Ok(())
}

fn main() {
    version::print();
    if let Err(ref e) = run_dun() {
        use std::io::Write;
        let stderr = &mut ::std::io::stderr();
        let errmsg = "Error writing to stderr";

        writeln!(stderr, "error: {}", e).expect(errmsg);

        for e in e.iter().skip(1) {
            writeln!(stderr, "caused by: {}", e).expect(errmsg);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            writeln!(stderr, "backtrace: {:?}", backtrace).expect(errmsg);
        }

        ::std::process::exit(1);
    }
}
