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

use crate::errors::{Error, Result};
use crate::util::{get_source_kind, SourceKind};
use async_std::task;
use clap::{App, ArgMatches};
use std::io::Write;
use std::sync::atomic::Ordering;
use tremor_api as api;
use tremor_common::file;
use tremor_runtime::system::World;
use tremor_runtime::{self, version};

fn fix_tide(r: api::Result<tide::Response>) -> tide::Result {
    Ok(match r {
        Ok(r) => r,
        Err(e) => e.into(),
    })
}

fn api_server(world: &World) -> Result<tide::Server<api::State>> {
    let mut app = tide::Server::with_state(api::State {
        world: world.clone(),
    });

    app.at("/version")
        .get(|r| async { fix_tide(api::version::get(r).await) });
    app.at("/binding")
        .get(|r| async { fix_tide(api::binding::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::binding::publish_artefact(r).await) });
    app.at("/binding/:aid")
        .get(|r| async { fix_tide(api::binding::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::binding::unpublish_artefact(r).await) });
    app.at("/binding/:aid/:sid")
        .get(|r| async { fix_tide(api::binding::get_servant(r).await) })
        .post(|r| async { fix_tide(api::binding::link_servant(r).await) })
        .delete(|r| async { fix_tide(api::binding::unlink_servant(r).await) });
    app.at("/pipeline")
        .get(|r| async { fix_tide(api::pipeline::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::pipeline::publish_artefact(r).await) });
    app.at("/pipeline/:aid")
        .get(|r| async { fix_tide(api::pipeline::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::pipeline::unpublish_artefact(r).await) });
    app.at("/onramp")
        .get(|r| async { fix_tide(api::onramp::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::onramp::publish_artefact(r).await) });
    app.at("/onramp/:aid")
        .get(|r| async { fix_tide(api::onramp::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::onramp::unpublish_artefact(r).await) });
    app.at("/offramp")
        .get(|r| async { fix_tide(api::offramp::list_artefact(r).await) })
        .post(|r| async { fix_tide(api::offramp::publish_artefact(r).await) });
    app.at("/offramp/:aid")
        .get(|r| async { fix_tide(api::offramp::get_artefact(r).await) })
        .delete(|r| async { fix_tide(api::offramp::unpublish_artefact(r).await) });

    Ok(app)
}

#[cfg(not(tarpaulin_include))]
pub(crate) async fn run_dun(matches: &ArgMatches) -> Result<()> {
    // Logging
    if let Some(logger_config) = matches.value_of("logger-config") {
        log4rs::init_file(logger_config, log4rs::file::Deserializers::default())?;
    } else {
        env_logger::init();
    }
    version::log();
    eprintln!("allocator: {}", crate::alloc::get_allocator_name());

    #[cfg(feature = "bert")]
    {
        let d = tch::Device::cuda_if_available();
        if d.is_cuda() {
            eprintln!("CUDA is supported");
        } else {
            eprintln!("CUDA is NOT  supported, falling back to the CPU");
        }
    }
    if let Some(pid_file) = matches.value_of("pid") {
        let mut file = file::create(pid_file)
            .map_err(|e| Error::from(format!("Failed to create pid file `{}`: {}", pid_file, e)))?;

        file.write(format!("{}\n", std::process::id()).as_ref())
            .map_err(|e| Error::from(format!("Failed to write pid to `{}`: {}", pid_file, e)))?;
    }

    let l: u32 = matches
        .value_of("recursion-limit")
        .and_then(|l| l.parse().ok())
        .ok_or_else(|| Error::from("invalid recursion limit"))?;
    tremor_script::RECURSION_LIMIT.store(l, Ordering::Relaxed);

    let storage_directory = matches
        .value_of("storage-directory")
        .map(std::string::ToString::to_string);
    // TODO: Allow configuring this for offramps and pipelines
    let (world, handle) = World::start(64, storage_directory).await?;

    if let Some(config_files) = matches.values_of("artefacts") {
        // We process trickle files first
        for config_file in config_files.clone() {
            match get_source_kind(config_file) {
                SourceKind::Trickle => tremor_runtime::load_query_file(&world, config_file).await?,
                _ => continue,
            };
        }
        // We process config files thereafter
        for config_file in config_files {
            match get_source_kind(config_file) {
                SourceKind::Trickle | SourceKind::Tremor => continue,
                _ => tremor_runtime::load_cfg_file(&world, config_file).await?,
            };
        }
    }

    if !matches.is_present("no-api") {
        let host = matches
            .value_of("api-host")
            .ok_or_else(|| Error::from("host argument missing"))?;
        let app = api_server(&world)?;
        eprintln!("Listening at: http://{}", host);
        info!("Listening at: http://{}", host);

        if let Err(e) = app.listen(host).await {
            error!("API Error: {}", e);
        }
        warn!("API stopped");
        world.stop().await?;
    }

    handle.await?;
    warn!("World stopped");
    Ok(())
}

fn server_run(matches: &ArgMatches) -> Result<()> {
    version::print();
    if let Err(ref e) = task::block_on(run_dun(matches)) {
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
    } else {
        Ok(())
    }
}

#[cfg(not(tarpaulin_include))]

pub(crate) fn run_cmd(mut app: App, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("run") {
        server_run(matches)
    } else {
        app.print_long_help()
            .map_err(|e| Error::from(format!("Failed to print help: {}", e)))?;
        // ALLOW: main.rs
        ::std::process::exit(1);
    }
}
