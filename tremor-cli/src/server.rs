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

use crate::errors::{Error, ErrorKind, Result};
use crate::util::{get_source_kind, SourceKind};
use anyhow::Context;
use async_std::stream::StreamExt;
use clap::{App, ArgMatches};
use futures::future;
use signal_hook::consts::signal::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook::low_level::signal_name;
use signal_hook_async_std::Signals;
use std::io::Write;
use std::sync::atomic::Ordering;
use tremor_api as api;
use tremor_common::file;
use tremor_runtime::system::{ShutdownMode, World};
use tremor_runtime::{self, version};

async fn handle_api_request<
    G: std::future::Future<Output = api::Result<tide::Response>>,
    F: Fn(api::Request) -> G,
>(
    req: api::Request,
    handler_func: F,
) -> tide::Result {
    let resource_type = api::accept(&req);

    // Handle request. If any api error is returned, serialize it into a tide response
    // as well, respecting the requested resource type. (and if there's error during
    // this serialization, fall back to the error's conversion into tide response)
    handler_func(req).await.or_else(|api_error| {
        api::serialize_error(resource_type, api_error)
            .or_else(|e| Ok(Into::<tide::Response>::into(e)))
    })
}

fn api_server(world: &World) -> tide::Server<api::State> {
    let mut app = tide::Server::with_state(api::State {
        world: world.clone(),
    });

    app.at("/version")
        .get(|r| handle_api_request(r, api::version::get));

    app
}

async fn handle_signals(signals: Signals, world: World) {
    let mut signals = signals.fuse();

    while let Some(signal) = signals.next().await {
        info!(
            "Received SIGNAL: {}",
            signal_name(signal).unwrap_or(&signal.to_string())
        );
        match signal {
            SIGINT | SIGTERM => {
                if let Err(_e) = world.stop(ShutdownMode::Graceful).await {
                    if let Err(e) = signal_hook::low_level::emulate_default_handler(signal) {
                        error!("Error handling signal {}: {}", signal, e);
                    }
                }
            }
            SIGQUIT => {
                if let Err(_e) = world.stop(ShutdownMode::Forceful).await {
                    if let Err(e) = signal_hook::low_level::emulate_default_handler(signal) {
                        error!("Error handling signal {}: {}", signal, e);
                    }
                }
            }
            signal => {
                if let Err(e) = signal_hook::low_level::emulate_default_handler(signal) {
                    error!("Error handling signal {}: {}", signal, e);
                }
            }
        }
    }
}

#[cfg(not(tarpaulin_include))]
#[allow(clippy::too_many_lines)]
#[must_use]
pub(crate) async fn run_dun(matches: &ArgMatches) -> Result<i32> {
    let mut result = 0;
    use tremor_runtime::system::WorldConfig;

    // Logging
    if let Some(logger_config) = matches.value_of("logger-config") {
        if let Err(e) = log4rs::init_file(logger_config, log4rs::config::Deserializers::default())
            .with_context(|| format!("Error loading logger-config from '{}'", logger_config))
        {
            return Err(e.into());
        }
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
            eprintln!("CUDA is NOT supported, falling back to the CPU");
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
    let config = WorldConfig {
        storage_directory,
        debug_connectors: matches.is_present("debug-connectors"),
        ..WorldConfig::default()
    };

    let (world, handle) = World::start(config).await?;

    // signal handling
    let signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
    let signal_handle = signals.handle();
    let signal_handler_task = async_std::task::spawn(handle_signals(signals, world.clone()));

    if let Some(config_files) = matches.values_of("artefacts") {
        let mut troy_files = Vec::with_capacity(16);
        // We process trickle files first
        for config_file in config_files {
            let kind = get_source_kind(config_file);
            if kind == SourceKind::Troy {
                troy_files.push(config_file)
            } else {
                return Err(
                    ErrorKind::UnsupportedFileType(config_file.to_string(), kind, "troy").into(),
                );
            };
        }

        // We process config files thereafter
        for config_file in troy_files {
            if let Err(e) = tremor_runtime::load_troy_file(&world, config_file).await {
                return Err(ErrorKind::FileLoadError(config_file.to_string(), e).into());
            }
        }
    }

    let api_handle = if matches.is_present("no-api") {
        // dummy task never finishing
        async_std::task::spawn(async move {
            future::pending::<()>().await;
        })
    } else {
        let host = matches
            .value_of("api-host")
            .map(ToString::to_string)
            .ok_or_else(|| Error::from("host argument missing"))?;
        let app = api_server(&world);
        eprintln!("Listening at: http://{}", host);
        info!("Listening at: http://{}", host);

        async_std::task::spawn(async move {
            if let Err(e) = app.listen(host).await {
                error!("API Error: {}", e);
            }
            warn!("API stopped.");
        })
    };
    // waiting for either
    match future::select(handle, api_handle).await {
        future::Either::Left((manager_res, api_handle)) => {
            // manager stopped
            if let Err(e) = manager_res {
                error!("Manager failed with: {}", e);
                result = 1;
            }
            api_handle.cancel().await;
        }
        future::Either::Right((_api_res, manager_handle)) => {
            // api stopped
            if let Err(e) = world.stop(ShutdownMode::Graceful).await {
                error!("Error shutting down gracefully: {}", e);
                result = 2;
            }
            manager_handle.cancel().await;
        }
    };
    signal_handle.close();
    signal_handler_task.cancel().await;
    warn!("Tremor stopped.");
    Ok(result)
}

macro_rules! log_and_print_error {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
        error!($($arg)*);
    };
}

async fn server_run(matches: &ArgMatches) {
    version::print();
    match run_dun(matches).await {
        Err(e) => {
            match e {
                Error(ErrorKind::AnyhowError(anyhow_e), _) => {
                    log_and_print_error!("{:?}", anyhow_e);
                }
                e => {
                    log_and_print_error!("Error: {}", e);
                    for e in e.iter().skip(1) {
                        eprintln!("Caused by: {}", e);
                    }
                }
            }
            log_and_print_error!("We are SHUTTING DOWN due to errors during initialization!");

            // ALLOW: main.rs
            ::std::process::exit(1);
        }
        Ok(res) => ::std::process::exit(res),
    }
}

#[cfg(not(tarpaulin_include))]
pub(crate) async fn run_cmd(mut app: App<'_>, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("run") {
        server_run(matches).await;
        Ok(())
    } else {
        app.print_long_help()
            .map_err(|e| Error::from(format!("Failed to print help: {}", e)))?;
        // ALLOW: main.rs
        ::std::process::exit(1);
    }
}
