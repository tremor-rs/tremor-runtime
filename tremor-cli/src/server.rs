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

use crate::{
    cli::{ServerCommand, ServerRun},
    errors::{Error, ErrorKind, Result},
    util::{get_source_kind, SourceKind},
};
use async_std::stream::StreamExt;
use async_std::task;
use futures::future;
use signal_hook::consts::signal::*;
use signal_hook::low_level::signal_name;
use signal_hook_async_std::Signals;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_api as api;
use tremor_common::file;
use tremor_runtime::system::{ShutdownMode, World};
use tremor_runtime::{self, version};

impl ServerCommand {
    pub(crate) fn run(&self) {
        match self {
            ServerCommand::Run(c) => c.run(),
        }
    }
}
impl ServerRun {
    pub(crate) fn run(&self) {
        version::print();
        if let Err(ref e) = task::block_on(self.run_dun()) {
            error!("error: {}", e);
            for e in e.iter().skip(1) {
                error!("error: {}", e);
            }
            error!("We are SHUTTING DOWN due to errors during initialization!");

            // ALLOW: main.rs
            ::std::process::exit(1);
        }
    }
    #[cfg(not(tarpaulin_include))]
    pub(crate) async fn run_dun(&self) -> Result<()> {
        // Logging
        if let Some(logger_config) = &self.logger_config {
            log4rs::init_file(logger_config, log4rs::config::Deserializers::default())?;
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
        if let Some(pid_file) = &self.pid {
            let mut file = file::create(pid_file).map_err(|e| {
                Error::from(format!("Failed to create pid file `{}`: {}", pid_file, e))
            })?;

            file.write(format!("{}\n", std::process::id()).as_ref())
                .map_err(|e| {
                    Error::from(format!("Failed to write pid to `{}`: {}", pid_file, e))
                })?;
        }

        tremor_script::RECURSION_LIMIT.store(self.recursion_limit, Ordering::Relaxed);

        // TODO: Allow configuring this for offramps and pipelines
        let (world, handle) = World::start().await?;

        // signal handling
        let signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let signal_handle = signals.handle();
        let signal_handler_task = async_std::task::spawn(handle_signals(signals, world.clone()));

        let mut yaml_files = Vec::with_capacity(16);
        // We process trickle files first
        for config_file in &self.artefacts {
            let kind = get_source_kind(config_file);
            match kind {
                SourceKind::Trickle => {
                    if let Err(e) = tremor_runtime::load_query_file(&world, config_file).await {
                        return Err(ErrorKind::FileLoadError(config_file.to_string(), e).into());
                    }
                }
                SourceKind::Tremor | SourceKind::Json | SourceKind::Unsupported(_) => {
                    return Err(ErrorKind::UnsupportedFileType(
                        config_file.to_string(),
                        kind,
                        "yaml",
                    )
                    .into());
                }
                SourceKind::Yaml => yaml_files.push(config_file),
            };
        }

        // We process config files thereafter
        for config_file in yaml_files {
            if let Err(e) = tremor_runtime::load_cfg_file(&world, config_file).await {
                return Err(ErrorKind::FileLoadError(config_file.to_string(), e).into());
            }
        }

        let api_handle = if !self.no_api {
            let host = self.api_host.clone();
            let app = api_server(&world);
            eprintln!("Listening at: http://{}", host);
            info!("Listening at: http://{}", host);

            async_std::task::spawn(async move {
                if let Err(e) = app.listen(host).await {
                    error!("API Error: {}", e);
                }
                warn!("API stopped.");
            })
        } else {
            // dummy task never finishing
            async_std::task::spawn(async move { future::pending().await })
        };
        match future::select(handle, api_handle).await {
            future::Either::Left((manager_res, api_handle)) => {
                // manager stopped
                if let Err(e) = manager_res {
                    error!("Manager failed with: {}", e);
                }
                api_handle.cancel().await;
            }
            future::Either::Right((_api_res, manager_handle)) => {
                // api stopped
                if let Err(e) = world
                    .stop(ShutdownMode::Graceful {
                        timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
                    })
                    .await
                {
                    error!("Error shutting down gracefully: {}", e);
                }
                manager_handle.cancel().await;
            }
        };

        signal_handle.close();
        signal_handler_task.cancel().await;
        warn!("World stopped");
        Ok(())
    }
}

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
    app.at("/binding")
        .get(|r| handle_api_request(r, api::binding::list_artefact))
        .post(|r| handle_api_request(r, api::binding::publish_artefact));
    app.at("/binding/:aid")
        .get(|r| handle_api_request(r, api::binding::get_artefact))
        .delete(|r| handle_api_request(r, api::binding::unpublish_artefact));
    app.at("/binding/:aid/:sid")
        .get(|r| handle_api_request(r, api::binding::get_servant))
        .put(|r| handle_api_request(r, api::binding::spawn_instance))
        .delete(|r| handle_api_request(r, api::binding::shutdown_instance));
    app.at("/pipeline")
        .get(|r| handle_api_request(r, api::pipeline::list_artefact))
        .post(|r| handle_api_request(r, api::pipeline::publish_artefact));
    app.at("/pipeline/:aid")
        .get(|r| handle_api_request(r, api::pipeline::get_artefact))
        .delete(|r| handle_api_request(r, api::pipeline::unpublish_artefact));
    app.at("/onramp")
        .get(|r| handle_api_request(r, api::onramp::list_artefact))
        .post(|r| handle_api_request(r, api::onramp::publish_artefact));
    app.at("/onramp/:aid")
        .get(|r| handle_api_request(r, api::onramp::get_artefact))
        .delete(|r| handle_api_request(r, api::onramp::unpublish_artefact));
    app.at("/offramp")
        .get(|r| handle_api_request(r, api::offramp::list_artefact))
        .post(|r| handle_api_request(r, api::offramp::publish_artefact));
    app.at("/offramp/:aid")
        .get(|r| handle_api_request(r, api::offramp::get_artefact))
        .delete(|r| handle_api_request(r, api::offramp::unpublish_artefact));
    // connectors api
    app.at("/connector")
        .get(|r| handle_api_request(r, api::connector::list_artefacts))
        .post(|r| handle_api_request(r, api::connector::publish_artefact));
    app.at("/connector/:aid")
        .get(|r| handle_api_request(r, api::connector::get_artefact))
        .delete(|r| handle_api_request(r, api::connector::unpublish_artefact));
    app.at("/connector/:aid/:sid")
        .get(|r| handle_api_request(r, api::connector::get_instance))
        // pause, resume
        .patch(|r| handle_api_request(r, api::connector::patch_instance));

    app
}

const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

async fn handle_signals(signals: Signals, world: World) {
    let mut signals = signals.fuse();

    while let Some(signal) = signals.next().await {
        info!(
            "Received SIGNAL: {}",
            signal_name(signal).unwrap_or(&signal.to_string())
        );
        match signal {
            SIGINT | SIGTERM => {
                if let Err(_e) = world
                    .stop(ShutdownMode::Graceful {
                        timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
                    })
                    .await
                {
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
