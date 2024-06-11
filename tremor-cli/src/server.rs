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

use crate::util::{get_source_kind, SourceKind};
use crate::{
    cli::{ServerCommand, ServerRun},
    errors::{Error, ErrorKind, Result},
};
use futures::{future, StreamExt};
use signal_hook::consts::signal::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook::low_level::signal_name;
use signal_hook_tokio::Signals;
use std::io::Write;
use std::sync::atomic::Ordering;
use tremor_api as api;
use tremor_common::file;
use tremor_runtime::system::Runtime;
use tremor_runtime::version;
use tremor_system::killswitch::ShutdownMode;

macro_rules! log_and_print_error {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
        error!($($arg)*);
    };
}

async fn handle_signals(mut signals: Signals, world: Runtime) {
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
impl ServerRun {
    #[allow(clippy::too_many_lines)]
    async fn run_dun(&self) -> Result<i32> {
        let mut result = 0;

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
        if let Some(pid_file) = &self.pid {
            let mut file = file::create(pid_file)
                .map_err(|e| Error::from(format!("Failed to create pid file `{pid_file}`: {e}")))?;

            file.write(format!("{}\n", std::process::id()).as_ref())
                .map_err(|e| Error::from(format!("Failed to write pid to `{pid_file}`: {e}")))?;
        }

        tremor_script::RECURSION_LIMIT.store(self.recursion_limit, Ordering::Relaxed);

        let (world, handle) = if self.debug_connectors {
            Runtime::builder()
                .default_include_connectors()
                .build()
                .await?
        } else {
            Runtime::builder()
                .without_debug_connectors()
                .default_include_connectors()
                .build()
                .await?
        };

        // signal handling
        let signals = Signals::new([SIGTERM, SIGINT, SIGQUIT])?;
        let signal_handle = signals.handle();
        let signal_handler_task = tokio::task::spawn(handle_signals(signals, world.clone()));

        let mut troy_files = Vec::with_capacity(16);
        let mut archives = Vec::with_capacity(16);
        // We process trickle files first
        for config_file in &self.artefacts {
            let kind = get_source_kind(config_file);
            if kind == SourceKind::Troy {
                troy_files.push(config_file);
            } else if kind == SourceKind::Archive {
                archives.push(config_file);
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

        // We process archives  thereafter
        for archive_file in archives {
            let mut archive = tremor_common::asy::file::open(&archive_file).await?;

            if let Err(e) = tremor_runtime::load_archive(&world, &mut archive, "main", None).await {
                return Err(ErrorKind::FileLoadError(archive_file.to_string(), e).into());
            }
        }
        let api_handle = if self.no_api {
            // dummy task never finishing
            tokio::task::spawn(async move {
                future::pending::<()>().await;
                Ok(())
            })
        } else {
            eprintln!("Listening at: http://{}", &self.api_host);
            info!("Listening at: http://{}", &self.api_host);
            api::serve(self.api_host.clone(), &world)
        };
        // waiting for either
        match future::select(handle, api_handle).await {
            future::Either::Left((manager_res, api_handle)) => {
                // manager stopped
                if let Err(e) = manager_res {
                    error!("Manager failed with: {}", e);
                    result = 1;
                }
                api_handle.abort();
            }
            future::Either::Right((_api_res, manager_handle)) => {
                // api stopped
                if let Err(e) = world.stop(ShutdownMode::Graceful).await {
                    error!("Error shutting down gracefully: {}", e);
                    result = 2;
                }
                manager_handle.abort();
            }
        };
        signal_handle.close();
        signal_handler_task.abort();
        warn!("Tremor stopped.");
        Ok(result)
    }

    async fn run(&self) {
        version::print();
        match self.run_dun().await {
            Err(e) => {
                match e {
                    Error(ErrorKind::AnyhowError(anyhow_e), _) => {
                        log_and_print_error!("{:?}", anyhow_e);
                    }
                    e => {
                        log_and_print_error!("Error: {}", e);
                        for e in e.iter().skip(1) {
                            eprintln!("Caused by: {e}");
                        }
                    }
                }
                log_and_print_error!("We are SHUTTING DOWN due to errors during initialization!");

                // ALLOW: we are purposefully exiting the process here
                ::std::process::exit(1);
            }
            Ok(res) => {
                // ALLOW: we are purposefully exiting the process here
                ::std::process::exit(res)
            }
        }
    }
}

impl ServerCommand {
    pub(crate) async fn run(&self) {
        match self {
            ServerCommand::Run(cmd) => cmd.run().await,
        }
    }
}
