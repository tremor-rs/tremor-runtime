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

// #![deny(warnings)]
// This isn't a external crate so we don't worry about docs
// #![deny(missing_docs)]
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

use crate::errors::Result;
use clap::Parser;
use cli::{Cli, Command};
use std::fs::File;
use std::path::{Path, PathBuf};
use tremor_common::file;
// use tremor_runtime::errors;

mod alloc;
// mod api;
mod completions;
mod debug;
mod doc;
mod env;
mod errors;
// mod explain;
pub(crate) mod cli;
mod job;
mod report;
mod run;
mod server;
pub(crate) mod status;
mod test;
mod util;

pub(crate) fn open_file<S>(path: &S, base: Option<&String>) -> Result<File>
where
    S: AsRef<Path> + ?Sized,
{
    match file::open(&path) {
        Ok(f) => Ok(f),
        Err(tremor_common::Error::FileOpen(io_error, msg)) => {
            let msg = if let Some(base) = base {
                let mut p = PathBuf::from(base);
                p.push(path);
                if let Ok(f) = file::open(&p) {
                    return Ok(f);
                }
                format!(
                    "Failed to open `{}` (or  `{}`)",
                    path.as_ref().display(),
                    p.to_str().unwrap_or("<unknown>")
                )
            } else {
                msg
            };
            Err(tremor_common::Error::FileOpen(io_error, msg).into())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(tarpaulin_include))]
#[async_std::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    tremor_runtime::functions::load()?;
    unsafe {
        // We know that instance will only get set once at
        // the very beginning nothing can access it yet,
        // this makes it allowable to use unsafe here.
        let s = &cli.instance;
        // ALLOW: We do this on startup and forget the memory once we drop it, that's on purpose
        let forget_s = std::mem::transmute(s as &str);
        // This means we're going to LEAK this memory, however
        // it is fine since as we do actually need it for the
        // rest of the program execution.
        tremor_runtime::INSTANCE = forget_s;
    }
    if let Err(e) = run(cli).await {
        eprintln!("error: {}", e);
        // ALLOW: this is supposed to exit
        std::process::exit(1);
    }
    Ok(())
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Completions { shell } => completions::run_cmd(shell),
        Command::Server { command } => {
            command.run().await;
            Ok(())
        }
        Command::Test(t) => t.run().await,
        Command::Dbg(d) => d.run(),
        Command::Run(r) => r.run(),
        Command::Doc(d) => d.run(),
        Command::Api(_) => todo!(),
        // Some(("api", Some(matches))) => task::block_on(api::run_cmd(
        //     TremorApp {
        //         format,
        //         config: load_config()?,
        //     },
        //     matches,
        // )),
    }
}
