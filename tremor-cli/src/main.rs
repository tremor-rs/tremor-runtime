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
extern crate serde;

#[macro_use]
extern crate log;

use crate::errors::Result;
use anyhow::Context;
use clap::Parser;
use cli::{Cli, Command};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use tremor_common::file;
// use tremor_runtime::errors;

mod alloc;
mod completions;
mod debug;
mod doc;
mod env;
mod errors;
// mod explain;
pub(crate) mod cli;
mod report;
mod run;
mod server;
pub(crate) mod status;
mod target_process;
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

// #[cfg_attr(coverage, no_coverage)]
#[tokio::main]
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
    let res = run(cli).await;

    if let Err(e) = res {
        eprintln!("error: {e}");
        // ALLOW: this is supposed to exit
        std::process::exit(1);
    }
    Ok(())
}

async fn run(cli: Cli) -> Result<()> {
    // Logging
    if let Some(logger_config) = &cli.logger_config {
        log4rs::init_file(logger_config, log4rs::config::Deserializers::default())
            .with_context(|| format!("Error loading logger-config from '{logger_config}'"))?;
    } else {
        env_logger::init();
    }

    match cli.command {
        Command::Completions { shell } => completions::run_cmd(shell),
        Command::Server { command } => {
            command.run().await;
            Ok(())
        }
        Command::Test(t) => t.run().await,
        Command::Dbg(d) => d.run(),
        Command::Run(r) => r.run().await,
        Command::Doc(d) => d.run(),
        Command::New { name } => create_template(std::env::current_dir()?, &name),
    }
}

fn create_template(mut path: PathBuf, name: &str) -> Result<()> {
    const MAIN_TROY: &str = include_str!("_template/main.troy");
    const FLOWS: &str = include_str!("_template/lib/flows.tremor");
    const PIPELINES: &str = include_str!("_template/lib/pipelines.tremor");
    const SCRIPTS: &str = include_str!("_template/lib/scripts.tremor");

    print!("Creating new tremor project: `{name}`");
    path.push(name);
    let mut lib_path = path.clone();
    lib_path.push("lib");
    print!(".");
    fs::create_dir(&path)?;
    print!(".");
    fs::create_dir(&lib_path)?;

    let mut file_path = path.clone();
    file_path.push("main.troy");
    print!(".");
    fs::write(&file_path, MAIN_TROY)?;

    let mut file_path = lib_path.clone();
    file_path.push("flows.tremor");
    print!(".");
    fs::write(&file_path, FLOWS)?;

    let mut file_path = lib_path.clone();
    file_path.push("pipelines.tremor");
    print!(".");
    fs::write(&file_path, PIPELINES)?;

    let mut file_path = lib_path.clone();
    file_path.push("scripts.tremor");
    print!(".");
    fs::write(&file_path, SCRIPTS)?;

    println!("done.\n");
    println!(
        "To run:\n TREMOR_PATH=\"${{TREMOR_PATH}}:${{PWD}}/{name}\" tremor run {name}/main.troy"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_dir::TempDir;
    #[test]
    fn test_template() -> Result<()> {
        let d = TempDir::new()?;
        let name = "template";
        create_template(PathBuf::from(d.path()), name)?;
        let mut template_root = d.child(name);

        let mut main = template_root.clone();
        main.push("main.troy");
        assert!(main.is_file());

        template_root.push("lib");

        let mut flows = template_root.clone();
        flows.push("flows.tremor");
        assert!(flows.is_file());

        let mut pipelines = template_root.clone();
        pipelines.push("pipelines.tremor");
        assert!(pipelines.is_file());

        let mut scripts = template_root.clone();
        scripts.push("scripts.tremor");
        assert!(scripts.is_file());

        Ok(())
    }
}
