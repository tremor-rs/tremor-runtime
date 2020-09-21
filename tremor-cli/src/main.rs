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

// #![forbid(warnings)]
// This isn't a external crate so we don't worry about docs
// #![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(
    // TODO clippy claims some filter_map uses are hard to read - investigate
    clippy::unnecessary_filter_map,
    clippy::filter_map_next,
    clippy::filter_map
)]

#[macro_use]
extern crate serde_derive;
// This is silly but serde is forcing you to import serde if you want serde_derive
#[allow(unused_extern_crates)]
extern crate serde;

extern crate serde_yaml;
extern crate simd_json;

#[macro_use]
extern crate log;

extern crate rental;

use std::{ffi::OsStr, fs::File, path::Path};

use crate::errors::{Error, Result};
use crate::util::{load_config, FormatKind, TremorApp};
use async_std::task;
use clap::App;
use clap::{load_yaml, AppSettings, ArgMatches};
// use tremor_runtime::errors;

mod alloc;
mod api;
mod completions;
mod debug;
mod doc;
mod errors;
// mod explain;
mod job;
mod report;
mod run;
mod server;
pub(crate) mod status;
mod test;
mod util;

pub(crate) fn open_file<S>(path: &S, base: Option<&String>) -> Result<File>
where
    S: AsRef<OsStr> + ?Sized,
{
    let path = Path::new(path);
    match File::open(path) {
        Ok(f) => Ok(f),
        Err(e) => {
            if let Some(base) = base {
                let mut p = Path::new(base).to_path_buf();
                p.push(path);
                if let Ok(f) = File::open(&p) {
                    return Ok(f);
                }
                Err(Error::from(format!(
                    "Failed to open `{}` (or  `{}`): {}",
                    path.to_str().unwrap_or("<unknown>"),
                    p.to_str().unwrap_or("<unknown>"),
                    e
                )))
            } else {
                Err(Error::from(format!(
                    "Failed to open {}: {}",
                    path.to_str().unwrap_or("<unknown>"),
                    e
                )))
            }
        }
    }
}

#[cfg(not(tarpaulin_include))]
fn main() -> Result<()> {
    let yaml = load_yaml!("./cli.yaml");
    let app = App::from(yaml);
    let app = app.version(option_env!("CARGO_PKG_VERSION").unwrap_or(""));
    let app = app.global_setting(AppSettings::ColoredHelp);
    let app = app.global_setting(AppSettings::ColorAlways);
    let matches = app.clone().get_matches();
    if let Err(e) = run(app, &matches) {
        eprintln!("{}", e);
        // ALLOW: this is supposed to exit
        std::process::exit(1);
    }
    Ok(())
}

fn run(mut app: App, cmd: &ArgMatches) -> Result<()> {
    let format = match &cmd.value_of("format") {
        Some("json") => FormatKind::Json,
        _ => FormatKind::Yaml,
    };

    let mut config = TremorApp {
        format,
        config: load_config()?,
    };

    // if env TREMOR_PATH is relative, canonicalize it
    if let Ok(tremor_path) = std::env::var("TREMOR_PATH") {
        let tremor_path = Path::new(&tremor_path);
        if tremor_path.is_relative() {
            let canonical = std::fs::canonicalize(tremor_path)?;
            std::env::set_var("TREMOR_PATH", canonical);
        }
    }

    if let Some(_matches) = cmd.subcommand_matches("explain") {
        Err("Not yet implemented".into())
    } else if let Some(matches) = cmd.subcommand_matches("completions") {
        completions::run_cmd(app, matches)
    } else if let Some(matches) = cmd.subcommand_matches("server") {
        server::run_cmd(app, matches)
    } else if let Some(matches) = cmd.subcommand_matches("run") {
        run::run_cmd(&matches)
    } else if let Some(matches) = cmd.subcommand_matches("doc") {
        doc::run_cmd(&matches)
    } else if let Some(matches) = cmd.subcommand_matches("api") {
        task::block_on(api::run_cmd(&mut config, &matches))
    } else if let Some(matches) = cmd.subcommand_matches("dbg") {
        debug::run_cmd(&matches)
    } else if let Some(matches) = cmd.subcommand_matches("test") {
        test::run_cmd(&matches)
    } else {
        app.print_long_help()
            .map_err(|e| Error::from(format!("failed to print help: {}", e)))
    }
}
