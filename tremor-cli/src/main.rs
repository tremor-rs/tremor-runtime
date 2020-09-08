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

// #![forbid(warnings)]
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

use crate::errors::Result;
use crate::util::*;
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

#[cfg_attr(tarpaulin, skip)]
fn main() -> Result<()> {
    let yaml = load_yaml!("./cli.yaml");
    let app = App::from(yaml);
    let app = app.version(option_env!("CARGO_PKG_VERSION").unwrap_or(""));
    let app = app.global_setting(AppSettings::ColoredHelp);
    let app = app.global_setting(AppSettings::ColorAlways);
    let matches = app.clone().get_matches();
    run(app, matches)
}

fn run(mut app: App, cmd: ArgMatches) -> Result<()> {
    let format = match &cmd.value_of("format") {
        Some("json") => FormatKind::Json,
        _ => FormatKind::Yaml,
    };

    let mut config = TremorApp {
        format,
        config: load_config()?,
    };

    if let Some(_matches) = cmd.subcommand_matches("explain") {
        return Err("Not yet implemented".into())
    } else if let Some(matches) = cmd.subcommand_matches("completions") {
        completions::run_cmd(app, matches)?;
    } else if let Some(matches) = cmd.subcommand_matches("server") {
        server::run_cmd(matches)?;
    } else if let Some(matches) = cmd.subcommand_matches("run") {
        run::run_cmd(matches.clone())?;
    } else if let Some(matches) = cmd.subcommand_matches("doc") {
        doc::run_cmd(&matches)?;
    } else if let Some(matches) = cmd.subcommand_matches("api") {
        task::block_on(api::run_cmd(&mut config, &matches))?;
    } else if let Some(matches) = cmd.subcommand_matches("dbg") {
        debug::run_cmd(&matches)?;
    } else if let Some(matches) = cmd.subcommand_matches("test") {
        test::run_cmd(&matches)?;
    } else {
        app.print_long_help().ok();
    }
    Ok(())
}
