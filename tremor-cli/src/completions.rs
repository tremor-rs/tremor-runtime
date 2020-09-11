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

use crate::errors::Result;
use clap::{self, ArgMatches};
use clap_generate::{
    generate,
    generators::{Bash, Elvish, Fish, PowerShell, Zsh},
};

pub(crate) fn run_cmd(mut app: clap::App, matches: &ArgMatches) -> Result<()> {
    if matches.subcommand_matches("bash").is_some() {
        generate::<Bash, _>(&mut app, "tremor", &mut std::io::stdout());
    } else if matches.subcommand_matches("elvish").is_some() {
        generate::<Elvish, _>(&mut app, "tremor", &mut std::io::stdout());
    } else if matches.subcommand_matches("fish").is_some() {
        generate::<Fish, _>(&mut app, "tremor", &mut std::io::stdout());
    } else if matches.subcommand_matches("powershell").is_some() {
        generate::<PowerShell, _>(&mut app, "tremor", &mut std::io::stdout());
    } else if matches.subcommand_matches("zsh").is_some() {
        generate::<Zsh, _>(&mut app, "tremor", &mut std::io::stdout());
    } else {
        app.print_long_help().ok();
    }
    Ok(())
}
