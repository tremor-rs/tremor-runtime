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
use std::path::Path;

const ERR_MSG: &str =
    "Unable to guess your shell, please provide an explicit shell to create completions for.";

pub(crate) fn run_cmd(app: clap::App, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some((shell, _)) => generate_for_shell(app, shell),
        None => guess_shell(app),
    }
}

fn generate_for_shell(mut app: clap::App, shell: &str) -> Result<()> {
    match shell {
        "bash" => Ok(generate::<Bash, _>(
            &mut app,
            "tremor",
            &mut std::io::stdout(),
        )),
        "elvish" => Ok(generate::<Elvish, _>(
            &mut app,
            "tremor",
            &mut std::io::stdout(),
        )),
        "fish" => Ok(generate::<Fish, _>(
            &mut app,
            "tremor",
            &mut std::io::stdout(),
        )),
        "powershell" => Ok(generate::<PowerShell, _>(
            &mut app,
            "tremor",
            &mut std::io::stdout(),
        )),
        "zsh" => Ok(generate::<Zsh, _>(
            &mut app,
            "tremor",
            &mut std::io::stdout(),
        )),
        _ => Err("Unsupported shell".into()),
    }
}

fn guess_shell(app: clap::App) -> Result<()> {
    if let Some(_) = std::env::var_os("ZSH_NAME") {
        generate_for_shell(app, "zsh")
    } else if let Some(_) = std::env::var_os("PSModulePath") {
        generate_for_shell(app, "powershell")
    } else if let Some(shell) = std::env::var_os("SHELL") {
        if let Some(shell_str) = Path::new(&shell.into_string().map_err(|_| "")?).file_name() {
            generate_for_shell(app, &shell_str.to_string_lossy()).map_err(|_| ERR_MSG.into())
        } else {
            Err(ERR_MSG.into())
        }
    } else {
        Err(ERR_MSG.into())
    }
}
