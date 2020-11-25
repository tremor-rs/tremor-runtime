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

use crate::errors::Result;
use clap::{self, ArgMatches};
use clap_generate::{
    generate,
    generators::{Bash, Elvish, Fish, PowerShell, Zsh},
};
use std::path::Path;

const ERR_MSG: &str =
    "Unable to guess your shell, please provide an explicit shell to create completions for.";

pub(crate) fn run_cmd(mut app: clap::App, matches: &ArgMatches) -> Result<()> {
    if let Some((shell, _)) = matches.subcommand() {
        generate_for_shell(app, shell)
    } else {
        // TODO There is no way in standard clap to narrow help to the subcommand
        app.print_long_help().map_err(|e| e.into())
    }
}

fn generate_for_shell(mut app: clap::App, shell: &str) -> Result<()> {
    match shell {
        "guess" => guess_shell(app),
        "bash" => {
            generate::<Bash, _>(&mut app, "tremor", &mut std::io::stdout());
            Ok(())
        }
        "elvish" => {
            generate::<Elvish, _>(&mut app, "tremor", &mut std::io::stdout());
            Ok(())
        }
        "fish" => {
            generate::<Fish, _>(&mut app, "tremor", &mut std::io::stdout());
            Ok(())
        }
        "powershell" => {
            generate::<PowerShell, _>(&mut app, "tremor", &mut std::io::stdout());
            Ok(())
        }
        "zsh" => {
            generate::<Zsh, _>(&mut app, "tremor", &mut std::io::stdout());
            Ok(())
        }
        _ => Err("Unsupported shell".into()),
    }
}

#[allow(clippy::map_err_ignore)]
fn guess_shell(app: clap::App) -> Result<()> {
    if std::env::var_os("ZSH_NAME").is_some() {
        generate_for_shell(app, "zsh")
    } else if std::env::var_os("PSModulePath").is_some() {
        generate_for_shell(app, "powershell")
    } else if let Some(shell) = std::env::var_os("SHELL") {
        if let Some(shell_str) = Path::new(&shell).file_name() {
            generate_for_shell(app, &shell_str.to_string_lossy()).map_err(|_| ERR_MSG.into())
        } else {
            Err(ERR_MSG.into())
        }
    } else {
        Err(ERR_MSG.into())
    }
}
