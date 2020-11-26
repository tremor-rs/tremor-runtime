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

use crate::errors::{Error, Result};
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
        _ => Err(format!("Unsupported shell: {}", shell).into()),
    }
}

fn guess_shell(app: clap::App) -> Result<()> {
    let shell = if std::env::var_os("ZSH_NAME").is_some() {
        Ok("zsh".to_string())
    } else if std::env::var_os("PSModulePath").is_some() {
        Ok("powershell".to_string())
    } else {
        match std::env::var_os("SHELL")
            .and_then(|s| Path::new(&s).file_name().map(std::ffi::OsStr::to_os_string))
        {
            Some(shell) => {
                #[allow(clippy::map_err_ignore)] // Error is OsString, unusable here
                shell.into_string().map_err(|_| Error::from(ERR_MSG))
            }
            None => Err(ERR_MSG.into()),
        }
    }?;
    generate_for_shell(app, &shell)
}
