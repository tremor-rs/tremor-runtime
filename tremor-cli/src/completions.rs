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

use crate::{cli::Cli, errors::Result};
use clap::{self, CommandFactory, ValueEnum};
use clap_complete::{generate, shells::Shell};
use std::path::Path;

const ERR_MSG: &str =
    "Unable to guess your shell, please provide an explicit shell to create completions for.";

pub(crate) fn run_cmd(shell: Option<Shell>) -> Result<()> {
    let shell = shell.map_or_else(guess_shell, Ok)?;
    let mut app = Cli::command();
    generate(shell, &mut app, "tremor", &mut std::io::stdout());
    Ok(())
}

fn guess_shell() -> Result<Shell> {
    if std::env::var_os("ZSH_NAME").is_some() {
        Ok(Shell::Zsh)
    } else if std::env::var_os("PSModulePath").is_some() {
        Ok(Shell::PowerShell)
    } else {
        let shell = std::env::var_os("SHELL")
            .and_then(|s| Path::new(&s).file_name().map(std::ffi::OsStr::to_os_string))
            .ok_or(ERR_MSG)?;
        let shell = shell.into_string().map_err(|_| ERR_MSG)?;
        let shell = Shell::from_str(&shell, true)?;
        Ok(shell)
    }
}
