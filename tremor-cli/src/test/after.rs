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

use crate::errors::{Error, Result};
use crate::job;
use crate::{job::TargetProcess, util::slurp_string};
use std::{collections::HashMap, fs, path::Path};

#[derive(Deserialize, Debug)]
pub(crate) struct After {
    run: String,
    cmd: String,
    args: Vec<String>,
}

impl After {
    pub(crate) fn spawn(&self, _base: &str) -> Result<Option<TargetProcess>> {
        let cmd = job::which(&self.cmd)?;

        let mut process = job::TargetProcess::new_with_stderr(&cmd, &self.args)?;
        process.wait_with_output()?;
        Ok(Some(process))
    }
}

pub(crate) fn load_after(path_str: &str) -> Result<After> {
    let tags_data = slurp_string(path_str)?;
    match serde_json::from_str(&tags_data) {
        Ok(s) => Ok(s),
        Err(_not_well_formed) => Err(Error::from(format!(
            "Unable to load `after.json` from path: {}",
            path_str
        ))),
    }
}

pub(crate) struct AfterController {
    base: String,
}

impl AfterController {
    pub(crate) fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
        }
    }

    pub(crate) fn spawn(&mut self) -> Result<()> {
        let root = &self.base;
        let after_str = &format!("{}/after.json", root);
        // This is optional
        if Path::new(after_str).is_file() {
            let after_json = load_after(after_str)?;
            let after_process = after_json.spawn(root)?;
            if let Some(mut process) = after_process {
                let after_out_file = format!("{}/after.out.log", root);
                let after_err_file = format!("{}/after.err.log", root);
                let after_process = std::thread::spawn(move || {
                    if let Err(e) = process.tail(&after_out_file, &after_err_file) {
                        eprintln!("failed to tail tremor process: {}", e);
                    }
                });

                if after_process.join().is_err() {
                    return Err("Failed to join test after thread/process error".into());
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn update_evidence(root: &str, evidence: &mut HashMap<String, String>) -> Result<()> {
    let after_out_file = format!("{}/after.out.log", &root);
    let after_err_file = format!("{}/after.err.log", &root);

    if let Ok(x) = fs::metadata(&after_out_file) {
        if x.is_file() {
            evidence.insert("after: stdout".to_string(), slurp_string(&after_out_file)?);
        }
    }
    if let Ok(x) = fs::metadata(&after_err_file) {
        if x.is_file() {
            evidence.insert("before: stderr".to_string(), slurp_string(&after_err_file)?);
        }
    }

    Ok(())
}
