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
use crate::util::nanotime;
use crate::{job::TargetProcess, util::slurp_string};
use std::{collections::HashMap, fs};

#[derive(Deserialize, Debug)]
pub(crate) struct Before {
    run: String,
    cmd: String,
    args: Vec<String>,
    #[serde(rename = "await")]
    conditionals: Option<HashMap<String, Vec<String>>>,
    #[serde(rename = "max-await-secs", default = "default_max_await_secs")]
    until: u16,
}

impl Before {
    pub(crate) fn spawn(&self) -> Result<Option<TargetProcess>> {
        let cmd = job::which(&self.cmd)?;

        let process = job::TargetProcess::new_with_stderr(&cmd, &self.args)?;
        self.block_on()?;
        Ok(Some(process))
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn block_on(&self) -> Result<()> {
        if let Some(conditions) = &self.conditionals {
            for (k, v) in conditions.iter() {
                match k.as_str() {
                    "port-open" => {
                        let epoch = nanotime();
                        for port in v {
                            loop {
                                let now = nanotime();
                                if ((now - epoch) / 1_000_000_000) as u16 > self.until {
                                    return Err("Upper bound exceeded error".into());
                                }
                                if let Ok(port) = port.parse::<u16>() {
                                    if !port_scanner::scan_port(port) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    _default => (),
                }
            }
        }
        Ok(())
    }
}
fn default_max_await_secs() -> u16 {
    0 // No delay by default as many tests won't depend on conditional resource allocation
}

pub(crate) fn load_before(path_str: &str) -> Result<Before> {
    let tags_data = slurp_string(path_str)?;
    match serde_json::from_str(&tags_data) {
        Ok(s) => Ok(s),
        Err(_) => Err(Error::from(format!(
            "Unable to load `before.json` from path: {}",
            path_str
        ))),
    }
}

pub(crate) struct BeforeController {
    base: String,
}

impl BeforeController {
    pub(crate) fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
        }
    }

    pub(crate) fn spawn(&mut self) -> Result<Option<TargetProcess>> {
        let root = &self.base;
        let before_str = &format!("{}/before.json", root);
        let before_json = load_before(before_str);
        match before_json {
            Ok(before_json) => {
                let ret = before_json.spawn();
                before_json.block_on()?;
                ret
            }
            Err(_not_found) => Ok(None),
        }
    }

    pub(crate) fn capture(&mut self, process: Option<TargetProcess>) -> Result<()> {
        let root = self.base.clone();
        let bg_out_file = format!("{}/bg.out.log", &root);
        let bg_err_file = format!("{}/bg.err.log", &root);
        if let Some(mut process) = process {
            process.tail(&bg_out_file, &bg_err_file)?;
        };
        Ok(())
    }
}

pub(crate) fn update_evidence(root: &str, evidence: &mut HashMap<String, String>) -> Result<()> {
    let bg_out_file = format!("{}/bg.out.log", &root);
    let bg_err_file = format!("{}/bg.err.log", &root);

    if let Ok(x) = fs::metadata(&bg_out_file) {
        if x.is_file() {
            evidence.insert("before: stdout".to_string(), slurp_string(&bg_out_file)?);
        }
    }
    if let Ok(x) = fs::metadata(&bg_err_file) {
        if x.is_file() {
            evidence.insert("before: stderr".to_string(), slurp_string(&bg_err_file)?);
        }
    }

    Ok(())
}
