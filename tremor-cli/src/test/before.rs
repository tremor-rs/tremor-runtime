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

use time::Instant;

use crate::errors::{Error, ErrorKind, Result};
use crate::util::slurp_string;
use crate::{job, job::TargetProcess};
use async_std::{future, task};
use std::{
    collections::HashMap,
    fs,
    time::{self, Duration},
};

use std::path::Path;
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
pub(crate) struct Before {
    run: String,
    cmd: String,
    args: Vec<String>,
    #[serde(default = "Default::default")]
    env: HashMap<String, String>,
    #[serde(rename = "await")]
    conditionals: Option<HashMap<String, Vec<String>>>,
    #[serde(rename = "max-await-secs", default = "default_max_await_secs")]
    until: u64,
    #[serde(rename = "min-await-secs", default = "default_min_await_secs")]
    before_start_delay: u64,
}

impl Before {
    pub(crate) fn spawn(&self) -> Result<Option<TargetProcess>> {
        let cmd = job::which(&self.cmd)?;
        let process = job::TargetProcess::new_with_stderr(&cmd, &self.args, &self.env)?;
        self.block_on()?;
        Ok(Some(process))
    }

    pub(crate) fn block_on(&self) -> Result<()> {
        let start = Instant::now();
        if let Some(conditions) = &self.conditionals {
            loop {
                let mut success = true;

                if start.elapsed() > Duration::from_secs(self.until) {
                    return Err("Upper bound exceeded error".into());
                }
                for (k, v) in conditions.iter() {
                    if "port-open" == k.as_str() {
                        for port in v {
                            if let Ok(port) = port.parse::<u16>() {
                                success &= port_scanner::scan_port(port);
                            }
                        }
                    }
                    if "wait-for-ms" == k.as_str() {
                        success &= v
                            .first()
                            .and_then(|delay| delay.parse().ok())
                            .map(|delay| start.elapsed() > Duration::from_millis(delay))
                            .unwrap_or_default();
                    }
                    if "http-ok" == k.as_str() {
                        for endpoint in v {
                            let res = task::block_on(future::timeout(
                                Duration::from_secs(self.until.min(5)),
                                surf::get(endpoint).send(),
                            ))?;
                            success &= match res {
                                Ok(res) => res.status().is_success(),
                                Err(_) => false,
                            }
                        }
                    }
                }
                if success {
                    break;
                }
                // do not overload the system, try a little (100ms) tenderness
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        std::thread::sleep(Duration::from_secs(self.before_start_delay));
        Ok(())
    }
}

fn default_max_await_secs() -> u64 {
    0 // No delay by default as many tests won't depend on conditional resource allocation
}

fn default_min_await_secs() -> u64 {
    0 // Wait for at least 1 seconds before starting tests that depend on background process
}

pub(crate) fn load_before(path: &Path) -> Result<Before> {
    let tags_data = slurp_string(path)?;
    match serde_json::from_str(&tags_data) {
        Ok(s) => Ok(s),
        Err(e) => Err(Error::from(format!(
            "Invalid `before.json` in path `{}`: {}",
            path.to_string_lossy(),
            e
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct BeforeController {
    base: PathBuf,
}

impl BeforeController {
    pub(crate) fn new(base: &Path) -> Self {
        Self {
            base: base.to_path_buf(),
        }
    }

    pub(crate) fn spawn(&mut self) -> Result<Option<TargetProcess>> {
        let root = &self.base;
        let before_path = root.join("before.json");
        if before_path.exists() {
            let before_json = load_before(&before_path);
            match before_json {
                Ok(before_json) => before_json.spawn(),
                Err(Error(ErrorKind::Common(tremor_common::Error::FileOpen(_, _)), _)) => {
                    // no before json found, all good
                    Ok(None)
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn capture(&mut self, process: Option<TargetProcess>) -> Result<()> {
        let root = self.base.clone();
        let bg_out_file = root.join("bg.out.log");
        let bg_err_file = root.join("bg.err.log");
        if let Some(mut process) = process {
            process.tail(&bg_out_file, &bg_err_file)?;
        };
        Ok(())
    }
}

pub(crate) fn update_evidence(root: &Path, evidence: &mut HashMap<String, String>) -> Result<()> {
    let bg_out_file = root.join("bg.out.log");
    let bg_err_file = root.join("bg.err.log");

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
