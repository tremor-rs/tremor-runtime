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

use async_std::prelude::FutureExt;
use time::Instant;

use crate::errors::{Error, ErrorKind, Result};
use crate::util::slurp_string;
use crate::{target_process, target_process::TargetProcess};
use std::{
    collections::HashMap,
    fs,
    time::{self, Duration},
};

use std::path::Path;
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
pub(crate) struct Before {
    #[serde(default = "default_dir")]
    dir: String,
    cmd: String,
    #[serde(default = "Default::default")]
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

fn default_dir() -> String {
    String::from(".")
}

fn default_max_await_secs() -> u64 {
    0 // No delay by default as many tests won't depend on conditional resource allocation
}

fn default_min_await_secs() -> u64 {
    0 // Wait for at least 1 seconds before starting tests that depend on background process
}

impl Before {
    pub(crate) async fn spawn(
        &self,
        base: &Path,
        env: &HashMap<String, String>,
        num: usize,
    ) -> Result<TargetProcess> {
        let cmd = target_process::which(&self.cmd)?;
        // interpret `dir` as relative to `base`
        let current_working_dir = base.join(&self.dir).canonicalize()?;
        let mut env = env.clone();
        for (k, v) in &self.env {
            env.insert(k.clone(), v.clone());
        }
        let mut process = target_process::TargetProcess::new_in_dir(
            &cmd,
            &self.args,
            &env,
            &current_working_dir,
        )?;

        let fg_out_file = base.join(format!("before.out.{num}.log"));
        let fg_err_file = base.join(format!("before.err.{num}.log"));
        process.stdio_tailer(&fg_out_file, &fg_err_file).await?;

        debug!(
            "Spawning before: {} in {}",
            self.cmdline(),
            current_working_dir.display()
        );

        self.wait_for(&mut process, base).await?;
        debug!("Before process ready.");
        Ok(process)
    }

    fn cmdline(&self) -> String {
        format!(
            "{}{} {}",
            self.env
                .iter()
                .map(|(k, v)| format!("{}={} ", k, v))
                .collect::<String>(),
            self.cmd,
            self.args.join(" ")
        )
    }

    pub(crate) async fn wait_for(
        &self,
        process: &mut target_process::TargetProcess,
        base: &Path,
    ) -> Result<()> {
        let start = Instant::now();
        if let Some(conditions) = &self.conditionals {
            loop {
                let mut success = true;

                if start.elapsed() > Duration::from_secs(self.until) {
                    let msg = format!(
                        "Before command: {} did not fulfil conditions {:?} and timed out after {}s",
                        self.cmdline(),
                        self.conditionals,
                        self.until
                    );
                    let err = Error::from(msg);
                    return Err(err);
                }
                for (k, v) in conditions.iter() {
                    match k.as_str() {
                        "port-open" => {
                            for port in v {
                                if let Ok(port) = port.parse::<u16>() {
                                    success &= port_scanner::scan_port(port);
                                }
                            }
                        }
                        "wait-for-ms" => {
                            success &= v
                                .first()
                                .and_then(|delay| delay.parse().ok())
                                .map(|delay| start.elapsed() > Duration::from_millis(delay))
                                .unwrap_or_default();
                        }
                        "http-ok" => {
                            for endpoint in v {
                                let res = surf::get(endpoint)
                                    .send()
                                    .timeout(Duration::from_secs(self.until.min(5)))
                                    .await?;
                                success &= match res {
                                    Ok(res) => res.status().is_success(),
                                    Err(_) => false,
                                }
                            }
                        }
                        "file-exists" => {
                            let base_dir = base.join(&self.dir);
                            for f in v {
                                if let Ok(path) = base_dir.join(f).canonicalize() {
                                    debug!("Checking for existence of {}", path.display());
                                    success &= path.exists();
                                }
                            }
                        }
                        "status" => {
                            let code = process.wait().await?.code().unwrap_or(99);
                            success &= v
                                .first()
                                .and_then(|code| code.parse::<i32>().ok())
                                .map(|expected_code| expected_code == code)
                                .unwrap_or_default();
                        }
                        _ => (),
                    }
                }
                if success {
                    break;
                }
                // do not overload the system, try a little (100ms) tenderness
                async_std::task::sleep(Duration::from_millis(100)).await;
            }
        }
        if self.before_start_delay > 0 {
            let dur = Duration::from_secs(self.before_start_delay);
            debug!("Sleeping for {}s ...", self.before_start_delay);
            async_std::task::sleep(dur).await;
        }
        Ok(())
    }
}

// load all the before definitions from before.yaml files
pub(crate) fn load_before_defs(path: &Path) -> Result<Vec<Before>> {
    let tags_data = slurp_string(path)?;
    match serde_yaml::from_str(&tags_data) {
        Ok(s) => Ok(s),
        Err(e) => Err(Error::from(format!(
            "Invalid `before.yaml` in path `{}`: {}",
            path.to_string_lossy(),
            e
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct BeforeController {
    base: PathBuf,
    env: HashMap<String, String>,
}

impl BeforeController {
    pub(crate) fn new(base: &Path, env: &HashMap<String, String>) -> Self {
        Self {
            base: base.to_path_buf(),
            env: env.clone(),
        }
    }

    pub(crate) async fn spawn(&mut self) -> Result<()> {
        let root = &self.base;
        let before_path = root.join("before.yaml");
        if before_path.exists() {
            let before_defs = load_before_defs(&before_path);
            match before_defs {
                Ok(before_defs) => {
                    for (i, before_def) in before_defs.into_iter().enumerate() {
                        let cmdline = before_def.cmdline();
                        let mut process = before_def.spawn(root, &self.env, i).await?;
                        async_std::task::spawn(async move {
                            let status = process.join().await?;
                            match status.code() {
                                None => {
                                    eprintln!("Before process {cmdline} terminated by some signal");
                                }
                                Some(0) => (),
                                Some(other) => {
                                    eprintln!("Before process {cmdline} terminated with exit code {other}");
                                }
                            }
                            Ok::<(), Error>(())
                        });
                    }
                    Ok(())
                }
                Err(Error(ErrorKind::Common(tremor_common::Error::FileOpen(_, _)), _)) => {
                    // no before yaml found, all good
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        }
    }
}

pub(crate) fn update_evidence(root: &Path, evidence: &mut HashMap<String, String>) -> Result<()> {
    let bg_out_file = root.join("before.out.log");
    let bg_err_file = root.join("before.err.log");

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
