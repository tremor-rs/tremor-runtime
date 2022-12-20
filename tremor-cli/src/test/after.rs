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

use crate::errors::{Error, Result};
use crate::target_process;
use crate::target_process::TargetProcess;
use crate::util::slurp_string;
use std::path::PathBuf;
use std::{collections::HashMap, fs, path::Path};

#[derive(Deserialize, Debug)]
pub(crate) struct After {
    dir: String,
    cmd: String,
    args: Vec<String>,
    #[serde(default = "Default::default")]
    env: HashMap<String, String>,
}

impl After {
    pub(crate) async fn spawn(
        &self,
        base: &Path,
        env: &HashMap<String, String>,
    ) -> Result<Option<TargetProcess>> {
        let cmd = target_process::which(&self.cmd)?;
        // interpret `dir` as relative to `base`
        let current_dir = base.join(&self.dir).canonicalize()?;

        let mut env = env.clone();
        for (k, v) in &self.env {
            env.insert(k.clone(), v.clone());
        }
        let mut process =
            target_process::TargetProcess::new_in_dir(&cmd, &self.args, &env, &current_dir)?;
        let out_file = base.join("after.out.log");
        let err_file = base.join("after.err.log");
        process.stdio_tailer(&out_file, &err_file).await?;

        debug!(
            "Spawning after: {} in {}",
            self.cmdline(),
            current_dir.display()
        );

        Ok(Some(process))
    }

    fn cmdline(&self) -> String {
        format!(
            "{}{} {}",
            self.env
                .iter()
                .map(|(k, v)| format!("{k}={v} "))
                .collect::<String>(),
            self.cmd,
            self.args.join(" ")
        )
    }
}

pub(crate) fn load_after_defs(path: &Path) -> Result<Vec<After>> {
    let after_data = slurp_string(path)?;
    match serde_yaml::from_str::<Vec<After>>(&after_data) {
        Ok(afters) => Ok(afters),
        Err(_no_vec) => match serde_yaml::from_str::<After>(&after_data) {
            Ok(after) => Ok(vec![after]),
            Err(_e) => Err(Error::from(format!(
                "Unable to load `after.yaml` from path: {}",
                path.display()
            ))),
        },
    }
}

pub(crate) struct AfterController {
    base: PathBuf,
    env: HashMap<String, String>,
}

impl AfterController {
    pub(crate) fn new(base: &Path, env: &HashMap<String, String>) -> Self {
        Self {
            base: base.to_path_buf(),
            env: env.clone(),
        }
    }

    pub(crate) async fn spawn(&mut self) -> Result<()> {
        let root = &self.base;
        let after_path = root.join("after.yaml");
        // This is optional
        if after_path.is_file() {
            let after_jsons = load_after_defs(&after_path)?;
            for (i, after_json) in after_jsons.into_iter().enumerate() {
                let after_process = after_json.spawn(root, &self.env).await?;
                if let Some(mut process) = after_process {
                    let after_out_file = root.join(format!("after.out.{i}.log"));
                    let after_err_file = root.join(format!("after.err.{i}.log"));
                    let after_process = tokio::task::spawn(async move {
                        if let Err(e) = process.tail(&after_out_file, &after_err_file).await {
                            eprintln!("failed to tail tremor process: {e}");
                        }
                    });

                    after_process.await?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn update_evidence(root: &Path, evidence: &mut HashMap<String, String>) -> Result<()> {
    let after_out_file = root.join("after.out.log");
    let after_err_file = root.join("after.err.log");

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
