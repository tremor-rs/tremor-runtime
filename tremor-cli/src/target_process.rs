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
use async_std::io::{BufReader, Read};
use std::ffi::OsStr;
use std::fmt::Display;
use tremor_common::asy::file;

use std::collections::HashMap;

use async_std::channel::{unbounded, Receiver, Sender};
use async_std::prelude::*;
use async_std::process::{Child, Command, ExitStatus, Stdio};
use async_std::task::{spawn, JoinHandle};
use std::path::{Path, PathBuf};

use std::env;

pub(crate) fn which<P>(exe_name: P) -> Result<PathBuf>
where
    P: AsRef<Path>,
{
    let name = exe_name.as_ref().to_string_lossy();

    // use the current binary if `tremor` is used as an executable
    // this can be overwritten by giving it a path
    if name == "tremor" {
        env::var("TREMOR_BIN")
            .map_or_else(|_| env::current_exe(), |p| Ok(PathBuf::from(p)))
            .map_err(|e| Error::from(format!("Unable to execute current tremor binary: {e}")))
    } else {
        env::var_os("PATH")
            .and_then(|paths| {
                env::split_paths(&paths).find_map(|dir| {
                    let path = dir.join(&exe_name);
                    if path.is_file() {
                        Some(path)
                    } else {
                        None
                    }
                })
            })
            .ok_or_else(|| Error::from(format!("Unable to find suitable `{name}` binary on path")))
    }
}

/// Read until EOF.
pub(crate) async fn readlines_until_eof<R: Read + std::marker::Unpin>(
    reader: R,
    sender: Sender<String>,
) -> Result<()> {
    let mut reader = BufReader::new(reader);
    loop {
        let mut buffy = String::new();
        let nread = reader.read_line(&mut buffy).await?;
        sender.send(buffy).await?;
        if nread == 0 {
            break; // EOS
        }
    }

    Ok(())
}

/// Manage target process and communications with it
#[derive(Debug)]
pub(crate) struct TargetProcess {
    cmdline: String,
    stdout_handle: Option<JoinHandle<Result<()>>>,
    stderr_handle: Option<JoinHandle<Result<()>>>,
    pub(crate) process: Child,
    pub(crate) stderr_receiver: Receiver<String>,
    pub(crate) stdout_receiver: Receiver<String>,
}

impl TargetProcess {
    /// create a process in the current directory
    pub fn new_in_current_dir<S>(
        cmd: S,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> Result<Self>
    where
        S: AsRef<OsStr>,
    {
        Self::new(cmd, args, env, std::env::current_dir()?)
    }

    /// create a process in the given directory via `cwd`
    pub fn new_in_dir<S, P>(
        cmd: S,
        args: &[String],
        env: &HashMap<String, String>,
        dir: P,
    ) -> Result<Self>
    where
        S: AsRef<OsStr>,
        P: AsRef<Path>,
    {
        Self::new(cmd, args, env, dir)
    }

    /// Spawn target process and pipe IO
    fn new<S, P>(cmd: S, args: &[String], env: &HashMap<String, String>, dir: P) -> Result<Self>
    where
        S: AsRef<OsStr>,
        P: AsRef<Path>,
    {
        let cmd: &OsStr = cmd.as_ref();

        let current_dir = {
            let tmp = dir.as_ref();
            let buf = if tmp.is_relative() {
                std::env::current_dir()?.join(tmp)
            } else {
                tmp.to_path_buf()
            };
            buf.canonicalize()?
        };
        // env var replacement
        let args = Box::new(args.iter().cloned())
            .map(|mut s| {
                for (var, value) in env {
                    s = s.replace(&format!("${var}"), value);
                }
                // replace $PWD if not provided yet in env
                s.replace("$PWD", &current_dir.to_string_lossy())
            })
            .collect::<Vec<_>>();
        let cmdline = format!(
            "{}{} {}",
            env.iter()
                .map(|(k, v)| format!("{k}={v} "))
                .collect::<String>(),
            cmd.to_string_lossy(),
            args.join(" ")
        );
        let mut target_cmd = Command::new(cmd)
            .args(args)
            .current_dir(current_dir)
            .envs(env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = target_cmd.stdout.take();
        let stderr = target_cmd.stderr.take();
        match stdout.zip(stderr) {
            Some((stdout, stderr)) => {
                let (stdout_tx, stdout_rx) = unbounded();
                let (stderr_tx, stderr_rx) = unbounded();

                let stdout_handle = Some(spawn(async move {
                    // Redirect target process stdout
                    readlines_until_eof(stdout, stdout_tx).await
                }));

                let stderr_handle = Some(spawn(async move {
                    // Redirect target process stderr
                    readlines_until_eof(stderr, stderr_tx).await
                }));

                Ok(Self {
                    cmdline,
                    process: target_cmd,
                    stdout_handle,
                    stderr_handle,
                    stdout_receiver: stdout_rx,
                    stderr_receiver: stderr_rx,
                })
            }
            None => Err("Unable to create stdout and stderr streams from target process".into()),
        }
    }

    pub(crate) async fn write_to_stdin<R>(&mut self, content: R) -> Result<()>
    where
        R: async_std::io::Read + Unpin,
    {
        if let Some(mut stdin) = self.process.stdin.take() {
            async_std::io::copy(content, &mut stdin).await?;
            futures::AsyncWriteExt::flush(&mut stdin).await?;
            futures::AsyncWriteExt::close(&mut stdin).await?;
        }
        Ok(())
    }

    pub(crate) async fn wait(&mut self) -> Result<ExitStatus> {
        Ok(self.process.status().await?)
    }

    pub(crate) async fn stdio_tailer(
        &mut self,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<(JoinHandle<Result<()>>, JoinHandle<Result<()>>)> {
        let stdout_rx = self.stdout_receiver.clone();
        let stdout_path = stdout_path.to_path_buf();
        let stdout_handle = spawn::<_, Result<()>>(async move {
            let mut tailout = file::create(&stdout_path).await?;
            while let Ok(line) = stdout_rx.recv().await {
                tailout.write_all(line.as_bytes()).await?;
                tailout.sync_data().await?;
            }
            Ok(())
        });
        let stderr_rx = self.stderr_receiver.clone();
        let stderr_path = stderr_path.to_path_buf();
        let stderr_handle = spawn::<_, Result<()>>(async move {
            let mut tailerr = file::create(&stderr_path).await?;
            while let Ok(line) = stderr_rx.recv().await {
                tailerr.write_all(line.as_bytes()).await?;
                tailerr.sync_data().await?;
            }
            Ok(())
        });

        Ok((stdout_handle, stderr_handle))
    }
    pub(crate) async fn join(&mut self) -> Result<ExitStatus> {
        let exit_status = self.process.status().await?;

        if self.process.kill().is_err() {
            // Do nothing
        };
        Ok(exit_status)
    }

    pub(crate) async fn tail(
        &mut self,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<ExitStatus> {
        let (stdout_handle, stderr_handle) = self.stdio_tailer(stdout_path, stderr_path).await?;

        let exit_status = self.join().await?;

        stdout_handle.await?;
        stderr_handle.await?;
        Ok(exit_status)
    }
}

impl Display for TargetProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.cmdline)
    }
}

impl Drop for TargetProcess {
    fn drop(&mut self) {
        if let Some(handle) = self.stdout_handle.take() {
            async_std::task::block_on(handle.cancel());
        }

        if let Some(handle) = self.stderr_handle.take() {
            async_std::task::block_on(handle.cancel());
        }
        // this errors if the process is already killed, but this is fine for us
        let _ = self.process.kill().is_err();
        if let Err(e) = async_std::task::block_on(self.process.status()) {
            eprintln!("target process drop error: {e:?}");
        }
    }
}
