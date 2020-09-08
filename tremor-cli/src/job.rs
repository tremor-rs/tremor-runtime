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
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc::TryRecvError;

use std::sync::mpsc;
use std::{env, process, thread};

pub(crate) fn which<P>(exe_name: P) -> Option<String>
where
    P: AsRef<Path>,
{
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .filter_map(|dir| {
                let path = dir.join(&exe_name);
                if path.is_file() {
                    Some(path.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .next()
    })
}

/// Read until EOF.
pub(crate) fn readlines_until_eof<R: Read, F: FnMut(String) -> Result<()>>(
    reader: R,
    mut handler: F,
) -> Result<()> {
    let mut reader = BufReader::new(reader);
    loop {
        let mut buffy = String::new();
        let nread = reader.read_line(&mut buffy)?;
        handler(buffy)?;
        if nread == 0 {
            break; // EOS
        }
    }

    Ok(())
}

/// Manage target process and communications with it
#[derive(Debug)]
pub(crate) struct TargetProcess {
    stdout_thread: Option<thread::JoinHandle<Result<()>>>,
    stderr_thread: Option<thread::JoinHandle<Result<()>>>,
    pub(crate) process: process::Child,
    pub(crate) stderr_receiver: mpsc::Receiver<String>,
    pub(crate) stdout_receiver: mpsc::Receiver<String>,
}

impl TargetProcess {
    pub fn new_with_stderr(cmd: &str, args: &[String]) -> Self {
        TargetProcess::new(cmd, args)
    }

    /// Spawn target process and pipe IO
    fn new(cmd: &str, args: &[String]) -> Self {
        let mut target_cmd = Command::new(cmd)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start child sub-process");

        let stdout = target_cmd.stdout.take().expect("not opened with stdout");
        let stderr = target_cmd.stderr.take().expect("not opened with stderr");

        let (stdout_tx, stdout_rx) = mpsc::channel();
        let (stderr_tx, stderr_rx) = mpsc::channel();

        let stdout_thread = Some(thread::spawn(move || -> Result<()> {
            // Redirect target process stdout
            readlines_until_eof(stdout, |resp| stdout_tx.send(resp).map_err(|e| e.into()))
        }));

        let stderr_thread = Some(thread::spawn(move || -> Result<()> {
            // Redirect target process stderr
            readlines_until_eof(stderr, |resp| stderr_tx.send(resp).map_err(|e| e.into()))
        }));

        Self {
            process: target_cmd,
            stdout_thread,
            stderr_thread,
            stdout_receiver: stdout_rx,
            stderr_receiver: stderr_rx,
        }
    }

    pub fn wait_with_output(&mut self) -> Result<std::process::ExitStatus> {
        Ok(self.process.wait()?)
    }

    pub(crate) fn tail(&mut self, stdout_path: &str, stderr_path: &str) -> Result<()> {
        let mut tailout = File::create(stdout_path)?;
        let mut tailerr = File::create(stderr_path)?;

        self.wait_with_output()?;

        loop {
            match self.stdout_receiver.try_recv() {
                Ok(line) => {
                    tailout.write(line.as_bytes())?;
                }
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        loop {
            match self.stderr_receiver.try_recv() {
                Ok(line) => {
                    tailerr.write(line.as_bytes())?;
                }
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        tailout.sync_all()?;
        tailerr.sync_all()?;
        if let Err(_) = self.process.kill() {
            // Do nothing, it already exited
        }
        Ok(())
    }
}

impl Drop for TargetProcess {
    fn drop(&mut self) {
        if thread::panicking() {
            process::exit(1);
        }

        if let Some(handle) = self.stdout_thread.take() {
            handle
                .join()
                .unwrap_or_else(|_| {
                    process::exit(1);
                })
                .ok();
        }

        if let Some(handle) = self.stderr_thread.take() {
            handle
                .join()
                .unwrap_or_else(|_| {
                    process::exit(1);
                })
                .ok();
        }

        match self.process.wait() {
            Ok(status) => if !status.success() {},
            Err(_) => unreachable!("never ran error"),
        }
    }
}
