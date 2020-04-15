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

use crate::onramp::prelude::*;
use async_std::stream::{Stream, StreamExt};
use async_std::task;
use async_std::task::{Context, Poll};
use fsio;
use fsio::error::FsIOError;
use futures_util::stream::SelectAll;
use pin_project_lite::pin_project;
use serde_yaml::Value;
use std::env::current_dir;
use std::fmt;
use std::io::{BufRead, BufReader};
use std::pin::Pin;
use std::str;
use std::time::Duration;
use subprocess::{ExitStatus, Popen, PopenConfig, PopenError, Redirection};

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    // TODO: This should be an ExecType
    // TODO: It should be named without underscore
    pub _type: String,
    pub timeout_ms: u64,
    pub interval_ms: u64,
    pub commands: Vec<String>,
}

pub struct Exec {
    pub config: Config,
}

pin_project! {
    pub struct ExecStream {
        _type: ExecType,
        script: String,
        args: Vec<String>,
        opts: ExecOpts,
        process: Popen,
        // File path that contains processed script
        file: String,
        exited: bool,
        // Exit codes that trigger completion
        complete_on_code: Vec<i32>,
        // Marks stream as ended
        completed: bool
    }
}

pub enum ExecType {
    Periodical,
    Continuous,
}

impl fmt::Display for ExecType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct ExecOpts {
    pub shell: Option<String>,
    pub in_redirect: IoOpts,
    pub out_redirect: IoOpts,
    pub exit_on_error: bool,
}

impl ExecOpts {
    pub fn new() -> ExecOpts {
        ExecOpts {
            shell: None,
            in_redirect: IoOpts::Inherit,
            out_redirect: IoOpts::Pipe,
            exit_on_error: false,
        }
    }
}

pub enum IoOpts {
    // TODO: Optional but questionable if relevant at all in Tremor context
    // Same for `Inherit`
    //Null,
    Pipe,
    Inherit,
}

#[derive(Debug)]
pub enum ErrorInfo {
    IOError(PopenError),
    FsIOError(FsIOError),
    Description(&'static str),
}

#[derive(Debug)]
pub struct ExecError {
    pub info: ErrorInfo,
}

// (exit_code, stdout, stderr)
pub type ExecResult =
    std::result::Result<(Option<i32>, Option<Vec<u8>>, Option<Vec<u8>>), ExecError>;

impl Stream for ExecStream {
    type Item = ExecResult;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // If the last execution exited the process and if stream is completed
        if *this.exited && *this.completed {
            match this._type {
                ExecType::Periodical => {
                    let result = create_popen(this.script, this.args, this.opts);
                    match result {
                        Ok((sub, file)) => {
                            *this.process = sub;
                            *this.completed = false;
                            *this.file = file;
                            *this.exited = false;
                        }
                        Err(error) => {
                            // TODO: Do something proper boy
                            error!("{:?}", error);
                            return Poll::Pending;
                        }
                    };
                }
                ExecType::Continuous => return Poll::Ready(None),
            }
        }

        let mut exit_code: Option<i32> = None;
        let status = this.process.poll();
        if status.is_some() {
            exit_code = Some(parse_exit_code(status.unwrap()));
            *this.exited = true;

            match this._type {
                // TODO: Depending on configuration, we could try
                // and restart continuous stream here.
                ExecType::Continuous => {
                    // TODO: Do something with this exit_code or get rid of it
                    debug!("Continuous stream ends {:?}", exit_code);
                    *this.completed = true
                }
                ExecType::Periodical => (),
            }

            fsio::file::delete_ignore_error(this.file);
        }

        match this._type {
            ExecType::Periodical => {
                let mut p = this
                    .process
                    .communicate_start(None)
                    .limit_time(Duration::new(10, 0));
                let r = p.read();
                match r {
                    Ok(v) => {
                        let (out, err) = v;

                        match this.process.wait() {
                            Ok(status) => {
                                exit_code = Some(parse_exit_code(status));
                                fsio::file::delete_ignore_error(this.file);
                                *this.completed = true;
                                return Poll::Ready(Some(Ok((exit_code, out, err))));
                            }
                            Err(error) => {
                                // TODO: Do something with this error
                                debug!("{:?}", error);
                                this.process.kill().unwrap();
                                this.process.wait().unwrap();
                                fsio::file::delete_ignore_error(this.file);
                                *this.completed = true;
                                Poll::Pending
                            }
                        }
                    }
                    Err(_) => {
                        fsio::file::delete_ignore_error(this.file);
                        *this.completed = true;
                        Poll::Pending
                    }
                }
            }
            ExecType::Continuous => {
                let stdout = this.process.stdout.as_ref().take();
                let stderr = this.process.stderr.as_ref().take();

                if let (Some(o), Some(_e)) = (stdout, stderr) {
                    let mut out_reader = BufReader::new(o);
                    let buf_o = out_reader.fill_buf().unwrap();

                    // TODO: Figure out a way to read stderr without
                    // waiting for buffer to fill when there's nothing
                    //let mut err_reader = BufReader::new(e);
                    //let buf_err = err_reader.fill_buf().unwrap();
                    let buf_err = vec![];
                    return Poll::Ready(Some(Ok((
                        exit_code,
                        Some(buf_o.to_vec()),
                        Some(buf_err.to_vec()),
                    ))));
                } else {
                    fsio::file::delete_ignore_error(this.file);
                    *this.completed = true;
                    Poll::Ready(None)
                }
            }
        }
    }
}

impl ExecStream {
    pub fn from(
        _type: ExecType,
        script: String,
        args: Vec<String>,
        opts: ExecOpts,
    ) -> std::result::Result<ExecStream, ExecError> {
        let result = create_popen(&script, &args, &opts);
        match result {
            Ok((child, file)) => Ok(ExecStream {
                _type,
                script,
                args,
                opts,
                file,
                complete_on_code: vec![0],
                completed: false,
                process: child,
                exited: false,
            }),
            Err(error) => Err(error),
        }
    }
}

fn create_popen(
    script: &str,
    args: &Vec<String>,
    options: &ExecOpts,
) -> std::result::Result<(Popen, String), ExecError> {
    match civilize_script(&script.to_string(), &options) {
        Ok(updated_script) => match create_script_file(&updated_script) {
            Ok(file) => {
                let command = match options.shell {
                    Some(ref value) => value,
                    None => "sh",
                };

                let mut all_args = vec![file.to_string()];

                all_args.extend(args.iter().cloned());
                all_args.insert(0, command.to_string());
                let popen_args: Vec<&str> = all_args.iter().map(|x| &**x).collect();
                let p = Popen::create(
                    &popen_args,
                    PopenConfig {
                        stdout: Redirection::Pipe,
                        stderr: Redirection::Pipe,
                        ..Default::default()
                    },
                );

                match p {
                    Ok(popen) => Ok((popen, file.clone())),
                    Err(error) => {
                        fsio::file::delete_ignore_error(&file);

                        Err(ExecError {
                            info: ErrorInfo::IOError(error),
                        })
                    }
                }
            }
            Err(error) => Err(ExecError {
                info: ErrorInfo::FsIOError(error),
            }),
        },
        Err(error) => Err(error),
    }
}

fn create_script_file(script: &String) -> std::result::Result<String, FsIOError> {
    let extension = "sh";
    let file_path = fsio::path::get_temporary_file_path(extension);

    match fsio::file::write_text_file(&file_path, script) {
        Ok(_) => Ok(file_path),
        Err(error) => {
            fsio::file::delete_ignore_error(&file_path);

            Err(error)
        }
    }
}

fn civilize_script(script: &String, _options: &ExecOpts) -> std::result::Result<String, ExecError> {
    match current_dir() {
        Ok(cwd_holder) => match cwd_holder.to_str() {
            Some(cwd) => {
                let mut cd_command = "cd ".to_string();
                cd_command.push_str(cwd);

                let mut script_lines: Vec<String> = script
                    .trim()
                    .split("\n")
                    .map(|string| string.to_string())
                    .collect();

                let insert_index = if script_lines.len() > 0 && script_lines[0].starts_with("#!") {
                    1
                } else {
                    0
                };

                script_lines.insert(insert_index, cd_command);

                script_lines.push("\n".to_string());

                let updated_script = script_lines.join("\n");

                Ok(updated_script)
            }
            None => Err(ExecError {
                info: ErrorInfo::Description("Cannot get current working dir."),
            }),
        },
        Err(error) => Err(ExecError {
            info: ErrorInfo::IOError(PopenError::IoError(error)),
        }),
    }
}

fn parse_exit_code(status: subprocess::ExitStatus) -> i32 {
    if !status.success() {
        match status {
            ExitStatus::Exited(value) => value as i32,
            ExitStatus::Signaled(value) => value as i32,
            ExitStatus::Other(value) => value as i32,
            ExitStatus::Undetermined => -1,
        }
    } else {
        0
    }
}

impl onramp::Impl for Exec {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

async fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: Config,
    _preprocessors: Vec<String>,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let mut pipelines = Vec::new();
    let id = 0;
    let mut ingest_ns = nanotime();
    let mut no_pp = vec![];

    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-exec".to_string(),
        host: "tremor-exec.remote".to_string(),
        port: None,
        path: vec![String::default()],
    };

    let args = vec![];
    let ref mut streams = SelectAll::new();
    for cmd in config.commands {
        // TODO: Pattern match on config where _type is ExecType (enum)
        match config._type.as_str() {
            "periodical" => {
                let result =
                    ExecStream::from(ExecType::Periodical, cmd, args.clone(), ExecOpts::new());
                match result {
                    Ok(stream) => {
                        streams.push(
                            stream
                                .timeout(Duration::from_millis(config.timeout_ms))
                                .throttle(Duration::from_millis(config.interval_ms)),
                        );
                    }
                    Err(e) => panic!(e),
                }
            }
            "continuous" => {
                let result =
                    ExecStream::from(ExecType::Continuous, cmd, args.clone(), ExecOpts::new());
                match result {
                    Ok(stream) => {
                        streams.push(
                            stream
                                .timeout(Duration::from_millis(config.timeout_ms))
                                .throttle(Duration::from_millis(config.interval_ms)),
                        );
                    }
                    Err(e) => panic!(e),
                }
            }
            &_ => {}
        }
    }

    loop {
        match task::block_on(handle_pipelines(&rx, &mut pipelines, &mut metrics_reporter))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            PipeHandlerResult::Normal => (),
        }
        streams
            .for_each(|rx| {
                match rx {
                    Ok(exec_result) => {
                        match exec_result {
                            Ok(tuple) => {
                                let (exit_code, stdout, stderr) = tuple;

                                if let Some(data) = stdout {
                                    send_event(
                                        &pipelines,
                                        &mut no_pp,
                                        &mut codec,
                                        &mut metrics_reporter,
                                        &mut ingest_ns,
                                        &origin_uri,
                                        id,
                                        // FIXME: This breaks if not UTF8
                                        std::str::from_utf8(&data).unwrap().into(),
                                    );
                                }

                                if stderr.is_some() {
                                    // TODO: Do something with stderr?
                                }

                                if let Some(exit_code) = exit_code {
                                    info!("Command exited with {}", exit_code);
                                }
                            }
                            Err(e) => panic!(e),
                        }
                    }
                    Err(_) => {
                        // This is async_std::stream::TimeoutError
                        info!("Execution timed out.");
                    }
                }
            })
            .await;
    }
}

impl Onramp for Exec {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        let preprocessors = preprocessors.to_vec();
        task::Builder::new()
            .name(format!("onramp-exec-{}", "???"))
            .spawn(async move {
                if let Err(e) =
                    onramp_loop(&rx, config, preprocessors, codec, metrics_reporter).await
                {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "string"
    }
}
