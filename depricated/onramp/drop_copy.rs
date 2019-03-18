// Copyright 2018-2019, Wayfair GmbH
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

//!
//! # File Onramp
//!
//! The `drop_copyt` onramp processes files put into in_dir  and enters them
//! into the pipeline as events.
//!
//! Files are moved from `'in_dir'` -> `during_processing` -> `after_processing`
//! where the later two are folder names that are living in the same directory
//! as `in_dir`
//!
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::errors::*;
use crate::onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp};
use crate::pipeline::prelude::*;
use serde_yaml;
use std::fs::{create_dir_all, rename};
use std::process::Command;
use std::thread;

use chrono::{DateTime, Utc};
use notify::{watcher, DebouncedEvent, RecursiveMode, Watcher};
use std::sync::mpsc::channel;
use std::time::Duration;

use crate::onramp::utils::process_file;
use crate::onramp::OnRampActor;
use actix::prelude::*;

#[derive(Deserialize)]
pub struct Onramp {
    in_dir: String,
}

#[derive(Deserialize)]
pub struct Config {
    pub in_dir: String,
}

impl Onramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let Config { in_dir } = serde_yaml::from_value(opts.clone())?;
        Ok(Onramp { in_dir })
    }
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let in_dir = self.in_dir.clone();

        thread::spawn(move || {
            let (tx, rx) = channel();
            let mut watcher = watcher(tx, Duration::from_secs(10)).unwrap();

            watcher.watch(in_dir, RecursiveMode::Recursive).unwrap();
            loop {
                match rx.recv() {
                    Ok(event) => {
                        if let DebouncedEvent::Create(path_created) = event {
                            process_dropped_file(&path_created, &pipelines[..]);
                        };
                    }
                    Err(e) => println!("watch error: {:?}", e),
                };
            }
        })
    }
}

fn make_new_path(dirpath: &mut std::path::PathBuf, dir: &str, filename: &str) {
    dirpath.pop();
    dirpath.push("../");
    dirpath.push(dir);
    dirpath.push(filename)
}

fn process_dropped_file(path_created: &std::path::PathBuf, pipelines: &[Addr<OnRampActor>]) {
    // first declare everything we need
    let file = path_created.file_name().ok_or("No filename").unwrap();
    let mut for_processing_filepath = path_created.clone();
    let mut after_processing_dir = path_created.clone();
    let now: DateTime<Utc> = Utc::now();
    let day_dir = now.format("%Y-%m-%d").to_string();
    let timestamp = now.format("%Y-%m-%d::%H-%M-%S.%f").to_string();
    let mut outputfile = file.to_os_string().into_string().unwrap();
    outputfile.push_str(".");
    outputfile.push_str(&timestamp);

    // make the path to the 'in processing' directory
    make_new_path(
        &mut for_processing_filepath,
        &"during_processing",
        &file.to_os_string().into_string().unwrap(),
    );

    // move the file from the 'in' directory to the 'in processing' one
    match rename(&path_created, &for_processing_filepath) {
        Ok(_) => {
            // now process the files
            process_file(
                for_processing_filepath.to_str().unwrap().to_string(),
                &pipelines,
            );

            // make the path to the 'after processing' directory
            make_new_path(&mut after_processing_dir, &"after_processing", &day_dir);

            // take a copy of the path for use later
            let mut after_processing_filepath = after_processing_dir.clone();

            let _ = match create_dir_all(&after_processing_dir) {
                Ok(_) => "ok",
                Err(error) => {
                    panic!("Can't create {:?} due to {}", after_processing_dir, error);
                }
            };

            after_processing_filepath.push(outputfile);
            let _ = match rename(&for_processing_filepath, &after_processing_filepath) {
                Ok(_) => "ok",
                Err(error) => {
                    panic!(
                        "can't move {:?} to {:?} due to {}",
                        for_processing_filepath, after_processing_filepath, error
                    );
                }
            };
        }
        Err(error) => {
            println!("Can't move the file {:?} to the directory {:?} due to {} - might be duplicate event?",
            path_created, for_processing_filepath, error);
        }
    };
}
