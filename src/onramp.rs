// Copyright 2018, Wayfair GmbH
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

//! # Onramp connectors
//!
//! Onramp connectors are used to get data into the system
//! to then be processed.

pub mod blaster;
pub mod file;
pub mod http;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "mssql")]
pub mod mssql;
pub mod stdin;

use crate::errors::*;
use crate::pipeline::prelude::*;
use actix;
use actix::prelude::*;
use std::thread::JoinHandle;

type PipelineOnrampElem = Addr<OnRampActor>;
type PipelineOnramp = Vec<PipelineOnrampElem>;

type EnterReturn = JoinHandle<()>;

pub trait Onramp {
    fn enter_loop(&mut self, pipeline: PipelineOnramp) -> EnterReturn;
}

pub fn create(name: &str, opts: &ConfValue) -> Result<Onramps> {
    match name {
        "blaster" => Ok(Onramps::Blaster(blaster::Onramp::create(opts)?)),
        "file" => Ok(Onramps::File(file::Onramp::create(opts)?)),
        "http" => Ok(Onramps::HTTP(http::Onramp::create(opts)?)),
        #[cfg(feature = "kafka")]
        "kafka" => Ok(Onramps::Kafka(kafka::Onramp::create(opts)?)),
        #[cfg(feature = "mssql")]
        "mssql" => Ok(Onramps::MSSql(mssql::Onramp::create(opts)?)),
        "stdin" => Ok(Onramps::Stdin(stdin::Onramp::create(opts)?)),
        _ => panic!("Unknown classifier: {}", name),
    }
}

pub enum Onramps {
    Blaster(blaster::Onramp),
    #[cfg(feature = "kafka")]
    Kafka(kafka::Onramp),
    #[cfg(feature = "mssql")]
    MSSql(mssql::Onramp),
    File(file::Onramp),
    HTTP(http::Onramp),
    Stdin(stdin::Onramp),
}

impl Onramp for Onramps {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        match self {
            Onramps::Blaster(i) => i.enter_loop(pipelines),
            #[cfg(feature = "kafka")]
            Onramps::Kafka(i) => i.enter_loop(pipelines),
            Onramps::File(i) => i.enter_loop(pipelines),
            #[cfg(feature = "mssql")]
            Onramps::MSSql(i) => i.enter_loop(pipelines),
            Onramps::HTTP(i) => i.enter_loop(pipelines),
            Onramps::Stdin(i) => i.enter_loop(pipelines),
        }
    }
}
