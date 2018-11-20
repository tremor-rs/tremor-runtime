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

//! # Offramps to send data to external systems

pub mod blackhole;
pub mod debug;
pub mod elastic;
pub mod file;
pub mod influx;
pub mod kafka;
pub mod null;
pub mod stdout;

use errors::*;
use pipeline::prelude::*;
use std::boxed::Box;

/// Enum of all offramp connectors we have implemented.
/// New connectors need to be added here.
#[derive(Debug)]
pub enum Offramp {
    Blackhole(Box<blackhole::Offramp>),
    Kafka(kafka::Offramp),
    Elastic(Box<elastic::Offramp>),
    Influx(Box<influx::Offramp>),
    Stdout(stdout::Offramp),
    Debug(debug::Offramp),
    Null(null::Offramp),
    File(file::Offramp),
}

opable!(Offramp, Blackhole, Kafka, Elastic, Influx, Stdout, Debug, Null, File);

impl Offramp {
    pub fn new(name: &str, opts: &ConfValue) -> Result<Offramp> {
        match name {
            "blackhole" => Ok(Offramp::Blackhole(Box::new(blackhole::Offramp::new(opts)?))),
            "debug" => Ok(Offramp::Debug(debug::Offramp::new(opts)?)),
            "elastic" => Ok(Offramp::Elastic(Box::new(elastic::Offramp::new(opts)?))),
            "file" => Ok(Offramp::File(file::Offramp::new(opts)?)),
            "influx" => Ok(Offramp::Influx(Box::new(influx::Offramp::new(opts)?))),
            "kafka" => Ok(Offramp::Kafka(kafka::Offramp::new(opts)?)),
            "null" => Ok(Offramp::Null(null::Offramp::new(opts)?)),
            "stdout" => Ok(Offramp::Stdout(stdout::Offramp::new(opts)?)),
            _ => Err(ErrorKind::UnknownOp("offramp".into(), name.into()).into()),
        }
    }
}
