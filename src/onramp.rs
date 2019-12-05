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

use crate::errors::*;
use crate::metrics::RampMetricsReporter;
use crate::repository::ServantId;
use crate::system::{PipelineAddr, Stop};
use crate::url::TremorURL;
use actix::prelude::*;
use crossbeam_channel::Sender;
use serde_yaml::Value;
use std::fmt;
mod blaster;
mod file;
mod gsub;
mod kafka;
mod metronome;
mod prelude;
mod rest;
pub mod tcp;
mod udp;
mod ws;

pub trait Impl {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>>;
}

#[derive(Clone, Debug)]
pub enum Msg {
    Connect(Vec<(TremorURL, PipelineAddr)>),
    Disconnect { id: TremorURL, tx: Sender<bool> },
}

pub type Addr = Sender<Msg>;

pub trait Onramp: Send {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampMetricsReporter,
    ) -> Result<Addr>;
    fn default_codec(&self) -> &str;
}

// just a lookup
#[cfg_attr(tarpaulin, skip)]
pub fn lookup(name: &str, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
    match name {
        "blaster" => blaster::Blaster::from_config(config),
        "file" => file::File::from_config(config),
        "gsub" => gsub::GSub::from_config(config),
        "kafka" => kafka::Kafka::from_config(config),
        "metronome" => metronome::Metronome::from_config(config),
        "udp" => udp::Udp::from_config(config),
        "tcp" => tcp::Tcp::from_config(config),
        "rest" => rest::Rest::from_config(config),
        "ws" => ws::Ws::from_config(config),
        _ => Err(format!("Onramp {} not known", name).into()),
    }
}

#[derive(Debug, Default)]
pub struct Manager {}

impl Actor for Manager {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Onramp manager started");
    }
}

pub struct Create {
    pub id: ServantId,
    pub stream: Box<dyn Onramp>,
    pub codec: String,
    pub preprocessors: Vec<String>,
    pub metrics_reporter: RampMetricsReporter,
}

impl fmt::Debug for Create {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StartOnramp({})", self.id)
    }
}

impl Message for Create {
    type Result = Result<Addr>;
}

impl Handler<Create> for Manager {
    type Result = Result<Addr>;
    fn handle(&mut self, mut req: Create, _ctx: &mut Context<Self>) -> Self::Result {
        req.stream
            .start(&req.codec, &req.preprocessors, req.metrics_reporter)
    }
}

impl Handler<Stop> for Manager {
    type Result = ();
    fn handle(&mut self, _req: Stop, _ctx: &mut Self::Context) -> Self::Result {
        // TODO: Propper shutdown needed?
        info!("Stopping onramps");
        System::current().stop();
    }
}
