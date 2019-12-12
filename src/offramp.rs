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

use crate::codec::Codec;
use crate::errors::*;
use crate::metrics::RampMetricsReporter;
use crate::registry::ServantId;
use crate::system::{PipelineAddr, Stop, METRICS_PIPELINE};
use crate::url::TremorURL;
use crate::{Event, OpConfig};
use actix::prelude::*;
use crossbeam_channel::{bounded, Sender};
use std::borrow::Cow;
use std::fmt;
use std::thread;

mod blackhole;
mod debug;
mod elastic;
mod file;
mod gcs;
mod gpub;
mod kafka;
mod prelude;
mod rest;
mod stderr;
mod stdout;
mod tcp;
mod udp;
mod ws;

pub enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    Connect {
        id: TremorURL,
        addr: PipelineAddr,
    },
    Disconnect {
        id: TremorURL,
        tx: Sender<bool>,
    },
}

pub type Addr = Sender<Msg>;

// We allow this here since we can't pass in &dyn Code as that would taint the
// overlying object with lifetimes.
// We also can't pass in Box<dyn Codec> as that would try to move it out of
// borrowed contest
#[allow(clippy::borrowed_box)]
pub trait Offramp: Send {
    fn start(&mut self, codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()>;
    fn on_event(&mut self, codec: &Box<dyn Codec>, input: String, event: Event) -> Result<()>;
    fn default_codec(&self) -> &str;
    fn add_pipeline(&mut self, _id: TremorURL, _addr: PipelineAddr);
    fn remove_pipeline(&mut self, _id: TremorURL) -> bool;
}

pub trait Impl {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>>;
}

// just a lookup
#[cfg_attr(tarpaulin, skip)]
pub fn lookup(name: &str, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
    match name {
        "blackhole" => blackhole::Blackhole::from_config(config),
        "debug" => debug::Debug::from_config(config),
        "elastic" => elastic::Elastic::from_config(config),
        "file" => file::File::from_config(config),
        "gcs" => gcs::GCS::from_config(config),
        "gpub" => gpub::GPub::from_config(config),
        "kafka" => kafka::Kafka::from_config(config),
        "rest" => rest::Rest::from_config(config),
        "stdout" => stdout::StdOut::from_config(config),
        "stderr" => stderr::StdErr::from_config(config),
        "tcp" => tcp::Tcp::from_config(config),
        "udp" => udp::Udp::from_config(config),
        "ws" => ws::Ws::from_config(config),
        _ => Err(format!("Offramp {} not known", name).into()),
    }
}

#[derive(Debug, Default)]
pub struct Manager {
    pub qsize: usize,
}

impl Actor for Manager {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Offramp manager started");
    }
}

pub struct Create {
    pub id: ServantId,
    pub offramp: Box<dyn Offramp>,
    pub codec: Box<dyn Codec>,
    pub postprocessors: Vec<String>,
    pub metrics_reporter: RampMetricsReporter,
}

impl fmt::Debug for Create {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StartOfframp({})", self.id)
    }
}

impl Message for Create {
    type Result = Result<Addr>;
}

impl Handler<Create> for Manager {
    type Result = Result<Addr>;
    fn handle(&mut self, mut req: Create, _ctx: &mut Context<Self>) -> Self::Result {
        req.offramp.start(&req.codec, &req.postprocessors)?;

        let (tx, rx) = bounded(self.qsize);
        let offramp_id = req.id.clone();
        // let mut s = req;
        thread::spawn(move || {
            info!("[Offramp::{}] started", offramp_id);
            for m in rx {
                match m {
                    Msg::Event { event, input } => {
                        req.metrics_reporter.periodic_flush(event.ingest_ns);

                        req.metrics_reporter.increment_in();
                        // TODO FIXME implement postprocessors
                        match req.offramp.on_event(&req.codec, input.into(), event) {
                            Ok(_) => req.metrics_reporter.increment_out(),
                            Err(e) => {
                                req.metrics_reporter.increment_error();
                                error!("[Offramp::{}] On Event error: {}", offramp_id, e);
                            }
                        }
                    }
                    Msg::Connect { id, addr } => {
                        if id == *METRICS_PIPELINE {
                            info!(
                                "[Offramp::{}] Connecting system metrics pipeline {}",
                                offramp_id, id
                            );
                            req.metrics_reporter.set_metrics_pipeline((id, addr));
                        } else {
                            info!("[Offramp::{}] Connecting pipeline {}", offramp_id, id);
                            req.offramp.add_pipeline(id, addr);
                        }
                    }
                    Msg::Disconnect { id, tx } => {
                        info!("[Offramp::{}] Disconnecting pipeline {}", offramp_id, id);
                        let r = req.offramp.remove_pipeline(id.clone());
                        info!("[Offramp::{}] Pipeline {} disconnected", offramp_id, id);
                        if r {
                            info!("[Offramp::{}] Marked as done ", offramp_id);
                        }
                        if let Err(e) = tx.send(r) {
                            error!("Failed to send reply: {}", e)
                        }
                    }
                }
            }
            info!("[Offramp::{}] stopped", offramp_id);
        });
        Ok(tx)
    }
}

impl Handler<Stop> for Manager {
    type Result = ();
    fn handle(&mut self, _req: Stop, _ctx: &mut Self::Context) -> Self::Result {
        // TODO: Propper shutdown needed?
        info!("Stopping offramps");
        System::current().stop();
    }
}
