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
use crate::errors::Result;
use crate::metrics::RampReporter;
use crate::pipeline;
use crate::registry::ServantId;
use crate::system::METRICS_PIPELINE;
use crate::url::TremorURL;
use crate::utils::nanotime;
use crate::{Event, OpConfig};
use async_std::sync::channel;
use async_std::task::{self, JoinHandle};
use crossbeam_channel::{bounded, Sender as CbSender};
use hashbrown::HashMap;
use std::borrow::Cow;
use std::fmt;
use std::thread;

mod blackhole;
mod debug;
mod elastic;
mod exit;
mod file;
#[cfg(feature = "gcp")]
mod gcs;
#[cfg(feature = "gcp")]
mod gpub;
mod kafka;
mod newrelic;
mod postgres;
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
    Signal(Event),
    Connect {
        id: TremorURL,
        addr: pipeline::Addr,
    },
    Disconnect {
        id: TremorURL,
        tx: CbSender<bool>,
    },
}

pub(crate) type Sender = async_std::sync::Sender<ManagerMsg>;
pub type Addr = CbSender<Msg>;

// We allow this here since we can't pass in &dyn Code as that would taint the
// overlying object with lifetimes.
// We also can't pass in Box<dyn Codec> as that would try to move it out of
// borrowed contest
#[allow(clippy::borrowed_box, unused_variables)]
pub trait Offramp: Send {
    fn start(&mut self, codec: &Box<dyn Codec>, postprocessors: &[String]) -> Result<()>;
    fn on_event(
        &mut self,
        codec: &Box<dyn Codec>,
        input: Cow<'static, str>,
        event: Event,
    ) -> Result<()>;
    fn default_codec(&self) -> &str;
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr);
    fn remove_pipeline(&mut self, id: TremorURL) -> bool;
    fn on_signal(&mut self, signal: Event) -> Option<Event> {
        None
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
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
        "exit" => exit::Exit::from_config(config),
        "file" => file::File::from_config(config),
        #[cfg(feature = "gcp")]
        "gcs" => gcs::GCS::from_config(config),
        #[cfg(feature = "gcp")]
        "gpub" => gpub::GPub::from_config(config),
        "kafka" => kafka::Kafka::from_config(config),
        "newrelic" => newrelic::NewRelic::from_config(config),
        "postgres" => postgres::Postgres::from_config(config),
        "rest" => rest::Rest::from_config(config),
        "stdout" => stdout::StdOut::from_config(config),
        "stderr" => stderr::StdErr::from_config(config),
        "tcp" => tcp::Tcp::from_config(config),
        "udp" => udp::Udp::from_config(config),
        "ws" => ws::Ws::from_config(config),
        _ => Err(format!("Offramp {} not known", name).into()),
    }
}

pub(crate) struct Create {
    pub id: ServantId,
    pub offramp: Box<dyn Offramp>,
    pub codec: Box<dyn Codec>,
    pub postprocessors: Vec<String>,
    pub metrics_reporter: RampReporter,
}

impl fmt::Debug for Create {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StartOfframp({})", self.id)
    }
}

/// This is control plane
pub(crate) enum ManagerMsg {
    Create(async_std::sync::Sender<Result<Addr>>, Box<Create>),
    Stop,
}

#[derive(Debug, Default)]
pub(crate) struct Manager {
    qsize: usize,
}

fn send_to_pipelines(
    offramp_id: &TremorURL,
    pipelines: &HashMap<TremorURL, pipeline::Addr>,
    e: &Event,
) {
    for p in pipelines.values() {
        if let Err(e) = p.send_insight(e.clone()) {
            error!("[Offramp::{}] Counterflow error: {}", offramp_id, e);
        };
    }
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }

    async fn offramp_thread(
        &self,
        r: async_std::sync::Sender<Result<Addr>>,
        Create {
            codec,
            mut offramp,
            postprocessors,
            mut metrics_reporter,
            id,
        }: Create,
    ) {
        if let Err(e) = offramp.start(&codec, &postprocessors) {
            error!("Failed to create onramp {}: {}", id, e);
            return;
        }

        let (tx, rx) = bounded(self.qsize);
        let offramp_id = id.clone();
        thread::spawn(move || {
            let mut pipelines: HashMap<TremorURL, pipeline::Addr> = HashMap::new();
            info!("[Offramp::{}] started", offramp_id);

            for m in rx {
                match m {
                    Msg::Signal(signal) => {
                        if let Some(insight) = offramp.on_signal(signal) {
                            send_to_pipelines(&offramp_id, &pipelines, &insight);
                        }
                    }
                    Msg::Event { event, input } => {
                        let ingest_ns = event.ingest_ns;
                        let id = event.id.clone(); // FIXME: restructure for perf .unwrap()
                        metrics_reporter.periodic_flush(ingest_ns);
                        metrics_reporter.increment_in();

                        // TODO FIXME implement postprocessors
                        let e = if let Err(e) = offramp.on_event(&codec, input, event) {
                            metrics_reporter.increment_error();
                            error!("[Offramp::{}] On Event error: {}", offramp_id, e);
                            Event::cb_fail(ingest_ns, id.clone())
                        } else {
                            metrics_reporter.increment_out();
                            Event::cb_ack(ingest_ns, id.clone())
                        };
                        if offramp.auto_ack() {
                            // FIXME: unwrap() how do we ensure we only send to the 'right' pipeline?
                            send_to_pipelines(&offramp_id, &pipelines, &e);
                        }
                    }
                    Msg::Connect { id, addr } => {
                        if id == *METRICS_PIPELINE {
                            info!(
                                "[Offramp::{}] Connecting system metrics pipeline {}",
                                offramp_id, id
                            );
                            metrics_reporter.set_metrics_pipeline((id, addr));
                        } else {
                            info!("[Offramp::{}] Connecting pipeline {}", offramp_id, id);
                            let insight = if offramp.is_active() {
                                Event::cb_restore(nanotime())
                            } else {
                                Event::cb_trigger(nanotime())
                            };
                            if let Err(e) = addr.send_insight(insight) {
                                error!(
                                    "[Offramp::{}] Could not send initial insight to {}: {}",
                                    offramp_id, id, e
                                );
                            };

                            pipelines.insert(id.clone(), addr.clone());
                            offramp.add_pipeline(id, addr);
                        }
                    }
                    Msg::Disconnect { id, tx } => {
                        info!("[Offramp::{}] Disconnecting pipeline {}", offramp_id, id);
                        pipelines.remove(&id);
                        let r = offramp.remove_pipeline(id.clone());
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
        r.send(Ok(tx)).await
    }

    pub fn start(self) -> (JoinHandle<bool>, Sender) {
        let (tx, rx) = channel(64);
        let h = task::spawn(async move {
            info!("Onramp manager started");
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::Stop => {
                        info!("Stopping onramps...");
                        break;
                    }
                    ManagerMsg::Create(r, c) => self.offramp_thread(r, *c).await,
                };
                info!("Stopping onramps...");
            }
            info!("Onramp manager stopped.");
            true
        });

        (h, tx)
    }
}
