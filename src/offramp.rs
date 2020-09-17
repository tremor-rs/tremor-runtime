// Copyright 2020, The Tremor Team
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
use crate::sink::{
    blackhole, debug, elastic, exit, file, kafka, newrelic, postgres, rest, stderr, stdout, tcp,
    udp, ws,
};
use crate::system::METRICS_PIPELINE;
use crate::url::TremorURL;
use crate::utils::nanotime;
use crate::{Event, OpConfig};
use async_channel::{self, bounded};
use async_std::task::{self, JoinHandle};
use hashbrown::HashMap;
use std::borrow::{Borrow, Cow};
use std::fmt;

pub enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    Signal(Event),
    Connect {
        id: TremorURL,
        addr: Box<pipeline::Addr>,
    },
    ConnectLinked {
        id: TremorURL,
        addr: Box<pipeline::Addr>,
    },
    Disconnect {
        id: TremorURL,
        tx: async_channel::Sender<bool>,
    },
}

pub(crate) type Sender = async_channel::Sender<ManagerMsg>;
pub type Addr = async_channel::Sender<Msg>;

#[async_trait::async_trait]
pub trait Offramp: Send {
    async fn start(&mut self, codec: &dyn Codec, postprocessors: &[String]) -> Result<()>;
    async fn on_event(&mut self, codec: &dyn Codec, input: &str, event: Event) -> Result<()>;
    #[allow(unused_variables)]
    async fn on_signal(&mut self, signal: Event) -> Option<Event> {
        None
    }
    async fn terminate(&mut self) {}
    fn default_codec(&self) -> &str;
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr);
    // TODO handle removal as well
    fn add_dest_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr);
    fn remove_pipeline(&mut self, id: TremorURL) -> bool;
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

pub fn lookup(name: &str, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
    match name {
        "blackhole" => blackhole::Blackhole::from_config(config),
        "debug" => debug::Debug::from_config(config),
        "elastic" => elastic::Elastic::from_config(config),
        "exit" => exit::Exit::from_config(config),
        "file" => file::File::from_config(config),
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
    Create(async_channel::Sender<Result<Addr>>, Box<Create>),
    Stop,
}

#[derive(Debug, Default)]
pub(crate) struct Manager {
    qsize: usize,
}

async fn send_to_pipelines(
    offramp_id: &TremorURL,
    pipelines: &mut HashMap<TremorURL, pipeline::Addr>,
    e: Event,
) {
    let mut i = pipelines.values_mut();
    if let Some(first) = i.next() {
        for p in i {
            if let Err(e) = p.send_insight(e.clone()).await {
                error!("[Offramp::{}] Counterflow error: {}", offramp_id, e);
            };
        }
        if let Err(e) = first.send_insight(e).await {
            error!("[Offramp::{}] Counterflow error: {}", offramp_id, e);
        };
    }
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }

    async fn offramp_task(
        &self,
        r: async_channel::Sender<Result<Addr>>,
        Create {
            codec,
            mut offramp,
            postprocessors,
            mut metrics_reporter,
            id,
        }: Create,
    ) -> Result<()> {
        if let Err(e) = offramp.start(codec.borrow(), &postprocessors).await {
            error!("Failed to create onramp {}: {}", id, e);
            return Err(e);
        }

        let (tx, rx) = bounded::<Msg>(self.qsize);
        let offramp_id = id.clone();
        task::spawn::<_, Result<()>>(async move {
            let mut pipelines: HashMap<TremorURL, pipeline::Addr> = HashMap::new();

            // for linked offramp output
            // TODO make this hashmap as well
            let mut dest_pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();

            info!("[Offramp::{}] started", offramp_id);

            while let Ok(m) = rx.recv().await {
                match m {
                    Msg::Signal(signal) => {
                        if let Some(insight) = offramp.on_signal(signal).await {
                            send_to_pipelines(&offramp_id, &mut pipelines, insight).await;
                        }
                    }
                    Msg::Event { event, input } => {
                        let ingest_ns = event.ingest_ns;
                        let transactional = event.transactional;
                        let ids = event.id.clone();

                        metrics_reporter.periodic_flush(ingest_ns);
                        metrics_reporter.increment_in();

                        // TODO FIXME implement postprocessors
                        let fial = if let Err(err) = offramp
                            .on_event(codec.borrow(), input.borrow(), event)
                            .await
                        {
                            error!("[Offramp::{}] On Event error: {}", offramp_id, err);
                            metrics_reporter.increment_error();
                            true
                        } else {
                            metrics_reporter.increment_out();
                            false
                        };
                        if offramp.auto_ack() && transactional {
                            let e = if fial {
                                Event::cb_fail(ingest_ns, ids)
                            } else {
                                Event::cb_ack(ingest_ns, ids)
                            };
                            send_to_pipelines(&offramp_id, &mut pipelines, e).await;
                        }
                    }
                    Msg::Connect { id, mut addr } => {
                        if id == *METRICS_PIPELINE {
                            info!(
                                "[Offramp::{}] Connecting system metrics pipeline {}",
                                offramp_id, id
                            );
                            metrics_reporter.set_metrics_pipeline((id, *addr));
                        } else {
                            info!("[Offramp::{}] Connecting pipeline {}", offramp_id, id);
                            let insight = if offramp.is_active() {
                                Event::cb_restore(nanotime())
                            } else {
                                Event::cb_trigger(nanotime())
                            };
                            if let Err(e) = addr.send_insight(insight).await {
                                error!(
                                    "[Offramp::{}] Could not send initial insight to {}: {}",
                                    offramp_id, id, e
                                );
                            };

                            pipelines.insert(id.clone(), (*addr).clone());
                            offramp.add_pipeline(id, *addr);
                        }
                    }
                    Msg::ConnectLinked { id, addr } => {
                        // TODO fix offramp_id here for display
                        info!(
                            "[Offramp::{}] Connecting out to pipeline {}",
                            offramp_id, id
                        );
                        dest_pipelines.push((id.clone(), (*addr).clone()));
                        offramp.add_dest_pipeline(id, *addr);
                    }
                    Msg::Disconnect { id, tx } => {
                        info!("[Offramp::{}] Disconnecting pipeline {}", offramp_id, id);
                        pipelines.remove(&id);
                        let r = offramp.remove_pipeline(id.clone());
                        info!("[Offramp::{}] Pipeline {} disconnected", offramp_id, id);
                        if r {
                            info!("[Offramp::{}] Marked as done ", offramp_id);
                            offramp.terminate().await
                        }
                        tx.send(r).await?
                    }
                }
            }
            info!("[Offramp::{}] stopped", offramp_id);
            Ok(())
        });
        r.send(Ok(tx)).await?;
        Ok(())
    }

    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(crate::QSIZE);
        let h = task::spawn(async move {
            info!("Onramp manager started");
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::Stop => {
                        info!("Stopping onramps...");
                        break;
                    }
                    ManagerMsg::Create(r, c) => self.offramp_task(r, *c).await?,
                };
                info!("Stopping onramps...");
            }
            info!("Onramp manager stopped.");
            Ok(())
        });

        (h, tx)
    }
}
