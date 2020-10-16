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
use crate::permge::PriorityMerge;
use crate::pipeline;
use crate::registry::ServantId;
use crate::sink::{
    self, blackhole, debug, elastic, exit, file, handle_response, kafka, newrelic, postgres, rest,
    stderr, stdout, tcp, udp, ws,
};
use crate::source::Processors;
use crate::url::ports::{IN, METRICS};
use crate::url::TremorURL;
use crate::{Event, OpConfig};
use async_channel::{self, bounded, unbounded};
use async_std::stream::StreamExt; // for .next() on PriorityMerge
use async_std::task::{self, JoinHandle};
use halfbrown::HashMap;
use std::borrow::{Borrow, Cow};
use std::fmt;
use tremor_common::time::nanotime;

pub enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    Signal(Event),
    Connect {
        port: Cow<'static, str>,
        id: TremorURL,
        addr: Box<pipeline::Addr>,
    },
    Disconnect {
        port: Cow<'static, str>,
        id: TremorURL,
        tx: async_channel::Sender<bool>,
    },
}

pub(crate) type Sender = async_channel::Sender<ManagerMsg>;
pub type Addr = async_channel::Sender<Msg>;

#[async_trait::async_trait]
pub trait Offramp: Send {
    #[allow(clippy::too_many_arguments)]
    async fn start(
        &mut self,
        offramp_uid: u64,
        offramp_url: &TremorURL,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: async_channel::Sender<sink::Reply>,
    ) -> Result<()>;
    async fn on_event(
        &mut self,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        input: &str,
        event: Event,
    ) -> Result<()>;
    async fn on_signal(&mut self, _signal: Event) -> Option<Event> {
        None
    }
    async fn terminate(&mut self) {}
    fn default_codec(&self) -> &str;
    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr);
    fn remove_pipeline(&mut self, id: TremorURL) -> bool;
    fn add_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorURL, addr: pipeline::Addr);
    fn remove_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorURL) -> bool;
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
    pub codec_map: halfbrown::HashMap<String, Box<dyn Codec>>,
    pub preprocessors: Vec<String>,
    pub postprocessors: Vec<String>,
    pub metrics_reporter: RampReporter,
    pub is_linked: bool,
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

pub(crate) enum OfframpMsg {
    Msg(Msg),
    Reply(sink::Reply),
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }

    #[allow(clippy::too_many_lines)]
    async fn offramp_task(
        &self,
        r: async_channel::Sender<Result<Addr>>,
        Create {
            codec,
            codec_map,
            mut offramp,
            preprocessors,
            postprocessors,
            mut metrics_reporter,
            is_linked,
            id,
        }: Create,
        offramp_uid: u64,
    ) -> Result<()> {
        let (msg_tx, msg_rx) = bounded::<Msg>(self.qsize);
        let (cf_tx, cf_rx) = unbounded::<sink::Reply>(); // we might need to wrap that somehow, but *shrug*

        if let Err(e) = offramp
            .start(
                offramp_uid,
                &id,
                codec.borrow(),
                &codec_map,
                Processors {
                    pre: &preprocessors,
                    post: &postprocessors,
                },
                is_linked,
                cf_tx.clone(),
            )
            .await
        {
            error!("Failed to create onramp {}: {}", id, e);
            return Err(e);
        }
        // merge channels and prioritize contraflow/insight events
        let m_rx = msg_rx.map(OfframpMsg::Msg);
        let c_rx = cf_rx.map(OfframpMsg::Reply);
        let mut to_and_from_offramp_rx = PriorityMerge::new(c_rx, m_rx);

        let offramp_url = id.clone();
        task::spawn::<_, Result<()>>(async move {
            let mut pipelines: HashMap<TremorURL, pipeline::Addr> = HashMap::new();

            // for linked offramp output (port to pipeline(s) mapping)
            let mut dest_pipelines: HashMap<Cow<'static, str>, Vec<(TremorURL, pipeline::Addr)>> =
                HashMap::new();

            info!("[Offramp::{}] started", offramp_url);

            while let Some(offramp_msg) = to_and_from_offramp_rx.next().await {
                match offramp_msg {
                    OfframpMsg::Msg(m) => {
                        match m {
                            Msg::Signal(signal) => {
                                if let Some(insight) = offramp.on_signal(signal).await {
                                    send_to_pipelines(&offramp_url, &mut pipelines, insight).await;
                                }
                            }
                            Msg::Event { event, input } => {
                                let ingest_ns = event.ingest_ns;
                                let transactional = event.transactional;
                                let ids = event.id.clone();

                                metrics_reporter.periodic_flush(ingest_ns);
                                metrics_reporter.increment_in();

                                let fail = if let Err(err) = offramp
                                    .on_event(codec.borrow(), &codec_map, input.borrow(), event)
                                    .await
                                {
                                    error!("[Offramp::{}] On Event error: {}", offramp_url, err);
                                    metrics_reporter.increment_error();
                                    true
                                } else {
                                    metrics_reporter.increment_out();
                                    false
                                };
                                if offramp.auto_ack() && transactional {
                                    let e = Event::ack_or_fail(!fail, ingest_ns, ids);
                                    send_to_pipelines(&offramp_url, &mut pipelines, e).await;
                                }
                            }
                            Msg::Connect { port, id, addr } => {
                                if port.eq_ignore_ascii_case(IN.as_ref()) {
                                    // connect incoming pipeline
                                    info!(
                                        "[Offramp::{}] Connecting pipeline {} to incoming port {}",
                                        offramp_url, id, port
                                    );
                                    let insight =
                                        Event::restore_or_break(offramp.is_active(), nanotime());
                                    if let Err(e) = addr.send_insight(insight).await {
                                        error!(
                                            "[Offramp::{}] Could not send initial insight to {}: {}",
                                            offramp_url, id, e
                                        );
                                    };

                                    pipelines.insert(id.clone(), (*addr).clone());
                                    offramp.add_pipeline(id, *addr);
                                } else if port.eq_ignore_ascii_case(METRICS.as_ref()) {
                                    info!(
                                        "[Offramp::{}] Connecting system metrics pipeline {}",
                                        offramp_url, id
                                    );
                                    metrics_reporter.set_metrics_pipeline((id, *addr));
                                } else {
                                    // connect pipeline to outgoing port
                                    info!(
                                        "[Offramp::{}] Connecting pipeline {} via outgoing port {}",
                                        offramp_url, id, port
                                    );
                                    let p = (id.clone(), (*addr).clone());
                                    if let Some(port_ps) = dest_pipelines.get_mut(&port) {
                                        port_ps.push(p);
                                    } else {
                                        dest_pipelines.insert(port.clone(), vec![p]);
                                    }
                                    offramp.add_dest_pipeline(port, id, *addr);
                                }
                            }
                            Msg::Disconnect { port, id, tx } => {
                                info!(
                                    "[Offramp::{}] Disconnecting pipeline {} on port {}",
                                    offramp_url, id, port
                                );
                                let marked_done = if port.eq_ignore_ascii_case(IN.as_ref()) {
                                    pipelines.remove(&id);
                                    offramp.remove_pipeline(id.clone())
                                } else if port.eq_ignore_ascii_case(METRICS.as_ref()) {
                                    warn!(
                                        "[Offramp::{}] Cannot unlink pipeline {} from port {}",
                                        offramp_url, &id, &port
                                    );
                                    false
                                } else {
                                    if let Some(port_ps) = dest_pipelines.get_mut(&port) {
                                        port_ps.retain(|(url, _)| url != &id)
                                    }
                                    offramp.remove_dest_pipeline(port.clone(), id.clone())
                                };
                                if marked_done {
                                    info!("[Offramp::{}] Marked as done ", offramp_url);
                                    offramp.terminate().await
                                }
                                tx.send(marked_done).await?;
                                info!(
                                    "[Offramp::{}] Pipeline {} disconnected from port {}",
                                    offramp_url, &id, &port
                                );
                            }
                        }
                    }
                    OfframpMsg::Reply(sink::Reply::Insight(event)) => {
                        send_to_pipelines(&offramp_url, &mut pipelines, event).await
                    }
                    OfframpMsg::Reply(sink::Reply::Response(port, event)) => {
                        if let Some(pipelines) = dest_pipelines.get_mut(&port) {
                            if let Err(e) = handle_response(event, pipelines.iter()).await {
                                error!("[Offramp::{}] Response error: {}", offramp_url, e)
                            }
                        }
                    }
                }
            }
            info!("[Offramp::{}] stopped", offramp_url);
            Ok(())
        });
        r.send(Ok(msg_tx)).await?;
        Ok(())
    }

    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(crate::QSIZE);
        let h = task::spawn(async move {
            info!("Onramp manager started");
            let mut offramp_uid: u64 = 0;
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::Stop => {
                        info!("Stopping onramps...");
                        break;
                    }
                    ManagerMsg::Create(r, c) => {
                        offramp_uid += 1;
                        self.offramp_task(r, *c, offramp_uid).await?
                    }
                };
                info!("Stopping offramps...");
            }
            info!("Offramp manager stopped.");
            Ok(())
        });

        (h, tx)
    }
}
