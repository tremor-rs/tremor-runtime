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
use crate::offramp;
use crate::registry::ServantId;
use crate::repository::PipelineArtefact;
use crate::url::TremorURL;
use async_std::sync::channel;
use async_std::task::{self, JoinHandle};
use crossbeam_channel::{bounded, Sender as CbSender};
use std::borrow::Cow;
use std::fmt;
use std::thread;
use tremor_pipeline::Event;

pub(crate) type Sender = async_std::sync::Sender<ManagerMsg>;

/// Address for a a pipeline
#[derive(Clone)]
pub struct Addr {
    pub(crate) addr: CbSender<Msg>,
    pub(crate) id: ServantId,
}

impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
    }
}

#[derive(Debug)]
pub(crate) enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    ConnectOfframp(Cow<'static, str>, TremorURL, offramp::Addr),
    ConnectPipeline(Cow<'static, str>, TremorURL, Addr),
    Disconnect(Cow<'static, str>, TremorURL),
    #[allow(dead_code)]
    Signal(Event),
    Insight(Event),
}

#[derive(Debug)]
pub enum Dest {
    Offramp(offramp::Addr),
    Pipeline(Addr),
}
impl Dest {
    pub fn send_event(&self, input: Cow<'static, str>, event: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.send(offramp::Msg::Event { input, event })?,
            Self::Pipeline(addr) => addr.addr.send(Msg::Event { input, event })?,
        }
        Ok(())
    }
}

pub struct Create {
    pub config: PipelineArtefact,
    pub id: ServantId,
}

pub(crate) enum ManagerMsg {
    Stop,
    Create(async_std::sync::Sender<Result<Addr>>, Create),
}

#[derive(Default, Debug)]
pub(crate) struct Manager {
    qsize: usize,
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }
    pub fn start(self) -> (JoinHandle<bool>, Sender) {
        let (tx, rx) = channel(64);
        let h = task::spawn(async move {
            info!("Pipeline manager started");
            loop {
                match rx.recv().await {
                    Some(ManagerMsg::Stop) => {
                        info!("Stopping onramps...");
                        break;
                    }
                    Some(ManagerMsg::Create(r, create)) => {
                        r.send(self.start_pipeline(create)).await
                    }
                    None => {
                        info!("Stopping onramps...");
                        break;
                    }
                }
            }
            info!("Pipeline manager stopped");
            true
        });
        (h, tx)
    }

    #[allow(clippy::too_many_lines)]
    fn start_pipeline(&self, req: Create) -> Result<Addr> {
        #[inline]
        fn send_events(
            eventset: &mut Vec<(Cow<'static, str>, Event)>,
            dests: &halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>>,
        ) -> Result<()> {
            for (output, event) in eventset.drain(..) {
                if let Some(dest) = dests.get(&output) {
                    let len = dest.len();
                    //We know we have len, so grabbing len - 1 elementsis safe
                    for (id, offramp) in unsafe { dest.get_unchecked(..len - 1) } {
                        offramp.send_event(
                            id.instance_port()
                                .ok_or_else(|| {
                                    Error::from(format!("missing instance port in {}.", id))
                                })?
                                .clone()
                                .into(),
                            event.clone(),
                        )?;
                    }
                    //We know we have len, so grabbing the last elementsis safe
                    let (id, offramp) = unsafe { dest.get_unchecked(len - 1) };
                    offramp.send_event(
                        id.instance_port()
                            .ok_or_else(|| {
                                Error::from(format!("missing instance port in {}.", id))
                            })?
                            .clone()
                            .into(),
                        event,
                    )?;
                };
            }
            Ok(())
        }
        let config = req.config;
        let id = req.id.clone();
        let mut dests: halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>> =
            halfbrown::HashMap::new();
        let mut eventset: Vec<(Cow<'static, str>, Event)> = Vec::new();
        let (tx, rx) = bounded::<Msg>(self.qsize);
        let mut pipeline = config.to_executable_graph(tremor_pipeline::buildin_ops)?;
        let mut pid = req.id.clone();
        pid.trim_to_instance();
        pipeline.id = pid.to_string();
        thread::Builder::new()
            .name(format!("pipeline-{}", id.clone()))
            .spawn(move || {
                info!("[Pipeline:{}] starting thread.", id);
                for req in rx {
                    match req {
                        Msg::Event { input, event } => {
                            match pipeline.enqueue(&input, event, &mut eventset) {
                                Ok(()) => {
                                    if let Err(e) = send_events(&mut eventset, &dests) {
                                        error!("Failed to send event: {}", e)
                                    }
                                }
                                Err(e) => error!("error: {:?}", e),
                            }
                        }
                        Msg::Insight(insight) => {
                            pipeline.contraflow(insight);
                        }
                        Msg::Signal(signal) => match pipeline.enqueue_signal(signal, &mut eventset)
                        {
                            Ok(()) => {
                                if let Err(e) = send_events(&mut eventset, &dests) {
                                    error!("Failed to send event: {}", e)
                                }
                            }
                            Err(e) => error!("error: {:?}", e),
                        },

                        Msg::ConnectOfframp(output, offramp_id, offramp) => {
                            info!(
                                "[Pipeline:{}] connecting {} to offramp {}",
                                id, output, offramp_id
                            );
                            if let Some(offramps) = dests.get_mut(&output) {
                                offramps.push((offramp_id, Dest::Offramp(offramp)));
                            } else {
                                dests.insert(output, vec![(offramp_id, Dest::Offramp(offramp))]);
                            }
                        }
                        Msg::ConnectPipeline(output, pipeline_id, pipeline) => {
                            info!(
                                "[Pipeline:{}] connecting {} to pipeline {}",
                                id, output, pipeline_id
                            );
                            if let Some(offramps) = dests.get_mut(&output) {
                                offramps.push((pipeline_id, Dest::Pipeline(pipeline)));
                            } else {
                                dests.insert(output, vec![(pipeline_id, Dest::Pipeline(pipeline))]);
                            }
                        }
                        Msg::Disconnect(output, to_delete) => {
                            let mut remove = false;
                            if let Some(offramp_vec) = dests.get_mut(&output) {
                                offramp_vec.retain(|(this_id, _)| this_id != &to_delete);
                                remove = offramp_vec.is_empty();
                            }
                            if remove {
                                dests.remove(&output);
                            }
                        }
                    };
                }
                info!("[Pipeline:{}] stopping thread.", id);
            })?;
        Ok(Addr {
            id: req.id,
            addr: tx,
        })
    }
}
