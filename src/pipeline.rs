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
use crate::errors::{Error, Result};
use crate::registry::ServantId;
use crate::repository::PipelineArtefact;
use crate::url::TremorURL;
use crate::utils::nanotime;
use crate::{offramp, onramp};
use async_std::sync::channel;
use async_std::task::{self, JoinHandle};
use crossbeam_channel::{bounded, Receiver as CbReceiver, Sender as CbSender, TrySendError};
use std::borrow::Cow;
use std::time::Duration;
use std::{fmt, thread};
use tremor_pipeline::{Event, ExecutableGraph, SignalKind};

const TICK_MS: u64 = 1000;
pub(crate) type Sender = async_std::sync::Sender<ManagerMsg>;

/// Address for a a pipeline
#[derive(Clone)]
pub struct Addr {
    addr: TrySender<Msg>,
    cf_addr: TrySender<CfMsg>,
    id: ServantId,
}

pub struct TrySender<M: Send> {
    addr: CbSender<M>,
    pending: Vec<M>,
    pending2: Vec<M>,
}

impl<M: Send> std::fmt::Debug for TrySender<M>
where
    CbSender<M>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.addr)
    }
}

impl<M: Send> Clone for TrySender<M> {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            pending: Vec::new(),
            pending2: Vec::new(),
        }
    }
}

impl<M: Send> From<CbSender<M>> for TrySender<M> {
    fn from(addr: CbSender<M>) -> Self {
        Self {
            addr,
            pending: Vec::new(),
            pending2: Vec::new(),
        }
    }
}

impl<M: Send> TrySender<M> {
    pub(crate) fn try_send_safe(&mut self, msg: M) -> Result<()> {
        match self.addr.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(msg)) => {
                self.pending.push(msg);
                Ok(())
            }
            Err(_e) => Err("disconnected".into()),
        }
    }
    pub(crate) fn ready(&self) -> bool {
        self.addr.capacity().unwrap_or_default() > self.addr.len() && self.pending.is_empty()
    }

    pub(crate) fn drain_ready(&mut self) -> bool {
        std::mem::swap(&mut self.pending, &mut self.pending2);
        for msg in self.pending2.drain(..) {
            match self.addr.try_send(msg) {
                Err(TrySendError::Full(msg)) => self.pending.push(msg),
                Ok(()) | Err(_) => (),
            }
        }
        self.ready()
    }
    pub(crate) fn send(&self, msg: M) -> Result<()> {
        Ok(self.addr.send(msg)?)
    }
    pub(crate) fn maybe_send(&self, msg: M) -> bool {
        self.addr.try_send(msg).is_ok()
    }
}

impl Addr {
    pub fn id(&self) -> &ServantId {
        &self.id
    }

    pub(crate) fn send_insight(&mut self, event: Event) -> Result<()> {
        Ok(self.cf_addr.try_send_safe(CfMsg::Insight(event))?)
    }

    pub(crate) fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.addr.send(msg)?)
    }

    pub(crate) fn maybe_send(&self, msg: Msg) -> bool {
        self.addr.maybe_send(msg)
    }

    pub(crate) fn try_send_safe(&mut self, msg: Msg) -> Result<()> {
        Ok(self.addr.try_send_safe(msg)?)
    }

    pub(crate) fn drain_ready(&mut self) -> bool {
        self.addr.drain_ready() && self.cf_addr.drain_ready()
    }
}

impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
    }
}

pub(crate) enum CfMsg {
    Insight(Event),
}

#[derive(Debug)]
pub(crate) enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    ConnectOfframp(Cow<'static, str>, TremorURL, offramp::Addr),
    ConnectOnramp(TremorURL, onramp::Addr),
    ConnectPipeline(Cow<'static, str>, TremorURL, Box<Addr>),
    DisconnectOutput(Cow<'static, str>, TremorURL),
    DisconnectInput(TremorURL),
    Signal(Event),
}

#[derive(Debug)]
pub enum Dest {
    Offramp(TrySender<offramp::Msg>),
    Pipeline(Addr),
}
impl Dest {
    pub fn drain_ready(&mut self) -> bool {
        match self {
            Self::Offramp(addr) => addr.drain_ready(),
            Self::Pipeline(addr) => addr.drain_ready(),
        }
    }
    pub fn send_event(&mut self, input: Cow<'static, str>, event: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.try_send_safe(offramp::Msg::Event { input, event })?,
            Self::Pipeline(addr) => addr.try_send_safe(Msg::Event { input, event })?,
        }
        Ok(())
    }
    pub fn send_signal(&mut self, signal: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.try_send_safe(offramp::Msg::Signal(signal))?,
            Self::Pipeline(addr) => {
                // Each pipeline has their own ticks, we don't
                // want to propagate them
                if signal.kind != Some(SignalKind::Tick) {
                    addr.try_send_safe(Msg::Signal(signal))?
                }
            }
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
    uid: u64,
}

#[inline]
fn send_events(
    eventset: &mut Vec<(Cow<'static, str>, Event)>,
    dests: &mut halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>>,
) -> Result<()> {
    for (output, event) in eventset.drain(..) {
        if let Some(dest) = dests.get_mut(&output) {
            let len = dest.len();
            //We know we have len, so grabbing len - 1 elementsis safe
            for (id, offramp) in unsafe { dest.get_unchecked_mut(..len - 1) } {
                offramp.send_event(
                    id.instance_port()
                        .ok_or_else(|| Error::from(format!("missing instance port in {}.", id)))?
                        .to_string()
                        .into(),
                    event.clone(),
                )?;
            }
            //We know we have len, so grabbing the last elementsis safe
            let (id, offramp) = unsafe { dest.get_unchecked_mut(len - 1) };
            offramp.send_event(
                id.instance_port()
                    .ok_or_else(|| Error::from(format!("missing instance port in {}.", id)))?
                    .to_string()
                    .into(),
                event,
            )?;
        };
    }
    Ok(())
}

#[inline]
fn send_signal(
    own_id: &TremorURL,
    signal: Event,
    dests: &mut halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>>,
) -> Result<()> {
    let mut offramps = dests.values_mut().flatten();
    let first = offramps.next();
    for (id, offramp) in offramps {
        if id != own_id {
            offramp.send_signal(signal.clone())?;
        }
    }
    if let Some((id, offramp)) = first {
        if id != own_id {
            offramp.send_signal(signal)?;
        }
    }
    Ok(())
}

#[inline]
async fn handle_insight(
    skip_to: Option<usize>,
    insight: Event,
    pipeline: &mut ExecutableGraph,
    onramps: &halfbrown::HashMap<TremorURL, onramp::Addr>,
) {
    let insight = pipeline.contraflow(skip_to, insight);
    if let Some(cb) = insight.cb {
        for (_k, o) in onramps {
            if let Err(_e) = o.try_send(onramp::Msg::Cb(cb, insight.id.clone())) {
                // error!("[Pipeline] failed to send to pipeline: {}", e);
            }
        }
        //this is a trigger event
    }
}

#[inline]
async fn handle_insights(
    pipeline: &mut ExecutableGraph,
    onramps: &halfbrown::HashMap<TremorURL, onramp::Addr>,
) {
    let mut insights = Vec::with_capacity(pipeline.insights.len());
    std::mem::swap(&mut insights, &mut pipeline.insights);
    for (skip_to, insight) in insights.drain(..) {
        handle_insight(Some(skip_to), insight, pipeline, onramps).await
    }
}

async fn tick(tick_tx: CbSender<Msg>) {
    let mut e = Event {
        ingest_ns: nanotime(),
        kind: Some(SignalKind::Tick),
        ..Event::default()
    };

    while tick_tx.send(Msg::Signal(e.clone())).is_ok() {
        task::sleep(Duration::from_millis(TICK_MS)).await;
        e.ingest_ns = nanotime()
    }
}

fn pipeline_thread(
    id: &TremorURL,
    mut pipeline: ExecutableGraph,
    rx: &CbReceiver<Msg>,
    cf_rx: &CbReceiver<CfMsg>,
) -> Result<()> {
    let mut pid = id.clone();
    pid.trim_to_instance();
    pipeline.id = pid.to_string();

    let mut dests: halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>> =
        halfbrown::HashMap::new();
    let mut onramps: halfbrown::HashMap<TremorURL, onramp::Addr> = halfbrown::HashMap::new();
    let mut eventset: Vec<(Cow<'static, str>, Event)> = Vec::new();

    info!("[Pipeline:{}] starting thread.", id);
    for req in rx {
        // FIXME eprintln!("pipeline");
        loop {
            while let Ok(CfMsg::Insight(insight)) = cf_rx.try_recv() {
                task::block_on(handle_insight(None, insight, &mut pipeline, &onramps));
            }
            if dests
                .values_mut()
                .all(|v| v.iter_mut().all(|(_, dst)| dst.drain_ready()))
            {
                break;
            }
        }
        match req {
            Msg::Event { input, event } => {
                // FIXME eprintln!("event");
                match pipeline.enqueue(&input, event, &mut eventset) {
                    Ok(()) => {
                        task::block_on(handle_insights(&mut pipeline, &onramps));

                        if let Err(e) = send_events(&mut eventset, &mut dests) {
                            error!("Failed to send event: {}", e)
                        }
                    }
                    Err(e) => error!("error: {:?}", e),
                }
            }
            Msg::Signal(signal) => {
                if let Err(e) = pipeline.enqueue_signal(signal.clone(), &mut eventset) {
                    error!("error: {:?}", e)
                } else {
                    if let Err(e) = send_signal(&id, signal, &mut dests) {
                        error!("Failed to send signal: {}", e)
                    }
                    task::block_on(handle_insights(&mut pipeline, &onramps));

                    if let Err(e) = send_events(&mut eventset, &mut dests) {
                        error!("Failed to send event: {}", e)
                    }
                }
            }
            Msg::ConnectOfframp(output, offramp_id, offramp) => {
                info!(
                    "[Pipeline:{}] connecting {} to offramp {}",
                    id, output, offramp_id
                );
                if let Some(offramps) = dests.get_mut(&output) {
                    offramps.push((offramp_id, Dest::Offramp(offramp.into())));
                } else {
                    dests.insert(output, vec![(offramp_id, Dest::Offramp(offramp.into()))]);
                }
            }
            Msg::ConnectPipeline(output, pipeline_id, pipeline) => {
                info!(
                    "[Pipeline:{}] connecting {} to pipeline {}",
                    id, output, pipeline_id
                );
                if let Some(offramps) = dests.get_mut(&output) {
                    offramps.push((pipeline_id, Dest::Pipeline(*pipeline)));
                } else {
                    dests.insert(output, vec![(pipeline_id, Dest::Pipeline(*pipeline))]);
                }
            }
            Msg::ConnectOnramp(onramp_id, onramp) => {
                // FIXME eprintln!("connecting onramp: {}", onramp_id);
                onramps.insert(onramp_id, onramp);
            }
            Msg::DisconnectOutput(output, to_delete) => {
                let mut remove = false;
                if let Some(offramp_vec) = dests.get_mut(&output) {
                    offramp_vec.retain(|(this_id, _)| this_id != &to_delete);
                    remove = offramp_vec.is_empty();
                }
                if remove {
                    dests.remove(&output);
                }
            }
            Msg::DisconnectInput(onramp_id) => {
                // FIXME eprintln!("disconnecting onramp: {}", onramp_id);
                onramps.remove(&onramp_id);
            }
        };
    }
    info!("[Pipeline:{}] stopping thread.", id);
    Ok(())
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self {
            qsize,
            /// We're using a different 'numberspace' for operators so their ID's
            /// are unique from the onramps
            uid: 0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_u64,
        }
    }
    pub fn start(mut self) -> (JoinHandle<bool>, Sender) {
        let (tx, rx) = channel(64);
        let h = task::spawn(async move {
            info!("Pipeline manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping onramps...");
                        break;
                    }
                    Ok(ManagerMsg::Create(r, create)) => r.send(self.start_pipeline(create)).await,
                    Err(e) => {
                        info!("Stopping onramps... {}", e);
                        break;
                    }
                }
            }
            info!("Pipeline manager stopped");
            true
        });
        (h, tx)
    }

    fn start_pipeline(&mut self, req: Create) -> Result<Addr> {
        let config = req.config;
        let pipeline = config.to_executable_graph(&mut self.uid, tremor_pipeline::buildin_ops)?;

        let id = req.id.clone();

        let (tx, rx) = bounded::<Msg>(self.qsize);
        let (cf_tx, cf_rx) = bounded::<CfMsg>(self.qsize);

        let tick_tx = tx.clone();

        task::spawn(tick(tick_tx));
        thread::Builder::new()
            .name(format!("pipeline-{}", id.clone()))
            .spawn(move || pipeline_thread(&id, pipeline, &rx, &cf_rx))?;
        Ok(Addr {
            id: req.id,
            addr: tx.into(),
            cf_addr: cf_tx.into(),
        })
    }
}
