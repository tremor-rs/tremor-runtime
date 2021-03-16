// Copyright 2020-2021, The Tremor Team
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
use crate::errors::Result;
use crate::permge::{PriorityMerge, M};
use crate::registry::ServantId;
use crate::repository::PipelineArtefact;
use crate::url::TremorURL;
use crate::{offramp, onramp};
use async_channel::{bounded, unbounded};
use async_std::stream::StreamExt;
use async_std::task::{self, JoinHandle};
use beef::Cow;
use std::fmt;
use std::time::Duration;
use tremor_common::ids::OperatorIdGen;
use tremor_common::time::nanotime;
use tremor_pipeline::errors::ErrorKind as PipelineErrorKind;
use tremor_pipeline::{CBAction, Event, ExecutableGraph, SignalKind};

const TICK_MS: u64 = 100;
pub(crate) type Sender = async_channel::Sender<ManagerMsg>;
type Onramps = halfbrown::HashMap<TremorURL, (bool, onramp::Addr)>;
type Dests = halfbrown::HashMap<Cow<'static, str>, Vec<(TremorURL, Dest)>>;
type Eventset = Vec<(Cow<'static, str>, Event)>;
/// Address for a a pipeline
#[derive(Clone)]
pub struct Addr {
    addr: async_channel::Sender<Msg>,
    cf_addr: async_channel::Sender<CfMsg>,
    mgmt_addr: async_channel::Sender<MgmtMsg>,
    id: ServantId,
}

impl Addr {
    /// creates a new address
    pub(crate) fn new(
        addr: async_channel::Sender<Msg>,
        cf_addr: async_channel::Sender<CfMsg>,
        mgmt_addr: async_channel::Sender<MgmtMsg>,
        id: ServantId,
    ) -> Self {
        Self {
            addr,
            cf_addr,
            mgmt_addr,
            id,
        }
    }
    #[cfg(not(tarpaulin_include))]
    pub fn len(&self) -> usize {
        self.addr.len()
    }
    #[cfg(not(tarpaulin_include))]
    pub fn id(&self) -> &ServantId {
        &self.id
    }

    pub(crate) async fn send_insight(&self, event: Event) -> Result<()> {
        Ok(self.cf_addr.send(CfMsg::Insight(event)).await?)
    }

    pub(crate) async fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.addr.send(msg).await?)
    }

    pub(crate) fn try_send(&self, msg: Msg) -> bool {
        self.addr.try_send(msg).is_ok()
    }

    pub(crate) async fn send_mgmt(&self, msg: MgmtMsg) -> Result<()> {
        Ok(self.mgmt_addr.send(msg).await?)
    }
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
    }
}
#[derive(Debug)]
pub(crate) enum CfMsg {
    Insight(Event),
}

#[derive(Debug)]
pub(crate) enum MgmtMsg {
    ConnectOfframp(Cow<'static, str>, TremorURL, offramp::Addr),
    ConnectPipeline(Cow<'static, str>, TremorURL, Box<Addr>),
    ConnectLinkedOnramp(Cow<'static, str>, TremorURL, onramp::Addr),
    ConnectOnramp {
        id: TremorURL,
        addr: onramp::Addr,
        reply: bool,
    },
    DisconnectOutput(Cow<'static, str>, TremorURL),
    DisconnectInput(TremorURL),
}

#[derive(Debug)]
pub(crate) enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    Signal(Event),
}

#[derive(Debug)]
pub enum Dest {
    Offramp(async_channel::Sender<offramp::Msg>),
    Pipeline(Addr),
    LinkedOnramp(async_channel::Sender<onramp::Msg>),
}

impl Dest {
    pub async fn send_event(&mut self, input: Cow<'static, str>, event: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.send(offramp::Msg::Event { input, event }).await?,
            Self::Pipeline(addr) => addr.send(Msg::Event { input, event }).await?,
            Self::LinkedOnramp(addr) => addr.send(onramp::Msg::Response(event)).await?,
        }
        Ok(())
    }
    pub async fn send_signal(&mut self, signal: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.send(offramp::Msg::Signal(signal)).await?,
            Self::Pipeline(addr) => {
                // Each pipeline has their own ticks, we don't
                // want to propagate them
                if signal.kind != Some(SignalKind::Tick) {
                    addr.send(Msg::Signal(signal)).await?
                }
            }
            Self::LinkedOnramp(_addr) => {
                // TODO implement?
                //addr.send(onramp::Msg::Signal(signal)).await?
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
    Create(async_channel::Sender<Result<Addr>>, Create),
}

#[derive(Default, Debug)]
pub(crate) struct Manager {
    qsize: usize,
    operator_id_gen: OperatorIdGen,
}

#[inline]
async fn send_events(eventset: &mut Eventset, dests: &mut Dests) -> Result<()> {
    for (output, event) in eventset.drain(..) {
        if let Some(dest) = dests.get_mut(&output) {
            if let Some((last, rest)) = dest.split_last_mut() {
                for (id, offramp) in rest {
                    let port = id.instance_port_required()?.to_string().into();
                    offramp.send_event(port, event.clone()).await?;
                }
                let last_port = last.0.instance_port_required()?.to_string().into();
                last.1.send_event(last_port, event).await?;
            }
        };
    }
    Ok(())
}

#[inline]
async fn send_signal(own_id: &TremorURL, signal: Event, dests: &mut Dests) -> Result<()> {
    let mut offramps = dests.values_mut().flatten();
    let first = offramps.next();
    for (id, offramp) in offramps {
        if id != own_id {
            offramp.send_signal(signal.clone()).await?;
        }
    }
    if let Some((id, offramp)) = first {
        if id != own_id {
            offramp.send_signal(signal).await?;
        }
    }
    Ok(())
}

#[inline]
async fn handle_insight(
    skip_to: Option<usize>,
    insight: Event,
    pipeline: &mut ExecutableGraph,
    onramps: &Onramps,
) {
    let insight = pipeline.contraflow(skip_to, insight);
    if insight.cb != CBAction::None {
        for (_k, (send, o)) in onramps {
            if *send || insight.cb.is_cb() {
                if let Err(e) = o
                    .send(onramp::Msg::Cb(insight.cb, insight.id.clone()))
                    .await
                {
                    error!("[Pipeline] failed to send to onramp: {} {:?}", e, &o);
                }
            }
        }
    }
}

#[inline]
async fn handle_insights(pipeline: &mut ExecutableGraph, onramps: &Onramps) {
    if !pipeline.insights.is_empty() {
        let mut insights = Vec::with_capacity(pipeline.insights.len());
        std::mem::swap(&mut insights, &mut pipeline.insights);
        for (skip_to, insight) in insights.drain(..) {
            handle_insight(Some(skip_to), insight, pipeline, onramps).await
        }
    }
}

async fn tick(tick_tx: async_channel::Sender<Msg>) {
    let mut e = Event {
        ingest_ns: nanotime(),
        kind: Some(SignalKind::Tick),
        ..Event::default()
    };

    while tick_tx.send(Msg::Signal(e.clone())).await.is_ok() {
        task::sleep(Duration::from_millis(TICK_MS)).await;
        e.ingest_ns = nanotime();
    }
}

async fn handle_cf_msg(
    msg: CfMsg,
    pipeline: &mut ExecutableGraph,
    onramps: &Onramps,
) -> Result<()> {
    match msg {
        CfMsg::Insight(insight) => handle_insight(None, insight, pipeline, onramps).await,
    }
    Ok(())
}

fn maybe_send(r: Result<()>) {
    if let Err(e) = r {
        error!("Failed to send : {}", e)
    }
}

#[allow(clippy::too_many_lines)]
async fn pipeline_task(
    id: TremorURL,
    mut pipeline: ExecutableGraph,
    rx: async_channel::Receiver<Msg>,
    cf_rx: async_channel::Receiver<CfMsg>,
    mgmt_rx: async_channel::Receiver<MgmtMsg>,
) -> Result<()> {
    let mut pid = id.clone();
    pid.trim_to_instance();
    pipeline.id = pid.to_string();

    let mut dests: Dests = halfbrown::HashMap::new();
    let mut onramps: Onramps = halfbrown::HashMap::new();
    let mut eventset: Eventset = Vec::new();

    info!("[Pipeline:{}] starting task.", id);

    let ff = rx.map(M::F);
    let cf = cf_rx.map(M::C);
    let mf = mgmt_rx.map(M::M);

    // priotirize management flow over contra flow over forward event flow

    let mut s = PriorityMerge::new(mf, PriorityMerge::new(cf, ff));
    while let Some(msg) = s.next().await {
        match msg {
            M::C(msg) => {
                handle_cf_msg(msg, &mut pipeline, &onramps).await?;
            }
            M::F(Msg::Event { input, event }) => {
                match pipeline.enqueue(&input, event, &mut eventset) {
                    Ok(()) => {
                        handle_insights(&mut pipeline, &onramps).await;
                        maybe_send(send_events(&mut eventset, &mut dests).await);
                    }
                    Err(e) => {
                        let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                            let script_error = tremor_script::errors::Error(script_kind, e.1);
                            // possibly a hygienic error
                            pipeline
                                .source
                                .as_ref()
                                .and_then(|s| script_error.locate_in_source(s))
                                .map_or_else(
                                    || format!(" {:?}", script_error),
                                    |located| format!("\n{}", located),
                                ) // add a newline to have the error nicely formatted in the log
                        } else {
                            format!(" {}", e)
                        };
                        error!("Error handling event:{}", err_str);
                    }
                }
            }
            M::F(Msg::Signal(signal)) => {
                if let Err(e) = pipeline.enqueue_signal(signal.clone(), &mut eventset) {
                    let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                        let script_error = tremor_script::errors::Error(script_kind, e.1);
                        // possibly a hygienic error
                        pipeline
                            .source
                            .as_ref()
                            .and_then(|s| script_error.locate_in_source(s))
                            .map_or_else(
                                || format!(" {:?}", script_error),
                                |located| format!("\n{}", located),
                            ) // add a newline to have the error nicely formatted in the log
                    } else {
                        format!(" {:?}", e)
                    };
                    error!("Error handling signal:{}", err_str);
                } else {
                    maybe_send(send_signal(&id, signal, &mut dests).await);
                    handle_insights(&mut pipeline, &onramps).await;
                    maybe_send(send_events(&mut eventset, &mut dests).await);
                }
            }
            M::M(MgmtMsg::ConnectOfframp(output, offramp_id, offramp)) => {
                info!(
                    "[Pipeline:{}] connecting {} to offramp {}",
                    id, output, offramp_id
                );
                if let Some(output_dests) = dests.get_mut(&output) {
                    output_dests.push((offramp_id, Dest::Offramp(offramp)));
                } else {
                    dests.insert(output, vec![(offramp_id, Dest::Offramp(offramp))]);
                }
            }
            M::M(MgmtMsg::ConnectPipeline(output, pipeline_id, pipeline)) => {
                info!(
                    "[Pipeline:{}] connecting {} to pipeline {}",
                    id, output, pipeline_id
                );
                if let Some(output_dests) = dests.get_mut(&output) {
                    output_dests.push((pipeline_id, Dest::Pipeline(*pipeline)));
                } else {
                    dests.insert(output, vec![(pipeline_id, Dest::Pipeline(*pipeline))]);
                }
            }
            M::M(MgmtMsg::ConnectLinkedOnramp(output, onramp_id, onramp)) => {
                info!(
                    "[Pipeline:{}] connecting {} to linked onramp {}",
                    id, output, onramp_id
                );
                if let Some(output_dests) = dests.get_mut(&output) {
                    output_dests.push((onramp_id, Dest::LinkedOnramp(onramp)));
                } else {
                    dests.insert(output, vec![(onramp_id, Dest::LinkedOnramp(onramp))]);
                }
            }
            M::M(MgmtMsg::ConnectOnramp { id, addr, reply }) => {
                onramps.insert(id, (reply, addr));
            }
            M::M(MgmtMsg::DisconnectOutput(output, to_delete)) => {
                let mut remove = false;
                if let Some(output_vec) = dests.get_mut(&output) {
                    output_vec.retain(|(this_id, _)| this_id != &to_delete);
                    remove = output_vec.is_empty();
                }
                if remove {
                    dests.remove(&output);
                }
            }
            M::M(MgmtMsg::DisconnectInput(onramp_id)) => {
                onramps.remove(&onramp_id);
            }
        }
    }

    info!("[Pipeline:{}] stopping task.", id);
    Ok(())
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self {
            qsize,
            operator_id_gen: OperatorIdGen::new(),
        }
    }
    pub fn start(mut self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(crate::QSIZE);
        let h = task::spawn(async move {
            info!("Pipeline manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping onramps...");
                        break;
                    }
                    Ok(ManagerMsg::Create(r, create)) => {
                        r.send(self.start_pipeline(create)).await?
                    }
                    Err(e) => {
                        info!("Stopping Pipeline manager... {}", e);
                        break;
                    }
                }
            }
            info!("Pipeline manager stopped");
            Ok(())
        });
        (h, tx)
    }

    fn start_pipeline(&mut self, req: Create) -> Result<Addr> {
        let config = req.config;
        let pipeline = config.to_pipe(&mut self.operator_id_gen)?;

        let id = req.id.clone();

        let (tx, rx) = bounded::<Msg>(self.qsize);
        // We use a unbounded channel for counterflow, while an unbounded channel seems dangerous
        // there is soundness to this.
        // The unbounded channel ensures that on counterflow we never have to block, or in other
        // words that sinks or pipelines sending data backwards always can progress passt
        // the sending.
        // This prevents a livelock where the sink is waiting for a full channel to send data to
        // the pipeline and the pipeline is waiting for a full channel to send data to the sink.
        // We prevent unbounded groth by two mechanisms:
        // 1) counterflow is ALWAYS and ONLY created in response to a message
        // 2) we always process counterflow prior to forward flow
        //
        // As long as we have counterflow messages to process, and channel size is growing we do
        // not process any forward flow. Without forwardflow we stave the counterflow ensuring that
        // the counterflow channel is always bounded by the forward flow in a 1:N relationship where
        // N is the maximum number of counterflow events a single event can trigger.
        // N is normally < 1.
        let (cf_tx, cf_rx) = unbounded::<CfMsg>();
        let (mgmt_tx, mgmt_rx) = bounded::<MgmtMsg>(self.qsize);

        task::spawn(tick(tx.clone()));
        task::Builder::new()
            .name(format!("pipeline-{}", id))
            .spawn(pipeline_task(id, pipeline, rx, cf_rx, mgmt_rx))?;
        Ok(Addr::new(tx, cf_tx, mgmt_tx, req.id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tremor_pipeline::FN_REGISTRY;

    use tremor_script::prelude::*;
    use tremor_script::{path::ModulePath, query::Query};

    #[async_std::test]
    async fn test_pipeline_event_error() -> Result<()> {
        let module_path = ModulePath { mounts: vec![] };
        let query = r#"
            select event.non_existent
            from in
            into out;
        "#;
        let aggr_reg: tremor_script::registry::Aggr = tremor_script::aggr_registry();
        let q = Query::parse(
            &module_path,
            "manager_error_test.trickle",
            query,
            vec![],
            &*FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        let config = tremor_pipeline::query::Query(q);
        let id = TremorURL::parse("/pipeline/manager_error_test/instance")?;
        let manager = Manager::new(12);
        let (handle, sender) = manager.start();
        let (tx, rx) = async_channel::bounded(1);
        let create = Create { config, id };
        let create_msg = ManagerMsg::Create(tx, create);
        sender.send(create_msg).await?;
        let addr = rx.recv().await??;
        let (offramp_tx, offramp_rx) = async_channel::unbounded();
        let offramp_url = TremorURL::parse("/offramp/fake_offramp/instance/in")?;
        // connect a channel so we can receive events from the back of the pipeline :)
        addr.send_mgmt(MgmtMsg::ConnectOfframp(
            "out".into(),
            offramp_url,
            offramp_tx,
        ))
        .await?;

        // sending an event, that triggers an error
        addr.send(Msg::Event {
            event: Event::default(),
            input: "in".into(),
        })
        .await?;

        match offramp_rx.try_recv() {
            Err(async_channel::TryRecvError::Empty) => (), // ok
            _ => return Err("Expected no msg at out fake offramp!".into()),
        };

        // send a second event, that gets through
        let mut second_event = Event::default();
        let mut obj = Value::object_with_capacity(1);
        obj.insert("non_existent", Value::from(true))?;
        second_event.data = obj.into();
        addr.send(Msg::Event {
            event: second_event,
            input: "in".into(),
        })
        .await?;

        loop {
            println!("checking fake offramp for event");
            match offramp_rx.recv().await {
                Ok(offramp::Msg::Event { event, .. }) => {
                    println!("{:?}", event);
                    let (value, _meta) = event.data.suffix().clone().into_parts();
                    assert_eq!(Value::from(true), value); // check that we received what we sent in, not the faulty event
                    break;
                }
                Ok(offramp::Msg::Signal(_)) => {
                    // ignore
                    continue;
                }
                _ => return Err("Expected 1 msg at out fake offramp!".into()),
            };
        }

        // stopping the manager
        sender.send(ManagerMsg::Stop).await?;
        handle.cancel().await;
        Ok(())
    }
}
