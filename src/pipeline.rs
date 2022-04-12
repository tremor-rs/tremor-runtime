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
use crate::permge::PriorityMerge;
use crate::{
    connectors::{self, sink::SinkMsg, source::SourceMsg},
    instance::InstanceState,
};
use async_std::channel::{bounded, unbounded, Receiver, Sender};
use async_std::stream::StreamExt;
use async_std::task;
use beef::Cow;
use std::time::Duration;
use std::{fmt, sync::atomic::Ordering};
use tremor_common::{ids::OperatorIdGen, time::nanotime};
use tremor_pipeline::errors::ErrorKind as PipelineErrorKind;
use tremor_pipeline::{CbAction, Event, ExecutableGraph, SignalKind};
use tremor_script::ast::DeployEndpoint;

const TICK_MS: u64 = 100;
type Inputs = halfbrown::HashMap<DeployEndpoint, (bool, InputTarget)>;
type Dests = halfbrown::HashMap<Cow<'static, str>, Vec<(DeployEndpoint, OutputTarget)>>;
type Eventset = Vec<(Cow<'static, str>, Event)>;
/// Address for a pipeline
#[derive(Clone)]
pub struct Addr {
    addr: Sender<Box<Msg>>,
    cf_addr: Sender<CfMsg>,
    mgmt_addr: Sender<MgmtMsg>,
    alias: String,
}

impl Addr {
    /// creates a new address
    #[must_use]
    pub fn new(
        addr: Sender<Box<Msg>>,
        cf_addr: Sender<CfMsg>,
        mgmt_addr: Sender<MgmtMsg>,
        alias: String,
    ) -> Self {
        Self {
            addr,
            cf_addr,
            mgmt_addr,
            alias,
        }
    }

    /// number of events in the pipelines channel
    #[cfg(not(tarpaulin_include))]
    #[must_use]
    pub fn len(&self) -> usize {
        self.addr.len()
    }

    /// true, if there are no events to be received at the moment
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.addr.is_empty()
    }

    /// pipeline instance id
    #[cfg(not(tarpaulin_include))]
    #[must_use]
    pub fn id(&self) -> &str {
        &self.alias
    }

    /// send a contraflow insight message back down the pipeline
    pub async fn send_insight(&self, event: Event) -> Result<()> {
        Ok(self.cf_addr.send(CfMsg::Insight(event)).await?)
    }

    /// send a data-plane message to the pipeline
    pub async fn send(&self, msg: Box<Msg>) -> Result<()> {
        Ok(self.addr.send(msg).await?)
    }

    pub(crate) async fn send_mgmt(&self, msg: MgmtMsg) -> Result<()> {
        Ok(self.mgmt_addr.send(msg).await?)
    }

    pub(crate) async fn stop(&self) -> Result<()> {
        self.send_mgmt(MgmtMsg::Stop).await
    }

    pub(crate) async fn start(&self) -> Result<()> {
        self.send_mgmt(MgmtMsg::Start).await
    }
    pub(crate) async fn pause(&self) -> Result<()> {
        self.send_mgmt(MgmtMsg::Pause).await
    }
    pub(crate) async fn resume(&self) -> Result<()> {
        self.send_mgmt(MgmtMsg::Resume).await
    }
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.alias)
    }
}

/// contraflow message
#[derive(Debug)]
pub enum CfMsg {
    /// insight
    Insight(Event),
}

/// Input targets
#[derive(Debug)]
pub enum InputTarget {
    /// another pipeline
    Pipeline(Box<Addr>),
    /// a connector
    Source(connectors::source::SourceAddr),
}

impl InputTarget {
    async fn send_insight(&self, insight: Event) -> Result<()> {
        match self {
            InputTarget::Pipeline(addr) => addr.send_insight(insight).await,
            InputTarget::Source(addr) => addr.send(SourceMsg::Cb(insight.cb, insight.id)).await,
        }
    }
}

/// Output targets
#[derive(Debug)]
pub enum OutputTarget {
    /// another pipeline
    Pipeline(Box<Addr>),
    /// a connector
    Sink(connectors::sink::SinkAddr),
}

impl From<Addr> for OutputTarget {
    fn from(p: Addr) -> Self {
        Self::Pipeline(Box::new(p))
    }
}
impl TryFrom<connectors::Addr> for OutputTarget {
    type Error = crate::errors::Error;

    fn try_from(addr: connectors::Addr) -> Result<Self> {
        Ok(Self::Sink(addr.sink.ok_or("Connector has no sink")?))
    }
}

pub(crate) async fn spawn(
    alias: String,
    config: tremor_pipeline::query::Query,
    operator_id_gen: &mut OperatorIdGen,
) -> Result<Addr> {
    let qsize = crate::QSIZE.load(Ordering::Relaxed);
    let pipeline = config.to_pipe(operator_id_gen)?;

    let (tx, rx) = bounded::<Box<Msg>>(qsize);
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
    let (mgmt_tx, mgmt_rx) = bounded::<MgmtMsg>(qsize);

    task::spawn(tick(tx.clone()));

    let addr = Addr::new(tx, cf_tx, mgmt_tx, alias.clone());
    task::Builder::new()
        .name(format!("pipeline-{}", alias.clone()))
        .spawn(pipeline_task(
            alias.clone(),
            pipeline,
            addr.clone(),
            rx,
            cf_rx,
            mgmt_rx,
        ))?;
    Ok(addr)
}

/// control plane message
#[derive(Debug)]
pub enum MgmtMsg {
    /// input can only ever be connected to the `in` port, so no need to include it here
    ConnectInput {
        /// url of the input to connect
        endpoint: DeployEndpoint,
        /// the target that connects to the `in` port
        target: InputTarget,
        /// should we send insights to this input
        is_transactional: bool,
    },
    /// connect a target to an output port
    ConnectOutput {
        /// the port to connect to
        port: Cow<'static, str>,
        /// the url of the output instance
        endpoint: DeployEndpoint,
        /// the actual target addr
        target: OutputTarget,
    },
    /// disconnect an output
    DisconnectOutput(Cow<'static, str>, DeployEndpoint),
    /// disconnect an input
    DisconnectInput(DeployEndpoint),

    /// FIXME: messages for transitioning from one state to the other
    /// start the pipeline
    Start,
    /// pause the pipeline - currently a no-op
    Pause,
    /// resume from pause - currently a no-op
    Resume,
    /// stop the pipeline
    Stop,

    /// for testing - ensures we drain the channel up to this message
    #[cfg(test)]
    Echo(Sender<()>),
}

/// an input dataplane message for this pipeline
#[derive(Debug)]
pub enum Msg {
    /// an event
    Event {
        /// the event
        event: Event,
        /// the port the event came in from
        input: Cow<'static, str>,
    },
    /// a signal
    Signal(Event),
}

/// wrapper for all possible messages handled by the pipeline task
#[derive(Debug)]
pub(crate) enum M {
    F(Msg),
    C(CfMsg),
    M(MgmtMsg),
}

impl OutputTarget {
    /// send an event out to this destination
    ///
    /// # Errors
    ///  * when sending the event via the dest channel fails
    pub async fn send_event(&mut self, input: Cow<'static, str>, event: Event) -> Result<()> {
        match self {
            Self::Pipeline(addr) => addr.send(Box::new(Msg::Event { input, event })).await?,
            Self::Sink(addr) => {
                addr.addr
                    .send(SinkMsg::Event { event, port: input })
                    .await?;
            }
        }
        Ok(())
    }

    /// send a signal out to this destination
    ///
    /// # Errors
    ///  * when sending the signal via the dest channel fails
    pub async fn send_signal(&mut self, signal: Event) -> Result<()> {
        match self {
            Self::Pipeline(addr) => {
                // Each pipeline has their own ticks, we don't
                // want to propagate them
                if signal.kind != Some(SignalKind::Tick) {
                    addr.send(Box::new(Msg::Signal(signal))).await?;
                }
            }
            Self::Sink(addr) => {
                addr.addr.send(SinkMsg::Signal { signal }).await?;
            }
        }
        Ok(())
    }
}

#[inline]
async fn send_events(eventset: &mut Eventset, dests: &mut Dests) -> Result<()> {
    for (output, event) in eventset.drain(..) {
        if let Some(destinations) = dests.get_mut(&output) {
            if let Some((last, rest)) = destinations.split_last_mut() {
                for (id, dest) in rest {
                    let port = id.port().to_string().into();
                    dest.send_event(port, event.clone()).await?;
                }
                let last_port = last.0.port().to_string().into();
                last.1.send_event(last_port, event).await?;
            }
        };
    }
    Ok(())
}

#[inline]
async fn send_signal(own_id: &str, signal: Event, dests: &mut Dests) -> Result<()> {
    let mut destinations = dests.values_mut().flatten();
    let first = destinations.next();
    for (id, dest) in destinations {
        // if we are connected to ourselves we should not forward signals
        if matches!(dest, OutputTarget::Sink(_)) || id.alias() != own_id {
            dest.send_signal(signal.clone()).await?;
        }
    }
    if let Some((id, dest)) = first {
        // if we are connected to ourselves we should not forward signals
        if matches!(dest, OutputTarget::Sink(_)) || id.alias() != own_id {
            dest.send_signal(signal).await?;
        }
    }
    Ok(())
}

#[inline]
async fn handle_insight(
    skip_to: Option<usize>,
    insight: Event,
    pipeline: &mut ExecutableGraph,
    inputs: &Inputs,
) {
    let insight = pipeline.contraflow(skip_to, insight);
    if insight.cb != CbAction::None {
        let mut input_iter = inputs.iter();
        let first = input_iter.next();
        let always_deliver = insight.cb.always_deliver();
        for (url, (input_is_transactional, input)) in input_iter {
            if always_deliver || *input_is_transactional {
                if let Err(e) = input.send_insight(insight.clone()).await {
                    error!(
                        "[Pipeline::{}] failed to send insight to input: {} {}",
                        pipeline.id, e, url
                    );
                }
            }
        }
        if let Some((url, (input_is_transactional, input))) = first {
            if always_deliver || *input_is_transactional {
                if let Err(e) = input.send_insight(insight).await {
                    error!(
                        "[Pipeline::{}] failed to send insight to input: {} {}",
                        pipeline.id, e, url
                    );
                }
            }
        }
    }
}

#[inline]
async fn handle_insights(pipeline: &mut ExecutableGraph, onramps: &Inputs) {
    if !pipeline.insights.is_empty() {
        let mut insights = Vec::with_capacity(pipeline.insights.len());
        std::mem::swap(&mut insights, &mut pipeline.insights);
        for (skip_to, insight) in insights.drain(..) {
            handle_insight(Some(skip_to), insight, pipeline, onramps).await;
        }
    }
}

pub(crate) async fn tick(tick_tx: Sender<Box<Msg>>) {
    let mut e = Event::signal_tick();
    while tick_tx.send(Box::new(Msg::Signal(e.clone()))).await.is_ok() {
        task::sleep(Duration::from_millis(TICK_MS)).await;
        e.ingest_ns = nanotime();
    }
}

async fn handle_cf_msg(msg: CfMsg, pipeline: &mut ExecutableGraph, inputs: &Inputs) -> Result<()> {
    match msg {
        CfMsg::Insight(insight) => handle_insight(None, insight, pipeline, inputs).await,
    }
    Ok(())
}

#[cfg(not(tarpaulin_include))]
fn maybe_send(r: Result<()>) {
    if let Err(e) = r {
        error!("Failed to send : {}", e);
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn pipeline_task(
    alias: String,
    mut pipeline: ExecutableGraph,
    addr: Addr,
    rx: Receiver<Box<Msg>>,
    cf_rx: Receiver<CfMsg>,
    mgmt_rx: Receiver<MgmtMsg>,
) -> Result<()> {
    pipeline.id = alias.clone();

    let mut dests: Dests = halfbrown::HashMap::new();
    let mut inputs: Inputs = halfbrown::HashMap::new();
    let mut eventset: Eventset = Vec::new();

    let mut state: InstanceState = InstanceState::Initializing;

    info!("[Pipeline::{}] Starting Pipeline.", alias);

    let ff = rx.map(|e| M::F(*e));
    let cf = cf_rx.map(M::C);
    let mf = mgmt_rx.map(M::M);

    // prioritize management flow over contra flow over forward event flow
    let mut s = PriorityMerge::new(mf, PriorityMerge::new(cf, ff));
    while let Some(msg) = s.next().await {
        match msg {
            M::C(msg) => {
                handle_cf_msg(msg, &mut pipeline, &inputs).await?;
            }
            M::F(Msg::Event { input, event }) => {
                match pipeline.enqueue(&input, event, &mut eventset).await {
                    Ok(()) => {
                        handle_insights(&mut pipeline, &inputs).await;
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
                    error!("[Pipeline::{}] Error handling signal:{}", alias, err_str);
                } else {
                    maybe_send(send_signal(&alias, signal, &mut dests).await);
                    handle_insights(&mut pipeline, &inputs).await;
                    maybe_send(send_events(&mut eventset, &mut dests).await);
                }
            }
            M::M(MgmtMsg::ConnectInput {
                endpoint,
                target,
                is_transactional,
            }) => {
                info!("[Pipeline::{}] Connecting {} to port 'in'", alias, endpoint);
                inputs.insert(endpoint, (is_transactional, target.into()));
            }
            M::M(MgmtMsg::ConnectOutput {
                port,
                endpoint,
                target,
            }) => {
                info!(
                    "[Pipeline::{}] Connecting port '{}' to {}",
                    alias, &port, &endpoint
                );
                // notify other pipeline about a new input
                if let OutputTarget::Pipeline(pipe) = &target {
                    // avoid linking the same pipeline as input to itself
                    // as this will create a nasty circle filling up queues.
                    // In general this does not avoid cycles via more complex constructs.
                    //
                    if alias == endpoint.alias() {
                        if let Err(e) = pipe
                            .send_mgmt(MgmtMsg::ConnectInput {
                                endpoint: DeployEndpoint::new(alias.to_string(), port.to_string()),
                                target: InputTarget::Pipeline(Box::new(addr.clone())),
                                is_transactional: true,
                            })
                            .await
                        {
                            error!(
                                "[Pipeline::{}] Error connecting input pipeline {}: {}",
                                alias, &endpoint, e
                            );
                        }
                    }
                }

                if let Some(output_dests) = dests.get_mut(&port) {
                    output_dests.push((endpoint, target.into()));
                } else {
                    dests.insert(port, vec![(endpoint, target.into())]);
                }
            }
            M::M(MgmtMsg::DisconnectOutput(port, to_delete)) => {
                info!(
                    "[Pipeline::{}] Disconnecting {} from '{}'",
                    alias, &to_delete, &port
                );

                let mut remove = false;
                if let Some(output_vec) = dests.get_mut(&port) {
                    while let Some(index) = output_vec.iter().position(|(k, _)| k == &to_delete) {
                        if let (delete_url, OutputTarget::Pipeline(pipe)) =
                            output_vec.swap_remove(index)
                        {
                            if let Err(e) = pipe
                                .send_mgmt(MgmtMsg::DisconnectInput(DeployEndpoint::new(
                                    alias.to_string(),
                                    port.to_string(),
                                )))
                                .await
                            {
                                error!(
                                    "[Pipeline::{}] Error disconnecting input pipeline {}: {}",
                                    alias, &delete_url, e
                                );
                            }
                        }
                    }
                    remove = output_vec.is_empty();
                }
                if remove {
                    dests.remove(&port);
                }
            }
            M::M(MgmtMsg::DisconnectInput(input_url)) => {
                info!(
                    "[Pipeline::{}] Disconnecting {} from 'in'",
                    alias, &input_url
                );
                inputs.remove(&input_url);
            }
            M::M(MgmtMsg::Start) if state == InstanceState::Initializing => {
                // No-op
                state = InstanceState::Running;
            }
            M::M(MgmtMsg::Start) => {
                info!(
                    "[Pipeline::{}] Ignoring Start Msg. Current state: {}",
                    alias, &state
                );
            }
            M::M(MgmtMsg::Pause) if state == InstanceState::Running => {
                // No-op
                state = InstanceState::Paused;
            }
            M::M(MgmtMsg::Pause) => {
                info!(
                    "[Pipeline::{}] Ignoring Pause Msg. Current state: {}",
                    alias, &state
                );
            }
            M::M(MgmtMsg::Resume) if state == InstanceState::Paused => {
                // No-op
                state = InstanceState::Running;
            }
            M::M(MgmtMsg::Resume) => {
                info!(
                    "[Pipeline::{}] Ignoring Resume Msg. Current state: {}",
                    alias, &state
                );
            }
            M::M(MgmtMsg::Stop) => {
                info!("[Pipeline::{}] Stopping...", alias);
                break;
            }
            #[cfg(test)]
            M::M(MgmtMsg::Echo(sender)) => {
                if let Err(e) = sender.send(()).await {
                    error!(
                        "[Pipeline::{}] Error responding to echo message: {}",
                        alias, e
                    );
                }
            }
        }
    }

    info!("[Pipeline::{}] Stopped.", alias);
    Ok(())
}
