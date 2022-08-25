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
use crate::{
    connectors::{self, sink::SinkMsg, source::SourceMsg},
    errors::{pipe_send_e, Result},
    instance::State,
    primerge::PriorityMerge,
    system::flow,
};
use async_std::{
    channel::{bounded, unbounded, Receiver, Sender},
    stream::StreamExt,
    task::{self, JoinHandle},
};
use beef::Cow;
use std::{fmt, sync::atomic::Ordering, time::Duration};
use tremor_common::{ids::OperatorIdGen, time::nanotime};
use tremor_pipeline::{
    errors::ErrorKind as PipelineErrorKind, CbAction, Event, ExecutableGraph, SignalKind,
};
use tremor_script::{ast::DeployEndpoint, highlighter::Dumb, prelude::BaseExpr};

const TICK_MS: u64 = 100;
type Inputs = halfbrown::HashMap<DeployEndpoint, (bool, InputTarget)>;
type Dests = halfbrown::HashMap<Cow<'static, str>, Vec<(DeployEndpoint, OutputTarget)>>;
type EventSet = Vec<(Cow<'static, str>, Event)>;

#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub(crate) struct Alias {
    flow_alias: flow::Alias,
    pipeline_alias: String,
}

impl Alias {
    pub(crate) fn new(
        flow_alias: impl Into<flow::Alias>,
        pipeline_alias: impl Into<String>,
    ) -> Self {
        Self {
            flow_alias: flow_alias.into(),
            pipeline_alias: pipeline_alias.into(),
        }
    }

    pub(crate) fn flow_alias(&self) -> &flow::Alias {
        &self.flow_alias
    }
    pub(crate) fn pipeline_alias(&self) -> &str {
        self.pipeline_alias.as_str()
    }
}

impl std::fmt::Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.flow_alias, self.pipeline_alias)
    }
}

/// Address for a pipeline
#[derive(Clone)]
pub struct Addr {
    addr: Sender<Box<Msg>>,
    cf_addr: Sender<CfMsg>,
    mgmt_addr: Sender<MgmtMsg>,
    alias: Alias,
}

impl Addr {
    /// creates a new address
    #[must_use]
    pub(crate) fn new(
        addr: Sender<Box<Msg>>,
        cf_addr: Sender<CfMsg>,
        mgmt_addr: Sender<MgmtMsg>,
        alias: Alias,
    ) -> Self {
        Self {
            addr,
            cf_addr,
            mgmt_addr,
            alias,
        }
    }

    /// send a contraflow insight message back down the pipeline
    pub(crate) async fn send_insight(&self, event: Event) -> Result<()> {
        use CfMsg::Insight;
        self.cf_addr.send(Insight(event)).await.map_err(pipe_send_e)
    }

    /// send a data-plane message to the pipeline
    pub(crate) async fn send(&self, msg: Box<Msg>) -> Result<()> {
        self.addr.send(msg).await.map_err(pipe_send_e)
    }

    pub(crate) async fn send_mgmt(&self, msg: MgmtMsg) -> Result<()> {
        self.mgmt_addr.send(msg).await.map_err(pipe_send_e)
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
pub(crate) enum InputTarget {
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
pub(crate) enum OutputTarget {
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

pub(crate) fn spawn(
    pipeline_alias: Alias,
    config: &tremor_pipeline::query::Query,
    operator_id_gen: &mut OperatorIdGen,
) -> Result<Addr> {
    let qsize = crate::QSIZE.load(Ordering::Relaxed);
    let mut pipeline = config.to_pipe(operator_id_gen)?;
    pipeline.optimize();

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

    let tick_handler = task::spawn(tick(tx.clone()));

    let addr = Addr::new(tx, cf_tx, mgmt_tx, pipeline_alias.clone());
    task::Builder::new()
        .name(format!(
            "pipeline-{}-{}",
            pipeline_alias.flow_alias(),
            pipeline_alias.pipeline_alias()
        ))
        .spawn(pipeline_task(
            pipeline_alias,
            pipeline,
            addr.clone(),
            rx,
            cf_rx,
            mgmt_rx,
            tick_handler,
        ))?;
    Ok(addr)
}

/// control plane message
#[derive(Debug)]
pub(crate) enum MgmtMsg {
    /// input can only ever be connected to the `in` port, so no need to include it here
    ConnectInput {
        /// port to connect in
        port: Cow<'static, str>,
        /// url of the input to connect
        endpoint: DeployEndpoint,
        /// sends the result
        tx: Sender<Result<()>>,
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
        /// sends the result
        tx: Sender<Result<()>>,
        /// the actual target addr
        target: OutputTarget,
    },
    /// start the pipeline
    Start,
    /// pause the pipeline - currently a no-op
    Pause,
    /// resume from pause - currently a no-op
    Resume,
    /// stop the pipeline
    Stop,
    #[cfg(test)]
    Inspect(Sender<report::StatusReport>),
}

#[cfg(test)]
#[allow(dead_code)]
mod report {
    use super::{DeployEndpoint, InputTarget, OutputTarget, State};

    #[derive(Debug, Clone)]
    pub(crate) struct StatusReport {
        pub(crate) state: State,
        pub(crate) inputs: Vec<InputReport>,
        pub(crate) outputs: halfbrown::HashMap<String, Vec<OutputReport>>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum InputReport {
        Pipeline { alias: String, port: String },
        Source { alias: String, port: String },
    }

    impl InputReport {
        pub(crate) fn new(endpoint: &DeployEndpoint, target: &InputTarget) -> Self {
            match target {
                InputTarget::Pipeline(_addr) => InputReport::Pipeline {
                    alias: endpoint.alias().to_string(),
                    port: endpoint.port().to_string(),
                },
                InputTarget::Source(_addr) => InputReport::Source {
                    alias: endpoint.alias().to_string(),
                    port: endpoint.port().to_string(),
                },
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum OutputReport {
        Pipeline { alias: String, port: String },
        Sink { alias: String, port: String },
    }

    impl From<&(DeployEndpoint, OutputTarget)> for OutputReport {
        fn from(target: &(DeployEndpoint, OutputTarget)) -> Self {
            match target {
                (endpoint, OutputTarget::Pipeline(_)) => OutputReport::Pipeline {
                    alias: endpoint.alias().to_string(),
                    port: endpoint.port().to_string(),
                },
                (endpoint, OutputTarget::Sink(_)) => OutputReport::Sink {
                    alias: endpoint.alias().to_string(),
                    port: endpoint.port().to_string(),
                },
            }
        }
    }
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
pub(crate) enum AnyMsg {
    Flow(Msg),
    Contraflow(CfMsg),
    Mgmt(MgmtMsg),
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
async fn send_events(eventset: &mut EventSet, dests: &mut Dests) -> Result<()> {
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
        }
    }
    Ok(())
}

#[inline]
async fn send_signal(own_id: &Alias, signal: Event, dests: &mut Dests) -> Result<()> {
    let mut destinations = dests.values_mut().flatten();
    let first = destinations.next();
    for (id, dest) in destinations {
        // if we are connected to ourselves we should not forward signals
        if matches!(dest, OutputTarget::Sink(_)) || id.alias() != own_id.pipeline_alias() {
            dest.send_signal(signal.clone()).await?;
        }
    }
    if let Some((id, dest)) = first {
        // if we are connected to ourselves we should not forward signals
        if matches!(dest, OutputTarget::Sink(_)) || id.alias() != own_id.pipeline_alias() {
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

fn maybe_send(r: Result<()>) {
    if let Err(e) = r {
        error!("Failed to send : {}", e);
    }
}

/// Context for pipeline instances
///
/// currently only used for printing
struct PipelineContext {
    alias: Alias,
}

impl std::fmt::Display for PipelineContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Pipeline::{}]", &self.alias)
    }
}

impl From<&Alias> for PipelineContext {
    fn from(alias: &Alias) -> Self {
        Self {
            alias: alias.clone(),
        }
    }
}

impl From<Alias> for PipelineContext {
    fn from(alias: Alias) -> Self {
        Self { alias }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn pipeline_task(
    id: Alias,
    mut pipeline: ExecutableGraph,
    addr: Addr,
    rx: Receiver<Box<Msg>>,
    cf_rx: Receiver<CfMsg>,
    mgmt_rx: Receiver<MgmtMsg>,
    tick_handler: JoinHandle<()>,
) -> Result<()> {
    pipeline.id = id.to_string();

    let ctx = PipelineContext::from(&id);

    let mut dests: Dests = halfbrown::HashMap::new();
    let mut inputs: Inputs = halfbrown::HashMap::new();
    let mut eventset = Vec::new();

    let mut state: State = State::Initializing;

    info!("{ctx} Starting Pipeline.");

    let ff = rx.map(|e| AnyMsg::Flow(*e));
    let cf = cf_rx.map(AnyMsg::Contraflow);
    let mf = mgmt_rx.map(AnyMsg::Mgmt);

    // prioritize management flow over contra flow over forward event flow
    let mut s = PriorityMerge::new(mf, PriorityMerge::new(cf, ff));
    while let Some(msg) = s.next().await {
        match msg {
            AnyMsg::Contraflow(msg) => {
                handle_cf_msg(msg, &mut pipeline, &inputs).await?;
            }
            AnyMsg::Flow(Msg::Event { input, event }) => {
                match pipeline.enqueue(&input, event, &mut eventset).await {
                    Ok(()) => {
                        handle_insights(&mut pipeline, &inputs).await;
                        maybe_send(send_events(&mut eventset, &mut dests).await);
                    }
                    Err(e) => {
                        let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                            let script_error = tremor_script::errors::Error(script_kind, e.1);

                            Dumb::error_to_string(&script_error)
                                .unwrap_or_else(|e| format!(" {script_error}: {e}"))
                        } else {
                            format!(" {e}")
                        };
                        error!("{ctx} Error handling event:{err_str}");
                    }
                }
            }
            AnyMsg::Flow(Msg::Signal(signal)) => {
                if let Err(e) = pipeline.enqueue_signal(signal.clone(), &mut eventset) {
                    let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                        let script_error = tremor_script::errors::Error(script_kind, e.1);
                        Dumb::error_to_string(&script_error)?
                    } else {
                        format!(" {:?}", e)
                    };
                    error!("{ctx} Error handling signal: {err_str}");
                } else {
                    maybe_send(send_signal(&id, signal, &mut dests).await);
                    handle_insights(&mut pipeline, &inputs).await;
                    maybe_send(send_events(&mut eventset, &mut dests).await);
                }
            }
            AnyMsg::Mgmt(MgmtMsg::ConnectInput {
                endpoint,
                port,
                tx,
                target,
                is_transactional,
            }) => {
                info!("{ctx} Connecting '{endpoint}' to port '{port}'");
                if !input_does_exist(&port, &pipeline) {
                    error!("{ctx} Error connecting input pipeline '{port}' as it does not exist",);
                    if tx
                        .send(Err("input port doesn't exist".into()))
                        .await
                        .is_err()
                    {
                        error!("{ctx} Error sending status report.");
                    }
                    continue;
                }
                inputs.insert(endpoint, (is_transactional, target));
                if tx.send(Ok(())).await.is_err() {
                    error!("{ctx} Status report sent.");
                }
            }

            AnyMsg::Mgmt(MgmtMsg::ConnectOutput {
                port,
                endpoint,
                target,
                tx,
            }) => {
                info!("{ctx} Connecting port '{port}' to {endpoint}",);
                // add error statement for port out in pipeline, currently no error messages
                if output_doesnt_exist(&port, &pipeline) {
                    error!("{ctx} Error connecting output pipeline {port}");
                    if tx
                        .send(Err("output port doesn't exist".into()))
                        .await
                        .is_err()
                    {
                        error!("{ctx} Error sending status report.");
                    }
                    continue;
                }
                // notify other pipeline about a new input
                if let OutputTarget::Pipeline(pipe) = &target {
                    // avoid linking the same pipeline as input to itself
                    // as this will create a nasty circle filling up queues.
                    // In general this does not avoid cycles via more complex constructs.
                    //
                    if id.pipeline_alias() != endpoint.alias() {
                        if let Err(e) = pipe
                            .send_mgmt(MgmtMsg::ConnectInput {
                                port: Cow::const_str("in"),
                                endpoint: DeployEndpoint::new(
                                    id.pipeline_alias(),
                                    &port,
                                    endpoint.meta(),
                                ),
                                tx,
                                target: InputTarget::Pipeline(Box::new(addr.clone())),
                                is_transactional: true,
                            })
                            .await
                        {
                            error!("{ctx} Error connecting input pipeline {endpoint}: {e}",);
                        }
                    }
                    else{
                        error!("{ctx} Error connecting output pipeline {port}");
                        if tx
                            .send(Err("Avoid linking the same pipeline as input to itself".into()))
                            .await
                            .is_err()
                        {
                            error!("{ctx} Error sending status report.");
                        }
                        continue;
                        }
                }
                else{
                    if tx.send(Ok(())).await.is_err() {
                        error!("{ctx} Status report sent.");
                    }
                }

                if let Some(output_dests) = dests.get_mut(&port) {
                    output_dests.push((endpoint, target));
                } else {
                    dests.insert(port, vec![(endpoint, target)]);
                }
            }
            AnyMsg::Mgmt(MgmtMsg::Start) if state == State::Initializing => {
                // No-op
                state = State::Running;
            }
            AnyMsg::Mgmt(MgmtMsg::Start) => {
                info!("{ctx} Ignoring Start Msg. Current state: {state}",);
            }
            AnyMsg::Mgmt(MgmtMsg::Pause) if state == State::Running => {
                // No-op
                state = State::Paused;
            }
            AnyMsg::Mgmt(MgmtMsg::Pause) => {
                info!("{ctx} Ignoring Pause Msg. Current state: {state}",);
            }
            AnyMsg::Mgmt(MgmtMsg::Resume) if state == State::Paused => {
                // No-op
                state = State::Running;
            }
            AnyMsg::Mgmt(MgmtMsg::Resume) => {
                info!("{ctx} Ignoring Resume Msg. Current state: {state}",);
            }
            AnyMsg::Mgmt(MgmtMsg::Stop) => {
                info!("{ctx} Stopping...");
                break;
            }
            #[cfg(test)]
            AnyMsg::Mgmt(MgmtMsg::Inspect(tx)) => {
                use report::*;
                let inputs: Vec<InputReport> = inputs
                    .iter()
                    .map(|(k, v)| InputReport::new(k, &v.1))
                    .collect::<Vec<_>>();
                let outputs: halfbrown::HashMap<String, Vec<OutputReport>> = dests
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            v.iter().map(OutputReport::from).collect::<Vec<_>>(),
                        )
                    })
                    .collect();

                let report = StatusReport {
                    state,
                    inputs,
                    outputs,
                };
                if tx.send(report).await.is_err() {
                    error!("{ctx} Error sending status report.");
                }
            }
        }
    }
    // stop ticks
    tick_handler.cancel().await;

    info!("{ctx} Stopped.");
    Ok(())
}

fn output_doesnt_exist(port: &Cow<'static, str>, pipeline: &ExecutableGraph) -> bool {
    // function checks if port is in pipeline
    !pipeline.outputs.contains_key(port)
}

fn input_does_exist(port: &Cow<'static, str>, pipeline: &ExecutableGraph) -> bool {
    // function checks if port is in pipeline
    pipeline.inputs().contains_key(port)
}
#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        connectors::{prelude::SinkAddr, source::SourceAddr},
        pipeline::report::{InputReport, OutputReport},
    };
    use std::time::Instant;
    use tremor_common::{
        ids::Id as _,
        ids::SourceId,
        ports::{IN, OUT},
    };
    use tremor_pipeline::{EventId, OpMeta};
    use tremor_script::{aggr_registry, lexer::Location, NodeMeta, FN_REGISTRY};
    use tremor_value::Value;

    #[async_std::test]
    async fn report() -> Result<()> {
        let _ = env_logger::try_init();
        let mut operator_id_gen = OperatorIdGen::new();
        let trickle = r#"select event from in into out;"#;
        let aggr_reg = aggr_registry();
        let query =
            tremor_pipeline::query::Query::parse(trickle, &*FN_REGISTRY.read()?, &aggr_reg)?;
        let addr = spawn(
            Alias::new("report", "test-pipe1"),
            &query,
            &mut operator_id_gen,
        )?;
        let addr2 = spawn(
            Alias::new("report", "test-pipe2"),
            &query,
            &mut operator_id_gen,
        )?;
        let addr3 = spawn(
            Alias::new("report", "test-pipe3"),
            &query,
            &mut operator_id_gen,
        )?;
        println!("{:?}", addr); // coverage
        let yolo_mid = NodeMeta::new(Location::yolo(), Location::yolo());
        let (tx, rx) = bounded(1);
        // interconnect 3 pipelines
        addr.send_mgmt(MgmtMsg::ConnectOutput {
            port: Cow::const_str("out"),
            endpoint: DeployEndpoint::new("snot2", "in", &yolo_mid),
            tx,
            target: OutputTarget::Pipeline(Box::new(addr2.clone())),
        })
        .await?;
        rx.recv().await??;
        let (tx, rx) = bounded(1);
        addr2
            .send_mgmt(MgmtMsg::ConnectInput {
                port: Cow::const_str("in"),
                endpoint: DeployEndpoint::new("snot", "out", &yolo_mid),
                tx,
                target: InputTarget::Pipeline(Box::new(addr.clone())),
                is_transactional: true,
            })
            .await?;
        rx.recv().await??;
        let (tx, rx) = bounded(1);
        addr2
            .send_mgmt(MgmtMsg::ConnectOutput {
                tx,
                port: Cow::const_str("out"),
                endpoint: DeployEndpoint::new("snot3", "in", &yolo_mid),
                target: OutputTarget::Pipeline(Box::new(addr3.clone())),
            })
            .await?;
        rx.recv().await??;
        let (tx, rx) = bounded(1);
        addr3
            .send_mgmt(MgmtMsg::ConnectInput {
                port: Cow::const_str("in"),
                endpoint: DeployEndpoint::new("snot2", "out", &yolo_mid),
                tx,
                target: InputTarget::Pipeline(Box::new(addr2.clone())),
                is_transactional: false,
            })
            .await?;
        rx.recv().await??;
        // get a status report from every single one
        let (tx, rx) = unbounded();
        addr.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let mut report1 = rx.recv().await?;
        assert!(report1.inputs.is_empty());
        let mut output1 = report1
            .outputs
            .remove("out")
            .expect("nothing at port `out`");
        assert!(report1.outputs.is_empty());
        assert_eq!(1, output1.len());
        let output1 = output1.pop().unwrap();
        assert_eq!(
            output1,
            OutputReport::Pipeline {
                alias: "snot2".to_string(),
                port: "in".to_string()
            }
        );
        addr2.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let mut report2 = rx.recv().await?;
        let input2 = report2.inputs.pop().expect("no input at port in");
        assert_eq!(
            input2,
            InputReport::Pipeline {
                alias: "snot".to_string(),
                port: "out".to_string()
            }
        );
        let mut output2 = report2
            .outputs
            .remove("out")
            .expect("no outputs on out port");
        assert!(report2.outputs.is_empty());
        assert_eq!(1, output2.len());
        let output2 = output2.pop().unwrap();
        assert_eq!(
            output2,
            OutputReport::Pipeline {
                alias: "snot3".to_string(),
                port: "in".to_string()
            }
        );

        addr3.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let mut report3 = rx.recv().await?;
        assert!(report3.outputs.is_empty());
        let input3 = report3.inputs.pop().expect("no inputs");
        assert_eq!(
            input3,
            InputReport::Pipeline {
                alias: "snot2".to_string(),
                port: "out".to_string()
            }
        );

        addr.stop().await?;
        addr2.stop().await?;
        addr3.stop().await?;
        Ok(())
    }

    #[async_std::test]
    async fn pipeline_spawn() -> Result<()> {
        let _ = env_logger::try_init();
        let mut operator_id_gen = OperatorIdGen::new();
        let trickle = r#"select event from in into out;"#;
        let aggr_reg = aggr_registry();
        let pipeline_id = Alias::new("flow", "test-pipe");
        let query =
            tremor_pipeline::query::Query::parse(trickle, &*FN_REGISTRY.read()?, &aggr_reg)?;
        let addr = spawn(pipeline_id, &query, &mut operator_id_gen)?;

        let (tx, rx) = unbounded();
        addr.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let report = rx.recv().await?;
        assert_eq!(State::Initializing, report.state);
        assert!(report.inputs.is_empty());
        assert!(report.outputs.is_empty());

        addr.start().await?;
        addr.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let report = rx.recv().await?;
        assert_eq!(State::Running, report.state);
        assert!(report.inputs.is_empty());
        assert!(report.outputs.is_empty());

        // connect a fake source as input
        let (source_tx, source_rx) = unbounded();
        let source_addr = SourceAddr { addr: source_tx };
        let target = InputTarget::Source(source_addr);
        let mid = NodeMeta::new(Location::yolo(), Location::yolo());
        let (tx, rx) = bounded(1);
        addr.send_mgmt(MgmtMsg::ConnectInput {
            port: IN,
            endpoint: DeployEndpoint::new(&"source_01", &OUT, &mid),
            tx,
            target,
            is_transactional: true,
        })
        .await?;
        rx.recv().await??;

        let (tx, rx) = unbounded();
        addr.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let report = rx.recv().await?;
        assert_eq!(1, report.inputs.len());
        assert_eq!(
            report::InputReport::Source {
                alias: "source_01".to_string(),
                port: OUT.to_string()
            },
            report.inputs[0]
        );

        // connect a fake sink as output
        let (sink_tx, sink_rx) = unbounded();
        let sink_addr = SinkAddr { addr: sink_tx };
        let target = OutputTarget::Sink(sink_addr);
        let (tx, rx) = bounded(1);
        addr.send_mgmt(MgmtMsg::ConnectOutput {
            endpoint: DeployEndpoint::new(&"sink_01", &IN, &mid),
            port: OUT,
            tx,
            target,
        })
        .await?;
        rx.recv().await??;

        let (tx, rx) = unbounded();
        addr.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let report = rx.recv().await?;
        assert_eq!(1, report.outputs.len());
        assert_eq!(
            Some(&vec![report::OutputReport::Sink {
                alias: "sink_01".to_string(),
                port: IN.to_string()
            }]),
            report.outputs.get(&OUT.to_string())
        );

        // send an event
        let event = Event {
            data: (Value::from(42_u64), Value::from(true)).into(),
            ..Event::default()
        };
        addr.send(Box::new(Msg::Event { event, input: IN })).await?;

        let mut sink_msg = sink_rx.recv().await?;
        while let SinkMsg::Signal { .. } = sink_msg {
            sink_msg = sink_rx.recv().await?;
        }
        match sink_msg {
            SinkMsg::Event { event, port: _ } => {
                assert_eq!(Value::from(42_usize), event.data.suffix().value());
                assert_eq!(Value::from(true), event.data.suffix().meta());
            }
            other => assert!(false, "Expected Event, got: {:?}", other),
        }

        // send a signal
        addr.send(Box::new(Msg::Signal(Event::signal_drain(SourceId::new(
            42,
        )))))
        .await?;

        let start = Instant::now();
        let mut sink_msg = sink_rx.recv().await?;

        while match sink_msg {
            SinkMsg::Signal {
                signal:
                    Event {
                        kind: Some(SignalKind::Drain(id)),
                        ..
                    },
            } if id.id() == 42 => false,
            _ => true,
        } {
            if start.elapsed() > Duration::from_secs(4) {
                assert!(false, "Timed out waiting for drain signal on the sink");
            }
            sink_msg = sink_rx.recv().await?;
        }

        let event_id = EventId::from_id(1, 1, 1);
        addr.send_insight(Event::cb_ack(0, event_id.clone(), OpMeta::default()))
            .await?;
        let source_msg = source_rx.recv().await?;
        if let SourceMsg::Cb(cb_action, cb_id) = source_msg {
            assert_eq!(event_id, cb_id);
            assert_eq!(CbAction::Ack, cb_action);
        } else {
            assert!(false, "Expected SourceMsg::Cb, got: {:?}", source_msg);
        }

        // test pause and resume
        addr.pause().await?;
        addr.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let report = rx.recv().await?;
        assert_eq!(State::Paused, report.state);
        assert_eq!(1, report.inputs.len());
        assert_eq!(1, report.outputs.len());

        addr.resume().await?;
        addr.send_mgmt(MgmtMsg::Inspect(tx.clone())).await?;
        let report = rx.recv().await?;
        assert_eq!(State::Running, report.state);
        assert_eq!(1, report.inputs.len());
        assert_eq!(1, report.outputs.len());

        // finally stop
        addr.stop().await?;
        // verify we cannot send to the pipeline after it is stopped
        let start = Instant::now();
        while !addr
            .send(Box::new(Msg::Signal(Event::signal_tick())))
            .await
            .is_err()
        {
            if start.elapsed() > Duration::from_secs(5) {
                assert!(false, "Pipeline didnt stop!");
            }
            task::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
}
