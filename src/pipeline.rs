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
    errors::Result,
    instance::State,
    permge::PriorityMerge,
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
    errors::ErrorKind as PipelineErrorKind, CbAction, Event, ExecutableGraph, GraphReturns,
    SignalKind,
};
use tremor_script::{ast::DeployEndpoint, highlighter::Dumb, prelude::BaseExpr};

const TICK_MS: u64 = 100;
type Inputs = halfbrown::HashMap<DeployEndpoint, (bool, InputTarget)>;
type Dests = halfbrown::HashMap<Cow<'static, str>, Vec<(DeployEndpoint, OutputTarget)>>;
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
    pub(crate) fn new(
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

    /// send a contraflow insight message back down the pipeline
    pub(crate) async fn send_insight(&self, event: Event) -> Result<()> {
        Ok(self.cf_addr.send(CfMsg::Insight(event)).await?)
    }

    /// send a data-plane message to the pipeline
    pub(crate) async fn send(&self, msg: Box<Msg>) -> Result<()> {
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
    alias: &str,
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

    let addr = Addr::new(tx, cf_tx, mgmt_tx, alias.to_string());
    task::Builder::new()
        .name(format!("pipeline-{}", alias))
        .spawn(pipeline_task(
            alias.to_string(),
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
async fn send_events(
    eventset: &mut GraphReturns,
    dests: &mut Dests,
    pipeline: &mut ExecutableGraph,
    inputs: &Inputs,
) -> Result<()> {
    for (output, event) in eventset.output.drain(..) {
        if let Some(destinations) = dests.get_mut(&output) {
            if let Some((last, rest)) = destinations.split_last_mut() {
                for (id, dest) in rest {
                    let port = id.port().to_string().into();
                    dest.send_event(port, event.clone()).await?;
                }
                let last_port = last.0.port().to_string().into();
                last.1.send_event(last_port, event).await?;
            } else if event.transactional {
                // We send to a non connected output port, so we acknowledge the event
                // TODO: some kind of linter here so we don't create silent errors?
                handle_cf_msg(CfMsg::Insight(event.insight_ack()), pipeline, inputs).await?;
            }
        } else if event.transactional {
            // We send to a non connected output port, so we acknowledge the event
            // TODO: some kind of linter here so we don't create silent errors?
            handle_cf_msg(CfMsg::Insight(event.insight_ack()), pipeline, inputs).await?;
        };
    }
    // Events dropped or send to a dead end
    for event in eventset.dead_ends.drain(..) {
        handle_cf_msg(CfMsg::Insight(event.insight_ack()), pipeline, inputs).await?;
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
    tick_handler: JoinHandle<()>,
) -> Result<()> {
    pipeline.id = alias.clone();

    let mut dests: Dests = halfbrown::HashMap::new();
    let mut inputs: Inputs = halfbrown::HashMap::new();
    let mut eventset = GraphReturns::default();

    let mut state: State = State::Initializing;

    info!("[Pipeline::{alias}] Starting Pipeline.");

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
                        maybe_send(
                            send_events(&mut eventset, &mut dests, &mut pipeline, &inputs).await,
                        );
                    }
                    Err(e) => {
                        let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                            let script_error = tremor_script::errors::Error(script_kind, e.1);

                            Dumb::error_to_string(&script_error)
                                .unwrap_or_else(|e| format!(" {script_error}: {e}"))
                        } else {
                            format!(" {e}")
                        };
                        error!("[Pipeline::{alias}] Error handling event:{err_str}");
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
                    error!("[Pipeline::{}] Error handling signal:{}", alias, err_str);
                } else {
                    maybe_send(send_signal(&alias, signal, &mut dests).await);
                    handle_insights(&mut pipeline, &inputs).await;
                    maybe_send(
                        send_events(&mut eventset, &mut dests, &mut pipeline, &inputs).await,
                    );
                }
            }
            AnyMsg::Mgmt(MgmtMsg::ConnectInput {
                endpoint,
                target,
                is_transactional,
            }) => {
                info!("[Pipeline::{}] Connecting {} to port 'in'", alias, endpoint);
                inputs.insert(endpoint, (is_transactional, target));
            }
            AnyMsg::Mgmt(MgmtMsg::ConnectOutput {
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
                                endpoint: DeployEndpoint::new(&alias, &port, endpoint.meta()),
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
                info!(
                    "[Pipeline::{}] Ignoring Start Msg. Current state: {}",
                    alias, &state
                );
            }
            AnyMsg::Mgmt(MgmtMsg::Pause) if state == State::Running => {
                // No-op
                state = State::Paused;
            }
            AnyMsg::Mgmt(MgmtMsg::Pause) => {
                info!(
                    "[Pipeline::{}] Ignoring Pause Msg. Current state: {}",
                    alias, &state
                );
            }
            AnyMsg::Mgmt(MgmtMsg::Resume) if state == State::Paused => {
                // No-op
                state = State::Running;
            }
            AnyMsg::Mgmt(MgmtMsg::Resume) => {
                info!(
                    "[Pipeline::{}] Ignoring Resume Msg. Current state: {}",
                    alias, &state
                );
            }
            AnyMsg::Mgmt(MgmtMsg::Stop) => {
                info!("[Pipeline::{}] Stopping...", alias);
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
                    error!("[Pipeline::{alias}] Error sending status report.");
                }
            }
        }
    }
    // stop ticks
    tick_handler.cancel().await;

    info!("[Pipeline::{alias}] Stopped.");
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::connectors::{prelude::SinkAddr, source::SourceAddr};
    use std::time::Instant;
    use tremor_common::ports::{IN, OUT};
    use tremor_pipeline::{EventId, OpMeta};
    use tremor_script::{aggr_registry, lexer::Location, NodeMeta, FN_REGISTRY};
    use tremor_value::Value;

    #[async_std::test]
    async fn pipeline_spawn() -> Result<()> {
        let _ = env_logger::try_init();
        let mut operator_id_gen = OperatorIdGen::new();
        let trickle = r#"select event from in into out;"#;
        let aggr_reg = aggr_registry();
        let query =
            tremor_pipeline::query::Query::parse(trickle, &*FN_REGISTRY.read()?, &aggr_reg)?;
        let addr = spawn("test-pipe", &query, &mut operator_id_gen)?;

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
        addr.send_mgmt(MgmtMsg::ConnectInput {
            endpoint: DeployEndpoint::new(&"source_01", &OUT, &mid),
            target,
            is_transactional: true,
        })
        .await?;

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
        addr.send_mgmt(MgmtMsg::ConnectOutput {
            endpoint: DeployEndpoint::new(&"sink_01", &IN, &mid),
            port: OUT,
            target,
        })
        .await?;
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
        addr.send(Box::new(Msg::Signal(Event::signal_drain(42))))
            .await?;

        let start = Instant::now();
        let mut sink_msg = sink_rx.recv().await?;
        while !matches!(
            sink_msg,
            SinkMsg::Signal {
                signal: Event {
                    kind: Some(SignalKind::Drain(42)),
                    ..
                }
            }
        ) {
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
