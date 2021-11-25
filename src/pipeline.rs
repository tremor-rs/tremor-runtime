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
use crate::connectors::{self, sink::SinkMsg, source::SourceMsg};
use crate::errors::Result;
use crate::permge::PriorityMerge;
use crate::registry::instance::InstanceState;
use crate::repository::PipelineArtefact;
use async_std::channel::{bounded, unbounded, Receiver, Sender};
use async_std::stream::StreamExt;
use async_std::task::{self, JoinHandle};
use beef::Cow;
use std::fmt;
use std::time::Duration;
use tremor_common::ids::OperatorIdGen;
use tremor_common::time::nanotime;
use tremor_common::url::TremorUrl;
use tremor_pipeline::errors::ErrorKind as PipelineErrorKind;
use tremor_pipeline::{CbAction, Event, ExecutableGraph, SignalKind};

const TICK_MS: u64 = 100;
pub(crate) type ManagerSender = Sender<ManagerMsg>;
type Inputs = halfbrown::HashMap<TremorUrl, (bool, InputTarget)>;
type Dests = halfbrown::HashMap<Cow<'static, str>, Vec<(TremorUrl, OutputTarget)>>;
type Eventset = Vec<(Cow<'static, str>, Event)>;
/// Address for a pipeline
#[derive(Clone)]
pub struct Addr {
    addr: Sender<Box<Msg>>,
    cf_addr: Sender<CfMsg>,
    mgmt_addr: Sender<MgmtMsg>,
    id: TremorUrl,
}

impl Addr {
    /// creates a new address
    #[must_use]
    pub fn new(
        addr: Sender<Box<Msg>>,
        cf_addr: Sender<CfMsg>,
        mgmt_addr: Sender<MgmtMsg>,
        id: TremorUrl,
    ) -> Self {
        Self {
            addr,
            cf_addr,
            mgmt_addr,
            id,
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
    pub fn id(&self) -> &TremorUrl {
        &self.id
    }

    pub(crate) async fn send_insight(&self, event: Event) -> Result<()> {
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
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
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

/// control plane message
#[derive(Debug)]
pub enum MgmtMsg {
    /// input can only ever be connected to the `in` port, so no need to include it here
    ConnectInput {
        /// url of the input to connect
        input_url: TremorUrl,
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
        output_url: TremorUrl,
        /// the actual target addr
        target: OutputTarget,
    },
    /// disconnect an output
    DisconnectOutput(Cow<'static, str>, TremorUrl),
    /// disconnect an input
    DisconnectInput(TremorUrl),

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

/// all the info for creating a pipeline
pub struct Create {
    /// the pipeline config
    pub config: PipelineArtefact,
    /// the pipeline id
    pub id: TremorUrl,
}

/// control plane message for pipeline manager
pub enum ManagerMsg {
    /// stop the manager
    Stop,
    /// create a new pipeline
    Create(Sender<Result<Addr>>, Box<Create>),
}

#[derive(Default, Debug)]
pub(crate) struct Manager {
    qsize: usize,
    operator_id_gen: OperatorIdGen,
}

#[inline]
async fn send_events(eventset: &mut Eventset, dests: &mut Dests) -> Result<()> {
    for (output, event) in eventset.drain(..) {
        if let Some(destinations) = dests.get_mut(&output) {
            if let Some((last, rest)) = destinations.split_last_mut() {
                for (id, dest) in rest {
                    let port = id.instance_port_required()?.to_string().into();
                    dest.send_event(port, event.clone()).await?;
                }
                let last_port = last.0.instance_port_required()?.to_string().into();
                last.1.send_event(last_port, event).await?;
            }
        };
    }
    Ok(())
}

#[inline]
async fn send_signal(own_id: &TremorUrl, signal: Event, dests: &mut Dests) -> Result<()> {
    let mut destinations = dests.values_mut().flatten();
    let first = destinations.next();
    for (id, dest) in destinations {
        if id != own_id {
            dest.send_signal(signal.clone()).await?;
        }
    }
    if let Some((id, dest)) = first {
        if id != own_id {
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

async fn tick(tick_tx: Sender<Box<Msg>>) {
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
async fn pipeline_task(
    id: TremorUrl,
    mut pipeline: ExecutableGraph,
    addr: Addr,
    rx: Receiver<Box<Msg>>,
    cf_rx: Receiver<CfMsg>,
    mgmt_rx: Receiver<MgmtMsg>,
) -> Result<()> {
    let mut pid = id.clone();
    pid.trim_to_instance();
    pipeline.id = pid.to_string();

    let mut dests: Dests = halfbrown::HashMap::new();
    let mut inputs: Inputs = halfbrown::HashMap::new();
    let mut eventset: Eventset = Vec::new();

    let mut state: InstanceState = InstanceState::Initialized;

    info!("[Pipeline:{}] Starting Pipeline.", id);

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
                match pipeline.enqueue(&input, event, &mut eventset) {
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
                    error!("[Pipeline::{}] Error handling signal:{}", pid, err_str);
                } else {
                    maybe_send(send_signal(&id, signal, &mut dests).await);
                    handle_insights(&mut pipeline, &inputs).await;
                    maybe_send(send_events(&mut eventset, &mut dests).await);
                }
            }
            M::M(MgmtMsg::ConnectInput {
                input_url,
                target,
                is_transactional,
            }) => {
                info!("[Pipeline::{}] Connecting input {} to 'in'", pid, input_url);
                inputs.insert(input_url, (is_transactional, target.into()));
            }
            M::M(MgmtMsg::ConnectOutput {
                port,
                output_url,
                target,
            }) => {
                info!(
                    "[Pipeline::{}] Connecting '{}' to {}",
                    pid, &port, &output_url
                );
                // notify other pipeline about a new input
                if let OutputTarget::Pipeline(pipe) = &target {
                    // avoid linking the same pipeline as input to itself
                    // as this will create a nasty circle filling up queues.
                    // In general this does not avoid cycles via more complex constructs.
                    //
                    if !pid.same_instance_as(&output_url) {
                        if let Err(e) = pipe
                            .send_mgmt(MgmtMsg::ConnectInput {
                                input_url: pid.clone(),
                                target: InputTarget::Pipeline(Box::new(addr.clone())),
                                is_transactional: true,
                            })
                            .await
                        {
                            error!(
                                "[Pipeline::{}] Error connecting input pipeline {}: {}",
                                pid, &output_url, e
                            );
                        }
                    }
                }

                if let Some(output_dests) = dests.get_mut(&port) {
                    output_dests.push((output_url, target.into()));
                } else {
                    dests.insert(port, vec![(output_url, target.into())]);
                }
            }
            M::M(MgmtMsg::DisconnectOutput(port, to_delete)) => {
                info!(
                    "[Pipeline::{}] Disconnecting {} from '{}'",
                    pid, &to_delete, &port
                );

                let mut remove = false;
                if let Some(output_vec) = dests.get_mut(&port) {
                    while let Some(index) = output_vec.iter().position(|(k, _)| k == &to_delete) {
                        if let (delete_url, OutputTarget::Pipeline(pipe)) =
                            output_vec.swap_remove(index)
                        {
                            if let Err(e) =
                                pipe.send_mgmt(MgmtMsg::DisconnectInput(id.clone())).await
                            {
                                error!(
                                    "[Pipeline::{}] Error disconnecting input pipeline {}: {}",
                                    pid, &delete_url, e
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
                info!("[Pipeline::{}] Disconnecting {} from 'in'", pid, &input_url);
                inputs.remove(&input_url);
            }
            M::M(MgmtMsg::Start) if state == InstanceState::Initialized => {
                // No-op
                state = InstanceState::Running;
            }
            M::M(MgmtMsg::Start) => {
                info!(
                    "[Pipeline::{}] Ignoring Start Msg. Current state: {}",
                    &pid, &state
                );
            }
            M::M(MgmtMsg::Pause) if state == InstanceState::Running => {
                // No-op
                state = InstanceState::Paused;
            }
            M::M(MgmtMsg::Pause) => {
                info!(
                    "[Pipeline::{}] Ignoring Pause Msg. Current state: {}",
                    &pid, &state
                );
            }
            M::M(MgmtMsg::Resume) if state == InstanceState::Paused => {
                // No-op
                state = InstanceState::Running;
            }
            M::M(MgmtMsg::Resume) => {
                info!(
                    "[Pipeline::{}] Ignoring Resume Msg. Current state: {}",
                    &pid, &state
                );
            }
            M::M(MgmtMsg::Stop) => {
                info!("[Pipeline::{}] Stopping...", &pid);
                break;
            }
            #[cfg(test)]
            M::M(MgmtMsg::Echo(sender)) => {
                if let Err(e) = sender.send(()).await {
                    error!(
                        "[Pipeline::{}] Error responding to echo message: {}",
                        pid, e
                    );
                }
            }
        }
    }

    info!("[Pipeline:{}] Stopped.", id);
    Ok(())
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self {
            qsize,
            operator_id_gen: OperatorIdGen::new(),
        }
    }
    pub fn start(mut self) -> (JoinHandle<Result<()>>, ManagerSender) {
        let (tx, rx) = bounded(self.qsize);
        let h = task::spawn(async move {
            info!("Pipeline manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping Pipeline manager...");
                        break;
                    }
                    Ok(ManagerMsg::Create(r, create)) => {
                        r.send(self.start_pipeline(*create)).await?;
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

        let (tx, rx) = bounded::<Box<Msg>>(self.qsize);
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

        let addr = Addr::new(tx, cf_tx, mgmt_tx, req.id);
        task::Builder::new()
            .name(format!("pipeline-{}", id))
            .spawn(pipeline_task(
                id,
                pipeline,
                addr.clone(),
                rx,
                cf_rx,
                mgmt_rx,
            ))?;
        Ok(addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::prelude::*;
    use tremor_common::url::ports::IN;
    use tremor_common::url::ports::OUT;
    use tremor_pipeline::EventId;
    use tremor_pipeline::FN_REGISTRY;

    use crate::connectors::sink::SinkAddr;
    use crate::connectors::sink::SinkMsg;
    use tremor_script::prelude::*;
    use tremor_script::{path::ModulePath, query::Query};

    // used when we expect something
    const POSITIVE_RECV_TIMEOUT: Duration = Duration::from_millis(10000);
    // used when we expect nothing, to not spend too much time in this test
    const NEGATIVE_RECV_TIMEOUT: Duration = Duration::from_millis(200);

    async fn echo(addr: &Addr) -> Result<()> {
        let (tx, rx) = bounded(1);
        addr.send_mgmt(MgmtMsg::Echo(tx)).await?;
        rx.recv().await?;
        Ok(())
    }

    /// ensure the last message has been processed by waiting for the manager to answer
    /// leveraging the sequential execution at the manager task
    /// this only works reliably for MgmtMsgs, not for insights or events/signals
    async fn manager_fence(addr: &Addr) -> Result<()> {
        echo(&addr).await
    }
    async fn wait_for_event(
        connector_rx: &Receiver<SinkMsg>,
        wait_for: Option<Duration>,
    ) -> Result<Event> {
        loop {
            match connector_rx
                .recv()
                .timeout(wait_for.unwrap_or(POSITIVE_RECV_TIMEOUT))
                .await
            {
                Ok(Ok(SinkMsg::Event { event, .. })) => return Ok(event),
                Ok(_) => continue, // ignore anything else, like signals
                Err(_) => return Err("timeout waiting for event at offramp".into()),
            }
        }
    }

    #[async_std::test]
    async fn test_pipeline_connect_disconnect() -> Result<()> {
        let module_path = ModulePath { mounts: vec![] };
        let query = r#"
            select event
            from in
            into out;
        "#;
        let aggr_reg: tremor_script::registry::Aggr = tremor_script::aggr_registry();
        let q = Query::parse(
            &module_path,
            "test_pipeline_connect.trickle",
            query,
            vec![],
            &*FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        let config = tremor_pipeline::query::Query(q);
        let id = TremorUrl::parse("/pipeline/test_pipeline_connect/instance")?;
        let manager = Manager::new(12);
        let (handle, sender) = manager.start();

        let (tx, rx) = bounded(1);
        let create = Create { config, id };
        let create_msg = ManagerMsg::Create(tx, Box::new(create));
        sender.send(create_msg).await?;
        let pipeline_addr = rx.recv().await??;

        // connect a fake connector source
        let (source_tx, source_rx) = unbounded();
        let connector_source_addr = connectors::source::SourceAddr {
            addr: source_tx.clone(),
        };
        let connector_url =
            TremorUrl::from_connector_instance("fake_connector", "instance")?.with_port(&OUT);
        pipeline_addr
            .send_mgmt(MgmtMsg::ConnectInput {
                input_url: connector_url.clone(),
                target: InputTarget::Source(connector_source_addr.clone()), // clone avoids the channel to be closed on disconnect below
                is_transactional: true,
            })
            .await?;

        // connect another fake connector source, not transactional
        let (source2_tx, source2_rx) = unbounded();
        let connector2_source_addr = connectors::source::SourceAddr {
            addr: source2_tx.clone(),
        };
        let connector2_url =
            TremorUrl::from_connector_instance("fake_connector2", "instance")?.with_port(&OUT);
        pipeline_addr
            .send_mgmt(MgmtMsg::ConnectInput {
                input_url: connector2_url.clone(),
                target: InputTarget::Source(connector2_source_addr.clone()),
                is_transactional: false,
            })
            .await?;
        // start connectors
        source_tx.send(SourceMsg::Start).await?;
        assert!(matches!(
            source_rx.recv().timeout(POSITIVE_RECV_TIMEOUT).await,
            Ok(Ok(SourceMsg::Start))
        ));
        source2_tx.send(SourceMsg::Start).await?;
        assert!(matches!(
            source2_rx.recv().timeout(POSITIVE_RECV_TIMEOUT).await,
            Ok(Ok(SourceMsg::Start))
        ));
        manager_fence(&pipeline_addr).await?;

        // send a non-cb insight
        let event_id = EventId::from((0, 0, 1));
        pipeline_addr
            .send_insight(Event {
                id: event_id.clone(),
                cb: CbAction::Ack,
                ..Event::default()
            })
            .await?;

        // transactional connector source received it
        // chose exceptionally big timeout, which we will only hit in the bad case that stuff isnt working, normally this should return fine
        match source_rx.recv().timeout(POSITIVE_RECV_TIMEOUT).await {
            Ok(Ok(SourceMsg::Cb(CbAction::Ack, cb_id))) => assert_eq!(cb_id, event_id),
            Err(_) => assert!(false, "No msg received."),
            m => assert!(false, "received unexpected msg: {:?}", m),
        }

        // non-transactional did not
        assert!(source2_rx.is_empty());

        // send a cb insight
        let event_id = EventId::from((0, 0, 1));
        pipeline_addr
            .send_insight(Event {
                id: event_id.clone(),
                cb: CbAction::Close,
                ..Event::default()
            })
            .await?;

        // transactional onramp received it
        // chose exceptionally big timeout, which we will only hit in the bad case that stuff isnt working, normally this should return fine
        match source_rx.recv().timeout(POSITIVE_RECV_TIMEOUT).await {
            Ok(Ok(SourceMsg::Cb(CbAction::Close, cb_id))) => assert_eq!(cb_id, event_id),
            Err(_) => assert!(false, "No msg received."),
            m => assert!(false, "received unexpected msg: {:?}", m),
        }

        // non-transactional did also receive it
        match source2_rx.recv().timeout(POSITIVE_RECV_TIMEOUT).await {
            Ok(Ok(SourceMsg::Cb(CbAction::Close, cb_id))) => assert_eq!(cb_id, event_id),
            Err(_) => assert!(false, "No msg received."),

            m => assert!(false, "received unexpected msh: {:?}", m),
        }

        // disconnect our fake offramp
        pipeline_addr
            .send_mgmt(MgmtMsg::DisconnectInput(connector_url))
            .await?;
        manager_fence(&pipeline_addr).await?; // ensure the last message has been processed

        // probe it with an insight
        let event_id2 = EventId::from((0, 0, 2));
        pipeline_addr
            .send_insight(Event {
                id: event_id2.clone(),
                cb: CbAction::Close,
                ..Event::default()
            })
            .await?;
        // we expect nothing after disconnect, so we run into a timeout
        match source_rx.recv().timeout(NEGATIVE_RECV_TIMEOUT).await {
            Ok(m) => assert!(false, "Didnt expect a message. Got: {:?}", m),
            Err(_e) => {}
        };

        // second onramp received it, as it is a CB insight
        match source2_rx.recv().await {
            Ok(SourceMsg::Cb(CbAction::Close, cb_id)) => assert_eq!(cb_id, event_id2),
            m => assert!(false, "received unexpected msg: {:?}", m),
        };

        pipeline_addr
            .send_mgmt(MgmtMsg::DisconnectInput(connector2_url))
            .await?;
        manager_fence(&pipeline_addr).await?; // ensure the last message has been processed

        // probe it with an insight
        let event_id3 = EventId::from((0, 0, 3));
        pipeline_addr
            .send_insight(Event {
                id: event_id3.clone(),
                cb: CbAction::Open,
                ..Event::default()
            })
            .await?;

        // we expect nothing after disconnect, so we run into a timeout
        match source2_rx.recv().timeout(NEGATIVE_RECV_TIMEOUT).await {
            Ok(m) => assert!(false, "Didnt expect a message. Got: {:?}", m),
            Err(_e) => {}
        };

        // stopping the manager
        sender.send(ManagerMsg::Stop).await?;
        handle.cancel().await;
        Ok(())
    }

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
        let id = TremorUrl::parse("/pipeline/manager_error_test/instance")?;
        let manager = Manager::new(12);
        let (handle, sender) = manager.start();
        let (tx, rx) = bounded(1);
        let create = Create { config, id };
        let create_msg = ManagerMsg::Create(tx, Box::new(create));
        sender.send(create_msg).await?;
        let addr = rx.recv().await??;
        let (sink_tx, sink_rx) = unbounded();
        let sink_addr = SinkAddr {
            addr: sink_tx.clone(),
        };

        let connector_url =
            TremorUrl::from_connector_instance("fake_connector", "instance")?.with_port(&IN);
        // connect a channel so we can receive events from the back of the pipeline :)
        addr.send_mgmt(MgmtMsg::ConnectOutput {
            port: OUT,
            output_url: connector_url.clone(),
            target: OutputTarget::Sink(sink_addr.clone()),
        })
        .await?;
        sink_tx.send(SinkMsg::Start).await?;
        manager_fence(&addr).await?;

        // sending an event, that triggers an error
        addr.send(Box::new(Msg::Event {
            event: Event::default(),
            input: IN,
        }))
        .await?;

        // no event at sink
        match sink_rx.recv().timeout(NEGATIVE_RECV_TIMEOUT).await {
            Ok(Ok(SinkMsg::Event { event, .. })) => {
                assert!(false, "Did not expect an event, but got: {:?}", event)
            }
            Ok(Err(e)) => return Err(e.into()),
            _ => {}
        };

        // send a second event, that gets through
        let mut second_event = Event::default();
        second_event.data = literal!({
            "non_existent": true
        })
        .into();
        addr.send(Box::new(Msg::Event {
            event: second_event,
            input: "in".into(),
        }))
        .await?;

        let event = wait_for_event(&sink_rx, None).await?;
        let (value, _meta) = event.data.suffix().clone().into_parts();
        assert_eq!(Value::from(true), value); // check that we received what we sent in, not the faulty event

        // disconnect the output
        addr.send_mgmt(MgmtMsg::DisconnectOutput(OUT, connector_url))
            .await?;
        manager_fence(&addr).await?;

        // probe it with an Event
        addr.send(Box::new(Msg::Event {
            event: Event::default(),
            input: "in".into(),
        }))
        .await?;

        // we expect nothing to arrive, so we run into a timeout
        match sink_rx.recv().timeout(NEGATIVE_RECV_TIMEOUT).await {
            Ok(Ok(SinkMsg::Event { event, .. })) => {
                assert!(false, "Didnt expect to receive something, got: {:?}", event)
            }
            Ok(Err(e)) => return Err(e.into()),
            _ => {}
        };

        // stopping the manager
        sender.send(ManagerMsg::Stop).await?;
        handle.cancel().await;
        Ok(())
    }
}
