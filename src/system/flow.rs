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
    connectors::{self, ConnectorResult, KnownConnectors},
    errors::{Error, Kind as ErrorKind, Result},
    instance::InstanceState,
    permge::PriorityMerge,
    pipeline::{self, InputTarget},
};
use async_std::prelude::*;
use async_std::{
    channel::{bounded, unbounded, Sender},
    task,
};
use hashbrown::HashMap;
use std::{borrow::Borrow, collections::HashSet};
use std::{sync::atomic::Ordering, time::Duration};
use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
use tremor_script::{
    ast::{ConnectStmt, DeployEndpoint},
    srs::{ConnectorDefinition, DeployFlow, QueryInstance},
};

/// unique identifier of a flow instance within a tremor instance
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize)]
pub struct FlowId(pub String);

impl From<&DeployFlow> for FlowId {
    fn from(f: &DeployFlow) -> Self {
        FlowId(f.instance_id.id().to_string())
    }
}

/// unique identifier of a connector within a deployment
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize)]
pub struct ConnectorId(String);

impl From<&DeployEndpoint> for ConnectorId {
    fn from(e: &DeployEndpoint) -> Self {
        ConnectorId(e.alias().to_string())
    }
}

impl From<&ConnectorDefinition> for ConnectorId {
    fn from(e: &ConnectorDefinition) -> Self {
        ConnectorId(e.instance_id.clone())
    }
}

impl Borrow<str> for ConnectorId {
    fn borrow(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash)]
pub(crate) struct PipelineId(pub String);
impl From<&DeployEndpoint> for PipelineId {
    fn from(e: &DeployEndpoint) -> Self {
        PipelineId(e.alias().to_string())
    }
}

impl From<&QueryInstance> for PipelineId {
    fn from(e: &QueryInstance) -> Self {
        PipelineId(e.instance_id.clone())
    }
}

impl Borrow<str> for PipelineId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[allow(dead_code)] // FIXME
#[derive(Debug)]
/// Control Plane message accepted by each binding control plane handler
pub enum Msg {
    /// start all contained instances
    Start,
    /// pause all contained instances
    Pause,
    /// resume all contained instance from pause
    Resume,
    /// stop all contained instances, and get notified via the given sender when the stop process is done
    Stop(Sender<Result<()>>),
    /// drain all contained instances, and get notified via the given sender when the drain process is done
    Drain(Sender<Result<()>>),
    /// Request a `StatusReport` from this instance.
    ///
    /// The sender expects a Result, which makes it easier to signal errors on the message handling path to the sender
    Report(Sender<Result<StatusReport>>),
    /// Get the addr for a single connector
    GetConnector(ConnectorId, Sender<Result<connectors::Addr>>),
    /// Get the addresses for all connectors of this flow
    GetConnectors(Sender<Result<Vec<connectors::Addr>>>),
}
type Addr = Sender<Msg>;

/// A deployed Flow instance
#[derive(Debug, Clone)]
pub struct Flow {
    alias: String,
    addr: Addr,
}

/// Status Report for a Flow instance
#[derive(Serialize, Debug)]
pub struct StatusReport {
    /// the url of the instance this report describes
    pub alias: String,
    /// the current state
    pub status: InstanceState,
    /// the crated connectors
    pub connectors: Vec<ConnectorId>
}
impl Flow {
    pub fn alias(&self) -> &str {
        self.alias.as_str()
    }
    pub(crate) async fn stop(&self, tx: Sender<Result<()>>) -> Result<()> {
        self.addr.send(Msg::Stop(tx)).await.map_err(Error::from)
    }
    pub(crate) async fn drain(&self, tx: Sender<Result<()>>) -> Result<()> {
        self.addr.send(Msg::Drain(tx)).await.map_err(Error::from)
    }

    pub async fn report_status(&self) -> Result<StatusReport> {
        let (tx, rx) = bounded(1);
        self.addr.send(Msg::Report(tx)).await?;
        rx.recv().await?
    }

    pub async fn get_connector(&self, connector_id: String) -> Result<connectors::Addr> {
        let connector_id = ConnectorId(connector_id);
        let (tx, rx) = bounded(1);
        self.addr.send(Msg::GetConnector(connector_id, tx)).await?;
        rx.recv().await?
    }

    pub async fn get_connectors(&self) -> Result<Vec<connectors::Addr>> {
        let (tx, rx) = bounded(1);
        self.addr.send(Msg::GetConnectors(tx)).await?;
        rx.recv().await?
    }

    pub async fn pause(&self) -> Result<()> {
        self.addr.send(Msg::Pause).await.map_err(Error::from)
    }

    pub async fn resume(&self) -> Result<()> {
        self.addr.send(Msg::Resume).await.map_err(Error::from)
    }

    pub(crate) async fn start(
        src: String,
        flow: DeployFlow,
        oidgen: &mut OperatorIdGen,
        cidgen: &mut ConnectorIdGen,
        known_connectors: &KnownConnectors,
    ) -> Result<Self> {
        let mut pipelines = HashMap::new();
        let mut connectors = HashMap::new();

        for decl in &flow.decl.connectors {
            let connector = crate::Connector::from_decl(decl)?;
            // FIXME
            connectors.insert(
                ConnectorId::from(decl),
                connectors::spawn(
                    decl.instance_id.clone(),
                    cidgen,
                    known_connectors,
                    connector,
                )
                .await?,
            );
        }
        for decl in &flow.decl.pipelines {
            let pipeline =
                tremor_pipeline::query::Query(tremor_script::Query::from_troy(&src, decl)?);
            let addr = pipeline::spawn(decl.instance_id.clone(), pipeline, oidgen).await?;
            pipelines.insert(PipelineId::from(decl), addr);
        }

        // link all the instances
        for connect in &flow.decl.links {
            link(&connectors, &pipelines, connect).await?;
        }

        let addr = spawn_task(
            flow.instance_id.id().to_string(),
            pipelines,
            connectors,
            &flow.decl.links,
        )
        .await?;

        addr.send(Msg::Start).await?;

        let this = Flow {
            alias: flow.instance_id.to_string(),
            addr,
        };

        Ok(this)
    }
}

async fn link(
    connectors: &HashMap<ConnectorId, connectors::Addr>,
    pipelines: &HashMap<PipelineId, pipeline::Addr>,
    link: &ConnectStmt,
) -> Result<()> {
    match link {
        ConnectStmt::ConnectorToPipeline { from, to, .. } => {
            let connector = connectors
                .get(from.alias())
                .ok_or(format!("FIXME: connector {} not found", from.alias()))?;

            let pipeline = pipelines
                .get(to.alias())
                .ok_or(format!("FIXME: pipeline {} not found", to.alias()))?
                .clone();

            // this is some odd stuff to have here
            let timeout = Duration::from_secs(2);

            let (tx, rx) = bounded(1);

            let msg = connectors::Msg::Link {
                port: from.port().to_string().into(),
                pipelines: vec![(to.clone(), pipeline)],
                result_tx: tx.clone(),
            };
            connector
                .send(msg)
                .await
                .map_err(|e| -> Error { format!("Could not send to connector: {}", e).into() })?;
            rx.recv().timeout(timeout).await???;
            // FIXME: move the connecto message from connector to pipeline here
        }
        ConnectStmt::PipelineToConnector { from, to, .. } => {
            let pipeline = pipelines
                .get(from.alias())
                .ok_or(format!("FIXME: pipeline {} not found", from.alias()))?;

            let connector = connectors
                .get(to.alias())
                .ok_or(format!("FIXME: connector {} not found", to.alias()))?
                .clone();

            // first link the pipeline to the connector
            let msg = crate::pipeline::MgmtMsg::ConnectOutput {
                port: from.port().to_string().into(),
                endpoint: to.clone(),
                target: connector.clone().try_into()?,
            };
            pipeline
                .send_mgmt(msg)
                .await
                .map_err(|e| -> Error { format!("Could not send to pipeline: {}", e).into() })?;

            // then link the connector to the pipeline
            // this is some odd stuff to have here
            let timeout = Duration::from_secs(2);

            let (tx, rx) = bounded(1);

            let msg = connectors::Msg::Link {
                port: to.port().to_string().into(),
                pipelines: vec![(from.clone(), pipeline.clone())],
                result_tx: tx.clone(),
            };
            connector
                .send(msg)
                .await
                .map_err(|e| -> Error { format!("Could not send to connector: {}", e).into() })?;
            rx.recv().timeout(timeout).await???;
        }
        ConnectStmt::PipelineToPipeline { from, to, .. } => {
            let from_pipeline = pipelines
                .get(from.alias())
                .ok_or(format!("FIXME: pipeline {} not found", from.alias()))?;
            let to_pipeline = pipelines
                .get(to.alias())
                .ok_or(format!("FIXME: pipeline {} not found", from.alias()))?;
            let msg_from = crate::pipeline::MgmtMsg::ConnectOutput {
                port: from.port().to_string().into(),
                endpoint: to.clone(),
                target: to_pipeline.clone().into(),
            };

            let msg_to = crate::pipeline::MgmtMsg::ConnectInput {
                endpoint: from.clone(),
                target: InputTarget::Pipeline(Box::new(from_pipeline.clone())),
                is_transactional: true,
            };

            from_pipeline
                .send_mgmt(msg_from)
                .await
                .map_err(|e| -> Error { format!("Could not send to pipeline: {}", e).into() })?;

            to_pipeline
                .send_mgmt(msg_to)
                .await
                .map_err(|e| -> Error { format!("Could not send to pipeline: {}", e).into() })?;
        }
    }
    Ok(())
}

/// task handling each binding instance control plane
#[allow(clippy::too_many_lines)]
async fn spawn_task(
    alias: String,
    pipelines: HashMap<PipelineId, pipeline::Addr>,
    connectors: HashMap<ConnectorId, connectors::Addr>,
    links: &[ConnectStmt],
) -> Result<Addr> {
    let (msg_tx, msg_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
    let (drain_tx, drain_rx) = unbounded();
    let (stop_tx, stop_rx) = unbounded();
    let (start_tx, start_rx) = unbounded();

    #[derive(Debug)]
    /// wrapper for all possible messages handled by the flow task
    enum MsgWrapper {
        Msg(Msg),
        StartResult(ConnectorResult<()>),
        DrainResult(ConnectorResult<()>),
        StopResult(ConnectorResult<()>),
    }

    let mut input_channel = PriorityMerge::new(
        msg_rx.map(MsgWrapper::Msg),
        PriorityMerge::new(
            drain_rx.map(MsgWrapper::DrainResult),
            PriorityMerge::new(
                stop_rx.map(MsgWrapper::StopResult),
                start_rx.map(MsgWrapper::StartResult)
            )
        ),
    );
    let addr = msg_tx;
    let mut state = InstanceState::Initializing;
    // let registries = self.reg.clone();

    // extracting connectors and pipes from the links
    let sink_connectors: HashSet<ConnectorId> = links
        .iter()
        .filter_map(|c| {
            if let ConnectStmt::PipelineToConnector { to, .. } = c {
                Some(ConnectorId::from(to))
            } else {
                None
            }
        })
        .collect();
    let source_connectors: HashSet<ConnectorId> = links
        .iter()
        .filter_map(|c| {
            if let ConnectStmt::ConnectorToPipeline { from, .. } = c {
                Some(ConnectorId::from(from))
            } else {
                None
            }
        })
        .collect();

    let pipelines: Vec<_> = pipelines.values().cloned().collect();

    let start_points: Vec<_> = source_connectors
        .difference(&sink_connectors)
        .map(|p| connectors.get(p).unwrap())
        .cloned()
        .collect();
    let mixed_pickles: Vec<_> = sink_connectors
        .intersection(&source_connectors)
        .map(|p| connectors.get(p).unwrap())
        .cloned()
        .collect();
    let end_points: Vec<_> = sink_connectors
        .difference(&source_connectors)
        .map(|p| connectors.get(p).unwrap())
        .cloned()
        .collect();

    // for receiving drain/stop completion notifications from connectors
    let mut expected_drains: usize = 0;
    let mut expected_stops: usize = 0;

    // for storing senders that have been sent to us
    let mut drain_senders = Vec::with_capacity(1);
    let mut stop_senders = Vec::with_capacity(1);

    task::spawn::<_, Result<()>>(async move {
        let mut wait_for_start_responses: usize = 0;
        while let Some(wrapped) = input_channel.next().await {
            match wrapped {
                MsgWrapper::Msg(Msg::Start) if state == InstanceState::Initializing => {
                    // start all pipelines first - order doesnt matter as connectors aren't started yet
                    for pipe in &pipelines {
                        pipe.start().await?;
                    }
                    // start sink connectors first
                    for conn in &end_points {
                        conn.start(start_tx.clone()).await?;
                    }
                    wait_for_start_responses += end_points.len();

                    // start source/sink connectors in random order
                    for conn in &mixed_pickles {
                        conn.start(start_tx.clone()).await?;
                    }
                    wait_for_start_responses += mixed_pickles.len();

                    // wait for mixed pickles to be connected
                    // start source only connectors
                    for conn in &start_points {
                        conn.start(start_tx.clone()).await?;
                    }
                    wait_for_start_responses += start_points.len();
                    debug!("[Flow::{alias}] Waiting for {wait_for_start_responses} connectors to start.");
                }
                MsgWrapper::Msg(Msg::Start) => {
                    info!("[Flow::{alias}] Ignoring Start message. Current state: {state}");
                }
                MsgWrapper::Msg(Msg::Pause) if state == InstanceState::Running => {
                    info!("[Flow::{alias}] Pausing...");
                    for source in &start_points {
                        source.pause().await?;
                    }
                    for source_n_sink in &mixed_pickles {
                        source_n_sink.pause().await?;
                    }
                    for sink in &end_points {
                        sink.pause().await?;
                    }
                    for pipeline in &pipelines {
                        pipeline.pause().await?;
                    }
                    state = InstanceState::Paused;
                    info!("[Flow::{alias}] Paused.");
                }
                MsgWrapper::Msg(Msg::Pause) => {
                    info!(
                        "[Flow::{}] Ignoring Pause message. Current state: {}",
                        &alias, &state
                    );
                }
                MsgWrapper::Msg(Msg::Resume) if state == InstanceState::Paused => {
                    info!("[Flow::{}] Resuming...", &alias);

                    for pipeline in &pipelines {
                        pipeline.resume().await?;
                    }
                    for sink in &end_points {
                        sink.resume().await?;
                    }
                    for source_n_sink in &mixed_pickles {
                        source_n_sink.resume().await?;
                    }
                    for source in &start_points {
                        source.resume().await?;
                    }
                    state = InstanceState::Running;
                    info!("[Flow::{alias}] Resumed.");
                }
                MsgWrapper::Msg(Msg::Resume) => {
                    info!(
                        "[Flow::{}] Ignoring Resume message. Current state: {}",
                        &alias, &state
                    );
                }
                MsgWrapper::Msg(Msg::Drain(_sender)) if state == InstanceState::Draining => {
                    info!(
                        "[Flow::{}] Ignoring Drain message. Current state: {}",
                        &alias, &state
                    );
                }
                MsgWrapper::Msg(Msg::Drain(sender)) => {
                    info!("[Flow::{}] Draining...", &alias);
                    drain_senders.push(sender);

                    // QUIESCENCE
                    // - send drain msg to all connectors
                    // - wait until
                    //   a) all connectors are drained (means all pipelines in between are also drained) or
                    //   b) we timed out

                    // source only connectors
                    for start_point in &start_points {
                        if let Err(e) = start_point.drain(drain_tx.clone()).await {
                            error!(
                                "[Flow::{}] Error starting Draining Connector {:?}: {}",
                                &alias, start_point, e
                            );
                        } else {
                            expected_drains += 1;
                        }
                    }
                    // source/sink connectors
                    for mixed_pickle in &mixed_pickles {
                        if let Err(e) = mixed_pickle.drain(drain_tx.clone()).await {
                            error!(
                                "[Flow::{}] Error starting Draining Connector {:?}: {}",
                                &alias, mixed_pickle, e
                            );
                        } else {
                            expected_drains += 1;
                        }
                    }
                    // sink only connectors
                    for end_point in &end_points {
                        if let Err(e) = end_point.drain(drain_tx.clone()).await {
                            error!(
                                "[Flow::{}] Error starting Draining Connector {:?}: {}",
                                &alias, end_point, e
                            );
                        } else {
                            expected_drains += 1;
                        }
                    }
                    state = InstanceState::Draining;
                }
                MsgWrapper::Msg(Msg::Stop(sender)) => {
                    info!("[Flow::{}] Stopping...", &alias);
                    stop_senders.push(sender);

                    for connector in end_points
                        .iter()
                        .chain(start_points.iter())
                        .chain(mixed_pickles.iter())
                    {
                        if let Err(e) = connector.stop(stop_tx.clone()).await {
                            error!(
                                "[Flow::{}] Error stopping connector {:?}: {}",
                                &alias, connector, e
                            );
                        } else {
                            expected_stops += 1;
                        }
                    }
                    for pipeline in &pipelines {
                        if let Err(e) = pipeline.stop().await {
                            error!(
                                "[Flow::{}] Error stopping pipeline {:?}: {}",
                                &alias, pipeline, e
                            );
                        }
                    }
                    state = InstanceState::Stopped;
                }
                MsgWrapper::Msg(Msg::Report(sender)) => {
                    // TODO: aggregate states of all containing instances
                    let connectors = connectors.keys().cloned().collect();
                    let report = StatusReport {
                        alias: alias.clone(),
                        status: state,
                        connectors
                    };
                    if let Err(e) = sender.send(Ok(report)).await {
                        error!("[Flow::{alias}] Error sending status report: {e}");
                    }
                }
                MsgWrapper::Msg(Msg::GetConnector(connector_id, reply_tx)) => {
                    if reply_tx
                        .send(connectors.get(&connector_id).cloned().ok_or_else(|| {
                            ErrorKind::ConnectorNotFound(alias.clone(), connector_id.0).into()
                        }))
                        .await
                        .is_err()
                    {
                        error!("[Flow::{alias}] Error sending GetConnector response");
                    }
                }
                MsgWrapper::Msg(Msg::GetConnectors(reply_tx)) => {
                    let res = connectors.values().cloned().collect::<Vec<_>>();
                    if reply_tx.send(Ok(res)).await.is_err() {
                        error!("[Flow::{alias}] Error sending GetConnectors response")
                    }
                }

                MsgWrapper::DrainResult(conn_res) => {
                    info!("[Flow::{}] Connector {} drained.", &alias, &conn_res.alias);
                    if let Err(e) = conn_res.res {
                        error!(
                            "[Flow::{}] Error during Draining in Connector {}: {}",
                            &alias, &conn_res.alias, e
                        );
                    }
                    let old = expected_drains;
                    expected_drains = expected_drains.saturating_sub(1);
                    if expected_drains == 0 && old > 0 {
                        info!("[Flow::{}] All connectors are drained.", &alias);
                        // upon last drain
                        for drain_sender in drain_senders.drain(..) {
                            if let Err(_) = drain_sender.send(Ok(())).await {
                                error!("[Flow::{}] Error sending successful Drain result", &alias);
                            }
                        }
                    }
                }
                MsgWrapper::StopResult(conn_res) => {
                    info!("[Flow::{}] Connector {} stopped.", &alias, &conn_res.alias);
                    if let Err(e) = conn_res.res {
                        error!(
                            "[Flow::{}] Error during Draining in Connector {}: {}",
                            &alias, &conn_res.alias, e
                        );
                    }
                    let old = expected_stops;
                    expected_stops = expected_stops.saturating_sub(1);
                    if expected_stops == 0 && old > 0 {
                        info!("[Flow::{}] All connectors are stopped.", &alias);
                        // upon last stop
                        for stop_sender in stop_senders.drain(..) {
                            if let Err(_) = stop_sender.send(Ok(())).await {
                                error!("[Flow::{alias}] Error sending successful Stop result");
                            }
                        }
                        break;
                    }
                }
                MsgWrapper::StartResult(conn_res) => {
                    if let Err(e) = conn_res.res {
                        error!("[Flow::{alias}] Error starting Connector {conn}: {e}", conn=conn_res.alias);
                        if state != InstanceState::Failed {
                            // only report failed upon the first connector failure
                            state = InstanceState::Failed;
                            info!("[Flow::{alias}] Failed.")
                        }
                    } else if state == InstanceState::Initializing {
                        // report started flow if all connectors started
                        wait_for_start_responses = wait_for_start_responses.saturating_sub(1);
                        if wait_for_start_responses == 0 {
                            state = InstanceState::Running;
                            info!("[Flow::{alias}] Started.");
                        }
                    }
                }
            }
        }
        info!("[Flow::{}] Binding Stopped.", &alias);
        Ok(())
    });
    Ok(addr)
}
