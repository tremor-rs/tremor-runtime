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
    channel::empty_e,
    channel::{bounded, oneshot, OneShotSender, Sender},
    connectors::AliasableConnectorResult,
    raft::{self, NodeId},
};
use crate::{
    connectors::{self, ConnectorResult, Known},
    errors::{ErrorKind, Result},
    instance::{IntendedState, State},
    log_error,
    pipeline::{self, InputTarget},
    primerge::PriorityMerge,
    system::{KillSwitch, ShutdownMode},
};
use futures::{Stream, StreamExt};
use hashbrown::HashMap;
use std::{collections::HashSet, ops::ControlFlow, pin::Pin, time::Duration};
use tokio::{
    task,
    time::{self, timeout},
};
use tokio_stream::wrappers::ReceiverStream;
use tremor_common::{
    alias,
    uids::{ConnectorUIdGen, OperatorUIdGen},
};
use tremor_pipeline::MetricsChannel;
use tremor_script::{
    ast::{self, ConnectStmt, Helper},
    errors::{error_generic, not_defined_err},
};

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("failed to deploy flow {0}: {1}")]
    Deploy(String, String),
    #[error("Pipeline {0} not fund in {1}")]
    PipelineNotFound(String, String),
    #[error("Connector {0} not fund in {1}")]
    ConnectorNotFound(String, String),
    #[error("Unknown connector type {1} in flow {0}")]
    UnknownConnector(alias::Flow, String),
    #[error("illegal state change")]
    IllegalStateChange,
    #[error("Duplicate flow: {0}")]
    Duplicate(alias::Flow),
    #[error("Flow not found: {0}")]
    NotFound(alias::Flow),
    #[error("Flow failed: {0}")]
    Failed(alias::Flow),
}

#[derive(Debug)]
/// Control Plane message accepted by each binding control plane handler
pub(crate) enum Msg {
    /// Change the state of this flow to `intended_state`
    ChangeState {
        // the state this flow should be changed to
        intended_state: IntendedState,
        // this is where we send the result
        reply_tx: OneShotSender<Result<()>>,
    },
    /// Request a `StatusReport` from this instance.
    ///
    /// The sender expects a Result, which makes it easier to signal errors on the message handling path to the sender
    Report(OneShotSender<Result<StatusReport>>),
    /// Get the addr for a single connector
    GetConnector(
        alias::Flow,
        alias::Connector,
        OneShotSender<Result<connectors::Addr>>,
    ),
    /// Get the addresses for all connectors of this flow
    GetConnectors(OneShotSender<Result<Vec<connectors::Addr>>>),
    /// Periodic tick
    Tick,
}
type Addr = Sender<Msg>;

/// A deployed Flow instance
#[derive(Debug, Clone)]
pub struct Flow {
    alias: alias::Flow,
    addr: Addr,
}

/// Status Report for a Flow instance
#[derive(Serialize, Deserialize, Debug)]
pub struct StatusReport {
    /// the id of the instance this report describes
    pub alias: alias::Flow,
    /// the current state
    pub status: State,
    /// the created connectors
    pub connectors: Vec<alias::Connector>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AppContext {
    pub(crate) id: alias::Flow,
    pub(crate) raft: raft::ClusterInterface,
    pub(crate) metrics: MetricsChannel,
}

impl AppContext {
    pub fn id(&self) -> &alias::Flow {
        &self.id
    }
    pub fn instance(&self) -> &alias::Instance {
        self.id.instance_id()
    }
    pub fn node_id(&self) -> NodeId {
        self.raft.id()
    }
}

impl std::fmt::Display for AppContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Node::{}][{}]", self.raft.id(), self.id)
    }
}

impl Flow {
    pub(crate) fn id(&self) -> &alias::Flow {
        &self.alias
    }

    pub(crate) async fn change_state(
        &self,
        new_state: IntendedState,
        tx: OneShotSender<Result<()>>,
    ) -> Result<()> {
        Ok(self
            .addr
            .send(Msg::ChangeState {
                intended_state: new_state,
                reply_tx: tx,
            })
            .await?)
    }

    /// request a `StatusReport` from this `Flow`
    ///
    /// # Errors
    /// if the flow is not running anymore and can't be reached
    pub async fn report_status(&self) -> Result<StatusReport> {
        let (tx, rx) = oneshot();
        self.addr.send(Msg::Report(tx)).await?;
        rx.await?
    }

    /// get the Address used to send messages of a connector within this flow, identified by `connector_id`
    ///
    /// # Errors
    /// if the flow is not running anymore and can't be reached or if the connector is not part of the flow
    pub async fn get_connector(&self, connector_alias: String) -> Result<connectors::Addr> {
        let connector_alias = alias::Connector::new(connector_alias);
        let (tx, rx) = oneshot();
        self.addr
            .send(Msg::GetConnector(self.id().clone(), connector_alias, tx))
            .await?;
        rx.await?
    }

    /// Get the Addresses of all connectors of this flow
    ///
    /// # Errors
    /// if the flow is not running anymore and can't be reached
    pub async fn get_connectors(&self) -> Result<Vec<connectors::Addr>> {
        let (tx, rx) = oneshot();
        self.addr.send(Msg::GetConnectors(tx)).await?;
        rx.await?
    }

    /// Start this flow and all connectors in it.
    ///
    /// # Errors
    /// if the connector is not running anymore and can't be reached
    pub async fn start(&self, tx: OneShotSender<Result<()>>) -> Result<()> {
        self.change_state(IntendedState::Running, tx).await
    }

    /// Pause this flow and all connectors in it.
    ///
    /// # Errors
    /// if the connector is not running anymore and can't be reached
    /// or if the connector is in a state where it can't be paused (e.g. failed)
    pub async fn pause(&self, tx: OneShotSender<Result<()>>) -> Result<()> {
        self.change_state(IntendedState::Paused, tx).await
    }

    /// Resume this flow and all connectors in it.
    ///
    /// # Errors
    /// if the connector is not running anymore and can't be reached
    /// or if the connector is in a state where it can't be resumed (e.g. failed)
    pub async fn resume(&self, tx: OneShotSender<Result<()>>) -> Result<()> {
        self.change_state(IntendedState::Running, tx).await
    }

    pub(crate) async fn stop(&self, tx: OneShotSender<Result<()>>) -> Result<()> {
        self.change_state(IntendedState::Stopped, tx).await
    }

    /// Deploy the given flow instance and all its pipelines and connectors
    /// but doesn't start them yet
    ///
    /// # Errors
    /// If any of the operations of spawning connectors, linking pipelines and connectors or spawning the flow instance
    /// fails.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn deploy(
        ctx: AppContext,
        flow: ast::DeployFlow<'static>,
        operator_id_gen: &mut OperatorUIdGen,
        connector_id_gen: &mut ConnectorUIdGen,
        known_connectors: &Known,
        kill_switch: &KillSwitch,
        deployment_type: DeploymentType,
    ) -> Result<Self> {
        let mut pipelines = HashMap::new();
        let mut connectors = HashMap::new();

        for create in &flow.defn.creates {
            let alias: &str = &create.instance_alias;
            match &create.defn {
                ast::CreateTargetDefinition::Connector(defn) => {
                    let mut defn = defn.clone();
                    defn.params.ingest_creational_with(&create.with)?;
                    let connector_alias = alias::Connector::new(alias);
                    let config = crate::Connector::from_defn(&connector_alias, &defn)?;
                    let builder =
                        known_connectors
                            .get(&config.connector_type)
                            .ok_or_else(|| {
                                Error::UnknownConnector(
                                    alias::Flow::from(alias),
                                    config.connector_type.to_string(),
                                )
                            })?;
                    connectors.insert(
                        alias.to_string(),
                        connectors::spawn(
                            &connector_alias,
                            connector_id_gen,
                            builder.as_ref(),
                            config,
                            kill_switch,
                            ctx.clone(),
                        )
                        .await?,
                    );
                }
                ast::CreateTargetDefinition::Pipeline(defn) => {
                    let query = {
                        let aggr_reg = tremor_script::aggr_registry();
                        let reg = tremor_script::FN_REGISTRY
                            .read()
                            .map_err(|_| ErrorKind::ReadLock)?;
                        let mut helper = Helper::new(&reg, &aggr_reg);

                        defn.to_query(&create.with, &mut helper)?
                    };
                    let pipeline_alias = alias::Pipeline::new(alias);
                    let pipeline = tremor_pipeline::query::Query(
                        tremor_script::query::Query::from_query(query),
                    );
                    let addr =
                        pipeline::spawn(ctx.clone(), pipeline_alias, &pipeline, operator_id_gen)?;
                    pipelines.insert(alias.to_string(), addr);
                }
            }
        }

        // link all the instances
        for connect in &flow.defn.connections {
            link(&connectors, &pipelines, connect).await?;
        }

        let addr = spawn_task(
            ctx.clone(),
            pipelines,
            &connectors,
            &flow.defn.connections,
            deployment_type,
        );

        let this = Flow {
            alias: ctx.id,
            addr,
        };

        Ok(this)
    }
}

fn key_list<K: ToString, V>(h: &HashMap<K, V>) -> String {
    h.keys()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
#[allow(clippy::too_many_lines)]
async fn link(
    connectors: &HashMap<String, connectors::Addr>,
    pipelines: &HashMap<String, pipeline::Addr>,
    link: &ConnectStmt,
) -> Result<()> {
    // this is some odd stuff to have here
    static TIMEOUT: Duration = Duration::from_secs(2);
    match link {
        ConnectStmt::ConnectorToPipeline { from, to, .. } => {
            let connector = connectors
                .get(from.alias())
                .ok_or_else(|| not_defined_err(from, "connector"))?;

            let pipeline = pipelines
                .get(to.alias())
                .ok_or_else(|| {
                    Error::PipelineNotFound(to.alias().to_string(), key_list(pipelines))
                })?
                .clone();

            let (tx, mut rx) = bounded(1);

            let msg = connectors::Msg::LinkOutput {
                port: from.port().to_string().into(),
                pipeline: (to.clone(), pipeline.clone()),
                result_tx: tx.clone(),
            };
            connector.send(msg).await?;

            timeout(TIMEOUT, rx.recv())
                .await?
                .ok_or_else(empty_e)?
                .map_err(|e| error_generic(link, from, &e))?;
        }
        ConnectStmt::PipelineToConnector { from, to, .. } => {
            let pipeline = pipelines.get(from.alias()).ok_or_else(|| {
                Error::PipelineNotFound(from.alias().to_string(), key_list(pipelines))
            })?;

            let connector = connectors
                .get(to.alias())
                .ok_or_else(|| {
                    Error::ConnectorNotFound(to.alias().to_string(), key_list(connectors))
                })?
                .clone();

            // first link the pipeline to the connector
            let (tx, mut rx) = bounded(1);
            let msg = crate::pipeline::MgmtMsg::ConnectOutput {
                tx,
                port: from.port().to_string().into(),
                endpoint: to.clone(),
                target: connector.clone().try_into()?,
            };
            pipeline.send_mgmt(msg).await?;
            timeout(TIMEOUT, rx.recv())
                .await?
                .ok_or_else(empty_e)?
                .map_err(|e| error_generic(link, from, &e))?;

            // then link the connector to the pipeline

            let (tx, mut rx) = bounded(1);

            let msg = connectors::Msg::LinkInput {
                port: to.port().to_string().into(),
                pipelines: vec![(from.clone(), pipeline.clone())],
                result_tx: tx.clone(),
            };
            connector.send(msg).await?;
            timeout(TIMEOUT, rx.recv())
                .await?
                .ok_or_else(empty_e)?
                .map_err(|e| error_generic(link, to, &e))?;
        }
        ConnectStmt::PipelineToPipeline { from, to, .. } => {
            let from_pipeline = pipelines.get(from.alias()).ok_or_else(|| {
                Error::PipelineNotFound(from.alias().to_string(), key_list(pipelines))
            })?;
            let to_pipeline = pipelines.get(to.alias()).ok_or_else(|| {
                Error::PipelineNotFound(from.alias().to_string(), key_list(pipelines))
            })?;
            let (tx_from, mut rx_from) = bounded(1);
            let msg_from = crate::pipeline::MgmtMsg::ConnectOutput {
                port: from.port().to_string().into(),
                endpoint: to.clone(),
                tx: tx_from,
                target: to_pipeline.clone().into(),
            };
            let (tx_to, mut rx_to) = bounded(1);
            let msg_to = crate::pipeline::MgmtMsg::ConnectInput {
                port: to.port().to_string().into(),
                endpoint: from.clone(),
                tx: tx_to,
                target: InputTarget::Pipeline(Box::new(from_pipeline.clone())),
                is_transactional: true,
            };

            from_pipeline.send_mgmt(msg_from).await?;
            timeout(TIMEOUT, rx_from.recv())
                .await?
                .ok_or_else(empty_e)?
                .map_err(|e| error_generic(link, to, &e))?;
            to_pipeline.send_mgmt(msg_to).await?;
            timeout(TIMEOUT, rx_to.recv())
                .await?
                .ok_or_else(empty_e)?
                .map_err(|e| error_generic(link, from, &e))?;
        }
    }
    Ok(())
}
#[derive(Debug)]
/// wrapper for all possible messages handled by the flow task
enum MsgWrapper {
    Msg(Msg),
    StartResult(ConnectorResult),
    DrainResult(ConnectorResult),
    StopResult(ConnectorResult),
}

/// How the depoloyment is distributed on a cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum DeploymentType {
    #[default]
    /// Let the pipeline run on all nodes in the cluster
    AllNodes,
    /// Run the pipeline on a single node on the cluster
    OneNode,
}

struct RunningFlow {
    app_ctx: AppContext,
    state: State,
    expected_starts: usize,
    expected_drains: usize,
    expected_stops: usize,
    pipelines: HashMap<String, pipeline::Addr>,
    source_only_connectors: Vec<connectors::Addr>,
    source_and_sink_connectors: Vec<connectors::Addr>,
    sink_only_connectors: Vec<connectors::Addr>,
    state_change_senders: HashMap<State, Vec<OneShotSender<Result<()>>>>,
    input_channel: Pin<Box<dyn Stream<Item = MsgWrapper> + Send + Sync>>,
    msg_tx: Sender<Msg>,
    drain_tx: Sender<ConnectorResult>,
    stop_tx: Sender<ConnectorResult>,
    start_tx: Sender<ConnectorResult>,
    deployment_type: DeploymentType,
}

impl RunningFlow {
    fn new(
        app_ctx: AppContext,
        pipelines: HashMap<String, pipeline::Addr>,
        connectors: &HashMap<String, connectors::Addr>,
        deployment_type: DeploymentType,
        links: &[ConnectStmt],
    ) -> Self {
        let (msg_tx, msg_rx) = bounded(crate::qsize());
        let (drain_tx, drain_rx) = bounded(crate::qsize());
        let (stop_tx, stop_rx) = bounded(crate::qsize());
        let (start_tx, start_rx) = bounded(crate::qsize());
        let input_channel = Box::pin(PriorityMerge::new(
            ReceiverStream::new(msg_rx).map(MsgWrapper::Msg),
            PriorityMerge::new(
                ReceiverStream::new(drain_rx).map(MsgWrapper::DrainResult),
                PriorityMerge::new(
                    ReceiverStream::new(stop_rx).map(MsgWrapper::StopResult),
                    ReceiverStream::new(start_rx).map(MsgWrapper::StartResult),
                ),
            ),
        ));
        // extracting connectors and pipes from the links
        let sink_connectors: HashSet<String> = links
            .iter()
            .filter_map(|c| {
                if let ConnectStmt::PipelineToConnector { to, .. } = c {
                    Some(to.alias().to_string())
                } else {
                    None
                }
            })
            .collect();
        let source_connectors: HashSet<String> = links
            .iter()
            .filter_map(|c| {
                if let ConnectStmt::ConnectorToPipeline { from, .. } = c {
                    Some(from.alias().to_string())
                } else {
                    None
                }
            })
            .collect();

        let source_only_connectors: Vec<connectors::Addr> = source_connectors
            .difference(&sink_connectors)
            .filter_map(|p| connectors.get(p))
            .cloned()
            .collect();
        let source_and_sink_connectors: Vec<connectors::Addr> = sink_connectors
            .intersection(&source_connectors)
            .filter_map(|p| connectors.get(p))
            .cloned()
            .collect();
        let sink_only_connectors: Vec<connectors::Addr> = sink_connectors
            .difference(&source_connectors)
            .filter_map(|p| connectors.get(p))
            .cloned()
            .collect();
        Self {
            app_ctx,
            state: State::Initializing,
            expected_starts: 0,
            expected_drains: 0,
            expected_stops: 0,
            pipelines,
            source_only_connectors,
            source_and_sink_connectors,
            sink_only_connectors,
            state_change_senders: HashMap::new(),
            input_channel,
            msg_tx,
            drain_tx,
            stop_tx,
            start_tx,
            deployment_type,
        }
    }

    fn addr(&self) -> Addr {
        self.msg_tx.clone()
    }

    fn has_connectors(&self) -> bool {
        !(self.source_only_connectors.is_empty()
            && self.source_and_sink_connectors.is_empty()
            && self.sink_only_connectors.is_empty())
    }

    fn connectors_start_to_end(&self) -> impl Iterator<Item = &connectors::Addr> {
        self.source_only_connectors
            .iter()
            .chain(&self.source_and_sink_connectors)
            .chain(&self.sink_only_connectors)
    }

    fn connectors_end_to_start(&self) -> impl Iterator<Item = &connectors::Addr> {
        self.sink_only_connectors
            .iter()
            .chain(&self.source_and_sink_connectors)
            .chain(&self.source_only_connectors)
    }
    fn insert_state_change_sender(
        &mut self,
        intended_state: IntendedState,
        sender: OneShotSender<Result<()>>,
    ) {
        self.state_change_senders
            .entry(State::from(intended_state))
            .or_insert_with(Vec::new)
            .push(sender);
    }

    #[allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
    async fn run(mut self) -> Result<()> {
        let prefix = self.app_ctx.to_string();
        let hash_key = self.app_ctx.id.to_string();
        let node_id = self.app_ctx.node_id();

        let mut current_nodes: Vec<NodeId> = vec![];
        let mut slot: usize = 0;
        let mut intended_active_state = IntendedState::Paused;

        let jh =
            jumphash::JumpHasher::new_with_keys(8_390_880_576_440_238_080, 128_034_676_764_530);

        // let activation_tx = self.msg_tx.clone();
        // We only need ticks for a OneNode deployment
        if self.deployment_type == DeploymentType::OneNode {
            let tick_tx = self.msg_tx.clone();
            task::spawn(async move {
                time::sleep(Duration::from_secs(20)).await;
                while tick_tx.send(Msg::Tick).await.is_ok() {
                    time::sleep(Duration::from_secs(20)).await;
                }
            });
        }

        while let Some(wrapped) = self.input_channel.next().await {
            match wrapped {
                MsgWrapper::Msg(Msg::Tick) => {
                    if let Ok(Ok(members)) = timeout(
                        Duration::from_millis(100),
                        self.app_ctx.raft.get_last_membership(),
                    )
                    .await
                    {
                        current_nodes = members.into_iter().collect();
                        slot = jh.slot(&hash_key, current_nodes.len() as u32) as usize;

                        if is_active_node(&current_nodes, slot, node_id)
                            && intended_active_state == IntendedState::Running
                        {
                            match self.state {
                                State::Paused => {
                                    if let Err(e) = self.handle_resume(&prefix).await {
                                        error!("{prefix} Error during resuming: {e}");
                                        self.change_state(State::Failed);
                                    }
                                }
                                State::Initializing => {
                                    if let Err(e) = self.handle_start(&prefix).await {
                                        error!("{prefix} Error starting: {e}");
                                        self.change_state(State::Failed);
                                    };
                                }
                                state => {
                                    debug!("not changing from state: {state}");
                                }
                            }
                        } else if self.state == State::Running {
                            self.handle_pause(&prefix).await?;
                            intended_active_state = IntendedState::Running;
                        }
                    }
                }
                MsgWrapper::Msg(Msg::ChangeState {
                    intended_state,
                    reply_tx,
                }) => {
                    // We are always active on a all node deployment
                    let is_active = self.deployment_type == DeploymentType::AllNodes
                        || is_active_node(&current_nodes, slot, node_id);

                    intended_active_state = intended_state;
                    match (self.state, intended_state) {
                        (State::Initializing, IntendedState::Running) if is_active => {
                            self.insert_state_change_sender(intended_state, reply_tx);
                            if let Err(e) = self.handle_start(&prefix).await {
                                error!("{prefix} Error starting: {e}");
                                self.change_state(State::Failed);
                            }
                        }
                        (State::Initializing, IntendedState::Running) => {
                            if reply_tx.send(Ok(())).is_err() {
                                error!("{prefix} Error sending StateChange response failed");
                            };
                            trace!("{prefix} Ignoring start request, not active node");
                        }
                        (State::Initializing, IntendedState::Paused) => {
                            // we ignore this
                            // we can only go from Initializing to Paused state by starting and then pausing right away
                            // this might lead to unwanted traffic along the way, so we keep it as it is
                            // Initializing is a good enough Pause for now
                            // We didn't connect yet, though, so we do not realize if config is broken just yet
                            log_error!(
                                reply_tx.send(Ok(())),
                                "{prefix} Error sending StateChange response: {e:?}"
                            );
                            info!("{prefix} Paused.");
                        }
                        (state, IntendedState::Stopped) => {
                            let mode = match state {
                                State::Initializing | State::Failed | State::Draining => {
                                    // no need to drain here
                                    ShutdownMode::Forceful
                                }
                                State::Running | State::Paused => ShutdownMode::Graceful, // always drain
                                State::Stopped => {
                                    log_error!(
                                        reply_tx.send(Ok(())),
                                        "{prefix} Error sending StateChagnge response: {e:?}"
                                    );
                                    info!("{prefix} Already in state {state}");
                                    continue;
                                }
                            };
                            self.insert_state_change_sender(intended_state, reply_tx);
                            match self.handle_stop(mode, &prefix).await {
                                Ok(ControlFlow::Continue(())) => {}
                                Ok(ControlFlow::Break(())) => {
                                    break;
                                }
                                Err(e) => {
                                    error!("{prefix} Error stopping: {e}");
                                    // we don't care if we failed here, we terminate anyways
                                    self.change_state(State::Stopped);
                                    break;
                                }
                            }
                        }
                        (state @ State::Running, IntendedState::Running)
                        | (state @ State::Paused, IntendedState::Paused) => {
                            log_error!(
                                reply_tx.send(Ok(())),
                                "{prefix} Error sending StateChagnge response: {e:?}"
                            );
                            info!("{prefix} Already in state {state}");
                        }
                        (State::Running, IntendedState::Paused) => {
                            self.insert_state_change_sender(intended_state, reply_tx);
                            if let Err(e) = self.handle_pause(&prefix).await {
                                error!("{prefix} Error during pausing: {e}");
                                self.change_state(State::Failed);
                            }
                        }
                        (State::Paused, IntendedState::Running) if is_active => {
                            self.insert_state_change_sender(intended_state, reply_tx);
                            if let Err(e) = self.handle_resume(&prefix).await {
                                error!("{prefix} Error during resuming: {e}");
                                self.change_state(State::Failed);
                            }
                        }
                        (State::Paused, IntendedState::Running) => {
                            if reply_tx.send(Ok(())).is_err() {
                                error!("{prefix} Error sending StateChange response failed");
                            };
                            trace!("{prefix} Ignoring start request, not active node");
                        }
                        (State::Draining, IntendedState::Running | IntendedState::Paused)
                        | (State::Stopped, _) => {
                            log_error!(
                                reply_tx.send(Err(Error::IllegalStateChange.into())),
                                "{prefix} Error sending StateChagnge response: {e:?}"
                            );
                            return Err(Error::IllegalStateChange.into());
                        }
                        (State::Failed, intended) => {
                            self.insert_state_change_sender(intended_state, reply_tx);
                            error!("{prefix} Cannot change state from failed to {intended}");
                            // trigger erroring all listeners
                            self.change_state(State::Failed);
                        }
                    }
                }
                MsgWrapper::Msg(Msg::Report(sender)) => {
                    // TODO: aggregate states of all containing instances
                    let connectors = self
                        .connectors_start_to_end()
                        .map(|c| &c.alias)
                        .cloned()
                        .collect();
                    let report = StatusReport {
                        alias: self.app_ctx.id.clone(),
                        status: self.state,
                        connectors,
                    };
                    log_error!(
                        sender.send(Ok(report)),
                        "{prefix} Error sending status report: {e:?}"
                    );
                }
                MsgWrapper::Msg(Msg::GetConnector(flow_id, connector_alias, reply_tx)) => {
                    // TODO: inefficient find, but on the other hand we don't need to store connectors in another way
                    let res = self
                        .connectors_start_to_end()
                        .find(|c| c.alias == connector_alias)
                        .cloned()
                        .ok_or_else(|| {
                            Error::UnknownConnector(flow_id.clone(), connector_alias.to_string())
                                .into()
                        });
                    log_error!(
                        reply_tx.send(res),
                        "{prefix} Error sending GetConnector response: {e:?}"
                    );
                }
                MsgWrapper::Msg(Msg::GetConnectors(reply_tx)) => {
                    let res = self.connectors_start_to_end().cloned().collect::<Vec<_>>();
                    log_error!(
                        reply_tx.send(Ok(res)),
                        "{prefix} Error sending GetConnectors response: {e:?}"
                    );
                }
                MsgWrapper::StartResult(res) => {
                    self.handle_start_result(res, &prefix);
                }
                MsgWrapper::DrainResult(res) => {
                    if let Err(e) = self.handle_drain_result(res, &prefix).await {
                        error!("{prefix} Error during draining: {e}");
                        self.change_state(State::Failed);
                    }
                }
                MsgWrapper::StopResult(res) => {
                    if self.handle_stop_result(&res, &prefix) == ControlFlow::Break(()) {
                        break;
                    }
                }
            }
        }
        info!("{prefix} Stopped.");
        Ok(())
    }

    async fn handle_start(&mut self, prefix: impl std::fmt::Display) -> Result<()> {
        info!("{prefix} Starting...");
        // start all pipelines first - order doesnt matter as connectors aren't started yet
        for pipe in self.pipelines.values() {
            pipe.start().await?;
        }

        if self.has_connectors() {
            // start sink connectors first then source/sink connectors then source only connectors
            let mut started = 0_usize;
            for conn in self.connectors_end_to_start() {
                conn.start(self.start_tx.clone()).await?;
                started += 1;
            }
            self.expected_starts = started;

            debug!(
                "{prefix} Waiting for {} connectors to start.",
                self.expected_starts
            );
        } else {
            self.change_state(State::Running);
            info!("{prefix} Started.");
        }
        Ok(())
    }

    fn handle_start_result(&mut self, conn_res: ConnectorResult, prefix: impl std::fmt::Display) {
        if let Some(e) = conn_res.err() {
            error!(
                "{prefix} Error starting Connector {conn}: {e}",
                conn = e.alias()
            );
            if self.state != State::Failed {
                // only report failed upon the first connector failure
                info!("{prefix} Failed.");
                self.change_state(State::Failed);
            }
        } else if self.state == State::Initializing {
            // report started flow if all connectors started
            self.expected_starts = self.expected_starts.saturating_sub(1);
            if self.expected_starts == 0 {
                info!("{prefix} Started.");
                self.change_state(State::Running);
            }
        }
    }

    async fn handle_pause(&mut self, prefix: impl std::fmt::Display) -> Result<()> {
        info!("{prefix} Pausing...");
        for source in self.connectors_start_to_end() {
            source.pause().await?;
        }
        for pipeline in self.pipelines.values() {
            pipeline.pause().await?;
        }
        self.change_state(State::Paused);
        info!("{prefix} Paused.");
        Ok(())
    }

    async fn handle_resume(&mut self, prefix: impl std::fmt::Display) -> Result<()> {
        info!("{prefix} Resuming...");

        for pipeline in self.pipelines.values() {
            pipeline.resume().await?;
        }
        for sink in self.connectors_end_to_start() {
            sink.resume().await?;
        }
        self.change_state(State::Running);
        info!("{prefix} Resumed.");
        Ok(())
    }

    async fn handle_stop(
        &mut self,
        mode: ShutdownMode,
        prefix: impl std::fmt::Display,
    ) -> Result<ControlFlow<()>> {
        if self.has_connectors() {
            if let ShutdownMode::Graceful = mode {
                info!("{prefix} Draining...");

                self.change_state(State::Draining);
                // QUIESCENCE
                // - send drain msg to all connectors
                // - wait until
                //   a) all connectors are drained (means all pipelines in between are also drained) or
                let mut drained = 0_usize;
                for addr in self.connectors_start_to_end() {
                    if !log_error!(
                        addr.drain(self.drain_tx.clone()).await,
                        "{prefix} Error starting Draining Connector {addr:?}: {e}"
                    ) {
                        drained += 1;
                    }
                }
                self.expected_drains = drained;
            } else {
                info!("{prefix} Stopping...");
                let mut stopped = 0_usize;
                for connector in self.connectors_end_to_start() {
                    if !log_error!(
                        connector.stop(self.stop_tx.clone()).await,
                        "{prefix} Error stopping connector {connector}: {e}"
                    ) {
                        stopped += 1;
                    }
                }
                self.expected_stops = stopped;
                for pipeline in self.pipelines.values() {
                    if let Err(e) = pipeline.stop().await {
                        error!("{prefix} Error stopping pipeline {pipeline:?}: {e}");
                    }
                }
            }
            // we continue the stop process in `handle_stop_result`
            Ok(ControlFlow::Continue(()))
        } else {
            // stop the (senseless) pipelines, as there are no events flowing through them
            for pipeline in self.pipelines.values() {
                if let Err(e) = pipeline.stop().await {
                    error!("{prefix} Error stopping pipeline {pipeline:?}: {e}");
                }
            }
            self.change_state(State::Stopped);
            // nothing to do, we can break right away
            Ok(ControlFlow::Break(()))
        }
    }

    fn handle_stop_result(
        &mut self,
        conn_res: &ConnectorResult,
        prefix: impl std::fmt::Display,
    ) -> ControlFlow<()> {
        let alias: &alias::Connector = conn_res.alias();

        info!("{prefix} Connector {alias} stopped.");

        log_error!(
            conn_res,
            "{prefix} Error during Stopping in Connector {alias}: {e}"
        );

        let old = self.expected_stops;
        self.expected_stops = self.expected_stops.saturating_sub(1);
        if self.expected_stops == 0 && old > 0 {
            info!("{prefix} All connectors are stopped.");
            // upon last stop we finally know we are stopped
            self.change_state(State::Stopped);
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    async fn handle_drain_result(
        &mut self,
        conn_res: ConnectorResult,
        prefix: impl std::fmt::Display,
    ) -> Result<ControlFlow<()>> {
        let alias: &alias::Connector = conn_res.alias();

        info!("{prefix} Connector {alias} drained.");

        log_error!(
            conn_res,
            "{prefix} Error during Draining in Connector {alias}: {e}"
        );

        let old = self.expected_drains;
        self.expected_drains = self.expected_drains.saturating_sub(1);
        if self.expected_drains == 0 && old > 0 {
            info!("{prefix} All connectors are drained.");
            // upon last drain, finally do a stop
            self.handle_stop(ShutdownMode::Forceful, prefix).await
        } else {
            Ok(ControlFlow::Continue(()))
        }
    }

    fn change_state(&mut self, new_state: State) {
        if self.state != new_state {
            if let Some(senders) = self.state_change_senders.get_mut(&new_state) {
                for sender in senders.drain(..) {
                    log_error!(
                        sender.send(Ok(())),
                        "Error notifying {new_state} state handler: {e:?}"
                    );
                }
            }
            self.state = new_state;
            // upon failed state, error out all other listeners of valid non-failed states, as there is no recovery
            if let State::Failed = new_state {
                for state in &[State::Running, State::Paused, State::Draining] {
                    if let Some(senders) = self.state_change_senders.get_mut(state) {
                        for sender in senders.drain(..) {
                            log_error!(
                                sender.send(Err(Error::Failed(self.app_ctx.id().clone()).into())),
                                "Error notifying {state} state handlers of failed state: {e:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// task handling flow instance control plane
#[allow(clippy::too_many_lines)]
fn spawn_task(
    app_ctx: AppContext,
    pipelines: HashMap<String, pipeline::Addr>,
    connectors: &HashMap<String, connectors::Addr>,
    links: &[ConnectStmt],
    deployment_type: DeploymentType,
) -> Addr {
    let flow = RunningFlow::new(app_ctx, pipelines, connectors, deployment_type, links);
    let addr = flow.addr();
    task::spawn(flow.run());
    addr
}

fn is_active_node(current_nodes: &[NodeId], slot: usize, node_id: NodeId) -> bool {
    match current_nodes.get(slot) {
        Some(selected) if *selected == node_id => true,
        Some(_selected) => false,
        None => {
            error!(" Slot {slot} is out of bounds for membership {current_nodes:?}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{connectors::ConnectorBuilder, instance};
    use tremor_common::uids::{ConnectorUIdGen, OperatorUIdGen};
    use tremor_script::{ast::DeployStmt, deploy::Deploy, FN_REGISTRY};
    use tremor_value::literal;

    mod connector {

        use crate::channel::UnboundedSender;
        use crate::connectors::prelude::*;

        struct FakeConnector {
            tx: UnboundedSender<Event>,
        }
        #[async_trait::async_trait]
        impl Connector for FakeConnector {
            async fn create_source(
                &mut self,
                ctx: SourceContext,
                builder: SourceManagerBuilder,
            ) -> Result<Option<SourceAddr>> {
                let source = FakeSource {};
                Ok(Some(builder.spawn(source, ctx)))
            }

            async fn create_sink(
                &mut self,
                ctx: SinkContext,
                builder: SinkManagerBuilder,
            ) -> Result<Option<SinkAddr>> {
                let sink = FakeSink::new(self.tx.clone());
                Ok(Some(builder.spawn(sink, ctx)))
            }

            fn codec_requirements(&self) -> CodecReq {
                CodecReq::Required
            }
        }

        struct FakeSource {}
        #[async_trait::async_trait]
        impl Source for FakeSource {
            async fn pull_data(
                &mut self,
                _pull_id: &mut u64,
                _ctx: &SourceContext,
            ) -> anyhow::Result<SourceReply> {
                Ok(SourceReply::Data {
                    origin_uri: EventOriginUri::default(),
                    data: r#"{"snot":"badger"}"#.as_bytes().to_vec(),
                    meta: Some(Value::object()),
                    stream: Some(DEFAULT_STREAM_ID),
                    port: None,
                    codec_overwrite: None,
                })
            }
            fn is_transactional(&self) -> bool {
                // for covering all the transactional bits
                true
            }
            fn asynchronous(&self) -> bool {
                false
            }
        }

        struct FakeSink {
            tx: UnboundedSender<Event>,
        }

        impl FakeSink {
            fn new(tx: UnboundedSender<Event>) -> Self {
                Self { tx }
            }
        }

        #[async_trait::async_trait]
        impl Sink for FakeSink {
            async fn on_event(
                &mut self,
                _input: &str,
                event: Event,
                _ctx: &SinkContext,
                _serializer: &mut EventSerializer,
                _start: u64,
            ) -> anyhow::Result<SinkReply> {
                self.tx.send(event)?;
                Ok(SinkReply::NONE)
            }

            fn auto_ack(&self) -> bool {
                true
            }
        }

        #[derive(Debug)]
        pub(crate) struct FakeBuilder {
            pub(crate) tx: UnboundedSender<Event>,
        }

        #[async_trait::async_trait]
        impl ConnectorBuilder for FakeBuilder {
            fn connector_type(&self) -> ConnectorType {
                "fake".into()
            }
            async fn build(
                &self,
                _alias: &alias::Connector,
                _config: &ConnectorConfig,
            ) -> Result<Box<dyn Connector>> {
                Ok(Box::new(FakeConnector {
                    tx: self.tx.clone(),
                }))
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flow_spawn() -> Result<()> {
        let mut operator_id_gen = OperatorUIdGen::default();
        let mut connector_id_gen = ConnectorUIdGen::default();
        let aggr_reg = tremor_script::aggr_registry();
        let src = r#"
        define flow test
        flow
            define connector foo from fake
            with
                codec = "json",
                config = {}
            end;

            define pipeline main
            pipeline
                select event from in into out;
            end;

            create connector foo;
            create pipeline main;

            connect /connector/foo to /pipeline/main;
            connect /pipeline/main to /connector/foo;
        end;
        deploy flow test;
        "#;
        let (tx, _rx) = bounded(128);
        let kill_switch = KillSwitch(tx);
        let deployable = Deploy::parse(
            &src,
            &*FN_REGISTRY.read().map_err(|_| ErrorKind::ReadLock)?,
            &aggr_reg,
        )?;
        let deploy = deployable
            .deploy
            .stmts
            .into_iter()
            .find_map(|stmt| match stmt {
                DeployStmt::DeployFlowStmt(deploy_flow) => Some((*deploy_flow).clone()),
                _other => None,
            })
            .expect("No deploy in the given troy file");
        let mut known_connectors = Known::new();
        let (connector_tx, mut connector_rx) = crate::channel::unbounded();
        let builder = connector::FakeBuilder { tx: connector_tx };
        known_connectors.insert(builder.connector_type(), Box::new(builder));
        let ctx = AppContext {
            id: alias::Flow::new("app", "test"),
            ..AppContext::default()
        };
        let flow = Flow::deploy(
            ctx,
            deploy,
            &mut operator_id_gen,
            &mut connector_id_gen,
            &known_connectors,
            &kill_switch,
            DeploymentType::AllNodes,
        )
        .await?;

        let (start_tx, start_rx) = crate::channel::oneshot();
        flow.start(start_tx).await?;
        start_rx.await??;

        let connector = flow.get_connector("foo".to_string()).await?;
        assert_eq!(String::from("foo"), connector.alias.to_string());

        let connectors = flow.get_connectors().await?;
        assert_eq!(1, connectors.len());
        assert_eq!(String::from("foo"), connectors[0].alias.to_string());

        // assert the flow has started and events are flowing
        let event = connector_rx.recv().await.expect("empty");
        assert_eq!(
            &literal!({
                "snot": "badger"
            }),
            event.data.suffix().value()
        );
        let mut report = flow.report_status().await?;
        while report.status == instance::State::Initializing {
            tokio::time::sleep(Duration::from_millis(100)).await;
            report = flow.report_status().await?;
        }
        assert_eq!(instance::State::Running, report.status);
        assert_eq!(1, report.connectors.len());

        let (tx, rx) = oneshot();
        flow.pause(tx).await?;
        rx.await??;
        let report = flow.report_status().await?;
        assert_eq!(instance::State::Paused, report.status);
        assert_eq!(1, report.connectors.len());

        let (tx, rx) = oneshot();
        flow.resume(tx).await?;
        rx.await??;
        let report = flow.report_status().await?;
        assert_eq!(instance::State::Running, report.status);
        assert_eq!(1, report.connectors.len());

        // stop the flow
        let (tx, rx) = oneshot();
        flow.stop(tx).await?;
        rx.await??;
        Ok(())
    }
}
