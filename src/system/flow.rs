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
    connectors::{self, ConnectorResult, Known},
    errors::{Error, Kind as ErrorKind, Result},
    instance::State,
    log_error,
    pipeline::{self, InputTarget},
    primerge::PriorityMerge,
    system::KillSwitch,
};
use async_std::prelude::*;
use async_std::{
    channel::{bounded, unbounded, Sender},
    task,
};
use hashbrown::HashMap;
use std::collections::HashSet;
use std::{sync::atomic::Ordering, time::Duration};
use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
use tremor_script::{
    ast::{self, ConnectStmt, DeployFlow, Helper},
    errors::{err_generic, not_defined_err},
};

/// unique identifier of a flow instance within a tremor instance
#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Alias(String);

impl Alias {
    /// construct a new flow if from some stringy thingy
    pub fn new(alias: impl Into<String>) -> Self {
        Self(alias.into())
    }

    /// reference this id as a stringy thing again
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&DeployFlow<'_>> for Alias {
    fn from(f: &DeployFlow) -> Self {
        Self(f.instance_alias.to_string())
    }
}

impl From<&str> for Alias {
    fn from(e: &str) -> Self {
        Self(e.to_string())
    }
}

impl From<String> for Alias {
    fn from(alias: String) -> Self {
        Self(alias)
    }
}

impl std::fmt::Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
/// Control Plane message accepted by each binding control plane handler
pub(crate) enum Msg {
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
    GetConnector(connectors::Alias, Sender<Result<connectors::Addr>>),
    /// Get the addresses for all connectors of this flow
    GetConnectors(Sender<Result<Vec<connectors::Addr>>>),
}
type Addr = Sender<Msg>;

/// A deployed Flow instance
#[derive(Debug, Clone)]
pub struct Flow {
    alias: Alias,
    addr: Addr,
}

/// Status Report for a Flow instance
#[derive(Serialize, Deserialize, Debug)]
pub struct StatusReport {
    /// the id of the instance this report describes
    pub alias: Alias,
    /// the current state
    pub status: State,
    /// the crated connectors
    pub connectors: Vec<connectors::Alias>,
}

impl Flow {
    pub(crate) fn id(&self) -> &Alias {
        &self.alias
    }
    pub(crate) async fn stop(&self, tx: Sender<Result<()>>) -> Result<()> {
        self.addr.send(Msg::Stop(tx)).await.map_err(Error::from)
    }
    pub(crate) async fn drain(&self, tx: Sender<Result<()>>) -> Result<()> {
        self.addr.send(Msg::Drain(tx)).await.map_err(Error::from)
    }

    /// request a `StatusReport` from this `Flow`
    ///
    /// # Errors
    /// if the flow is not running anymore and can't be reached
    pub async fn report_status(&self) -> Result<StatusReport> {
        let (tx, rx) = bounded(1);
        self.addr.send(Msg::Report(tx)).await?;
        rx.recv().await?
    }

    /// get the Address used to send messages of a connector within this flow, identified by `connector_id`
    ///
    /// # Errors
    /// if the flow is not running anymore and can't be reached or if the connector is not part of the flow
    pub async fn get_connector(&self, connector_alias: String) -> Result<connectors::Addr> {
        let connector_alias = connectors::Alias::new(self.id().clone(), connector_alias);
        let (tx, rx) = bounded(1);
        self.addr
            .send(Msg::GetConnector(connector_alias, tx))
            .await?;
        rx.recv().await?
    }

    /// Get the Addresses of all connectors of this flow
    ///
    /// # Errors
    /// if the flow is not running anymore and can't be reached
    pub async fn get_connectors(&self) -> Result<Vec<connectors::Addr>> {
        let (tx, rx) = bounded(1);
        self.addr.send(Msg::GetConnectors(tx)).await?;
        rx.recv().await?
    }

    /// Pause this flow and all connectors in it.
    ///
    /// # Errors
    /// if the connector is not running anymore and can't be reached
    /// or if the connector is in a state where it can't be paused (e.g. failed)
    pub async fn pause(&self) -> Result<()> {
        self.addr.send(Msg::Pause).await.map_err(Error::from)
    }

    /// Resume this flow and all connectors in it.
    ///
    /// # Errors
    /// if the connector is not running anymore and can't be reached
    /// or if the connector is in a state where it can't be resumed (e.g. failed)
    pub async fn resume(&self) -> Result<()> {
        self.addr.send(Msg::Resume).await.map_err(Error::from)
    }

    pub(crate) async fn start(
        flow: ast::DeployFlow<'static>,
        operator_id_gen: &mut OperatorIdGen,
        connector_id_gen: &mut ConnectorIdGen,
        known_connectors: &Known,
        kill_switch: &KillSwitch,
    ) -> Result<Self> {
        let mut pipelines = HashMap::new();
        let mut connectors = HashMap::new();
        let flow_alias = Alias::from(&flow);

        for create in &flow.defn.creates {
            let alias: &str = &create.instance_alias;
            match &create.defn {
                ast::CreateTargetDefinition::Connector(defn) => {
                    let mut defn = defn.clone();
                    defn.params.ingest_creational_with(&create.with)?;
                    let connector_alias = connectors::Alias::new(flow_alias.clone(), alias);
                    let config = crate::Connector::from_defn(&connector_alias, &defn)?;
                    let builder =
                        known_connectors
                            .get(&config.connector_type)
                            .ok_or_else(|| {
                                ErrorKind::UnknownConnectorType(config.connector_type.to_string())
                            })?;
                    connectors.insert(
                        alias.to_string(),
                        connectors::spawn(
                            &connector_alias,
                            connector_id_gen,
                            builder.as_ref(),
                            config,
                            kill_switch,
                        )
                        .await?,
                    );
                }
                ast::CreateTargetDefinition::Pipeline(defn) => {
                    let query = {
                        let aggr_reg = tremor_script::aggr_registry();
                        let reg = tremor_script::FN_REGISTRY.read()?;
                        let mut helper = Helper::new(&reg, &aggr_reg);

                        defn.to_query(&create.with, &mut helper)?
                    };
                    let pipeline_alias = pipeline::Alias::new(flow_alias.clone(), alias);
                    let pipeline = tremor_pipeline::query::Query(
                        tremor_script::query::Query::from_query(query),
                    );
                    let addr = pipeline::spawn(pipeline_alias, &pipeline, operator_id_gen)?;
                    pipelines.insert(alias.to_string(), addr);
                }
            }
        }

        // link all the instances
        for connect in &flow.defn.connections {
            link(&connectors, &pipelines, connect).await?;
        }

        let addr = spawn_task(
            flow_alias.clone(),
            pipelines,
            connectors,
            &flow.defn.connections,
        )
        .await?;

        addr.send(Msg::Start).await?;

        let this = Flow {
            alias: flow_alias.clone(),
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

async fn link(
    connectors: &HashMap<String, connectors::Addr>,
    pipelines: &HashMap<String, pipeline::Addr>,
    link: &ConnectStmt,
) -> Result<()> {
    // this is some odd stuff to have here
    let timeout = Duration::from_secs(2);
    match link {
        ConnectStmt::ConnectorToPipeline { from, to, .. } => {
            let connector = connectors
                .get(from.alias())
                .ok_or_else(|| not_defined_err(from, "connector"))?;

            let pipeline = pipelines
                .get(to.alias())
                .ok_or(format!(
                    "Pipeline {} not found, in: {}",
                    to.alias(),
                    key_list(pipelines)
                ))?
                .clone();

            let (tx, rx) = bounded(1);

            let msg = connectors::Msg::LinkOutput {
                port: from.port().to_string().into(),
                pipelines: vec![(to.clone(), pipeline.clone())],
                result_tx: tx.clone(),
            };
            connector
                .send(msg)
                .await
                .map_err(|e| -> Error { format!("Could not send to connector: {}", e).into() })?;
            rx.recv()
                .timeout(timeout)
                .await??
                .map_err(|e| err_generic(link, from, &e))?;
        }
        ConnectStmt::PipelineToConnector { from, to, .. } => {
            let pipeline = pipelines.get(from.alias()).ok_or(format!(
                "Pipeline {} not found in: {}",
                from.alias(),
                key_list(pipelines)
            ))?;

            let connector = connectors
                .get(to.alias())
                .ok_or(format!(
                    "Connector {} not found: {}",
                    to.alias(),
                    key_list(pipelines)
                ))?
                .clone();

            // first link the pipeline to the connector
            let msg = crate::pipeline::MgmtMsg::ConnectOutput {
                port: from.port().to_string().into(),
                endpoint: to.clone(),
                target: connector.clone().try_into()?,
            };
            pipeline.send_mgmt(msg).await?;

            // then link the connector to the pipeline

            let (tx, rx) = bounded(1);

            let msg = connectors::Msg::LinkInput {
                port: to.port().to_string().into(),
                pipelines: vec![(from.clone(), pipeline.clone())],
                result_tx: tx.clone(),
            };
            connector
                .send(msg)
                .await
                .map_err(|e| -> Error { format!("Could not send to connector: {}", e).into() })?;
            rx.recv()
                .timeout(timeout)
                .await??
                .map_err(|e| err_generic(link, to, &e))?;
        }
        ConnectStmt::PipelineToPipeline { from, to, .. } => {
            let from_pipeline = pipelines.get(from.alias()).ok_or(format!(
                "Pipeline {} not found in: {}",
                from.alias(),
                key_list(pipelines)
            ))?;
            let to_pipeline = pipelines.get(to.alias()).ok_or(format!(
                "Pipeline {} not found in: {}",
                from.alias(),
                key_list(pipelines)
            ))?;
            let msg_from = crate::pipeline::MgmtMsg::ConnectOutput {
                port: from.port().to_string().into(),
                endpoint: to.clone(),
                target: to_pipeline.clone().into(),
            };
            let (tx, rx) = bounded(1);
            let msg_to = crate::pipeline::MgmtMsg::ConnectInput {
                port: to.port().to_string().into(),
                endpoint: from.clone(),
                tx,
                target: InputTarget::Pipeline(Box::new(from_pipeline.clone())),
                is_transactional: true,
            };

            from_pipeline.send_mgmt(msg_from).await?;

            to_pipeline.send_mgmt(msg_to).await?;
            rx.recv()
                .timeout(timeout)
                .await??
                .map_err(|e| err_generic(link, from, &e))?;
        }
    }
    Ok(())
}

/// task handling flow instance control plane
#[allow(clippy::too_many_lines)]
async fn spawn_task(
    id: Alias,
    pipelines: HashMap<String, pipeline::Addr>,
    connectors: HashMap<String, connectors::Addr>,
    links: &[ConnectStmt],
) -> Result<Addr> {
    #[derive(Debug)]
    /// wrapper for all possible messages handled by the flow task
    enum MsgWrapper {
        Msg(Msg),
        StartResult(ConnectorResult<()>),
        DrainResult(ConnectorResult<()>),
        StopResult(ConnectorResult<()>),
    }

    let (msg_tx, msg_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
    let (drain_tx, drain_rx) = unbounded();
    let (stop_tx, stop_rx) = unbounded();
    let (start_tx, start_rx) = unbounded();

    let mut input_channel = PriorityMerge::new(
        msg_rx.map(MsgWrapper::Msg),
        PriorityMerge::new(
            drain_rx.map(MsgWrapper::DrainResult),
            PriorityMerge::new(
                stop_rx.map(MsgWrapper::StopResult),
                start_rx.map(MsgWrapper::StartResult),
            ),
        ),
    );
    let addr = msg_tx;
    let mut state = State::Initializing;
    // let registries = self.reg.clone();

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

    let pipelines: Vec<_> = pipelines.values().cloned().collect();

    let start_points: Vec<_> = source_connectors
        .difference(&sink_connectors)
        .filter_map(|p| connectors.get(p))
        .cloned()
        .collect();
    let mixed_pickles: Vec<_> = sink_connectors
        .intersection(&source_connectors)
        .filter_map(|p| connectors.get(p))
        .cloned()
        .collect();
    let end_points: Vec<_> = sink_connectors
        .difference(&source_connectors)
        .filter_map(|p| connectors.get(p))
        .cloned()
        .collect();

    // for receiving drain/stop completion notifications from connectors
    let mut expected_drains: usize = 0;
    let mut expected_stops: usize = 0;

    // for storing senders that have been sent to us
    let mut drain_senders = Vec::with_capacity(1);
    let mut stop_senders = Vec::with_capacity(1);

    let prefix = format!("[Flow::{id}]");

    task::spawn::<_, Result<()>>(async move {
        let mut wait_for_start_responses: usize = 0;
        while let Some(wrapped) = input_channel.next().await {
            match wrapped {
                MsgWrapper::Msg(Msg::Start) if state == State::Initializing => {
                    info!("{prefix} Starting...");
                    // start all pipelines first - order doesnt matter as connectors aren't started yet
                    for pipe in &pipelines {
                        pipe.start().await?;
                    }

                    if connectors.is_empty() {
                        state = State::Running;
                        info!("{prefix} Started.");
                    } else {
                        // start sink connectors first then source/sink connectors then source only connectors
                        for conn in end_points.iter().chain(&mixed_pickles).chain(&start_points) {
                            conn.start(start_tx.clone()).await?;
                            wait_for_start_responses += 1;
                        }

                        debug!(
                            "{prefix} Waiting for {wait_for_start_responses} connectors to start."
                        );
                    }
                }
                MsgWrapper::Msg(Msg::Start) => {
                    info!("{prefix} Ignoring Start message. Current state: {state}");
                }
                MsgWrapper::Msg(Msg::Pause) if state == State::Running => {
                    info!("{prefix} Pausing...");
                    for source in start_points.iter().chain(&mixed_pickles).chain(&end_points) {
                        source.pause().await?;
                    }
                    for pipeline in &pipelines {
                        pipeline.pause().await?;
                    }
                    state = State::Paused;
                    info!("{prefix} Paused.");
                }
                MsgWrapper::Msg(Msg::Pause) => {
                    info!("{prefix} Ignoring Pause message. Current state: {state}",);
                }
                MsgWrapper::Msg(Msg::Resume) if state == State::Paused => {
                    info!("{prefix} Resuming...");

                    for pipeline in &pipelines {
                        pipeline.resume().await?;
                    }
                    for sink in end_points.iter().chain(&mixed_pickles).chain(&start_points) {
                        sink.resume().await?;
                    }
                    state = State::Running;
                    info!("{prefix} Resumed.");
                }
                MsgWrapper::Msg(Msg::Resume) => {
                    info!("{prefix} Ignoring Resume message. Current state: {state}",);
                }
                MsgWrapper::Msg(Msg::Drain(_sender)) if state == State::Draining => {
                    info!("{prefix} Ignoring Drain message. Current state: {state}",);
                }
                MsgWrapper::Msg(Msg::Drain(sender)) => {
                    info!("{prefix} Draining...");

                    state = State::Draining;

                    // handling the weird case of no connectors
                    if connectors.is_empty() {
                        info!("{prefix} Nothing to drain.");
                        log_error!(
                            sender.send(Ok(())).await,
                            "{prefix} Error sending drain result: {e}"
                        );
                    } else {
                        drain_senders.push(sender);

                        // QUIESCENCE
                        // - send drain msg to all connectors
                        // - wait until
                        //   a) all connectors are drained (means all pipelines in between are also drained) or
                        //   b) we timed out

                        // source only connectors
                        for addr in start_points.iter().chain(&mixed_pickles).chain(&end_points) {
                            if !log_error!(
                                addr.drain(drain_tx.clone()).await,
                                "{prefix} Error starting Draining Connector {addr:?}: {e}"
                            ) {
                                expected_drains += 1;
                            }
                        }
                    }
                }
                MsgWrapper::Msg(Msg::Stop(sender)) => {
                    info!("{prefix} Stopping...");
                    if connectors.is_empty() {
                        log_error!(
                            sender.send(Ok(())).await,
                            "{prefix} Error sending Stop result: {e}"
                        );
                    } else {
                        stop_senders.push(sender);
                        for connector in
                            end_points.iter().chain(&start_points).chain(&mixed_pickles)
                        {
                            if !log_error!(
                                connector.stop(stop_tx.clone()).await,
                                "{prefix} Error stopping connector {connector}: {e}"
                            ) {
                                expected_stops += 1;
                            }
                        }
                    }

                    for pipeline in &pipelines {
                        if let Err(e) = pipeline.stop().await {
                            error!("{prefix} Error stopping pipeline {pipeline:?}: {e}");
                        }
                    }

                    state = State::Stopped;
                }
                MsgWrapper::Msg(Msg::Report(sender)) => {
                    // TODO: aggregate states of all containing instances
                    let connectors = connectors
                        .keys()
                        .map(|c| connectors::Alias::new(id.clone(), c))
                        .collect();
                    let report = StatusReport {
                        alias: id.clone(),
                        status: state,
                        connectors,
                    };
                    log_error!(
                        sender.send(Ok(report)).await,
                        "{prefix} Error sending status report: {e}"
                    );
                }
                MsgWrapper::Msg(Msg::GetConnector(connector_alias, reply_tx)) => {
                    log_error!(
                        reply_tx
                            .send(
                                connectors
                                    .get(&connector_alias.connector_alias().to_string())
                                    .cloned()
                                    .ok_or_else(|| {
                                        ErrorKind::ConnectorNotFound(
                                            connector_alias.flow_alias().to_string(),
                                            connector_alias.connector_alias().to_string(),
                                        )
                                        .into()
                                    })
                            )
                            .await,
                        "{prefix} Error sending GetConnector response: {e}"
                    );
                }
                MsgWrapper::Msg(Msg::GetConnectors(reply_tx)) => {
                    let res = connectors.values().cloned().collect::<Vec<_>>();
                    log_error!(
                        reply_tx.send(Ok(res)).await,
                        "{prefix} Error sending GetConnectors response: {e}"
                    );
                }

                MsgWrapper::DrainResult(conn_res) => {
                    info!("[Flow::{}] Connector {} drained.", &id, &conn_res.alias);

                    log_error!(
                        conn_res.res,
                        "{prefix} Error during Draining in Connector {}: {e}",
                        &conn_res.alias
                    );

                    let old = expected_drains;
                    expected_drains = expected_drains.saturating_sub(1);
                    if expected_drains == 0 && old > 0 {
                        info!("{prefix} All connectors are drained.");
                        // upon last drain
                        for drain_sender in drain_senders.drain(..) {
                            log_error!(
                                drain_sender.send(Ok(())).await,
                                "{prefix} Error sending successful Drain result: {e}"
                            );
                        }
                    }
                }
                MsgWrapper::StopResult(conn_res) => {
                    info!("[Flow::{}] Connector {} stopped.", &id, &conn_res.alias);

                    log_error!(
                        conn_res.res,
                        "{prefix} Error during Draining in Connector {}: {e}",
                        &conn_res.alias
                    );

                    let old = expected_stops;
                    expected_stops = expected_stops.saturating_sub(1);
                    if expected_stops == 0 && old > 0 {
                        info!("{prefix} All connectors are stopped.");
                        // upon last stop
                        for stop_sender in stop_senders.drain(..) {
                            log_error!(
                                stop_sender.send(Ok(())).await,
                                "{prefix} Error sending successful Stop result: {e}"
                            );
                        }
                        break;
                    }
                }
                MsgWrapper::StartResult(conn_res) => {
                    if let Err(e) = conn_res.res {
                        error!(
                            "{prefix} Error starting Connector {conn}: {e}",
                            conn = conn_res.alias
                        );
                        if state != State::Failed {
                            // only report failed upon the first connector failure
                            state = State::Failed;
                            info!("{prefix} Failed.");
                        }
                    } else if state == State::Initializing {
                        // report started flow if all connectors started
                        wait_for_start_responses = wait_for_start_responses.saturating_sub(1);
                        if wait_for_start_responses == 0 {
                            state = State::Running;
                            info!("{prefix} Started.");
                        }
                    }
                }
            }
        }
        info!("{prefix} Stopped.");
        Ok(())
    });
    Ok(addr)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{connectors::ConnectorBuilder, instance};
    use tremor_common::ids::{ConnectorIdGen, OperatorIdGen};
    use tremor_script::{ast::DeployStmt, deploy::Deploy, FN_REGISTRY};
    use tremor_value::literal;

    mod connector {

        use async_std::channel::Sender;

        use crate::connectors::prelude::*;

        struct FakeConnector {
            tx: Sender<Event>,
        }
        #[async_trait::async_trait]
        impl Connector for FakeConnector {
            async fn create_source(
                &mut self,
                source_context: SourceContext,
                builder: SourceManagerBuilder,
            ) -> Result<Option<SourceAddr>> {
                let source = FakeSource {};
                builder.spawn(source, source_context).map(Some)
            }

            async fn create_sink(
                &mut self,
                sink_context: SinkContext,
                builder: SinkManagerBuilder,
            ) -> Result<Option<SinkAddr>> {
                let sink = FakeSink::new(self.tx.clone());
                builder.spawn(sink, sink_context).map(Some)
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
            ) -> Result<SourceReply> {
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
            tx: Sender<Event>,
        }

        impl FakeSink {
            fn new(tx: Sender<Event>) -> Self {
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
            ) -> Result<SinkReply> {
                self.tx.send(event).await?;
                Ok(SinkReply::NONE)
            }

            fn auto_ack(&self) -> bool {
                true
            }
        }

        #[derive(Debug)]
        pub(crate) struct FakeBuilder {
            pub(crate) tx: Sender<Event>,
        }

        #[async_trait::async_trait]
        impl ConnectorBuilder for FakeBuilder {
            fn connector_type(&self) -> ConnectorType {
                "fake".into()
            }
            async fn build(
                &self,
                _alias: &Alias,
                _config: &ConnectorConfig,
                _kill_switch: &KillSwitch,
            ) -> Result<Box<dyn Connector>> {
                Ok(Box::new(FakeConnector {
                    tx: self.tx.clone(),
                }))
            }
        }
    }

    #[async_std::test]
    async fn flow_spawn() -> Result<()> {
        let mut operator_id_gen = OperatorIdGen::default();
        let mut connector_id_gen = ConnectorIdGen::default();
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
        let (tx, _rx) = bounded(1);
        let kill_switch = KillSwitch(tx);
        let deployable = Deploy::parse(&src, &*FN_REGISTRY.read()?, &aggr_reg)?;
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
        let (connector_tx, connector_rx) = unbounded();
        let builder = connector::FakeBuilder { tx: connector_tx };
        known_connectors.insert(builder.connector_type(), Box::new(builder));
        let flow = Flow::start(
            deploy,
            &mut operator_id_gen,
            &mut connector_id_gen,
            &known_connectors,
            &kill_switch,
        )
        .await?;

        let connector = flow.get_connector("foo".to_string()).await?;
        assert_eq!(String::from("test::foo"), connector.alias.to_string());

        let connectors = flow.get_connectors().await?;
        assert_eq!(1, connectors.len());
        assert_eq!(String::from("test::foo"), connectors[0].alias.to_string());

        // assert the flow has started and events are flowing
        let event = connector_rx.recv().await?;
        assert_eq!(
            &literal!({
                "snot": "badger"
            }),
            event.data.suffix().value()
        );

        let mut report = flow.report_status().await?;
        while report.status == instance::State::Initializing {
            task::sleep(Duration::from_millis(100)).await;
            report = flow.report_status().await?;
        }
        assert_eq!(instance::State::Running, report.status);
        assert_eq!(1, report.connectors.len());

        flow.pause().await?;
        let report = flow.report_status().await?;
        assert_eq!(instance::State::Paused, report.status);
        assert_eq!(1, report.connectors.len());

        flow.resume().await?;
        let report = flow.report_status().await?;
        assert_eq!(instance::State::Running, report.status);
        assert_eq!(1, report.connectors.len());

        // drain and stop the flow
        let (tx, rx) = bounded(1);
        flow.drain(tx.clone()).await?;
        rx.recv().await??;

        flow.stop(tx).await?;
        rx.recv().await??;

        Ok(())
    }
}
