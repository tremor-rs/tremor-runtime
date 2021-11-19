// Copyright 2021, The Tremor Team
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

//! Home of the binding, linking connectors to pipelines to connectors

use crate::config::Binding as BindingConfig;
use crate::connectors::ConnectorResult;
use crate::errors::Result;
use crate::permge::PriorityMerge;
use crate::registry::instance::InstanceState;
use crate::registry::Registries;
use crate::repository::BindingArtefact;
use async_std::channel::{bounded, unbounded, Sender};
use async_std::task::{self, JoinHandle};
use hashbrown::HashSet;
use smol::stream::StreamExt;
use tremor_common::ids::BindingIdGen;
use tremor_common::url::TremorUrl;

/// sender for sending mgmt messages to the Binding Manager
pub type ManagerSender = Sender<ManagerMsg>;

/// Status Report for a Binding
pub struct StatusReport {
    /// the url of the instance this report describes
    pub url: TremorUrl,
    /// the current state
    pub status: InstanceState,
}

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
    /// request a `StatusReport` from this instance
    Report(Sender<StatusReport>),
}

/// Address for reaching a Binding instance
#[derive(Clone, Debug)]
pub struct Addr {
    uid: u64,
    /// the url of the binding instance
    pub url: TremorUrl,
    sender: Sender<Msg>,
}

impl Addr {
    /// send a `Msg` to the binding instance
    pub async fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.sender.send(msg).await?)
    }

    /// stop the binding instance
    pub async fn stop(&self, sender: Sender<Result<()>>) -> Result<()> {
        self.send(Msg::Stop(sender)).await
    }

    /// start the binding instance
    pub async fn start(&self) -> Result<()> {
        self.send(Msg::Start).await
    }
}

/// all the info for creating a binding instance in one struct
#[derive(Debug)]
pub struct Create {
    instance_id: TremorUrl,
    config: BindingArtefact,
}

impl Create {
    /// constructor
    pub fn new(instance_id: TremorUrl, config: BindingArtefact) -> Self {
        Self {
            instance_id,
            config,
        }
    }
}

/// Messages accepted by the binding `Manager`
pub enum ManagerMsg {
    /// message requesting a binding instance to be created and its address to be sent back
    Create {
        /// result sender
        tx: Sender<Result<Addr>>,
        /// all the information for spawning a binding instance
        create: Box<Create>,
    },
    /// message requesting the manager to stop and not accept any more messages
    Stop,
}

/// wrapper for all possible messages handled by the binding task
enum MsgWrapper {
    Msg(Msg),
    DrainResult(ConnectorResult<()>),
    StopResult(ConnectorResult<()>),
}

/// The binding `Manager`, handles `Binding` instance creation
pub(crate) struct Manager {
    qsize: usize,
    reg: Registries,
}

impl Manager {
    /// constructor
    pub fn new(qsize: usize, registries: Registries) -> Self {
        Self {
            qsize,
            reg: registries,
        }
    }

    /// consume the `Manager` and start its control plane loop, receiving and handling messages
    pub fn start(self) -> (JoinHandle<Result<()>>, ManagerSender) {
        let (tx, rx) = bounded(self.qsize);
        let h = task::spawn(async move {
            info!("Binding Manager started.");
            let mut binding_id_gen = BindingIdGen::new();
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Create { tx, create }) => {
                        let url = create.instance_id.to_instance();
                        let artefact = create.config;
                        if let Err(e) = self
                            .binding_task(
                                tx.clone(),
                                url.clone(),
                                artefact.binding,
                                binding_id_gen.next_id(),
                            )
                            .await
                        {
                            error!("[Binding] Error spawning task for binding {}: {}", &url, e);
                            tx.send(Err(e)).await?;
                        }
                    }
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping Binding Manager...");
                        break;
                    }
                    Err(e) => {
                        info!("Error! Stopping Binding Manager... {}", e);
                        break;
                    }
                }
            }
            info!("Binding Manager stopped.");
            Ok(())
        });
        (h, tx)
    }

    /// task handling each binding instance control plane
    #[allow(clippy::too_many_lines)]
    async fn binding_task(
        &self,
        addr_tx: Sender<Result<Addr>>,
        url: TremorUrl,
        binding: BindingConfig,
        uid: u64,
    ) -> Result<()> {
        let (msg_tx, msg_rx) = bounded(self.qsize);
        let (drain_tx, drain_rx) = unbounded();
        let (stop_tx, stop_rx) = unbounded();

        let mut input_channel = PriorityMerge::new(
            msg_rx.map(MsgWrapper::Msg),
            PriorityMerge::new(
                drain_rx.map(MsgWrapper::DrainResult),
                stop_rx.map(MsgWrapper::StopResult),
            ),
        );
        let addr = Addr {
            uid,
            url: url.clone(),
            sender: msg_tx,
        };
        let mut binding_state = InstanceState::Initialized;
        let registries = self.reg.clone();

        // extracting connectors and pipes from the links
        let sink_connectors: HashSet<TremorUrl> = binding
            .links
            .iter()
            .flat_map(|(_from, tos)| tos.iter())
            .map(TremorUrl::to_instance)
            .filter(TremorUrl::is_connector)
            .collect();
        let source_connectors: HashSet<TremorUrl> = binding
            .links
            .iter()
            .map(|(from, _tos)| from.to_instance())
            .filter(TremorUrl::is_connector)
            .collect();
        let pipelines: HashSet<TremorUrl> = binding
            .links
            .iter()
            .flat_map(|(from, tos)| std::iter::once(from).chain(tos.iter()))
            .filter(|url| url.is_pipeline())
            .map(TremorUrl::to_instance)
            .collect();

        let start_points: HashSet<TremorUrl> = source_connectors
            .difference(&sink_connectors)
            .cloned()
            .collect();
        let mixed_pickles: HashSet<TremorUrl> = sink_connectors
            .intersection(&source_connectors)
            .cloned()
            .collect();
        let end_points: HashSet<TremorUrl> = sink_connectors
            .difference(&source_connectors)
            .cloned()
            .collect();

        // for receiving drain/stop completion notifications from connectors
        let mut expected_drains: usize = 0;
        let mut expected_stops: usize = 0;

        // for storing senders that have been sent to us
        let mut drain_senders = Vec::with_capacity(1);
        let mut stop_senders = Vec::with_capacity(1);

        task::spawn::<_, Result<()>>(async move {
            while let Some(wrapped) = input_channel.next().await {
                match wrapped {
                    MsgWrapper::Msg(Msg::Start) if binding_state == InstanceState::Initialized => {
                        // start all pipelines first - order doesnt matter as connectors aren't started yet
                        for pipe in &pipelines {
                            registries.start_pipeline(pipe).await?;
                        }
                        // start sink connectors
                        for conn in &end_points {
                            registries.start_connector(conn).await?;
                        }
                        // start source/sink connectors in random order
                        for conn in &mixed_pickles {
                            registries.start_connector(conn).await?;
                        }
                        // start source only connectors
                        for conn in &start_points {
                            registries.start_connector(conn).await?;
                        }

                        binding_state = InstanceState::Running;
                    }
                    MsgWrapper::Msg(Msg::Start) => {
                        info!(
                            "[Binding::{}] Ignoring Start message. Current state: {}",
                            &url, &binding_state
                        );
                    }
                    MsgWrapper::Msg(Msg::Pause) if binding_state == InstanceState::Running => {
                        info!("[Binding::{}] Pausing...", &url);
                        for source in &start_points {
                            registries.pause_connector(source).await?;
                        }
                        for source_n_sink in &mixed_pickles {
                            registries.pause_connector(source_n_sink).await?;
                        }
                        for sink in &end_points {
                            registries.pause_connector(sink).await?;
                        }
                        for url in &pipelines {
                            registries.pause_pipeline(url).await?;
                        }
                    }
                    MsgWrapper::Msg(Msg::Pause) => {
                        info!(
                            "[Binding::{}] Ignoring Pause message. Current state: {}",
                            &url, &binding_state
                        );
                    }
                    MsgWrapper::Msg(Msg::Resume) if binding_state == InstanceState::Paused => {
                        info!("[Binding::{}] Resuming...", &url);

                        for url in &pipelines {
                            registries.resume_pipeline(url).await?;
                        }

                        for sink in &end_points {
                            registries.resume_connector(sink).await?;
                        }
                        for source_n_sink in &mixed_pickles {
                            registries.resume_connector(source_n_sink).await?;
                        }
                        for source in &start_points {
                            registries.resume_connector(source).await?;
                        }
                    }
                    MsgWrapper::Msg(Msg::Resume) => {
                        info!(
                            "[Binding::{}] Ignoring Resume message. Current state: {}",
                            &url, &binding_state
                        );
                    }
                    MsgWrapper::Msg(Msg::Drain(_sender))
                        if binding_state == InstanceState::Draining =>
                    {
                        info!(
                            "[Binding::{}] Ignoring Drain message. Current state: {}",
                            &url, &binding_state
                        );
                    }
                    MsgWrapper::Msg(Msg::Drain(sender)) => {
                        info!("[Binding::{}] Draining...", &url);
                        drain_senders.push(sender);

                        // QUIESCENCE
                        // - send drain msg to all connectors
                        // - wait until
                        //   a) all connectors are drained (means all pipelines in between are also drained) or
                        //   b) we timed out

                        // source only connectors
                        for start_point in &start_points {
                            if let Err(e) = registries
                                .drain_connector(start_point, drain_tx.clone())
                                .await
                            {
                                error!(
                                    "[Binding::{}] Error starting Draining Connector {}: {}",
                                    &url, start_point, e
                                );
                            } else {
                                expected_drains += 1;
                            }
                        }
                        // source/sink connectors
                        for mixed_pickle in &mixed_pickles {
                            if let Err(e) = registries
                                .drain_connector(mixed_pickle, drain_tx.clone())
                                .await
                            {
                                error!(
                                    "[Binding::{}] Error starting Draining Connector {}: {}",
                                    &url, mixed_pickle, e
                                );
                            } else {
                                expected_drains += 1;
                            }
                        }
                        // sink only connectors
                        for end_point in &end_points {
                            if let Err(e) = registries
                                .drain_connector(end_point, drain_tx.clone())
                                .await
                            {
                                error!(
                                    "[Binding::{}] Error starting Draining Connector {}: {}",
                                    &url, end_point, e
                                );
                            } else {
                                expected_drains += 1;
                            }
                        }
                    }
                    MsgWrapper::Msg(Msg::Stop(sender)) => {
                        info!("[Binding::{}] Stopping...", &url);
                        stop_senders.push(sender);

                        for connector_url in source_connectors.union(&sink_connectors) {
                            if let Err(e) = registries
                                .stop_connector(connector_url, stop_tx.clone())
                                .await
                            {
                                error!(
                                    "[Binding::{}] Error stopping connector {}: {}",
                                    &url, connector_url, e
                                );
                            } else {
                                expected_stops += 1;
                            }
                        }
                        for pipeline_url in &pipelines {
                            if let Err(e) = registries.stop_pipeline(pipeline_url).await {
                                error!(
                                    "[Binding::{}] Error stopping pipeline {}: {}",
                                    &url, pipeline_url, e
                                );
                            }
                        }
                    }
                    MsgWrapper::Msg(Msg::Report(sender)) => {
                        // TODO: aggregate states of all containing instances
                        let report = StatusReport {
                            url: url.clone(),
                            status: binding_state.clone(),
                        };
                        if let Err(e) = sender.send(report).await {
                            error!("[Binding::{}] Error sending status report: {}", &url, e);
                        }
                    }
                    MsgWrapper::DrainResult(conn_res) => {
                        info!("[Binding::{}] Connector {} drained.", &url, &conn_res.url);
                        if let Err(e) = conn_res.res {
                            error!(
                                "[Binding::{}] Error during Draining in Connector {}: {}",
                                &url, &conn_res.url, e
                            );
                        }
                        let old = expected_drains;
                        expected_drains = expected_drains.saturating_sub(1);
                        if expected_drains == 0 && old > 0 {
                            info!("[Binding::{}] All connectors are drained.", &url);
                            // upon last drain
                            for drain_sender in drain_senders.drain(..) {
                                if let Err(_) = drain_sender.send(Ok(())).await {
                                    error!(
                                        "[Binding::{}] Error sending successful Drain result",
                                        &url
                                    );
                                }
                            }
                        }
                    }
                    MsgWrapper::StopResult(conn_res) => {
                        info!("[Binding::{}] Connector {} stopped.", &url, &conn_res.url);
                        if let Err(e) = conn_res.res {
                            error!(
                                "[Binding::{}] Error during Draining in Connector {}: {}",
                                &url, &conn_res.url, e
                            );
                        }
                        let old = expected_stops;
                        expected_stops = expected_stops.saturating_sub(1);
                        if expected_stops == 0 && old > 0 {
                            info!("[Binding::{}] All connectors are stopped.", &url);
                            // upon last stop
                            for stop_sender in stop_senders.drain(..) {
                                if let Err(_) = stop_sender.send(Ok(())).await {
                                    error!(
                                        "[Binding::{}] Error sending successful Stop result",
                                        &url
                                    );
                                }
                            }
                            break;
                        }
                    }
                }
            }
            info!("[Binding::{}] Binding Stopped.", &url);
            Ok(())
        });
        addr_tx.send(Ok(addr)).await?;
        Ok(())
    }
}
