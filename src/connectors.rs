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

/// quality of service utilities
pub(crate) mod qos;

/// prelude with commonly needed stuff imported
pub(crate) mod prelude;
/// Sink part of a connector
pub(crate) mod sink;
/// source part of a connector
pub(crate) mod source;

/// reconnect logic for connectors
pub(crate) mod reconnect;

/// home for connector specific function
pub(crate) mod functions;

/// google cloud pubsub/storage/auth
pub(crate) mod gcp;
/// opentelemetry
pub(crate) mod otel;
/// protobuf helpers
pub(crate) mod pb;

/// tcp server connector impl
pub(crate) mod tcp_server;

/// udp server connector impl
pub(crate) mod udp_server;

/// std streams connector (stdout, stderr, stdin)
pub(crate) mod std_streams;

/// Home of the famous metrics collector
pub(crate) mod metrics;

/// Metronome
pub(crate) mod metronome;

/// Exit Connector
pub(crate) mod exit;
/// quiescence stuff
pub(crate) mod quiescence;

use std::fmt::Display;

use async_std::task::{self, JoinHandle};
use beef::Cow;

use crate::config::Connector as ConnectorConfig;
use crate::connectors::metrics::{MetricsSinkReporter, SourceReporter};
use crate::connectors::sink::{SinkAddr, SinkContext, SinkMsg};
use crate::connectors::source::{SourceAddr, SourceContext, SourceMsg};
use crate::errors::{Error, Kind as ErrorKind, Result};
use crate::pipeline;
use crate::system::World;
use crate::url::ports::{ERR, IN, OUT};
use crate::url::TremorUrl;
use crate::OpConfig;
use async_std::channel::{bounded, Sender};
use halfbrown::{Entry, HashMap};
use reconnect::Reconnect;
use tremor_common::ids::ConnectorIdGen;

use self::metrics::MetricsSender;
use self::quiescence::QuiescenceBeacon;

/// sender for connector manager messages
pub type ManagerSender = Sender<ManagerMsg>;

/// connector address
#[derive(Clone, Debug)]
pub struct Addr {
    uid: u64,
    /// connector instance url
    pub url: TremorUrl,
    sender: Sender<Msg>,
    source: Option<SourceAddr>,
    sink: Option<SinkAddr>,
}

impl Addr {
    /// send a message
    ///
    /// # Errors
    ///  * If sending failed
    pub async fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.sender.send(msg).await?)
    }

    pub(crate) async fn send_sink(&self, msg: SinkMsg) -> Result<()> {
        if let Some(sink) = self.sink.as_ref() {
            sink.addr.send(msg).await?;
        }
        Ok(())
    }

    pub(crate) async fn send_source(&self, msg: SourceMsg) -> Result<()> {
        if let Some(source) = self.source.as_ref() {
            source.addr.send(msg).await?;
        }
        Ok(())
    }

    fn has_source(&self) -> bool {
        self.source.is_some()
    }

    fn has_sink(&self) -> bool {
        self.sink.is_some()
    }
}

/// Messages a Connector instance receives and acts upon
pub enum Msg {
    /// connect 1 or more pipelines to a port
    Link {
        /// port to which to connect
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
        /// result receiver
        result_tx: Sender<Result<()>>,
    },
    /// disconnect pipeline `id` from the given `port`
    Unlink {
        /// port from which to disconnect
        port: Cow<'static, str>,
        /// id of the pipeline to disconnect
        id: TremorUrl,
        /// sender to receive a boolean whether this connector is not connected to anything
        tx: Sender<Result<bool>>,
    },
    /// notification from the connector implementation that connectivity is lost and should be reestablished
    ConnectionLost,
    /// initiate a reconnect attempt
    Reconnect,
    // TODO: fill as needed
    /// start the connector
    Start,
    /// pause the connector
    ///
    /// source part is not polling for new data
    /// sink part issues a CB trigger
    /// until Resume is called, sink is restoring the CB again
    Pause,
    /// resume the connector after a pause
    Resume,
    /// Drain events from this connector
    ///
    /// - stop reading events from external connections
    /// - decline events received via the sink part
    /// - wait for drainage to be finished
    Drain(async_std::channel::Sender<Result<()>>),
    /// notify this connector that its source part has been drained
    SourceDrained,
    /// notify this connector that its sink part has been drained
    SinkDrained,
    /// stop the connector
    Stop,
    /// request a status report
    Report(Sender<StatusReport>),
}

/// Connector instance status report
#[derive(Debug, Serialize)]
pub struct StatusReport {
    /// connector instance url
    pub url: TremorUrl,
    /// state of the connector
    pub status: ConnectorState,
    /// current connectivity
    pub connectivity: Connectivity,
    /// connected pipelines
    pub pipelines: HashMap<Cow<'static, str>, Vec<TremorUrl>>,
}

/// msg used for connector creation
#[derive(Debug)]
pub struct Create {
    servant_id: TremorUrl,
    config: ConnectorConfig,
}

impl Create {
    /// constructor
    #[must_use]
    pub fn new(servant_id: TremorUrl, config: ConnectorConfig) -> Self {
        Self { servant_id, config }
    }
}

/// Stream id generator
#[derive(Debug, Default)]
pub struct StreamIdGen(u64);

impl StreamIdGen {
    /// get the next stream id and increment the internal state
    pub fn next_stream_id(&mut self) -> u64 {
        let res = self.0;
        self.0 = self.0.wrapping_add(1);
        res
    }
}

/// msg for the `ConnectorManager` handling all connectors
pub enum ManagerMsg {
    /// register a new connector type
    Register {
        /// the type of connector
        connector_type: String,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
        /// if this one is a builtin connector
        builtin: bool,
    },
    /// unregister a connector type
    Unregister(String),
    /// create a new connector
    Create {
        /// sender to send the create result to
        tx: Sender<Result<Addr>>,
        /// the create command
        create: Box<Create>,
    },
    /// stop the connector manager
    Stop {
        /// reason
        reason: String,
    },
}

/// The connector manager - handling creation of connectors
/// and handles available connector types
pub struct Manager {
    qsize: usize,
    metrics_sender: MetricsSender,
}

impl Manager {
    /// constructor
    #[must_use]
    pub fn new(qsize: usize, metrics_sender: MetricsSender) -> Self {
        Self {
            qsize,
            metrics_sender,
        }
    }

    /// start the manager
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn start(self) -> (JoinHandle<Result<()>>, ManagerSender) {
        let (tx, rx) = bounded(self.qsize);
        let h = task::spawn(async move {
            info!("Connector manager started.");
            let mut connector_id_gen = ConnectorIdGen::new();
            let mut known_connectors: HashMap<String, (Box<dyn ConnectorBuilder>, bool)> =
                HashMap::with_capacity(16);

            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Create { tx, create }) => {
                        let url = create.servant_id.clone();
                        // lookup and instantiate connector
                        let connector = if let Some((builder, _)) =
                            known_connectors.get(&create.config.binding_type)
                        {
                            let connector_res = builder.from_config(&url, &create.config.config);
                            match connector_res {
                                Ok(connector) => connector,
                                Err(e) => {
                                    error!(
                                        "[Connector] Error instantiating connector {}: {}",
                                        &url, e
                                    );
                                    tx.send(Err(e)).await?;
                                    continue;
                                }
                            }
                        } else {
                            error!(
                                "[Connector] Connector Type '{}' unknown",
                                &create.config.binding_type
                            );
                            tx.send(Err(ErrorKind::UnknownConnectorType(
                                create.config.binding_type,
                            )
                            .into()))
                                .await?;
                            continue;
                        };
                        if let Err(e) = self
                            .connector_task(
                                tx.clone(),
                                create.servant_id,
                                connector,
                                create.config,
                                connector_id_gen.next_id(),
                            )
                            .await
                        {
                            error!(
                                "[Connector] Error spawning task for connector {}: {}",
                                &url, e
                            );
                            tx.send(Err(e)).await?;
                        }
                    }
                    Ok(ManagerMsg::Register {
                        connector_type,
                        builder,
                        builtin,
                    }) => {
                        debug!("Registering {} Connector Type.", &connector_type);
                        match known_connectors.entry(connector_type) {
                            Entry::Occupied(e) => {
                                warn!("Connector Type {} already registered.", e.key());
                            }
                            Entry::Vacant(e) => {
                                debug!(
                                    "Connector Type {} registered{}.",
                                    e.key(),
                                    if builtin { " as builtin" } else { " " }
                                );
                                e.insert((builder, builtin));
                            }
                        }
                    }
                    Ok(ManagerMsg::Unregister(connector_type)) => {
                        debug!("Unregistering {} Connector Type.", &connector_type);
                        match known_connectors.entry(connector_type) {
                            Entry::Occupied(e) => {
                                let (_, builtin) = e.get();
                                if *builtin {
                                    error!("Cannot unregister builtin Connector Type {}", e.key());
                                } else {
                                    debug!("Connector Type {} unregistered.", e.key());
                                    e.remove_entry();
                                }
                            }
                            Entry::Vacant(e) => {
                                error!("Connector Type {} not registered", e.key());
                            }
                        }
                    }
                    Ok(ManagerMsg::Stop { reason }) => {
                        info!("Stopping Connector Manager... {}", reason);
                        break;
                    }
                    Err(e) => {
                        info!("Error! Stopping Connector Manager... {}", e);
                        break;
                    }
                }
            }
            info!("Connector Manager stopped.");
            Ok(())
        });
        (h, tx)
    }

    #[allow(clippy::too_many_lines)]
    // instantiates the connector and starts listening for control plane messages
    async fn connector_task(
        &self,
        addr_tx: Sender<Result<Addr>>,
        url: TremorUrl,
        mut connector: Box<dyn Connector>,
        config: ConnectorConfig,
        uid: u64,
    ) -> Result<()> {
        // channel for connector-level control plane communication
        let (msg_tx, msg_rx) = bounded(self.qsize);

        let mut connectivity = Connectivity::Disconnected;
        let mut quiescence_beacon = QuiescenceBeacon::new();

        let source_metrics_reporter = SourceReporter::new(
            url.clone(),
            self.metrics_sender.clone(),
            config.metrics_interval_s,
        );

        let default_codec = connector.default_codec();
        let source_builder = source::builder(
            uid,
            &config,
            default_codec,
            self.qsize,
            source_metrics_reporter,
        )?;
        let source_ctx = SourceContext {
            uid,
            url: url.clone(),
            quiescence_beacon: quiescence_beacon.clone(),
        };

        let sink_metrics_reporter = MetricsSinkReporter::new(
            url.clone(),
            self.metrics_sender.clone(),
            config.metrics_interval_s,
        );
        let sink_builder =
            sink::builder(&config, default_codec, self.qsize, sink_metrics_reporter)?;
        let sink_ctx = SinkContext {
            uid,
            url: url.clone(),
        };
        // create source instance
        let source = connector.create_source(source_ctx, source_builder).await?;

        // create sink instance
        let sink = connector.create_sink(sink_ctx, sink_builder).await?;

        let ctx = ConnectorContext {
            uid,
            url: url.clone(),
            quiescence_beacon: quiescence_beacon.clone(),
        };

        let addr = Addr {
            uid,
            url: url.clone(),
            sender: msg_tx,
            source,
            sink,
        };

        let mut reconnect: Reconnect = Reconnect::new(&addr, config.reconnect);
        let send_addr = addr.clone();
        let mut connector_state = ConnectorState::Initialized;
        let mut drainage = None;
        // TODO: add connector metrics reporter (e.g. for reconnect attempts, cb's received, uptime, etc.)
        task::spawn::<_, Result<()>>(async move {
            // typical 1 pipeline connected to IN, OUT, ERR
            let mut pipelines: HashMap<Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>> =
                HashMap::with_capacity(3);
            // connector control plane loop
            while let Ok(msg) = msg_rx.recv().await {
                match msg {
                    Msg::Report(tx) => {
                        // request a status report from this connector
                        let pipes: HashMap<Cow<'static, str>, Vec<TremorUrl>> = pipelines
                            .iter()
                            .map(|(port, connected)| {
                                (
                                    port.clone(),
                                    connected
                                        .iter()
                                        .map(|(url, _)| url)
                                        .cloned()
                                        .collect::<Vec<_>>(),
                                )
                            })
                            .collect();
                        if let Err(e) = tx
                            .send(StatusReport {
                                url: url.clone(),
                                status: connector_state,
                                connectivity,
                                pipelines: pipes,
                            })
                            .await
                        {
                            error!("[Connector::{}] Error sending status report {}.", &url, e);
                        }
                    }
                    Msg::Link {
                        port,
                        pipelines: mut mapping,
                        result_tx,
                    } => {
                        for (url, _) in &mapping {
                            info!(
                                "[Connector::{}] Connecting {} via port {}",
                                &url, &url, port
                            );
                        }
                        if let Some(port_pipes) = pipelines.get_mut(&port) {
                            port_pipes.append(&mut mapping);
                        } else {
                            pipelines.insert(port.clone(), mapping.clone());
                        }
                        let res = if port.eq_ignore_ascii_case(IN.as_ref()) {
                            // connect to sink part
                            match addr.sink.as_ref() {
                                Some(sink) => sink
                                    .addr
                                    .send(SinkMsg::Connect {
                                        port,
                                        pipelines: mapping,
                                    })
                                    .await
                                    .map_err(|e| e.into()),
                                None => Err(ErrorKind::InvalidConnect(
                                    addr.url.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        } else if port.eq_ignore_ascii_case(OUT.as_ref())
                            || port.eq_ignore_ascii_case(ERR.as_ref())
                        {
                            // connect to source part
                            match addr.source.as_ref() {
                                Some(source) => source
                                    .addr
                                    .send(SourceMsg::Link {
                                        port,
                                        pipelines: mapping,
                                    })
                                    .await
                                    .map_err(|e| e.into()),
                                None => Err(ErrorKind::InvalidConnect(
                                    addr.url.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        } else {
                            error!(
                                "[Connector::{}] Tried to connect to unsupported port: {}",
                                &addr.url, &port
                            );
                            Err(
                                ErrorKind::InvalidConnect(addr.url.to_string(), port.clone())
                                    .into(),
                            )
                        };
                        // send back the connect result
                        if let Err(e) = result_tx.send(res).await {
                            error!("Error sending connect result: {}", e);
                        }
                    }
                    Msg::Unlink { port, id, tx } => {
                        let delete = pipelines.get_mut(&port).map_or(false, |port_pipes| {
                            port_pipes.retain(|(url, _)| url != &id);
                            port_pipes.is_empty()
                        });
                        // make sure we can simply use `is_empty` for checking for emptiness
                        if delete {
                            pipelines.remove(&port);
                        }
                        let res: Result<()> = if port.eq_ignore_ascii_case(IN.as_ref()) {
                            // disconnect from source part
                            match addr.source.as_ref() {
                                Some(source) => source
                                    .addr
                                    .send(SourceMsg::Unlink { port, id })
                                    .await
                                    .map_err(Error::from),
                                None => Err(ErrorKind::InvalidDisconnect(
                                    addr.url.to_string(),
                                    id.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        } else {
                            // disconnect from sink part
                            match addr.sink.as_ref() {
                                Some(sink) => sink
                                    .addr
                                    .send(SinkMsg::Disconnect { port, id })
                                    .await
                                    .map_err(Error::from),
                                None => Err(ErrorKind::InvalidDisconnect(
                                    addr.url.to_string(),
                                    id.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        };
                        // TODO: work out more fine grained "empty" semantics
                        tx.send(res.map(|_| pipelines.is_empty())).await?;
                    }
                    Msg::ConnectionLost => {
                        // react on the connection being lost
                        // immediately try to reconnect if we are not in draining state.
                        //
                        // TODO: this might lead to very fast retry loops if the connection is established as connector.connect returns successful
                        //       but in the next instant fails and sends this message.
                        connectivity = Connectivity::Disconnected;
                        info!("[Connector::{}] Disconnected.", &addr.url);
                        addr.send_sink(SinkMsg::ConnectionLost).await?;
                        addr.send_source(SourceMsg::ConnectionLost).await?;

                        // reconnect if running - wait with reconnect if paused (until resume)
                        if connector_state == ConnectorState::Running {
                            info!("[Connector::{}] Triggering reconnect.", &addr.url);
                            addr.sender.send(Msg::Reconnect).await?;
                        }
                    }
                    Msg::Reconnect => {
                        // reconnect if we are below max_retries, otherwise bail out and fail the connector
                        info!("[Connector::{}] Connecting...", &addr.url);
                        let new = reconnect.attempt(connector.as_mut(), &ctx).await?;
                        match (&connectivity, &new) {
                            (Connectivity::Disconnected, Connectivity::Connected) => {
                                info!("[Connector::{}] Connected.", &addr.url);
                                // notify sink
                                addr.send_sink(SinkMsg::ConnectionEstablished).await?;
                                addr.send_source(SourceMsg::ConnectionEstablished).await?;
                            }
                            (Connectivity::Connected, Connectivity::Disconnected) => {
                                info!("[Connector::{}] Disconnected.", &addr.url);
                                addr.send_sink(SinkMsg::ConnectionLost).await?;
                                addr.send_source(SourceMsg::ConnectionLost).await?;
                            }
                            _ => {
                                debug!("[Connector::{}] No change: {:?}", &addr.url, &new);
                            }
                        }
                        connectivity = new;
                    }
                    Msg::Start if connector_state == ConnectorState::Initialized => {
                        info!("[Connector::{}] Starting...", &addr.url);
                        // start connector
                        connector_state = match connector.on_start(&ctx).await {
                            Ok(new_state) => new_state,
                            Err(e) => {
                                error!("[Connector::{}] on_start Error: {}", &addr.url, e);
                                ConnectorState::Failed
                            }
                        };
                        info!(
                            "[Connector::{}] Started. New state: {:?}",
                            &addr.url, &connector_state
                        );
                        // forward to source/sink if available
                        addr.send_source(SourceMsg::Start).await?;
                        addr.send_sink(SinkMsg::Start).await?;

                        // initiate connect asynchronously
                        addr.sender.send(Msg::Reconnect).await?;
                    }
                    Msg::Start => {
                        info!(
                            "[Connector::{}] Ignoring Start Msg. Current state: {:?}",
                            &addr.url, &connector_state
                        );
                    }

                    Msg::Pause if connector_state == ConnectorState::Running => {
                        info!("[Connector::{}] Pausing...", &addr.url);

                        // TODO: in implementations that don't really support pausing
                        //       issue a warning/error message
                        //       e.g. UDP, TCP, Rest
                        //
                        connector.on_pause(&ctx).await;
                        connector_state = ConnectorState::Paused;

                        addr.send_source(SourceMsg::Pause).await?;
                        addr.send_sink(SinkMsg::Pause).await?;

                        info!("[Connector::{}] Paused.", &addr.url);
                    }
                    Msg::Pause => {
                        info!(
                            "[Connector::{}] Ignoring Pause Msg. Current state: {:?}",
                            &addr.url, &connector_state
                        );
                    }
                    Msg::Resume if connector_state == ConnectorState::Paused => {
                        info!("[Connector::{}] Resuming...", &addr.url);
                        connector.on_resume(&ctx).await;
                        connector_state = ConnectorState::Running;

                        addr.send_source(SourceMsg::Resume).await?;
                        addr.send_sink(SinkMsg::Resume).await?;

                        if connectivity == Connectivity::Disconnected {
                            info!(
                                "[Connector::{}] Triggering reconnect as part of resume.",
                                &addr.url
                            );
                            addr.send(Msg::Reconnect).await?;
                        }

                        info!("[Connector::{}] Resumed.", &addr.url);
                    }
                    Msg::Resume => {
                        info!(
                            "[Connector::{}] Ignoring Resume Msg. Current state: {:?}",
                            &addr.url, &connector_state
                        );
                    }
                    Msg::Drain(_) if connector_state == ConnectorState::Draining => {
                        info!(
                            "[Connector::{}] Ignoring Drain Msg. Current state: {:?}",
                            &addr.url, &connector_state
                        );
                    }
                    Msg::Drain(tx) => {
                        info!("[Connector::{}] Draining...", &addr.url);

                        // notify connector that it should stop reading - so no more new events arrive at its source part
                        quiescence_beacon.stop_reading();
                        // FIXME: add stop_writing()
                        // let connector stop emitting anything to its source part - if possible here
                        connector.on_drain(&ctx).await;
                        connector_state = ConnectorState::Draining;

                        // notify source to drain the source channel and then send the drain signal
                        if let Some(source) = addr.source.as_ref() {
                            source
                                .addr
                                .send(SourceMsg::Drain(addr.sender.clone()))
                                .await?;
                        } else {
                            // proceed to the next step
                            addr.send(Msg::SourceDrained).await?;
                        }

                        let d = Drainage::new(&addr, tx);
                        if d.all_drained() {
                            if let Err(e) = d.send_all_drained().await {
                                error!(
                                    "[Connector::{}] error signalling being fully drained: {}",
                                    &addr.url, e
                                );
                            }
                        }
                        drainage = Some(d);
                    }
                    Msg::SourceDrained => {
                        if let Some(drainage) = drainage.as_mut() {
                            drainage.set_source_drained();
                            if drainage.all_drained() {
                                if let Err(e) = drainage.send_all_drained().await {
                                    error!(
                                        "[Connector::{}] Error signalling being fully drained: {}",
                                        &addr.url, e
                                    );
                                }
                            } else {
                                // notify sink to go into DRAIN state
                                // flush all events until we received a drain signal from all inputs
                                if let Some(sink) = addr.sink.as_ref() {
                                    sink.addr.send(SinkMsg::Drain(addr.sender.clone())).await?;
                                }
                            }
                        }
                    }
                    Msg::SinkDrained => {
                        if let Some(drainage) = drainage.as_mut() {
                            drainage.set_sink_drained();
                            if drainage.all_drained() {
                                if let Err(e) = drainage.send_all_drained().await {
                                    error!(
                                        "[Connector::{}] Error signalling being fully drained: {}",
                                        &addr.url, e
                                    );
                                }
                            }
                        }
                    }
                    Msg::Stop => {
                        info!("[Connector::{}] Stopping...", &addr.url);
                        connector.on_stop(&ctx).await;
                        connector_state = ConnectorState::Stopped;

                        addr.send_source(SourceMsg::Stop).await?;
                        addr.send_sink(SinkMsg::Stop).await?;

                        info!("[Connector::{}] Stopped.", &addr.url);
                        break;
                    }
                } // match
            } // while
            info!(
                "[Connector::{}] Connector Stopped. Reason: {:?}",
                &addr.url, &connector_state
            );
            // TODO: inform registry that this instance is gone now
            Ok(())
        });
        addr_tx.send(Ok(send_addr)).await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum DrainState {
    None,
    Expect,
    Drained,
}

struct Drainage {
    tx: Sender<Result<()>>,
    source_drained: DrainState,
    sink_drained: DrainState,
}

impl Drainage {
    fn new(addr: &Addr, tx: Sender<Result<()>>) -> Self {
        Self {
            tx,
            source_drained: if addr.has_source() {
                DrainState::Expect
            } else {
                DrainState::None
            },
            sink_drained: if addr.has_sink() {
                DrainState::Expect
            } else {
                DrainState::None
            },
        }
    }

    fn set_sink_drained(&mut self) {
        self.sink_drained = DrainState::Drained;
    }

    fn set_source_drained(&mut self) {
        self.source_drained = DrainState::Drained;
    }

    fn all_drained(&self) -> bool {
        // None and Drained are valid here
        self.source_drained != DrainState::Expect && self.sink_drained != DrainState::Expect
    }

    async fn send_all_drained(&self) -> Result<()> {
        self.tx.send(Ok(())).await?;
        Ok(())
    }
}

/// state of a connector
#[derive(Debug, PartialEq, Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorState {
    /// connector has been initialized, but not yet started
    Initialized,
    /// connector is running
    Running,
    /// connector has been paused
    Paused,
    /// connector was stopped
    Stopped,
    /// Draining - getting rid of in-flight events and avoid emitting new ones
    Draining,
    /// connector failed to start
    Failed,
}

impl Display for ConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Initialized => "initialized",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Draining => "draining",
            Self::Failed => "failed",
        })
    }
}

/// connector context
pub struct ConnectorContext {
    /// unique identifier
    pub uid: u64,
    /// url of the connector
    pub url: TremorUrl,
    /// The Quiescence Beacon
    pub quiescence_beacon: QuiescenceBeacon,
}

/// describes connectivity state of the connector
#[derive(Debug, Serialize, Copy, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Connectivity {
    /// connector is connected
    Connected,
    /// connector is disconnected
    Disconnected,
}

/// A Connector connects the tremor runtime to the outside world.
///
/// It can be a source of events, as such it is polled for new data.
/// It can also be a sink for events, as such events are sent to it from pipelines.
/// A connector can act as sink and source or just as one of those.
///
/// A connector encapsulates the establishment and maintenance of connections to the outside world,
/// such as tcp connections, file handles etc. etc.
///
/// It is a meta entity on top of the sink and source part.
/// The connector has its own control plane and is an artefact in the tremor repository.
/// It controls the sink and source parts which are connected to the rest of the runtime via links to pipelines.
#[async_trait::async_trait]
pub trait Connector: Send {
    /// create a source part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a source, return `Ok(None)`.
    async fn create_source(
        &mut self,
        _source_context: SourceContext,
        _builder: source::SourceManagerBuilder,
    ) -> Result<Option<source::SourceAddr>> {
        Ok(None)
    }

    /// Create a sink part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a sink, return `Ok(None)`.
    async fn create_sink(
        &mut self,
        _sink_context: SinkContext,
        _builder: sink::SinkManagerBuilder,
    ) -> Result<Option<sink::SinkAddr>> {
        Ok(None)
    }

    /// Attempt to connect to the outside world.
    /// Return `Ok(true)` if a connection could be established.
    /// This method will be retried if it fails or returns `Ok(false)`.
    ///
    /// To notify the runtime of the main connectivity being lost, a `notifier` is passed in.
    /// Call `notifier.notify().await` as the last thing when you notice the connection is lost.
    /// This is well suited when handling the connection in another task.
    ///
    /// To know when to stop reading new data from the external connection, the `quiescence` beacon
    /// can be used. Call `.reading()` and `.writing()` to see if you should continue doing so, if not, just stop and rest.
    async fn connect(
        &mut self,
        ctx: &ConnectorContext,
        notifier: reconnect::ConnectionLostNotifier,
    ) -> Result<bool>;

    /// called once when the connector is started
    /// `connect` will be called after this for the first time, leave connection attempts in `connect`.
    async fn on_start(&mut self, _ctx: &ConnectorContext) -> Result<ConnectorState> {
        Ok(ConnectorState::Running)
    }

    /// called when the connector pauses
    async fn on_pause(&mut self, _ctx: &ConnectorContext) {}
    /// called when the connector resumes
    async fn on_resume(&mut self, _ctx: &ConnectorContext) {}

    /// Drain
    ///
    /// Ensure no new events arrive at the source part of this connector when this function returns
    /// So we can safely send the `Drain` signal.
    async fn on_drain(&mut self, _ctx: &ConnectorContext) {}

    /// called when the connector is stopped
    async fn on_stop(&mut self, _ctx: &ConnectorContext) {}

    /// returns the default codec for this connector
    fn default_codec(&self) -> &str;
}

/// something that is able to create a connector instance
pub trait ConnectorBuilder: Sync + Send {
    /// create a connector from the given `id` and `config`
    ///
    /// # Errors
    ///  * If the config is invalid for the connector
    fn from_config(&self, id: &TremorUrl, config: &Option<OpConfig>) -> Result<Box<dyn Connector>>;
}

/// registering builtin connector types
///
/// # Errors
///  * If a builtin connector couldn't be registered
#[cfg(not(tarpaulin_include))]
pub async fn register_builtin_connector_types(world: &World) -> Result<()> {
    world
        .register_builtin_connector_type("metrics", Box::new(metrics::Builder::default()))
        .await?;
    world
        .register_builtin_connector_type("tcp_server", Box::new(tcp_server::Builder::default()))
        .await?;
    world
        .register_builtin_connector_type("udp_server", Box::new(udp_server::Builder::default()))
        .await?;
    world
        .register_builtin_connector_type("std_stream", Box::new(std_streams::Builder::default()))
        .await?;
    world
        .register_builtin_connector_type("exit", Box::new(exit::Builder::new(world)))
        .await?;
    Ok(())
}
