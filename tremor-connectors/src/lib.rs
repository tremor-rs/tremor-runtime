// Copyright 2020-2024, The Tremor Team
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
#![recursion_limit = "1024"]

/// connectors
pub mod impls;
/// prelude with commonly needed stuff imported
pub mod prelude;

/// sink parts
pub(crate) mod sink;

/// source parts
pub(crate) mod source;

#[macro_use]
pub(crate) mod utils;

/// connector configuration
pub mod config;

/// connector errors
pub mod errors;

mod channel;

#[cfg(test)]
pub mod tests;

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

use self::metrics::{SinkReporter, SourceReporter};
use self::sink::SinkContext;
use self::source::SourceContext;
use self::utils::quiescence::QuiescenceBeacon;
pub(crate) use crate::config::Connector as ConnectorConfig;
use crate::{
    channel::{bounded, Sender},
    errors::{error_impl_def, Error},
};
use beef::Cow;
use futures::Future;
use halfbrown::HashMap;
use std::{fmt::Display, time::Duration};
use tokio::task::{self, JoinHandle};
use tremor_common::{
    alias,
    ids::{ConnectorId, ConnectorIdGen, SourceId},
    ports::{Port, ERR, IN, OUT},
};
use tremor_pipeline::METRICS_CHANNEL;
use tremor_script::ast::DeployEndpoint;
use tremor_system::{
    connector::{self, Attempt, Connectivity, Msg, ResultWrapper},
    instance::State,
    killswitch::KillSwitch,
    pipeline, qsize,
};
use tremor_value::Value;
use utils::reconnect::{ConnectionLostNotifier, ReconnectRuntime};
/// quiescence stuff
pub(crate) use utils::{metrics, reconnect};
use value_trait::prelude::*;

/// Accept timeout
pub(crate) const ACCEPT_TIMEOUT: Duration = Duration::from_millis(100);

/// Logs but ignores an error
#[macro_export]
#[doc(hidden)]
macro_rules! log_error {
    ($maybe_error:expr,  $($args:tt)+) => (
        if let Err(e) = $maybe_error {
            error!($($args)+, e = e);
            true
        } else {
            false
        }
    )
}

/// context for a Connector or its parts
pub(crate) trait Context: Display + Clone {
    /// provide the alias of the connector
    fn alias(&self) -> &alias::Connector;

    /// get the quiescence beacon for checking if we should continue reading/writing
    fn quiescence_beacon(&self) -> &QuiescenceBeacon;

    /// get the notifier to signal to the runtime that we are disconnected
    fn notifier(&self) -> &reconnect::ConnectionLostNotifier;

    /// get the connector type
    fn connector_type(&self) -> &ConnectorType;

    /// only log an error and swallow the result
    #[inline]
    fn swallow_err<T, E, M>(&self, expr: std::result::Result<T, E>, msg: &M)
    where
        E: Display,
        M: Display + ?Sized,
    {
        if let Err(e) = expr {
            error!("{self} {msg}: {e}");
        }
    }

    /// log an error and return the result
    #[inline]
    fn bail_err<T, E, M>(
        &self,
        expr: std::result::Result<T, E>,
        msg: &M,
    ) -> std::result::Result<T, E>
    where
        E: Display,
        M: Display + ?Sized,
    {
        if let Err(e) = &expr {
            error!("{self} {msg}: {e}");
        }
        expr
    }

    /// enclose the given meta in the right connector namespace
    ///
    /// Namespace: "connector.<connector-type>"
    #[must_use]
    fn meta(&self, inner: Value<'static>) -> Value<'static> {
        let mut map = Value::object_with_capacity(1);
        map.try_insert(self.connector_type().to_string(), inner);
        map
    }

    /// extract the connector specific metadata
    fn extract_meta<'ct, 'event>(
        &'ct self,
        event_meta: &'ct Value<'event>,
    ) -> Option<&'ct Value<'event>>
    where
        'ct: 'event,
    {
        let t: &str = self.connector_type().into();
        event_meta.get(&Cow::borrowed(t))
    }
}

/// connector context
#[derive(Clone)]
pub struct ConnectorContext {
    /// alias of the connector instance
    pub(crate) alias: alias::Connector,
    /// type of the connector
    connector_type: ConnectorType,
    /// The Quiescence Beacon
    quiescence_beacon: QuiescenceBeacon,
    /// Notifier
    notifier: reconnect::ConnectionLostNotifier,
}

impl ConnectorContext {
    /// result wrapper error
    pub(crate) fn result_err<E>(&self, e: E) -> ResultWrapper<()>
    where
        E: Into<anyhow::Error>,
    {
        ResultWrapper::err(self.alias(), e)
    }

    /// result wrapper ok for `()`
    pub(crate) fn result_ok(&self) -> ResultWrapper<()> {
        ResultWrapper::ok(self.alias())
    }
}

impl Display for ConnectorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Connector::{}]", &self.alias)
    }
}

impl Context for ConnectorContext {
    fn alias(&self) -> &alias::Connector {
        &self.alias
    }

    fn connector_type(&self) -> &ConnectorType {
        &self.connector_type
    }

    fn quiescence_beacon(&self) -> &QuiescenceBeacon {
        &self.quiescence_beacon
    }

    fn notifier(&self) -> &reconnect::ConnectionLostNotifier {
        &self.notifier
    }
}

/// Stream id generator
#[derive(Debug, Default)]
pub(crate) struct StreamIdGen(u64);

impl StreamIdGen {
    /// get the next stream id and increment the internal state
    pub(crate) fn next_stream_id(&mut self) -> u64 {
        let res = self.0;
        self.0 = self.0.wrapping_add(1);
        res
    }
}

/// How should we treat a stream being done
///
/// * `StreamClosed` -> Only this stream is closed
/// * `ConnectorClosed` -> The entire connector is closed, notify that we are disconnected, reconnect according to chosen reconnect config
#[derive(Debug, Clone, PartialEq, Copy)]
pub(crate) enum StreamDone {
    /// Only this stream is closed, (only one of many)
    StreamClosed,
    /// With this stream being closed, the whole connector can be considered done/closed
    ConnectorClosed,
}

/// Lookup table for known connectors
pub(crate) type Known =
    std::collections::HashMap<ConnectorType, Box<dyn ConnectorBuilder + 'static>>;

/// Spawns a connector
///
/// # Errors
/// if the connector can not be built or the config is invalid
pub(crate) async fn spawn(
    alias: &alias::Connector,
    connector_id_gen: &mut ConnectorIdGen,
    builder: &dyn ConnectorBuilder,
    config: ConnectorConfig,
    kill_switch: &KillSwitch,
) -> anyhow::Result<connector::Addr> {
    // instantiate connector
    let connector = builder.build(alias, &config, kill_switch).await?;
    let r = connector_task(alias.clone(), connector, config, connector_id_gen.next_id()).await?;

    Ok(r)
}

#[allow(clippy::too_many_lines)]
// instantiates the connector and starts listening for control plane messages
async fn connector_task(
    alias: alias::Connector,
    mut connector: Box<dyn Connector>,
    config: ConnectorConfig,
    uid: ConnectorId,
) -> anyhow::Result<connector::Addr> {
    let qsize = qsize();
    // channel for connector-level control plane communication
    let (msg_tx, mut msg_rx) = bounded(qsize);

    let mut connectivity = Connectivity::Disconnected;
    let mut quiescence_beacon = QuiescenceBeacon::default();
    let notifier = ConnectionLostNotifier::new(&alias, msg_tx.clone());

    let source_metrics_reporter = SourceReporter::new(
        alias.clone(),
        METRICS_CHANNEL.tx(),
        config.metrics_interval_s,
    );

    let codec_requirement = connector.codec_requirements();
    if connector.codec_requirements() == CodecReq::Structured && (config.codec.is_some()) {
        return Err(Error::UnsupportedCodec(alias).into());
    }
    let source_builder = source::builder(
        SourceId::from(uid),
        &config,
        codec_requirement,
        source_metrics_reporter,
        &alias,
    )?;
    let source_ctx = SourceContext {
        alias: alias.clone(),
        uid: uid.into(),
        connector_type: config.connector_type.clone(),
        quiescence_beacon: quiescence_beacon.clone(),
        notifier: notifier.clone(),
    };

    let sink_metrics_reporter = SinkReporter::new(
        alias.clone(),
        METRICS_CHANNEL.tx(),
        config.metrics_interval_s,
    );
    let sink_builder = sink::builder(&config, codec_requirement, &alias, sink_metrics_reporter)?;
    let sink_ctx = SinkContext::new(
        uid.into(),
        alias.clone(),
        config.connector_type.clone(),
        quiescence_beacon.clone(),
        notifier.clone(),
    );
    // create source instance
    let source_addr = connector
        .create_source(source_ctx, source_builder)
        .await
        .map_err(|e| Error::CreateSink(alias.clone(), e))?;

    // create sink instance
    let sink_addr = connector
        .create_sink(sink_ctx, sink_builder)
        .await
        .map_err(|e| Error::CreateSource(alias.clone(), e))?;

    let connector_addr = connector::Addr::new(alias.clone(), msg_tx, source_addr, sink_addr);

    let mut reconnect: ReconnectRuntime =
        ReconnectRuntime::new(&connector_addr, notifier.clone(), &config.reconnect);
    let notifier = reconnect.notifier();

    let ctx = ConnectorContext {
        alias: alias.clone(),
        connector_type: config.connector_type.clone(),
        quiescence_beacon: quiescence_beacon.clone(),
        notifier,
    };

    let send_addr = connector_addr.clone();
    let mut connector_state = State::Initializing;
    let mut drainage = None;
    let mut start_sender: Option<Sender<ResultWrapper<()>>> = None;

    // TODO: add connector metrics reporter (e.g. for reconnect attempts, cb's received, uptime, etc.)
    task::spawn(async move {
        // typical 1 pipeline connected to IN, OUT, ERR
        let mut connected_pipelines: HashMap<Port<'static>, Vec<(DeployEndpoint, pipeline::Addr)>> =
            HashMap::with_capacity(3);
        // connector control plane loop
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                Msg::Report(tx) => {
                    // request a status report from this connector
                    let pipes: std::collections::HashMap<Port<'static>, Vec<DeployEndpoint>> =
                        connected_pipelines
                            .iter()
                            .map(|(port, connected)| {
                                (
                                    port.clone(),
                                    connected
                                        .iter()
                                        .map(|(endpoint, _)| endpoint)
                                        .cloned()
                                        .collect::<Vec<_>>(),
                                )
                            })
                            .collect();

                    if tx
                        .send(connector::StatusReport::new(
                            alias.clone(),
                            connector_state,
                            connectivity,
                            pipes,
                        ))
                        .is_err()
                    {
                        error!("{ctx} Error sending status report.");
                    }
                }
                Msg::LinkInput {
                    port,
                    pipelines: pipelines_to_link,
                    result_tx,
                } => {
                    for (url, _) in &pipelines_to_link {
                        info!("{ctx} Connecting {url} via port {port}");
                    }

                    if let Some(port_pipes) = connected_pipelines.get_mut(&port) {
                        port_pipes.extend(pipelines_to_link.iter().cloned());
                    } else {
                        connected_pipelines.insert(port.clone(), pipelines_to_link.clone());
                    }
                    let res = if connector.is_valid_input_port(&port) {
                        // connect to sink part
                        if let Some(sink) = connector_addr.sink() {
                            sink.send(connector::sink::Msg::Link {
                                pipelines: pipelines_to_link,
                            })
                            .await
                            .map_err(|e| error_impl_def(connector_addr.alias(), e))
                        } else {
                            Err(Error::InvalidPort(
                                connector_addr.alias().clone(),
                                port.clone(),
                            ))
                        }
                    } else {
                        error!("{ctx} Tried to connect to unsupported port: \"{port}\"");
                        Err(Error::InvalidPort(
                            connector_addr.alias().clone(),
                            port.clone(),
                        ))
                    };
                    // send back the connect result
                    if let Err(e) = result_tx.send(res.map_err(anyhow::Error::new)).await {
                        error!("{ctx} Error sending connect result: {e}");
                    }
                }
                Msg::LinkOutput {
                    port,
                    pipeline: pipeline_to_link,
                    result_tx,
                } => {
                    let (url, _) = &pipeline_to_link;
                    info!("{ctx} Connecting {url} via port {port}");

                    if let Some(port_pipes) = connected_pipelines.get_mut(&port) {
                        port_pipes.push(pipeline_to_link.clone());
                    } else {
                        connected_pipelines.insert(port.clone(), vec![pipeline_to_link.clone()]);
                    }
                    if connector.is_valid_output_port(&port) {
                        // connect to source part
                        if let Some(source) = connector_addr.source().as_ref() {
                            // delegate error reporting to source
                            let m = connector::source::Msg::Link {
                                port,
                                tx: result_tx,
                                pipeline: pipeline_to_link,
                            };
                            let res = source.addr.send(m);
                            log_error!(res, "{ctx} Error sending to source: {e}");
                        } else {
                            let e = Err(Error::InvalidPort(
                                connector_addr.alias().clone(),
                                port.clone(),
                            )
                            .into());
                            let res = result_tx.send(e).await;
                            log_error!(res, "{ctx} Error sending connect result: {e}");
                        }
                    } else {
                        error!("{ctx} Tried to connect to unsupported port: \"{port}\"");
                        // send back the connect result
                        let e = Err(Error::InvalidPort(
                            connector_addr.alias().clone(),
                            port.clone(),
                        )
                        .into());
                        let res = result_tx.send(e).await;
                        log_error!(res, "{ctx} Error sending connect result: {e}");
                    };
                }
                Msg::ConnectionLost => {
                    // react on the connection being lost
                    // immediately try to reconnect if we are not in draining state.
                    //
                    // TODO: this might lead to very fast retry loops if the connection is established as connector.connect() returns successful
                    //       but in the next instant fails and sends this message.
                    connectivity = Connectivity::Disconnected;
                    info!("{} Connection lost.", &ctx);
                    connector_addr
                        .send_sink(connector::sink::Msg::ConnectionLost)
                        .await?;
                    connector_addr.send_source(connector::source::Msg::ConnectionLost)?;

                    // reconnect if running - wait with reconnect if paused (until resume)
                    if connector_state == State::Running {
                        // ensure we don't reconnect in a hot loop
                        // ensure we adhere to the reconnect strategy, waiting and possibly not reconnecting at all
                        reconnect.enqueue_retry().await;
                    }
                }
                Msg::Reconnect => {
                    // reconnect if we are below max_retries, otherwise bail out and fail the connector
                    info!("{} Connecting...", &ctx);
                    let (new, will_retry) = reconnect.attempt(connector.as_mut(), &ctx).await?;
                    match (&connectivity, &new) {
                        (Connectivity::Disconnected, Connectivity::Connected) => {
                            info!("{} Connected.", &ctx);
                            // notify sink
                            connector_addr
                                .send_sink(connector::sink::Msg::ConnectionEstablished)
                                .await?;
                            connector_addr
                                .send_source(connector::source::Msg::ConnectionEstablished)?;
                            if let Some(start_sender) = start_sender.take() {
                                ctx.swallow_err(
                                    start_sender.send(ResultWrapper::ok(ctx.alias())).await,
                                    "Error sending start response.",
                                );
                            }
                        }
                        (Connectivity::Connected, Connectivity::Disconnected) => {
                            info!("{} Disconnected.", &ctx);
                            connector_addr
                                .send_sink(connector::sink::Msg::ConnectionLost)
                                .await?;
                            connector_addr.send_source(connector::source::Msg::ConnectionLost)?;
                        }
                        _ => {
                            debug!("{} No change after reconnect: {:?}", &ctx, &new);
                        }
                    }
                    // ugly extra check
                    if new == Connectivity::Disconnected && !will_retry {
                        // if we weren't able to connect and gave up retrying, we are failed. That's life.
                        connector_state = State::Failed;
                        if let Some(start_sender) = start_sender.take() {
                            let e = Error::ConnectionFailed(ctx.alias().clone());
                            ctx.swallow_err(
                                start_sender.send(ctx.result_err(e)).await,
                                "Error sending start response",
                            );
                        }
                    }
                    connectivity = new;
                }
                Msg::Start(sender) if connector_state == State::Initializing => {
                    info!("{ctx} Starting...");
                    start_sender = Some(sender);

                    // start connector
                    connector_state = match connector.on_start(&ctx).await {
                        Ok(()) => State::Running,
                        Err(e) => {
                            error!("{ctx} on_start Error: {e}");
                            State::Failed
                        }
                    };
                    info!("{ctx} Started. New state: {connector_state}",);
                    // forward to source/sink if available
                    connector_addr.send_source(connector::source::Msg::Start)?;
                    connector_addr
                        .send_sink(connector::sink::Msg::Start)
                        .await?;

                    // initiate connect asynchronously
                    connector_addr.send(Msg::Reconnect).await?;
                }
                Msg::Start(sender) => {
                    info!("{ctx} Ignoring Start Msg. Current state: {connector_state}",);
                    if connector_state == State::Running && connectivity == Connectivity::Connected
                    {
                        // sending an answer if we are connected
                        ctx.swallow_err(
                            sender.send(ctx.result_ok()).await,
                            "Error sending Start result",
                        );
                    }
                }

                Msg::Pause if connector_state == State::Running => {
                    info!("{ctx} Pausing...");

                    // TODO: in implementations that don't really support pausing
                    //       issue a warning/error message
                    //       e.g. UDP, TCP, HTTP
                    //
                    ctx.swallow_err(connector.on_pause(&ctx).await, "Error during on_pause");

                    connector_addr.send_source(connector::source::Msg::Pause)?;
                    connector_addr
                        .send_sink(connector::sink::Msg::Pause)
                        .await?;

                    connector_state = State::Paused;
                    quiescence_beacon.pause()?;

                    info!("{ctx} Paused.");
                }
                Msg::Pause => {
                    info!("{ctx} Ignoring Pause Msg. Current state: {connector_state}",);
                }
                Msg::Resume if connector_state == State::Paused => {
                    info!("{ctx} Resuming...");
                    ctx.swallow_err(connector.on_resume(&ctx).await, "Error during on_resume");
                    connector_state = State::Running;
                    quiescence_beacon.resume()?;

                    connector_addr.send_source(connector::source::Msg::Resume)?;
                    connector_addr
                        .send_sink(connector::sink::Msg::Resume)
                        .await?;

                    if connectivity == Connectivity::Disconnected {
                        info!("{ctx} Triggering reconnect as part of resume.",);
                        connector_addr.send(Msg::Reconnect).await?;
                    }

                    info!("{ctx} Resumed.");
                }
                Msg::Resume => {
                    info!("{ctx} Ignoring Resume Msg. Current state: {connector_state}",);
                }
                Msg::Drain(_) if connector_state == State::Draining => {
                    info!("{ctx} Ignoring Drain Msg. Current state: {connector_state}",);
                }
                Msg::Drain(tx) => {
                    info!("{ctx} Draining...");

                    // notify connector that it should stop reading - so no more new events arrive at its source part
                    quiescence_beacon.stop_reading();
                    // let connector stop emitting anything to its source part - if possible here
                    ctx.swallow_err(connector.on_drain(&ctx).await, "Error during on_drain");
                    connector_state = State::Draining;

                    // notify source to drain the source channel and then send the drain signal
                    if let Some(source) = connector_addr.source() {
                        source
                            .send(connector::source::Msg::Drain(
                                connector_addr.sender().clone(),
                            ))
                            .map_err(|e| Error::SourceBackplane(alias.clone(), e))?;
                    } else {
                        // proceed to the next step, even without source
                        connector_addr.send(Msg::SourceDrained).await?;
                    }

                    let d = Drainage::new(&connector_addr, tx);
                    if d.all_drained() {
                        info!("{ctx} Drained.");
                        log_error!(
                            d.send_all_drained().await,
                            "{ctx} error signalling being fully drained: {e}"
                        );
                    }
                    drainage = Some(d);
                }
                Msg::SourceDrained if connector_state == State::Draining => {
                    info!("{ctx} Source-part is drained.",);
                    if let Some(drainage) = drainage.as_mut() {
                        drainage.set_source_drained();
                        if drainage.all_drained() {
                            info!("{ctx} Drained.");
                            log_error!(
                                drainage.send_all_drained().await,
                                "{ctx} Error signalling being fully drained: {e}"
                            );
                        } else {
                            // notify sink to go into DRAIN state
                            // flush all events until we received a drain signal from all inputs
                            if let Some(sink) = connector_addr.sink() {
                                sink.send(connector::sink::Msg::Drain(
                                    connector_addr.sender().clone(),
                                ))
                                .await
                                .map_err(|e| Error::SinkBackplane(alias.clone(), e))?;
                            } else {
                                // proceed to the next step, even without sink
                                connector_addr.send(Msg::SinkDrained).await?;
                            }
                        }
                    }
                }
                Msg::SinkDrained if connector_state == State::Draining => {
                    info!("{ctx} Sink-part is drained.",);
                    if let Some(drainage) = drainage.as_mut() {
                        drainage.set_sink_drained();
                        quiescence_beacon.full_stop(); // TODO: maybe this should be done in the SinkManager?
                        if drainage.all_drained() {
                            info!("{ctx} Drained.");
                            log_error!(
                                drainage.send_all_drained().await,
                                "{ctx} Error signalling being fully drained: {e}"
                            );
                        }
                    }
                }
                Msg::SourceDrained => {
                    info!("{ctx} Ignoring SourceDrained Msg. Current state: {connector_state}",);
                }
                Msg::SinkDrained => {
                    info!("{ctx} Ignoring SourceDrained Msg. Current state: {connector_state}",);
                }
                Msg::Stop(sender) => {
                    info!("{ctx} Stopping...");
                    ctx.swallow_err(connector.on_stop(&ctx).await, "Error during on_stop");
                    connector_state = State::Stopped;
                    quiescence_beacon.full_stop();
                    let (stop_tx, mut stop_rx) = bounded(2);
                    let mut expect = usize::from(connector_addr.has_source())
                        + usize::from(connector_addr.has_sink());
                    ctx.swallow_err(
                        connector_addr.send_source(connector::source::Msg::Stop(stop_tx.clone())),
                        "Error sending Stop msg to Source",
                    );
                    ctx.swallow_err(
                        connector_addr
                            .send_sink(connector::sink::Msg::Stop(stop_tx))
                            .await,
                        "Error sending Stop msg to Sink",
                    );
                    while expect > 0 {
                        stop_rx.recv().await;
                        expect -= 1;
                    }
                    log_error!(
                        sender.send(ctx.result_ok()).await,
                        "{ctx} Error sending Stop result: {e}"
                    );

                    info!("{ctx} Stopped.");
                    break;
                }
            } // match
        } // while
        info!("{ctx} Connector Stopped. Reason: {connector_state}");
        // TODO: inform registry that this instance is gone now
        anyhow::Ok(())
    });
    Ok(send_addr)
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum DrainState {
    None,
    Expect,
    Drained,
}

struct Drainage {
    tx: Sender<ResultWrapper<()>>,
    alias: alias::Connector,
    source_drained: DrainState,
    sink_drained: DrainState,
}

impl Drainage {
    fn new(addr: &connector::Addr, tx: Sender<ResultWrapper<()>>) -> Self {
        Self {
            tx,
            alias: addr.alias().clone(),
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

    async fn send_all_drained(&self) -> anyhow::Result<()> {
        Ok(self
            .tx
            .send(ResultWrapper {
                alias: self.alias.clone(),
                res: Ok(()),
            })
            .await
            .map_err(|_| Error::DrainageSend(self.alias.clone()))?)
    }
}

const IN_PORTS: [Port<'static>; 1] = [IN];
const IN_PORTS_REF: &[Port<'static>; 1] = &IN_PORTS;
const OUT_PORTS: [Port<'static>; 2] = [OUT, ERR];
const OUT_PORTS_REF: &[Port<'static>; 2] = &OUT_PORTS;

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
    /// Valid input ports for the connector, by default this is `in`
    fn input_ports(&self) -> &[Port<'static>] {
        IN_PORTS_REF
    }
    /// Valid output ports for the connector, by default this is `out` and `err`
    fn output_ports(&self) -> &[Port<'static>] {
        OUT_PORTS_REF
    }

    /// Tests if a input port is valid, by default does a case insensitive search against
    /// `self.input_ports()`
    fn is_valid_input_port(&self, port: &Port) -> bool {
        for valid in self.input_ports() {
            if port == valid {
                return true;
            }
        }
        false
    }

    /// Tests if a input port is valid, by default does a case insensitive search against
    /// `self.output_ports()`
    fn is_valid_output_port(&self, port: &Port) -> bool {
        for valid in self.output_ports() {
            if port == valid {
                return true;
            }
        }
        false
    }

    /// create a source part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a source, return `Ok(None)`.
    async fn create_source(
        &mut self,
        _ctx: SourceContext,
        _builder: source::SourceManagerBuilder,
    ) -> anyhow::Result<Option<connector::source::Addr>> {
        Ok(None)
    }

    /// Create a sink part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a sink, return `Ok(None)`.
    async fn create_sink(
        &mut self,
        _ctx: SinkContext,
        _builder: sink::SinkManagerBuilder,
    ) -> anyhow::Result<Option<connector::sink::Addr>> {
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
    /// The attempt is the number of the connection attempt, the number this method has been called on this connector.
    /// The very first attempt to establish a connection will be `0`.
    /// All further attempts will be
    ///
    /// To know when to stop reading new data from the external connection, the `quiescence` beacon
    /// can be used. Call `.reading()` and `.writing()` to see if you should continue doing so, if not, just stop and rest.
    async fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        _attempt: &Attempt,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    /// called once when the connector is started
    /// `connect` will be called after this for the first time, leave connection attempts in `connect`.
    async fn on_start(&mut self, _ctx: &ConnectorContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// called when the connector pauses
    async fn on_pause(&mut self, _ctx: &ConnectorContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// called when the connector resumes
    async fn on_resume(&mut self, _ctx: &ConnectorContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// Drain
    ///
    /// Ensure no new events arrive at the source part of this connector when this function returns
    /// So we can safely send the `Drain` signal.
    async fn on_drain(&mut self, _ctx: &ConnectorContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// called when the connector is stopped
    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// Returns the codec requirements for the connector
    fn codec_requirements(&self) -> CodecReq;
}

/// Specifeis if a connector requires a codec
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CodecReq {
    /// No codec can be provided for this connector it always returns structured data
    Structured,
    /// A codec must be provided for this connector
    Required,
    /// A codec can be provided for this connector otherwise the default is used
    Optional(&'static str),
}

/// the type of a connector
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct ConnectorType(String);

impl From<ConnectorType> for String {
    fn from(ct: ConnectorType) -> Self {
        ct.0
    }
}

impl<'t> From<&'t ConnectorType> for &'t str {
    fn from(ct: &'t ConnectorType) -> Self {
        ct.0.as_str()
    }
}

impl Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl From<String> for ConnectorType {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl<T> From<&T> for ConnectorType
where
    T: ToString + ?Sized,
{
    fn from(s: &T) -> Self {
        Self(s.to_string())
    }
}

/// something that is able to create a connector instance
#[async_trait::async_trait]
pub trait ConnectorBuilder: Sync + Send + std::fmt::Debug {
    /// the type of the connector
    fn connector_type(&self) -> ConnectorType;

    /// create a connector from the given `id` and `config`, if a connector config is mandatory
    /// implement `build_cfg` instead
    ///
    /// # Errors
    ///  * If the config is invalid for the connector
    async fn build(
        &self,
        alias: &alias::Connector,
        config: &ConnectorConfig,
        kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let cc = config
            .config
            .as_ref()
            .ok_or_else(|| Error::MissingConfiguration(alias.clone()))?;
        self.build_cfg(alias, config, cc, kill_switch).await
    }

    /// create a connector from the given `alias`, outer `ConnectorConfig` and the connector-specific `connector_config`
    ///
    /// # Errors
    ///  * If the config is invalid for the connector
    async fn build_cfg(
        &self,
        alias: &alias::Connector,
        _config: &ConnectorConfig,
        _connector_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        Err(Error::BuildCfg(alias.clone()).into())
    }
}

/// builtin connector types

#[must_use]
pub(crate) fn builtin_connector_types() -> Vec<Box<dyn ConnectorBuilder + 'static>> {
    vec![
        Box::<impls::file::Builder>::default(),
        Box::<impls::metrics::Builder>::default(),
        Box::<impls::stdio::Builder>::default(),
        Box::<impls::tcp::client::Builder>::default(),
        Box::<impls::tcp::server::Builder>::default(),
        Box::<impls::udp::client::Builder>::default(),
        Box::<impls::udp::server::Builder>::default(),
        Box::<impls::kv::Builder>::default(),
        Box::<impls::metronome::Builder>::default(),
        Box::<impls::wal::Builder>::default(),
        Box::<impls::dns::client::Builder>::default(),
        Box::<impls::discord::Builder>::default(),
        Box::<impls::ws::client::Builder>::default(),
        Box::<impls::ws::server::Builder>::default(),
        Box::<impls::elastic::Builder>::default(),
        Box::<impls::crononome::Builder>::default(),
        Box::<impls::s3::streamer::Builder>::default(),
        Box::<impls::s3::reader::Builder>::default(),
        Box::<impls::kafka::consumer::Builder>::default(),
        Box::<impls::kafka::producer::Builder>::default(),
        #[cfg(unix)]
        Box::<impls::unix_socket::server::Builder>::default(),
        #[cfg(unix)]
        Box::<impls::unix_socket::client::Builder>::default(),
        Box::<impls::http::client::Builder>::default(),
        Box::<impls::http::server::Builder>::default(),
        Box::<impls::otel::client::Builder>::default(),
        Box::<impls::otel::server::Builder>::default(),
        Box::<impls::gbq::writer::Builder>::default(),
        Box::<impls::gpubsub::consumer::Builder>::default(),
        Box::<impls::gpubsub::producer::Builder>::default(),
        Box::<impls::clickhouse::Builder>::default(),
        Box::<impls::gcl::writer::Builder>::default(),
        Box::<impls::gcs::streamer::Builder>::default(),
        Box::<impls::null::Builder>::default(),
    ]
}

/// debug connector types

#[must_use]
pub(crate) fn debug_connector_types() -> Vec<Box<dyn ConnectorBuilder + 'static>> {
    vec![
        Box::<impls::cb::Builder>::default(),
        Box::<impls::bench::Builder>::default(),
        Box::<impls::exit::Builder>::default(),
    ]
}

/// registering builtin and debug connector types
///
/// # Errors
///  * If a builtin connector couldn't be registered

pub(crate) async fn register_builtin_connector_types(
    world: &World,
    debug: bool,
) -> anyhow::Result<()> {
    for builder in builtin_connector_types() {
        world.register_builtin_connector_type(builder).await?;
    }
    if debug {
        for builder in debug_connector_types() {
            world.register_builtin_connector_type(builder).await?;
        }
    }

    Ok(())
}

/// Function to spawn a long-running task representing a connector connection
/// and ensuring that upon error the runtime is notified about the lost connection, as the task is gone.
pub(crate) fn spawn_task<F, C>(ctx: C, t: F) -> JoinHandle<()>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
    C: Context + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = t.await {
            error!("{ctx} Connector loop error: {e}");
            // notify connector task about a terminated connection loop
            let n = ctx.notifier();
            log_error!(
                n.connection_lost().await,
                "{ctx} Failed to notify on connection lost: {e}"
            );
        } else {
            debug!("{ctx} Connector loop finished.");
        }
    })
}

#[cfg(test)]
pub(crate) mod unit_tests {
    use super::*;

    #[derive(Clone)]
    pub(crate) struct FakeContext {
        t: ConnectorType,
        alias: alias::Connector,
        notifier: reconnect::ConnectionLostNotifier,
        beacon: QuiescenceBeacon,
    }

    impl FakeContext {
        pub(crate) fn new(tx: Sender<Msg>) -> Self {
            let alias = alias::Connector::new(alias::Flow::new("fake"), "fake");
            let notifier = reconnect::ConnectionLostNotifier::new(&alias, tx);
            Self {
                t: ConnectorType::from("snot"),
                alias,
                notifier,
                beacon: QuiescenceBeacon::default(),
            }
        }
    }

    impl Display for FakeContext {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "snot")
        }
    }

    impl Context for FakeContext {
        fn alias(&self) -> &alias::Connector {
            &self.alias
        }

        fn quiescence_beacon(&self) -> &QuiescenceBeacon {
            &self.beacon
        }

        fn notifier(&self) -> &reconnect::ConnectionLostNotifier {
            &self.notifier
        }

        fn connector_type(&self) -> &ConnectorType {
            &self.t
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_task_error() -> anyhow::Result<()> {
        let (tx, mut rx) = bounded(1);
        let ctx = FakeContext::new(tx);
        // thanks coverage
        ctx.quiescence_beacon();
        ctx.notifier();
        ctx.connector_type();
        ctx.alias();
        // end
        spawn_task(ctx.clone(), async move { Err(anyhow::anyhow!("snot")) }).await?;
        assert!(matches!(rx.recv().await, Some(Msg::ConnectionLost)));

        spawn_task(ctx.clone(), async move { Ok(()) }).await?;

        rx.close();

        // this one is just here for coverage for when the call to notify is failing
        spawn_task(ctx, async move { Err(anyhow::anyhow!("snot 2")) }).await?;

        Ok(())
    }
}

#[macro_use]
mod macros {
    // stolen from https://github.com/popzxc/stdext-rs and slightly adapted
    #[cfg(test)]
    #[macro_export]
    macro_rules! function_name {
        () => {{
            // Okay, this is ugly, I get it. However, this is the best we can get on a stable rust.
            fn f() {}
            fn type_name_of<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            let mut name = type_name_of(f);
            if let Some(stripped) = name.strip_suffix("::f") {
                name = stripped;
            };
            while let Some(stripped) = name.strip_suffix("::{{closure}}") {
                name = stripped;
            }
            if let Some(last) = name.split("::").last() {
                last
            } else {
                name
            }
        }};
    }
}
