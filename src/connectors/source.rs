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

#![allow(clippy::module_name_repetitions)]

use async_std::task;
use async_std::{channel::unbounded, future::timeout};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::time::{Duration, Instant};
use tremor_common::time::nanotime;
use tremor_script::{ast::DeployEndpoint, EventPayload, ValueAndMeta};

use crate::config::{
    self, Codec as CodecConfig, Connector as ConnectorConfig, Preprocessor as PreprocessorConfig,
};
use crate::connectors::{
    metrics::SourceReporter,
    utils::reconnect::{Attempt, ConnectionLostNotifier},
    ConnectorType, Context, Msg, QuiescenceBeacon, StreamDone,
};
use crate::errors::{Error, Result};
use crate::pipeline;
use crate::preprocessor::{finish, make_preprocessors, preprocess, Preprocessors};
use crate::{
    codec::{self, Codec},
    pipeline::InputTarget,
};
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use beef::Cow;
use tremor_common::url::ports::{ERR, OUT};
use tremor_pipeline::{
    CbAction, Event, EventId, EventIdGenerator, EventOriginUri, DEFAULT_STREAM_ID,
};
use tremor_value::{literal, Value};
use value_trait::Builder;

/// The default poll interval for `try_recv` on channels in connectors
pub const DEFAULT_POLL_INTERVAL: u64 = 10;
/// A duration for the default poll interval
pub const DEFAULT_POLL_DURATION: Duration = Duration::from_millis(10);

#[derive(Debug)]
/// Messages a Source can receive
pub enum SourceMsg {
    /// connect a pipeline
    Link {
        /// port
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
    },
    /// disconnect a pipeline from a port
    Unlink {
        /// port
        port: Cow<'static, str>,
        /// url of the pipeline
        id: DeployEndpoint,
    },
    /// Connect to the outside world and send the result back
    Connect(Sender<Result<bool>>, Attempt),
    /// connectivity is lost in the connector
    ConnectionLost,
    /// connectivity is re-established
    ConnectionEstablished,
    /// Circuit Breaker Contraflow Event
    Cb(CbAction, EventId),
    /// start the source
    Start,
    /// pause the source
    Pause,
    /// resume the source
    Resume,
    /// stop the source
    Stop(Sender<Result<()>>),
    /// drain the source - bears a sender for sending out a SourceDrained status notification
    Drain(Sender<Msg>),
}

/// reply from `Source::on_event`
#[derive(Debug)]
pub enum SourceReply {
    /// A normal data event with a `Vec<u8>` for data
    Data {
        /// origin uri
        origin_uri: EventOriginUri,
        /// the data
        data: Vec<u8>,
        /// metadata associated with this data
        meta: Option<Value<'static>>,
        /// stream id of the data
        stream: u64,
        /// Port to send to, defaults to `out`
        port: Option<Cow<'static, str>>,
    },
    /// an already structured event payload
    Structured {
        /// origin uri
        origin_uri: EventOriginUri,
        /// payload
        payload: EventPayload,
        /// stream id
        stream: u64,
        /// Port to send to, defaults to `out`
        port: Option<Cow<'static, str>>,
    },
    /// a bunch of separated `Vec<u8>` with optional metadata
    /// for when the source knows where boundaries are, maybe because it receives chunks already
    BatchData {
        /// origin uri
        origin_uri: EventOriginUri,
        /// batched raw data with optional metadata
        batch_data: Vec<(Vec<u8>, Option<Value<'static>>)>,
        /// Port to send to, defaults to `out`
        port: Option<Cow<'static, str>>,
        /// stream id
        stream: u64,
    },
    /// A stream is opened
    StartStream(u64),
    /// A stream is closed
    /// This might result in additional events being flushed from
    /// preprocessors, that is why we have `origin_uri` and `meta`
    EndStream {
        /// origin uri
        origin_uri: EventOriginUri,
        /// stream id
        stream: u64,
        /// optional metadata
        meta: Option<Value<'static>>,
    },
    /// no new data/event, wait for the given ms
    Empty(u64),
}

/// sender for source reply
pub type SourceReplySender = Sender<SourceReply>;

/// source part of a connector
#[async_trait::async_trait]
pub trait Source: Send {
    /// Pulls an event from the source if one exists
    /// `idgen` is passed in so the source can inspect what event id it would get if it was producing 1 event from the pulled data
    async fn pull_data(&mut self, pull_id: u64, ctx: &SourceContext) -> Result<SourceReply>;
    /// This callback is called when the data provided from
    /// pull_event did not create any events, this is needed for
    /// linked sources that require a 1:1 mapping between requests
    /// and responses, we're looking at you REST
    async fn on_no_events(
        &mut self,
        _pull_id: u64,
        _stream: u64,
        _ctx: &SourceContext,
    ) -> Result<()> {
        Ok(())
    }

    /// Pulls custom metrics from the source
    fn metrics(&mut self, _timestamp: u64) -> Vec<EventPayload> {
        vec![]
    }

    ///////////////////////////
    /// lifecycle callbacks ///
    ///////////////////////////

    /// called when the source is started. This happens only once in the whole source lifecycle, before any other callbacks
    async fn on_start(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }

    /// Connect to the external thingy.
    /// This function is called definitely after `on_start` has been called.
    ///
    /// This function might be called multiple times, check the `attempt` where you are at.
    /// The intended result of this function is to re-establish a connection. It might reuse a working connection.
    ///
    /// Return `Ok(true)` if the connection could be successfully established.
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    /// called when the source is explicitly paused as result of a user/operator interaction
    /// in contrast to `on_cb_close` which happens automatically depending on downstream pipeline or sink connector logic.
    async fn on_pause(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }
    /// called when the source is explicitly resumed from being paused
    async fn on_resume(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }
    /// called when the source is stopped. This happens only once in the whole source lifecycle, as the very last callback
    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }

    // circuit breaker callbacks
    /// called when we receive a `close` Circuit breaker event from any connected pipeline
    /// Expected reaction is to pause receiving messages, which is handled automatically by the runtime
    /// Source implementations might want to close connections or signal a pause to the upstream entity it connects to if not done in the connector (the default)
    // TODO: add info of Cb event origin (port, origin_uri)?
    async fn on_cb_close(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }
    /// Called when we receive a `open` Circuit breaker event from any connected pipeline
    /// This means we can start/continue polling this source for messages
    /// Source implementations might want to start establishing connections if not done in the connector (the default)
    async fn on_cb_open(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }

    // guaranteed delivery callbacks
    /// an event has been acknowledged and can be considered delivered
    /// multiple acks for the same set of ids are always possible
    async fn ack(&mut self, _stream_id: u64, _pull_id: u64) -> Result<()> {
        Ok(())
    }
    /// an event has failed along its way and can be considered failed
    /// multiple fails for the same set of ids are always possible
    async fn fail(&mut self, _stream_id: u64, _pull_id: u64) -> Result<()> {
        Ok(())
    }

    // connectivity stuff
    /// called when connector lost connectivity
    async fn on_connection_lost(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }
    /// called when connector re-established connectivity
    async fn on_connection_established(&mut self, _ctx: &SourceContext) -> Result<()> {
        Ok(())
    }

    /// Is this source transactional or can acks/fails be ignored
    fn is_transactional(&self) -> bool;
}

/// A source that receives `SourceReply` messages via a channel.
/// It does not handle acks/fails.
///
/// Connector implementations handling their stuff in a separate task can use the
/// channel obtained by `ChannelSource::sender()` to send `SourceReply`s to the
/// runtime.
pub struct ChannelSource {
    rx: Receiver<SourceReply>,
    tx: SourceReplySender,
}

impl ChannelSource {
    /// constructor
    pub fn new(qsize: usize) -> Self {
        let (tx, rx) = bounded(qsize);
        Self { rx, tx }
    }

    /// construct a `ChannelSource` from a given channel (`Sender` and `Receiver`)
    pub fn from_channel(tx: Sender<SourceReply>, rx: Receiver<SourceReply>) -> Self {
        Self { tx, rx }
    }

    /// get the sender for the source
    /// FIXME: change the name
    pub fn runtime(&self) -> ChannelSourceRuntime {
        ChannelSourceRuntime {
            sender: self.tx.clone(),
        }
    }
}

///
#[async_trait::async_trait]
pub trait StreamReader: Send {
    /// reads from the source reader
    async fn read(&mut self, stream: u64) -> Result<SourceReply>;

    /// called when the reader is finished or encountered an error
    async fn on_done(&mut self, _stream: u64) -> StreamDone {
        StreamDone::StreamClosed
    }
}

/// FIXME: this needs renaming and docs
#[derive(Clone)]
pub struct ChannelSourceRuntime {
    sender: Sender<SourceReply>,
}

impl ChannelSourceRuntime {
    const READ_TIMEOUT_MS: Duration = Duration::from_millis(100);

    pub(crate) fn new(sender: Sender<SourceReply>) -> Self {
        Self { sender }
    }
    pub(crate) fn register_stream_reader<R, C>(&self, stream: u64, ctx: &C, mut reader: R)
    where
        R: StreamReader + std::marker::Sync + 'static,
        C: Context + Sync + Send + 'static,
    {
        let ctx = ctx.clone();
        let tx = self.sender.clone();
        task::spawn(async move {
            if tx.send(SourceReply::StartStream(stream)).await.is_err() {
                error!("{} Failed to start stream", &ctx);
                return;
            };

            while ctx.quiescence_beacon().continue_reading().await {
                let sc_data = timeout(Self::READ_TIMEOUT_MS, reader.read(stream)).await;

                let sc_data = match sc_data {
                    Err(_) => continue,
                    Ok(Ok(d)) => d,
                    Ok(Err(e)) => {
                        error!("{} reader error: {}", &ctx, e);
                        break;
                    }
                };
                let last = matches!(&sc_data, SourceReply::EndStream { .. });
                if tx.send(sc_data).await.is_err() || last {
                    break;
                };
            }
            if reader.on_done(stream).await == StreamDone::ConnectorClosed {
                if let Err(e) = ctx.notifier().notify().await {
                    error!("{} Failed to notify connector: {}", &ctx, e);
                };
            }
        });
    }
}

#[async_trait::async_trait()]
impl Source for ChannelSource {
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok(reply) => Ok(reply),
            Err(TryRecvError::Empty) => {
                // TODO: configure pull interval in connector config?
                Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// this source is not handling acks/fails
    fn is_transactional(&self) -> bool {
        false
    }
}

// TODO make fields private and add some nice methods
/// context for a source
#[derive(Clone)]
pub struct SourceContext {
    /// connector uid
    pub uid: u64,
    /// connector alias
    pub(crate) alias: String,

    /// connector type
    pub(crate) connector_type: ConnectorType,
    /// The Quiescence Beacon
    pub(crate) quiescence_beacon: QuiescenceBeacon,

    /// tool to notify the connector when the connection is lost
    pub(crate) notifier: ConnectionLostNotifier,
}

impl Display for SourceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Source::{}]", &self.alias)
    }
}

impl Context for SourceContext {
    fn alias(&self) -> &str {
        &self.alias
    }

    fn quiescence_beacon(&self) -> &QuiescenceBeacon {
        &self.quiescence_beacon
    }

    fn notifier(&self) -> &ConnectionLostNotifier {
        &self.notifier
    }

    fn connector_type(&self) -> &ConnectorType {
        &self.connector_type
    }
}

/// address of a source
#[derive(Clone, Debug)]
pub struct SourceAddr {
    /// the actual address
    pub addr: Sender<SourceMsg>,
}

impl SourceAddr {
    /// send a message
    ///
    /// # Errors
    ///  * If sending failed
    pub async fn send(&self, msg: SourceMsg) -> Result<()> {
        Ok(self.addr.send(msg).await?)
    }
}

/// Builder for the SourceManager
#[allow(clippy::module_name_repetitions)]
pub struct SourceManagerBuilder {
    qsize: usize,
    streams: Streams,
    source_metrics_reporter: SourceReporter,
}

impl SourceManagerBuilder {
    /// queue size configured by the tremor runtime
    pub fn qsize(&self) -> usize {
        self.qsize
    }

    /// spawn a Manager with the given source implementation
    pub fn spawn<S>(self, source: S, ctx: SourceContext) -> Result<SourceAddr>
    where
        S: Source + Send + 'static,
    {
        // We use a unbounded channel for counterflow, while an unbounded channel seems dangerous
        // there is soundness to this.
        // The unbounded channel ensures that on counterflow we never have to block, or in other
        // words that sinks or pipelines sending data backwards always can progress past
        // the sending.
        // This prevents a deadlock where the pipeline is waiting for a full channel to send data to
        // the source and the source is waiting for a full channel to send data to the pipeline.
        // We prevent unbounded growth by two mechanisms:
        // 1) counterflow is ALWAYS and ONLY created in response to a message
        // 2) we always process counterflow prior to forward flow
        //
        // As long as we have counterflow messages to process, and channel size is growing we do
        // not process any forward flow. Without forward flow we stave the counterflow ensuring that
        // the counterflow channel is always bounded by the forward flow in a 1:N relationship where
        // N is the maximum number of counterflow events a single event can trigger.
        // N is normally < 1.
        //
        // In other words, DO NOT REMOVE THE UNBOUNDED QUEUE, it will lead to deadlocks where
        // the pipeline is waiting for the source to process contraflow and the source waits for
        // the pipeline to process forward flow.

        let name = ctx.alias.clone(); // FIXME: .short_id("c-src"); // connector source
        let (source_tx, source_rx) = unbounded();
        let source_addr = SourceAddr { addr: source_tx };
        let manager = SourceManager::new(source, ctx, self, source_rx, source_addr.clone());
        // spawn manager task
        task::Builder::new().name(name).spawn(manager.run())?;

        Ok(source_addr)
    }
}

/// create a builder for a `SourceManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SourceManager`.
pub fn builder(
    connector_uid: u64,
    config: &ConnectorConfig,
    connector_default_codec: &str,
    qsize: usize,
    source_metrics_reporter: SourceReporter,
) -> Result<SourceManagerBuilder> {
    let preprocessor_configs = config.preprocessors.clone().unwrap_or_default();
    let codec_config = config
        .codec
        .clone()
        .unwrap_or_else(|| CodecConfig::from(connector_default_codec));
    let streams = Streams::new(connector_uid, codec_config, preprocessor_configs)?;

    Ok(SourceManagerBuilder {
        qsize,
        streams,
        source_metrics_reporter,
    })
}

/// maintaining stream state
// TODO: there is optimization potential here for reusing codec and preprocessors after a stream got ended
struct Streams {
    uid: u64,
    codec_config: CodecConfig,
    preprocessor_configs: Vec<PreprocessorConfig>,
    states: BTreeMap<u64, StreamState>,
}

impl Streams {
    /// constructor
    fn new(
        uid: u64,
        codec_config: config::Codec,
        preprocessor_configs: Vec<PreprocessorConfig>,
    ) -> Result<Self> {
        let default = Self::build_stream(
            uid,
            DEFAULT_STREAM_ID,
            &codec_config,
            preprocessor_configs.as_slice(),
        )?;
        let mut states = BTreeMap::new();
        states.insert(DEFAULT_STREAM_ID, default);
        Ok(Self {
            uid,
            codec_config,
            preprocessor_configs,
            states,
        })
    }

    /// start a new stream if no such stream exists yet
    /// do nothing if the stream already exists
    fn start_stream(&mut self, stream_id: u64) -> Result<()> {
        if let Entry::Vacant(e) = self.states.entry(stream_id) {
            let state = Self::build_stream(
                self.uid,
                stream_id,
                &self.codec_config,
                self.preprocessor_configs.as_slice(),
            )?;
            e.insert(state);
        }
        Ok(())
    }

    /// end a stream
    fn end_stream(&mut self, stream_id: u64) -> Option<StreamState> {
        self.states.remove(&stream_id)
    }

    /// get or create a stream
    fn get_or_create_stream(&mut self, stream_id: u64) -> Result<&mut StreamState> {
        Ok(match self.states.entry(stream_id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let state = Self::build_stream(
                    self.uid,
                    stream_id,
                    &self.codec_config,
                    &self.preprocessor_configs,
                )?;
                e.insert(state)
            }
        })
    }

    /// build a stream
    fn build_stream(
        connector_uid: u64,
        stream_id: u64,
        codec_config: &CodecConfig,
        preprocessor_configs: &[PreprocessorConfig],
    ) -> Result<StreamState> {
        let codec = codec::resolve(codec_config)?;
        let preprocessors = make_preprocessors(preprocessor_configs)?;
        let idgen = EventIdGenerator::with_stream(connector_uid, stream_id);
        Ok(StreamState {
            stream_id,
            idgen,
            codec,
            preprocessors,
        })
    }
}

/// everything that is scoped to a single stream
struct StreamState {
    stream_id: u64,
    idgen: EventIdGenerator,
    codec: Box<dyn Codec>,
    preprocessors: Preprocessors,
}

/// possible states of a source implementation
#[derive(Debug, PartialEq)]
enum SourceState {
    Initialized,
    Running,
    Paused,
    Draining,
    Drained,
    Stopped,
}

impl SourceState {
    /// returns true if the runtime should pull the source for new data in this state
    fn should_pull_data(&self) -> bool {
        *self == SourceState::Running || *self == SourceState::Draining
    }
}

/// entity driving the source task
/// and keeping the source state around
pub(crate) struct SourceManager<S>
where
    S: Source,
{
    source: S,
    ctx: SourceContext,
    rx: Receiver<SourceMsg>,
    addr: SourceAddr,
    pipelines_out: Vec<(DeployEndpoint, pipeline::Addr)>,
    pipelines_err: Vec<(DeployEndpoint, pipeline::Addr)>,
    streams: Streams,
    metrics_reporter: SourceReporter,
    // `Paused` is used for both explicitly pausing and CB close/open
    // this way we can explicitly resume a Cb triggered source if need be
    // but also an explicitly paused source might receive a Cb open and continue sending data :scream:
    state: SourceState,
    // if set we can use .elapsed() to check agains `pull_wait` if we are done waiting
    pull_wait_start: Option<Instant>,
    pull_wait: Duration,
    is_transactional: bool,
    connector_channel: Option<Sender<Msg>>,
    expected_drained: usize,
    pull_counter: u64,
    cb_open_received: bool,
}

/// control flow enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Control {
    Continue,
    Terminate,
}

impl<S> SourceManager<S>
where
    S: Source,
{
    /// constructor
    fn new(
        source: S,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
        rx: Receiver<SourceMsg>,
        addr: SourceAddr,
    ) -> Self {
        let SourceManagerBuilder {
            streams,
            source_metrics_reporter,
            ..
        } = builder;
        let is_transactional = source.is_transactional();

        Self {
            source,
            ctx,
            rx,
            addr,
            streams,
            metrics_reporter: source_metrics_reporter,
            pipelines_out: Vec::with_capacity(1),
            pipelines_err: Vec::with_capacity(1),
            state: SourceState::Initialized,
            pull_wait_start: None,
            pull_wait: DEFAULT_POLL_DURATION,
            is_transactional,
            connector_channel: None,
            expected_drained: 0,
            pull_counter: 0,
            cb_open_received: false,
        }
    }

    /// we wait for control plane messages iff
    ///
    /// - we are paused
    /// - we have some control plane messages (here we don't need to wait)
    /// - if we have no pipelines connected
    fn needs_control_plane_msg(&self) -> bool {
        matches!(self.state, SourceState::Paused | SourceState::Initialized)
            || !self.rx.is_empty()
            || self.pipelines_out.is_empty()
    }

    /// Handle a control plane message
    async fn handle_control_plane_msg(&mut self, msg: SourceMsg) -> Result<Control> {
        use SourceState::{Drained, Draining, Initialized, Paused, Running, Stopped};

        match msg {
            SourceMsg::Link {
                port,
                mut pipelines,
            } => {
                let pipes = if port.eq_ignore_ascii_case(OUT.as_ref()) {
                    &mut self.pipelines_out
                } else if port.eq_ignore_ascii_case(ERR.as_ref()) {
                    &mut self.pipelines_err
                } else {
                    error!("{} Tried to connect to invalid port: {}", &self.ctx, &port);
                    return Ok(Control::Continue);
                };
                for (pipeline_url, p) in &pipelines {
                    self.ctx.log_err(
                        p.send_mgmt(pipeline::MgmtMsg::ConnectInput {
                            endpoint: DeployEndpoint::new(self.ctx.alias.clone(), port.to_string()),
                            target: InputTarget::Source(self.addr.clone()),
                            is_transactional: self.is_transactional,
                        })
                        .await,
                        &format!("Failed sending ConnectInput to pipeline {}", pipeline_url),
                    );
                }
                pipes.append(&mut pipelines);
                Ok(Control::Continue)
            }
            SourceMsg::Unlink { id, port } => {
                let pipelines = if port.eq_ignore_ascii_case(OUT.as_ref()) {
                    &mut self.pipelines_out
                } else if port.eq_ignore_ascii_case(ERR.as_ref()) {
                    &mut self.pipelines_err
                } else {
                    error!(
                        "[Source::{}] Tried to disconnect from an invalid port: {}",
                        &self.ctx.alias, &port
                    );
                    return Ok(Control::Continue);
                };
                pipelines.retain(|(url, _)| url == &id);
                if self.pipelines_out.is_empty() && self.pipelines_err.is_empty() {
                    self.ctx.log_err(
                        self.source.on_stop(&self.ctx).await,
                        "on_stop after unlinking failed",
                    );
                    return Ok(Control::Terminate);
                }
                Ok(Control::Continue)
            }
            SourceMsg::Start if self.state == Initialized => {
                info!("{} Starting...", &self.ctx);
                self.state = Running;
                self.ctx
                    .log_err(self.source.on_start(&self.ctx).await, "on_start failed");

                if let Err(e) = self.send_signal(Event::signal_start(self.ctx.uid)).await {
                    error!(
                        "[Source::{}] Error sending start signal: {}",
                        &self.ctx.alias, e
                    );
                }
                Ok(Control::Continue)
            }
            SourceMsg::Start => {
                info!(
                    "{} Ignoring Start msg in {:?} state",
                    &self.ctx, &self.state
                );
                Ok(Control::Continue)
            }
            SourceMsg::Connect(sender, attempt) => {
                info!("{} Connecting...", &self.ctx);
                let connect_result = self.source.connect(&self.ctx, &attempt).await;
                if let Ok(true) = connect_result {
                    info!("{} Source connected.", &self.ctx);
                }
                self.ctx.log_err(
                    sender.send(connect_result).await,
                    "Error sending source connect result",
                );
                Ok(Control::Continue)
            }
            SourceMsg::Resume if self.state == Paused => {
                self.state = Running;
                self.ctx
                    .log_err(self.source.on_resume(&self.ctx).await, "on_resume failed");
                Ok(Control::Continue)
            }
            SourceMsg::Resume => {
                info!(
                    "[Source::{}] Ignoring Resume msg in {:?} state",
                    &self.ctx.alias, &self.state
                );
                Ok(Control::Continue)
            }
            SourceMsg::Pause if self.state == Running => {
                // TODO: execute pause strategy chosen by source / connector / configured by user
                info!("[Source::{}] Paused.", self.ctx.alias);
                self.state = Paused;
                self.ctx
                    .log_err(self.source.on_pause(&self.ctx).await, "on_pause failed");
                Ok(Control::Continue)
            }
            SourceMsg::Pause => {
                info!(
                    "[Source::{}] Ignoring Pause msg in {:?} state",
                    &self.ctx.alias, &self.state
                );
                Ok(Control::Continue)
            }
            SourceMsg::Stop(sender) => {
                info!("{} Stopping...", &self.ctx);
                self.state = Stopped;
                self.ctx.log_err(
                    sender.send(self.source.on_stop(&self.ctx).await).await,
                    "Error sending Stop reply",
                );
                Ok(Control::Terminate)
            }
            SourceMsg::Drain(_sender) if self.state == Draining => {
                info!(
                    "[Source::{}] Ignoring incoming Drain message in {:?} state",
                    &self.ctx.alias, &self.state
                );
                Ok(Control::Continue)
            }
            SourceMsg::Drain(sender) if self.state == Drained => {
                if sender.send(Msg::SourceDrained).await.is_err() {
                    error!(
                        "[Source::{}] Error sending SourceDrained message",
                        &self.ctx.alias
                    );
                }
                Ok(Control::Continue)
            }
            SourceMsg::Drain(drained_sender) => {
                // At this point the connector has advised all reading from external connections to stop via the `QuiescenceBeacon`
                // We change the source state to `Draining` and wait for `SourceReply::Empty` as we drain out everything that might be in flight
                // when reached the `Empty` point, we emit the `Drain` signal and wait for the CB answer (one for each connected pipeline)
                if self.pipelines_out.is_empty() && self.pipelines_err.is_empty() {
                    self.ctx.log_err(
                        drained_sender.send(Msg::SourceDrained).await,
                        "sending SourceDrained message failed",
                    );
                    self.state = Drained;
                } else {
                    self.connector_channel = Some(drained_sender);
                    self.state = Draining;
                }
                Ok(Control::Continue)
            }
            SourceMsg::ConnectionLost => {
                self.ctx.log_err(
                    self.source.on_connection_lost(&self.ctx).await,
                    "on_connection_lost failed",
                );
                Ok(Control::Continue)
            }
            SourceMsg::ConnectionEstablished => {
                self.ctx.log_err(
                    self.source.on_connection_established(&self.ctx).await,
                    "on_connection_established failed",
                );
                Ok(Control::Continue)
            }
            SourceMsg::Cb(CbAction::Fail, id) => {
                if let Some((stream_id, id)) = id.get_min_by_source(self.ctx.uid) {
                    self.ctx
                        .log_err(self.source.fail(stream_id, id).await, "fail failed");
                }
                Ok(Control::Continue)
            }
            SourceMsg::Cb(CbAction::Ack, id) => {
                if let Some((stream_id, id)) = id.get_max_by_source(self.ctx.uid) {
                    self.ctx
                        .log_err(self.source.ack(stream_id, id).await, "ack failed");
                }
                Ok(Control::Continue)
            }
            SourceMsg::Cb(CbAction::Close, _id) => {
                // TODO: execute pause strategy chosen by source / connector / configured by user
                info!("[Source::{}] Circuit Breaker: Close.", self.ctx.alias);
                let res = self.source.on_cb_close(&self.ctx).await;
                self.ctx.log_err(res, "on_cb_close failed");
                self.state = Paused;
                Ok(Control::Continue)
            }
            SourceMsg::Cb(CbAction::Open, _id) => {
                // FIXME: only start polling for data if we received at least 1 CbAction::Open
                info!("[Source::{}] Circuit Breaker: Open.", self.ctx.alias);
                self.cb_open_received = true;
                self.ctx
                    .log_err(self.source.on_cb_open(&self.ctx).await, "on_cb_open failed");
                // avoid a race condition where the necessary start routine wasnt executed
                // because a `CbAction::Open` was there first, and thus the `Start` msg was ignored
                if self.state != Initialized {
                    self.state = Running;
                }
                Ok(Control::Continue)
            }
            SourceMsg::Cb(CbAction::Drained(uid), _id) => {
                debug!(
                    "[Source::{}] Drained contraflow message for {}",
                    self.ctx.alias, uid
                );
                // only account for Drained CF which we caused
                // as CF is sent back the DAG to all destinations
                if uid == self.ctx.uid {
                    self.expected_drained = self.expected_drained.saturating_sub(1);
                    debug!(
                        "[Source::{}] Drained this is us! we still have {} drains to go",
                        self.ctx.alias, self.expected_drained
                    );
                    if self.expected_drained == 0 {
                        // we received 1 drain CB event per connected pipeline (hopefully)
                        if let Some(connector_channel) = self.connector_channel.as_ref() {
                            debug!(
                                "[Source::{}] Drain completed, sending data now!",
                                self.ctx.alias
                            );
                            if connector_channel.send(Msg::SourceDrained).await.is_err() {
                                error!(
                                    "[Source::{}] Error sending SourceDrained message to Connector",
                                    &self.ctx.alias
                                );
                            }
                        }
                    }
                }
                Ok(Control::Continue)
            }
            SourceMsg::Cb(CbAction::None, _id) => Ok(Control::Continue),
        }
    }

    /// send a signal to all connected pipelines
    async fn send_signal(&mut self, signal: Event) -> Result<()> {
        for (_url, addr) in self.pipelines_out.iter().chain(self.pipelines_err.iter()) {
            debug!("Send signal {:?} to {}", signal.kind, &_url);
            addr.send(Box::new(pipeline::Msg::Signal(signal.clone())))
                .await?;
        }
        Ok(())
    }

    /// send events to pipelines
    async fn route_events(&mut self, events: Vec<(Cow<'static, str>, Event)>) -> bool {
        let mut send_error = false;

        for (port, event) in events {
            let pipelines = if port.eq_ignore_ascii_case(OUT.as_ref()) {
                self.metrics_reporter.increment_out();
                &mut self.pipelines_out
            } else if port.eq_ignore_ascii_case(ERR.as_ref()) {
                self.metrics_reporter.increment_err();
                &mut self.pipelines_err
            } else {
                error!(
                    "[Source::{}] Trying to send event to invalid port: {}",
                    &self.ctx.alias, &port
                );
                continue;
            };

            // flush metrics reporter or similar
            if let Some(t) = self.metrics_reporter.periodic_flush(event.ingest_ns) {
                self.metrics_reporter
                    .send_source_metrics(self.source.metrics(t));
            }

            if let Some((last, pipelines)) = pipelines.split_last_mut() {
                for (pipe_url, addr) in pipelines {
                    if let Err(e) = addr
                        .send(Box::new(pipeline::Msg::Event {
                            input: pipe_url.port().to_string().into(),
                            event: event.clone(),
                        }))
                        .await
                    {
                        error!(
                            "[Source::{}] Failed to send event {} to pipeline {}: {}",
                            &self.ctx.alias, &event.id, &pipe_url, e
                        );
                        send_error = true;
                    }
                }

                if let Err(e) = last
                    .1
                    .send(Box::new(pipeline::Msg::Event {
                        input: last.0.port().to_string().into(),
                        event,
                    }))
                    .await
                {
                    error!(
                        "[Source::{}] Failed to send event to pipeline {}: {}",
                        &self.ctx.alias, &last.0, e
                    );
                    send_error = true;
                }
            } else {
                // NO PIPELINES TO SEND THE EVENT TO
                // handle with ack if event is transactional
                // FIXME: discuss dead-letter behaviour for events going nowhere
                // if event.transactional {
                //     self.source.ack(stream_id, pull_id).await;
                // }
            }
        }
        send_error
    }

    /// should this manager pull data from its source?
    fn should_pull_data(&mut self) -> bool {
        let needs_to_wait = if let Some(pull_wait_start) = self.pull_wait_start {
            if pull_wait_start.elapsed() > self.pull_wait {
                self.pull_wait_start = None;
                false
            } else {
                true
            }
        } else {
            false
        };
        !needs_to_wait
            && self.state.should_pull_data()
            && !self.pipelines_out.is_empty()
            && self.cb_open_received
    }

    /// handle data from the source
    async fn handle_data(&mut self, data: Result<SourceReply>) -> Result<()> {
        let data = match data {
            Ok(d) => d,
            Err(e) => {
                warn!("{} Error pulling data: {}", &self.ctx, e);
                // TODO: increment error metric
                // FIXME: emit event to err port
                return Ok(());
            }
        };
        match data {
            SourceReply::Data {
                origin_uri,
                data,
                meta,
                stream,
                port,
            } => {
                let mut ingest_ns = nanotime();
                let stream_state = self.streams.get_or_create_stream(stream)?; // we fail if we cannot create a stream (due to misconfigured codec, preprocessors, ...) (should not happen)
                let results = build_events(
                    &self.ctx.alias,
                    stream_state,
                    &mut ingest_ns,
                    self.pull_counter,
                    origin_uri,
                    port.as_ref(),
                    data,
                    &meta.unwrap_or_else(Value::object),
                    self.is_transactional,
                );
                if results.is_empty() {
                    if let Err(e) = self
                        .source
                        .on_no_events(self.pull_counter, stream, &self.ctx)
                        .await
                    {
                        error!(
                            "[Source::{}] Error on no events callback: {}",
                            &self.ctx.alias, e
                        );
                    }
                } else {
                    let error = self.route_events(results).await;
                    if error {
                        self.ctx.log_err(
                            self.source.fail(stream, self.pull_counter).await,
                            "fail upon error sending events from data source reply failed",
                        );
                    }
                }
            }
            SourceReply::BatchData {
                origin_uri,
                batch_data,
                stream,
                port,
            } => {
                let mut ingest_ns = nanotime();
                let stream_state = self.streams.get_or_create_stream(stream)?; // we only error here due to misconfigured codec etc
                let connector_url = &self.ctx.alias;

                let mut results = Vec::with_capacity(batch_data.len()); // assuming 1:1 mapping
                for (data, meta) in batch_data {
                    let mut events = build_events(
                        connector_url,
                        stream_state,
                        &mut ingest_ns,
                        self.pull_counter,
                        origin_uri.clone(), // TODO: use split_last on batch_data to avoid last clone
                        port.as_ref(),
                        data,
                        &meta.unwrap_or_else(Value::object),
                        self.is_transactional,
                    );
                    results.append(&mut events);
                }
                if results.is_empty() {
                    if let Err(e) = self
                        .source
                        .on_no_events(self.pull_counter, stream, &self.ctx)
                        .await
                    {
                        error!(
                            "[Source::{}] Error on no events callback: {}",
                            &self.ctx.alias, e
                        );
                    }
                } else {
                    let error = self.route_events(results).await;
                    if error {
                        self.ctx.log_err(
                            self.source.fail(stream, self.pull_counter).await,
                            "fail upon error sending events from batched data source reply failed",
                        );
                    }
                }
            }
            SourceReply::Structured {
                origin_uri,
                payload,
                stream,
                port,
            } => {
                let ingest_ns = nanotime();
                let stream_state = self.streams.get_or_create_stream(stream)?;
                let event = build_event(
                    stream_state,
                    self.pull_counter,
                    ingest_ns,
                    payload,
                    origin_uri,
                    self.is_transactional,
                );
                let error = self.route_events(vec![(port.unwrap_or(OUT), event)]).await;
                if error {
                    self.ctx.log_err(
                        self.source.fail(stream, self.pull_counter).await,
                        "fail upon error sending events from structured data source reply failed",
                    );
                }
            }
            SourceReply::StartStream(stream_id) => {
                debug!(
                    "[Source::{}] Starting stream {}",
                    &self.ctx.alias, stream_id
                );
                self.streams.start_stream(stream_id)?; // failing here only due to misconfig, in that case, bail out, #yolo
            }
            SourceReply::EndStream {
                origin_uri,
                meta,
                stream: stream_id,
            } => {
                debug!("[Source::{}] Ending stream {}", &self.ctx.alias, stream_id);
                let mut ingest_ns = nanotime();
                if let Some(mut stream_state) = self.streams.end_stream(stream_id) {
                    let results = build_last_events(
                        &self.ctx.alias,
                        &mut stream_state,
                        &mut ingest_ns,
                        self.pull_counter,
                        origin_uri,
                        None,
                        &meta.unwrap_or_else(Value::object),
                        self.is_transactional,
                    );
                    if results.is_empty() {
                        if let Err(e) = self
                            .source
                            .on_no_events(self.pull_counter, stream_id, &self.ctx)
                            .await
                        {
                            error!(
                                "[Source::{}] Error on no events callback: {}",
                                &self.ctx.alias, e
                            );
                        }
                    } else {
                        let error = self.route_events(results).await;
                        if error {
                            self.ctx.log_err(
                                self.source.fail(stream_id, self.pull_counter).await,
                                "fail upon error sending events from endstream source reply failed",
                            );
                        }
                    }
                }
            }
            SourceReply::Empty(wait_ms) => {
                if self.state == SourceState::Draining {
                    // this source has been fully drained
                    self.state = SourceState::Drained;
                    // send Drain signal
                    debug!("{} Sending DRAIN Signal.", &self.ctx);
                    let signal = Event::signal_drain(self.ctx.uid);
                    if let Err(e) = self.send_signal(signal).await {
                        error!("{} Error sending DRAIN signal: {}", &self.ctx, e);
                    }
                    // if we get a disconnect in between we might never receive every drain CB
                    // but we will stop everything forcefully after a certain timeout at some point anyways
                    // TODO: account for all connector uids that sent some Cb to us upon startup or so, to get the real number to wait for
                    // otherwise there are cases (branching etc. where quiescence also would be too quick)
                    self.expected_drained = self.pipelines_err.len() + self.pipelines_out.len();
                    debug!(
                        "[Source::{}] We are looking to drain {} connections.",
                        self.ctx.alias, self.expected_drained
                    );
                } else {
                    // set the timer for the given ms
                    self.pull_wait_start = Some(Instant::now());
                    self.pull_wait = Duration::from_millis(wait_ms);
                }
            }
        }
        Ok(())
    }

    /// returns `Ok(Control::Terminate)` if this source should be terminated
    #[allow(clippy::too_many_lines)]
    async fn control_plane(&mut self) -> Result<Control> {
        loop {
            if !self.needs_control_plane_msg() {
                return Ok(Control::Continue);
            }
            if let Ok(msg) = self.rx.recv().await {
                if self.handle_control_plane_msg(msg).await? == Control::Terminate {
                    return Ok(Control::Terminate);
                }
            }
        }
    }
    /// the source task
    ///
    /// handling control plane and data plane in a loop
    // TODO: data plane
    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<()> {
        // this one serves as simple counter for our pulls from the source
        // we expect 1 source transport unit (stu) per pull, so this counter is equivalent to a stu counter
        // it is not unique per stream only, but per source
        loop {
            // FIXME: change reply from true/false to something descriptive like enum {Stop Continue} or the rust controlflow thingy
            if self.control_plane().await? == Control::Terminate {
                // source has been stopped, lets stop running here
                debug!("[Source::{}] Terminating source task...", self.ctx.alias);
                return Ok(());
            }

            if self.should_pull_data() {
                let data = self.source.pull_data(self.pull_counter, &self.ctx).await;
                self.handle_data(data).await?;
                self.pull_counter += 1;
            };
            if self.pull_wait_start.is_some() {
                // sleep for a quick 10ms in order to stay responsive
                task::sleep(DEFAULT_POLL_DURATION.min(self.pull_wait)).await;
            }
        }
    }
}

/// source manager functions moved out

/// build any number of `Event`s from a given Source Transport Unit (`data`)
/// preprocessor or codec errors are turned into events to the ERR port of the source/connector
#[allow(clippy::too_many_arguments)]
fn build_events(
    alias: &str,
    stream_state: &mut StreamState,
    ingest_ns: &mut u64,
    pull_id: u64,
    origin_uri: EventOriginUri,
    port: Option<&Cow<'static, str>>,
    data: Vec<u8>,
    meta: &Value<'static>,
    is_transactional: bool,
) -> Vec<(Cow<'static, str>, Event)> {
    match preprocess(
        stream_state.preprocessors.as_mut_slice(),
        ingest_ns,
        data,
        alias,
    ) {
        Ok(processed) => {
            let mut res = Vec::with_capacity(processed.len());
            for chunk in processed {
                let line_value = EventPayload::try_new::<Option<Error>, _>(chunk, |mut_data| {
                    match stream_state.codec.decode(mut_data, *ingest_ns) {
                        Ok(None) => Err(None),
                        Err(e) => Err(Some(e)),
                        Ok(Some(decoded)) => {
                            Ok(ValueAndMeta::from_parts(decoded, meta.clone()))
                            // TODO: avoid clone on last iterator element
                        }
                    }
                });
                let (port, payload) = match line_value {
                    Ok(decoded) => (port.unwrap_or(&OUT).clone(), decoded),
                    Err(None) => continue,
                    Err(Some(e)) => (ERR, make_error(alias, &e, stream_state.stream_id, pull_id)),
                };
                let event = build_event(
                    stream_state,
                    pull_id,
                    *ingest_ns,
                    payload,
                    origin_uri.clone(), // TODO: use split_last to avoid this clone for the last item
                    is_transactional,
                );
                res.push((port, event));
            }
            res
        }
        Err(e) => {
            // preprocessor error
            let err_payload = make_error(alias, &e, stream_state.stream_id, pull_id);
            let event = build_event(
                stream_state,
                pull_id,
                *ingest_ns,
                err_payload,
                origin_uri,
                is_transactional,
            );
            vec![(ERR, event)]
        }
    }
}

/// build any number of `Event`s from a given Source Transport Unit (`data`)
/// preprocessor or codec errors are turned into events to the ERR port of the source/connector
#[allow(clippy::too_many_arguments)]
fn build_last_events(
    alias: &str,
    stream_state: &mut StreamState,
    ingest_ns: &mut u64,
    pull_id: u64,
    origin_uri: EventOriginUri,
    port: Option<&Cow<'static, str>>,
    meta: &Value<'static>,
    is_transactional: bool,
) -> Vec<(Cow<'static, str>, Event)> {
    match finish(stream_state.preprocessors.as_mut_slice(), alias) {
        Ok(processed) => {
            let mut res = Vec::with_capacity(processed.len());
            for chunk in processed {
                let line_value = EventPayload::try_new::<Option<Error>, _>(chunk, |mut_data| {
                    match stream_state.codec.decode(mut_data, *ingest_ns) {
                        Ok(None) => Err(None),
                        Err(e) => Err(Some(e)),
                        Ok(Some(decoded)) => {
                            Ok(ValueAndMeta::from_parts(decoded, meta.clone()))
                            // TODO: avoid clone on last iterator element
                        }
                    }
                });
                let (port, payload) = match line_value {
                    Ok(decoded) => (port.unwrap_or(&OUT).clone(), decoded),
                    Err(None) => continue,
                    Err(Some(e)) => (ERR, make_error(alias, &e, stream_state.stream_id, pull_id)),
                };
                let event = build_event(
                    stream_state,
                    pull_id,
                    *ingest_ns,
                    payload,
                    origin_uri.clone(), // TODO: use split_last to avoid this clone for the last item
                    is_transactional,
                );
                res.push((port, event));
            }
            res
        }
        Err(e) => {
            // preprocessor error
            let err_payload = make_error(alias, &e, stream_state.stream_id, pull_id);
            let event = build_event(
                stream_state,
                pull_id,
                *ingest_ns,
                err_payload,
                origin_uri,
                is_transactional,
            );
            vec![(ERR, event)]
        }
    }
}

/// create an error payload
fn make_error(connector_alias: &str, error: &Error, stream_id: u64, pull_id: u64) -> EventPayload {
    let e_string = error.to_string();
    let data = literal!({
        "error": e_string.clone(),
        "source": connector_alias.to_string(),
        "stream_id": stream_id,
        "pull_id": pull_id
    });
    let meta = literal!({ "error": e_string });
    EventPayload::from(ValueAndMeta::from_parts(data, meta))
}

/// create an event
fn build_event(
    stream_state: &mut StreamState,
    pull_id: u64,
    ingest_ns: u64,
    payload: EventPayload,
    origin_uri: EventOriginUri,
    is_transactional: bool,
) -> Event {
    Event {
        id: stream_state.idgen.next_with_pull_id(pull_id),
        data: payload,
        ingest_ns,
        origin_uri: Some(origin_uri),
        transactional: is_transactional,
        ..Event::default()
    }
}
