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

/// A simple source that is fed with `SourceReply` via a channel.
pub mod channel_source;

/// Impurts all souces need
pub mod prelude;

use crate::{
    channel::{unbounded, UnboundedReceiver},
    metrics::SourceReporter,
    utils::reconnect::ConnectionLostNotifier,
    CodecReq, ConnectorType, Context, Error, QuiescenceBeacon, StreamDone,
};
use std::collections::{btree_map::Entry, BTreeMap, HashSet};
use std::fmt::Display;
use tokio::{sync::mpsc::Sender, task};
use tremor_codec::{self as codec, Codec};
use tremor_common::{
    alias,
    ids::{Id, SinkId, SourceId},
    ports::{Port, ERR, OUT},
    time::nanotime,
};
use tremor_config::NameWithConfig;
use tremor_interceptor::preprocessor::{
    self, finish, make_preprocessors, preprocess, Preprocessors,
};
use tremor_script::{
    ast::{BaseExpr, DeployEndpoint},
    EventOriginUri, EventPayload, ValueAndMeta,
};
use tremor_system::{
    connector::{self, source, Attempt, Connectivity},
    controlplane::{self, CbAction},
    dataplane::{self, InputTarget},
    event::{self, Event, EventId, DEFAULT_STREAM_ID},
    pipeline,
};
use tremor_value::prelude::*;

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
        /// if no stream is provided, this data is treated as a discrete unit,
        /// not part of any stream. preprocessors will be finalized after this etc.
        /// The `event_id` will have the `DEFAULT_STREAM_ID` set as `stream_id`.
        stream: Option<u64>,
        /// Port to send to, defaults to `out`
        port: Option<Port<'static>>,
        /// Overwrite the codec being used for deserializing this data.
        /// Should only be used when setting `stream` to `None`
        codec_overwrite: Option<NameWithConfig>,
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
        port: Option<Port<'static>>,
    },
    /// A stream is closed
    /// This might result in additional events being flushed from
    /// preprocessors, that is why we have `origin_uri` and `meta`
    ///
    /// A stream is automatically started once we receive its first event.
    EndStream {
        /// origin uri
        origin_uri: EventOriginUri,
        /// stream id
        stream: u64,
        /// optional metadata
        meta: Option<Value<'static>>,
    },
    /// Stream Failed, resources related to that stream should be cleaned up
    StreamFail(u64),
    /// This connector will never provide data again
    Finished,
}

/// sender for source reply
pub(crate) type SourceReplySender = Sender<SourceReply>;

/// source part of a connector
#[async_trait::async_trait]
pub trait Source: Send {
    /// Pulls an event from the source if one exists
    /// the `pull_id` identifies the number of the call to `pull_data` and is passed in so
    /// sources can keep track of which event stems from which call of `pull_data` and so can
    /// form a connection between source-specific units and events when receiving `ack`/`fail` notifications.
    ///
    /// `pull_id` can be modified, but users need to beware that it needs to remain unique per event stream. The modified `pull_id`
    /// will be used in the `EventId` and will be passed backl into the `ack`/`fail` methods. This allows sources to encode
    /// information into the `pull_id` to keep track of internal state.
    async fn pull_data(
        &mut self,
        pull_id: &mut u64,
        ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply>;
    /// This callback is called when the data provided from
    /// pull_event did not create any events, this is needed for
    /// linked sources that require a 1:1 mapping between requests
    /// and responses, we're looking at you REST
    async fn on_no_events(
        &mut self,
        _pull_id: u64,
        _stream: u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Pulls custom metrics from the source
    fn metrics(&mut self, _timestamp: u64, _ctx: &SourceContext) -> Vec<EventPayload> {
        vec![]
    }

    ///////////////////////////
    /// lifecycle callbacks ///
    ///////////////////////////

    /// called when the source is started. This happens only once in the whole source lifecycle, before any other callbacks
    async fn on_start(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// Connect to the external thingy.
    /// This function is called definitely after `on_start` has been called.
    ///
    /// This function might be called multiple times, check the `attempt` where you are at.
    /// The intended result of this function is to re-establish a connection. It might reuse a working connection.
    ///
    /// Return `Ok(true)` if the connection could be successfully established.
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        Ok(true)
    }

    /// called when the source is explicitly paused as result of a user/operator interaction
    /// in contrast to `on_cb_trigger` which happens automatically depending on downstream pipeline or sink connector logic.
    async fn on_pause(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// called when the source is explicitly resumed from being paused
    async fn on_resume(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// called when the source is stopped. This happens only once in the whole source lifecycle, as the very last callback
    async fn on_stop(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }

    // circuit breaker callbacks
    /// called when we receive a `close` Circuit breaker event from any connected pipeline
    /// Expected reaction is to pause receiving messages, which is handled automatically by the runtime
    /// Source implementations might want to close connections or signal a pause to the upstream entity it connects to if not done in the connector (the default)
    // TODO: add info of Cb event origin (port, origin_uri)?
    async fn on_cb_trigger(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// Called when we receive a `open` Circuit breaker event from any connected pipeline
    /// This means we can start/continue polling this source for messages
    /// Source implementations might want to start establishing connections if not done in the connector (the default)
    async fn on_cb_restore(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }

    // guaranteed delivery callbacks
    /// an event has been acknowledged and can be considered delivered
    /// multiple acks for the same set of ids are always possible
    async fn ack(
        &mut self,
        _stream_id: u64,
        _pull_id: u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    /// an event has failed along its way and can be considered failed
    /// multiple fails for the same set of ids are always possible
    async fn fail(
        &mut self,
        _stream_id: u64,
        _pull_id: u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    // connectivity stuff
    /// called when connector lost connectivity
    async fn on_connection_lost(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// called when connector re-established connectivity
    async fn on_connection_established(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// Is this source transactional or can acks/fails be ignored
    fn is_transactional(&self) -> bool;

    /// if true events are consumed from an external resource asynchronously
    /// and not directly in the call to `pull_data`, but in another task.
    ///
    /// This distinction is important for the runtime to handle pausing/resuming
    /// and quiescence correctly.
    fn asynchronous(&self) -> bool;
}

/// Stream reader abstraction
#[async_trait::async_trait]
pub trait StreamReader: Send {
    /// reads from the source reader
    async fn read(&mut self, stream: u64) -> anyhow::Result<SourceReply>;

    /// Informs the reader that is should quiesce, can optionally return
    /// a source reply if the quiescence is already reached.
    /// Alternatively the reader HAS TO return a SourceReply::StreamDone, StremFailed
    /// or Finished in the next few messages or be shut down forcefully.
    async fn quiesce(&mut self, stream: u64) -> Option<SourceReply>;

    /// called when the reader is finished or encountered an error
    async fn on_done(&mut self, _stream: u64) -> StreamDone {
        StreamDone::StreamClosed
    }
}

// TODO make fields private and add some nice methods
/// context for a source
#[derive(Clone)]
pub struct SourceContext {
    /// connector uid
    pub uid: SourceId,
    /// connector alias
    pub(crate) alias: alias::Connector,

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
    fn alias(&self) -> &alias::Connector {
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

/// Builder for the `SourceManager`
#[allow(clippy::module_name_repetitions)]
pub struct SourceManagerBuilder {
    streams: Streams,
    source_metrics_reporter: SourceReporter,
}

impl SourceManagerBuilder {
    /// spawn a Manager with the given source implementation
    /// # Errors
    /// if the source can not be spawned into a own process
    pub fn spawn<S>(self, source: S, ctx: SourceContext) -> source::Addr
    where
        S: Source + Send + Sync + 'static,
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

        let (source_tx, source_rx) = unbounded();
        let source_addr = source::Addr::new(source_tx);
        let manager = SourceManager::new(source, ctx, self, source_addr.clone());

        task::spawn(manager.run(source_rx));

        source_addr
    }
}

/// create a builder for a `SourceManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SourceManager`.
///
/// # Errors
/// - on invalid connector configuration
pub(crate) fn builder(
    source_uid: SourceId,
    config: &super::ConnectorConfig,
    connector_default_codec: CodecReq,
    source_metrics_reporter: SourceReporter,
    alias: &alias::Connector,
) -> Result<SourceManagerBuilder, Error> {
    let preprocessor_configs = config.preprocessors.clone().unwrap_or_default();
    let codec_config = match connector_default_codec {
        CodecReq::Structured => {
            if config.codec.is_some() {
                return Err(Error::UnsupportedCodec(alias.clone()));
            }
            tremor_codec::Config::from("null")
        }
        CodecReq::Required => config
            .codec
            .clone()
            .ok_or_else(|| Error::MissingCodec(alias.clone()))?,
        CodecReq::Optional(opt) => config
            .codec
            .clone()
            .unwrap_or_else(|| tremor_codec::Config::from(opt)),
    };
    let streams = Streams::new(source_uid, codec_config, preprocessor_configs);

    Ok(SourceManagerBuilder {
        streams,
        source_metrics_reporter,
    })
}

/// maintaining stream state
// TODO: there is optimization potential here for reusing codec and preprocessors after a stream got ended
struct Streams {
    uid: SourceId,
    codec_config: tremor_codec::Config,
    preprocessor_configs: Vec<preprocessor::Config>,
    states: BTreeMap<u64, StreamState>,
}

impl Streams {
    fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
    /// constructor
    fn new(
        uid: SourceId,
        codec_config: tremor_codec::Config,
        preprocessor_configs: Vec<preprocessor::Config>,
    ) -> Self {
        let states = BTreeMap::new();
        // We used to initialize the default stream here,
        // but this little optimization here might fuck up quiescence
        Self {
            uid,
            codec_config,
            preprocessor_configs,
            states,
        }
    }

    /// end a stream
    fn end_stream(&mut self, stream_id: u64) -> Option<StreamState> {
        self.states.remove(&stream_id)
    }

    /// get or create a stream
    fn get_or_create_stream<C: Context>(
        &mut self,
        stream_id: u64,
        ctx: &C,
    ) -> anyhow::Result<&mut StreamState> {
        Ok(match self.states.entry(stream_id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                debug!("{} starting stream {}", ctx, stream_id);
                let state = Self::build_stream(
                    self.uid,
                    stream_id,
                    &self.codec_config,
                    None,
                    &self.preprocessor_configs,
                )?;
                e.insert(state)
            }
        })
    }

    fn create_anonymous_stream(
        &self,
        codec_overwrite: Option<NameWithConfig>,
    ) -> anyhow::Result<StreamState> {
        Self::build_stream(
            self.uid,
            DEFAULT_STREAM_ID,
            &self.codec_config,
            codec_overwrite,
            &self.preprocessor_configs,
        )
    }

    /// build a stream
    fn build_stream(
        source_uid: SourceId,
        stream_id: u64,
        codec_config: &tremor_codec::Config,
        codec_overwrite: Option<NameWithConfig>,
        preprocessor_configs: &[preprocessor::Config],
    ) -> anyhow::Result<StreamState> {
        let codec = if let Some(codec_overwrite) = codec_overwrite {
            codec::resolve(&codec_overwrite)?
        } else {
            codec::resolve(codec_config)?
        };
        let preprocessors = make_preprocessors(preprocessor_configs)?;
        let idgen = event::IdGenerator::new_with_stream(source_uid, stream_id);
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
    idgen: event::IdGenerator,
    codec: Box<dyn Codec>,
    preprocessors: Preprocessors,
}

/// possible states of a source implementation
#[derive(Debug, PartialEq, Copy, Clone)]
enum SourceState {
    Initialized,
    Running,
    Paused,
    Draining,
    Drained,
    Stopped,
}

impl Display for SourceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
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
    addr: source::Addr,
    pipelines_out: Vec<(DeployEndpoint, pipeline::Addr)>,
    pipelines_err: Vec<(DeployEndpoint, pipeline::Addr)>,
    streams: Streams,
    metrics_reporter: SourceReporter,
    // `Paused` is used for both explicitly pausing and CB close/open
    // this way we can explicitly resume a Cb triggered source if need be
    // but also an explicitly paused source might receive a Cb open and continue sending data :scream:
    state: SourceState,
    connectivity: Connectivity,
    is_transactional: bool,
    is_asynchronous: bool,
    connector_channel: Option<Sender<connector::Msg>>,
    /// Gather all the sinks that reported being started.
    /// This will give us some knowledge on the topology and most importantly
    /// on how many `Drained` messages to wait. Assumes a static topology.
    started_sinks: HashSet<SinkId>,
    num_started_sinks: u64,
    /// is counted up for each call to `pull_data` in order to identify the pull call
    /// an event is originating from. We can only ack or fail pulls.
    pull_counter: u64,
    cb_restore_received: u64,
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
        addr: source::Addr,
    ) -> Self {
        let SourceManagerBuilder {
            streams,
            source_metrics_reporter,
            ..
        } = builder;
        let is_transactional = source.is_transactional();
        let is_asynchronous = source.asynchronous();

        Self {
            source,
            ctx,
            addr,
            streams,
            metrics_reporter: source_metrics_reporter,
            pipelines_out: Vec::with_capacity(1),
            pipelines_err: Vec::with_capacity(1),
            state: SourceState::Initialized,
            connectivity: Connectivity::Disconnected, // we always start as disconnected until `.connect()` connects us
            is_transactional,
            is_asynchronous,
            connector_channel: None,
            started_sinks: HashSet::new(),
            num_started_sinks: 0,
            pull_counter: 0,
            cb_restore_received: 0,
        }
    }

    /// Handle a control plane message
    async fn handle_control_plane_msg(&mut self, msg: source::Msg) -> Result<Control, Error> {
        use SourceState::{Initialized, Paused, Running, Stopped};
        let state = self.state;
        match msg {
            source::Msg::Link { port, tx, pipeline } => self.handle_link(port, tx, pipeline).await,
            source::Msg::Start if self.state == Initialized => {
                info!("{} Starting...", self.ctx);
                self.state = Running;
                self.ctx
                    .swallow_err(self.source.on_start(&self.ctx).await, "on_start failed");
                let res = self.send_signal(Event::signal_start(self.ctx.uid)).await;
                self.ctx.swallow_err(res, "Error sending start signal");
                Ok(Control::Continue)
            }

            source::Msg::Connect(sender, attempt) => {
                info!("{} Connecting...", self.ctx);
                let connect_result = self.source.connect(&self.ctx, &attempt).await;
                self.connectivity = if matches!(connect_result, Ok(true)) {
                    info!("{} Connected.", self.ctx);
                    Connectivity::Connected
                } else {
                    Connectivity::Disconnected
                };
                let res = sender.send(connect_result).await;
                self.ctx
                    .swallow_err(res, "Error sending source connect result");
                Ok(Control::Continue)
            }
            source::Msg::Resume if self.state == Paused => {
                self.state = Running;
                let res = self.source.on_resume(&self.ctx).await;
                self.ctx.swallow_err(res, "on_resume failed");
                Ok(Control::Continue)
            }

            source::Msg::Pause if self.state == Running => {
                // TODO: execute pause strategy chosen by source / connector / configured by user
                info!("{} Paused.", self.ctx);
                self.state = Paused;
                let res = self.source.on_pause(&self.ctx).await;
                self.ctx.swallow_err(res, "on_pause failed");
                Ok(Control::Continue)
            }
            source::Msg::Stop(sender) => {
                info!("{} Stopping...", self.ctx);
                self.state = Stopped;
                let res = sender.send(self.source.on_stop(&self.ctx).await).await;
                self.ctx.swallow_err(res, "Error sending Stop reply");
                Ok(Control::Terminate)
            }
            source::Msg::Drain(drained_sender) => Ok(self.handle_drain(drained_sender).await),
            source::Msg::ConnectionLost => {
                self.connectivity = Connectivity::Disconnected;
                let res = self.source.on_connection_lost(&self.ctx).await;
                self.ctx.swallow_err(res, "on_connection_lost failed");
                Ok(Control::Continue)
            }
            source::Msg::ConnectionEstablished => {
                self.connectivity = Connectivity::Connected;
                let res = self.source.on_connection_established(&self.ctx).await;
                self.ctx
                    .swallow_err(res, "on_connection_established failed");
                Ok(Control::Continue)
            }
            source::Msg::Cb(cb, id) => Ok(self.handle_cb(cb, id).await),
            source::Msg::Synchronize(sender) => {
                self.ctx.swallow_err(
                    sender.send(()).map_err(|()| "send err"),
                    "Error synchronizing with source ",
                );
                Ok(Control::Continue)
            }
            m @ (source::Msg::Start | source::Msg::Resume | source::Msg::Pause) => {
                info!("{} Ignoring {m:?} msg in {state} state", self.ctx);
                Ok(Control::Continue)
            }
        }
    }

    async fn handle_drain(&mut self, drained_sender: Sender<connector::Msg>) -> Control {
        let state = self.state;
        match self.state {
            SourceState::Drained => {
                debug!("{} Source Already drained.", self.ctx);
                self.ctx.swallow_err(
                    drained_sender.send(connector::Msg::SourceDrained).await,
                    "Error sending SourceDrained message",
                );
            }
            SourceState::Draining => {
                info!(
                    "{} Ignoring incoming Drain message in {state} state",
                    self.ctx
                );
            }
            _ => {
                // if we are not connected to any pipeline, we cannot take part in the draining protocol like this
                // we are drained
                info!("{} Draining...", self.ctx);
                if self.started_sinks.is_empty() {
                    info!("{} Source Drained.", self.ctx);
                    debug!("{} (not connected to any pipeline)", self.ctx);
                    let res = drained_sender.send(connector::Msg::SourceDrained).await;
                    self.ctx
                        .swallow_err(res, "sending SourceDrained message failed");
                    // no need to send a DRAIN signal here, as we are not connected to anything
                    self.state = SourceState::Drained;
                } else {
                    self.connector_channel = Some(drained_sender);
                    if !self.is_asynchronous
                        || self.connectivity == Connectivity::Disconnected
                        || self.streams.is_empty()
                    {
                        info!("{} Source Drained.", self.ctx);
                        debug!(
                            "{} (is_asynchronous={}, {:?}, streams_empty={})",
                            self.ctx,
                            self.is_asynchronous,
                            self.connectivity,
                            self.streams.is_empty()
                        );
                        // non-asynchronous sources or disconnected sources are considered immediately drained
                        let res = self.on_fully_drained().await;
                        self.ctx
                            .swallow_err(res, "Error on handling fully-drained state");
                    } else {
                        // At this point the connector has advised all reading from external connections to stop via the `QuiescenceBeacon`
                        // We change the source state to `Draining` and wait for all the streams to finish as we drain out everything that might be in flight
                        // when reached the `Finished` point, we emit the `Drain` signal and wait for the CB answer (one for each connected sink that sent a start message)
                        // TODO: this assumption is not working correctly with e.g. tcp and http
                        self.state = SourceState::Draining;
                    }
                }
            }
        };
        Control::Continue
    }

    async fn handle_cb(&mut self, cb: CbAction, id: EventId) -> Control {
        let ctx = &self.ctx;
        match cb {
            CbAction::Fail => {
                for (stream_id, pull_id) in id.get_min_streams_by_source(self.ctx.uid.id()) {
                    ctx.swallow_err(
                        self.source.fail(stream_id, pull_id, ctx).await,
                        "fail failed",
                    );
                }
                Control::Continue
            }
            CbAction::Ack => {
                for (stream_id, pull_id) in id.get_max_streams_by_source(self.ctx.uid.id()) {
                    ctx.swallow_err(self.source.ack(stream_id, pull_id, ctx).await, "ack failed");
                }
                Control::Continue
            }
            CbAction::Trigger => {
                // TODO: execute pause strategy chosen by source / connector / configured by user
                info!("{ctx} Circuit Breaker: Trigger.");
                let res = self.source.on_cb_trigger(ctx).await;
                ctx.swallow_err(res, "on_cb_trigger failed");
                self.state = SourceState::Paused;
                Control::Continue
            }
            CbAction::Restore => {
                info!("{ctx} Circuit Breaker: Restore.");
                self.cb_restore_received += 1;
                ctx.swallow_err(self.source.on_cb_restore(ctx).await, "on_cb_restore failed");
                // avoid a race condition where the necessary start routine wasnt executed
                // because a `CbAction::Restore` was there first, and thus the `Start` msg was ignored
                if self.state != SourceState::Initialized {
                    self.state = SourceState::Running;
                }
                Control::Continue
            }
            CbAction::SinkStart(uid) => {
                debug!("{ctx} Received SinkStart contraflow message from {uid}");
                self.started_sinks.insert(uid);
                self.num_started_sinks = self.started_sinks.len() as u64;
                Control::Continue
            }
            CbAction::Drained(source_id, sink_id) => {
                debug!(
                    "{ctx} Drained contraflow message from sink {sink_id} for source {source_id}"
                );
                // only account for Drained CF which we caused
                // as CF is sent back the DAG to all destinations
                if source_id == self.ctx.uid {
                    self.started_sinks.remove(&sink_id);
                    debug!("{ctx} Drained message for us from {sink_id}");
                    if self.started_sinks.is_empty() {
                        // we received 1 drain CB event per connected pipeline (hopefully)
                        if let Some(connector_channel) = self.connector_channel.as_ref() {
                            debug!("{ctx} Drain completed, sending data now!");
                            let res = connector_channel.send(connector::Msg::SourceDrained).await;
                            ctx.swallow_err(
                                res,
                                "Error sending SourceDrained message to Connector",
                            );
                        }
                    }
                }
                Control::Continue
            }
            CbAction::None => Control::Continue,
        }
    }

    async fn handle_link(
        &mut self,
        port: Port<'static>,
        tx: Sender<anyhow::Result<()>>,
        pipeline: (DeployEndpoint, pipeline::Addr),
    ) -> Result<Control, Error> {
        let pipes = if port == OUT {
            &mut self.pipelines_out
        } else if port == ERR {
            &mut self.pipelines_err
        } else {
            error!("{} Tried to connect to invalid port: {}", &self.ctx, &port);
            tx.send(Err(anyhow::format_err!("Connecting to invalid port")))
                .await
                .map_err(|_| Error::ControlplaneReply(self.ctx.alias().clone()))?;
            return Ok(Control::Continue);
        };
        // We can not move this to the system flow since we need to know about transactionality
        let (pipeline_url, p) = &pipeline;
        // delegate error reporting to pipeline
        let msg = controlplane::Msg::ConnectInput {
            endpoint: DeployEndpoint::new(&self.ctx.alias, port, pipeline_url.meta()),
            port: Port::from(pipeline_url.port().to_string()),
            tx,
            target: InputTarget::Source(self.addr.clone()),
            is_transactional: self.is_transactional,
        };
        self.ctx.swallow_err(
            p.send_mgmt(msg).await,
            &format!("Failed sending ConnectInput to pipeline {pipeline_url}"),
        );

        pipes.push(pipeline);
        Ok(Control::Continue)
    }

    /// send a signal to all connected pipelines
    async fn send_signal(&self, signal: Event) -> anyhow::Result<()> {
        for (_url, addr) in self
            .pipelines_out
            .as_slice()
            .iter()
            .chain(self.pipelines_err.as_slice().iter())
        /* */
        {
            addr.send(Box::new(dataplane::Msg::Signal(signal.clone())))
                .await?;
        }
        for (_url, addr) in &self.pipelines_out
        /*.chain(self.pipelines_err.iter()) */
        {
            addr.send(Box::new(dataplane::Msg::Signal(signal.clone())))
                .await?;
        }
        Ok(())
    }

    /// send events to pipelines
    async fn route_events(&mut self, events: Vec<(Port<'static>, Event)>) -> bool {
        let mut send_error = false;

        let ctx = &self.ctx;
        for (port, event) in events {
            let pipelines = if port == OUT {
                self.metrics_reporter.increment_out();
                &mut self.pipelines_out
            } else if port == ERR {
                self.metrics_reporter.increment_err();
                &mut self.pipelines_err
            } else {
                error!("{ctx} Trying to send event to invalid port: {port}");
                continue;
            };

            // flush metrics reporter or similar
            if let Some(t) = self.metrics_reporter.periodic_flush(event.ingest_ns) {
                self.metrics_reporter
                    .send_source_metrics(self.source.metrics(t, ctx));
            }

            if let Some((last, pipelines)) = pipelines.split_last_mut() {
                for (pipe_url, addr) in pipelines {
                    let input = pipe_url.port().to_string().into();
                    let msg = Box::new(dataplane::Msg::Event {
                        input,
                        event: event.clone(),
                    });
                    if let Err(e) = addr.send(msg).await {
                        error!(
                            "{ctx} Failed to send {} to pipeline {pipe_url}: {e}",
                            event.id,
                        );
                        send_error = true;
                    }
                }
                let input = last.0.port().to_string().into();
                let msg = Box::new(dataplane::Msg::Event { input, event });
                if let Err(e) = last.1.send(msg).await {
                    error!("{ctx} Failed to send event to pipeline {}: {e}", &last.0);
                    send_error = true;
                }
            }
            // transactional events going nowhere are neither acked nor failed
        }
        send_error
    }

    /// should this manager pull data from its source?
    fn should_pull_data(&mut self) -> bool {
        // asynchronous sources need to be drained from their asynchronous task which consumes from
        // the external resource, we pull data from it until we receive a `SourceReply::Empty`.
        // synchronous sources (polling the external resource directly in `Source::pull_data`) should not be called anymore
        // when being drained, they can be considered flushed in that case. There is no more lingering data.
        let state_should_pull = self.state == SourceState::Running
            || (self.state == SourceState::Draining && self.is_asynchronous);

        // combine all the conditions
        state_should_pull
            && !self.pipelines_out.is_empty() // we have pipelines connected
            && self.connectivity == Connectivity::Connected // we are connected to our thingy
            && self.cb_restore_received > 0                       // we did receive at least 1 `CbAction::Restore` in order to ensure we do not accidentally send events 
                                                                  // before we received any CbAction::SinkStart and the corresponding CbAction::Restore
            && self.cb_restore_received >= self.num_started_sinks // we did receive a `CbAction::Restore` from all connected sinks
                                                                  // so we know the downstream side is ready to receive something
    }

    /// handle data from the source
    async fn handle_source_reply(
        &mut self,
        data: anyhow::Result<SourceReply>,
        pull_id: u64,
    ) -> anyhow::Result<()> {
        let data = match data {
            Ok(d) => d,
            Err(e) => {
                error!("{} Error pulling data: {}", &self.ctx, e);
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
                codec_overwrite,
            } => {
                self.handle_data(
                    stream,
                    pull_id,
                    origin_uri,
                    port,
                    data,
                    meta,
                    codec_overwrite,
                )
                .await?;
            }
            SourceReply::Structured {
                origin_uri,
                payload,
                stream,
                port,
            } => {
                self.handle_structured(stream, pull_id, payload, port, origin_uri)
                    .await?;
            }
            SourceReply::EndStream {
                origin_uri,
                meta,
                stream: stream_id,
            } => {
                self.handle_end_stream(stream_id, pull_id, origin_uri, meta)
                    .await;
                if self.state == SourceState::Draining && self.streams.is_empty() {
                    self.on_fully_drained().await?;
                }
            }
            SourceReply::StreamFail(stream_id) => {
                // clean out stream state
                self.streams.end_stream(stream_id);
                if self.state == SourceState::Draining && self.streams.is_empty() {
                    self.on_fully_drained().await?;
                }
            }
            SourceReply::Finished => {
                info!("{} Finished", self.ctx);
                self.on_fully_drained().await?;
            }
        }
        Ok(())
    }

    async fn handle_end_stream(
        &mut self,
        stream_id: u64,
        pull_id: u64,
        origin_uri: EventOriginUri,
        meta: Option<Value<'static>>,
    ) {
        debug!("{} Ending stream {stream_id}", self.ctx);
        let mut ingest_ns = nanotime();
        if let Some(mut stream_state) = self.streams.end_stream(stream_id) {
            let results = build_last_events(
                &self.ctx.alias,
                &mut stream_state,
                &mut ingest_ns,
                pull_id,
                &origin_uri,
                None,
                meta.unwrap_or_else(Value::object),
                self.is_transactional,
            )
            .await;
            if results.is_empty() {
                let res = self
                    .source
                    .on_no_events(pull_id, stream_id, &self.ctx)
                    .await;
                self.ctx.swallow_err(res, "Error on no events callback");
            } else {
                let error = self.route_events(results).await;
                if error {
                    self.ctx.swallow_err(
                        self.source.fail(stream_id, pull_id, &self.ctx).await,
                        "fail upon error sending events from endstream source reply failed",
                    );
                }
            }
        }
    }

    async fn handle_structured(
        &mut self,
        stream: u64,
        pull_id: u64,
        payload: EventPayload,
        port: Option<Port<'static>>,
        origin_uri: EventOriginUri,
    ) -> anyhow::Result<()> {
        let ingest_ns = nanotime();
        let stream_state = self.streams.get_or_create_stream(stream, &self.ctx)?;
        let event = build_event(
            stream_state,
            pull_id,
            ingest_ns,
            payload,
            origin_uri,
            self.is_transactional,
        );
        let error = self.route_events(vec![(port.unwrap_or(OUT), event)]).await;
        if error {
            self.ctx.swallow_err(
                self.source.fail(stream, pull_id, &self.ctx).await,
                "fail upon error sending events from structured data source reply failed",
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_data(
        &mut self,
        stream: Option<u64>,
        pull_id: u64,
        origin_uri: EventOriginUri,
        port: Option<Port<'static>>,
        data: Vec<u8>,
        meta: Option<Value<'static>>,
        codec_overwrite: Option<NameWithConfig>,
    ) -> anyhow::Result<()> {
        let mut ingest_ns = nanotime();
        if let Some(stream) = stream {
            let stream_state = self.streams.get_or_create_stream(stream, &self.ctx)?;
            let results = build_events(
                &self.ctx.alias,
                stream_state,
                &mut ingest_ns,
                pull_id,
                &origin_uri,
                port.as_ref(),
                data,
                meta.unwrap_or_else(Value::object),
                self.is_transactional,
            )
            .await;
            if results.is_empty() {
                let expr = self.source.on_no_events(pull_id, stream, &self.ctx).await;
                self.ctx.swallow_err(expr, "Error on no events callback");
            } else {
                let error = self.route_events(results).await;
                if error {
                    self.ctx.swallow_err(
                        self.source.fail(stream, pull_id, &self.ctx).await,
                        "fail upon error sending events from data source reply failed",
                    );
                }
            }
        } else {
            // no stream
            let mut stream_state = self.streams.create_anonymous_stream(codec_overwrite)?;
            let meta = meta.unwrap_or_else(Value::object);
            let mut results = build_events(
                &self.ctx.alias,
                &mut stream_state,
                &mut ingest_ns,
                pull_id,
                &origin_uri,
                port.as_ref(),
                data,
                meta.clone(),
                self.is_transactional,
            )
            .await;
            // finish up the stream immediately
            let mut last_events = build_last_events(
                &self.ctx.alias,
                &mut stream_state,
                &mut ingest_ns,
                pull_id,
                &origin_uri,
                None,
                meta,
                self.is_transactional,
            )
            .await;
            results.append(&mut last_events);

            if results.is_empty() {
                let res = self
                    .source
                    .on_no_events(pull_id, DEFAULT_STREAM_ID, &self.ctx)
                    .await;
                self.ctx.swallow_err(res, "Error on no events callback");
            } else {
                let error = self.route_events(results).await;
                if error {
                    self.ctx.swallow_err(
                        self.source
                            .fail(DEFAULT_STREAM_ID, pull_id, &self.ctx)
                            .await,
                        "fail upon error sending events from data source reply failed",
                    );
                }
            }
        }
        Ok(())
    }

    async fn on_fully_drained(&mut self) -> anyhow::Result<()> {
        // TODO: we actually need to end all streams and flush preprocessors, they might buffer some data
        //       The only problem is that we don't have the data around (meta, origin_uri).
        //       This would be fixed if such metadata would be solely bound to the stream, not to the message
        //       Right now we are losing the rest in the buffers.

        // this source has been fully drained
        self.state = SourceState::Drained;
        // send Drain signal
        debug!(
            "{} Sending DRAIN Signal (from {}).",
            &self.ctx, self.ctx.uid
        );
        let signal = Event::signal_drain(self.ctx.uid);
        let res = self.send_signal(signal).await;
        self.ctx.swallow_err(res, "Error sending DRAIN signal");

        // if we get a disconnect in between we might never receive every drain CB
        // but we will stop everything forcefully after a certain timeout at some point anyways
        // TODO: account for all connector uids that sent some Cb to us upon startup or so, to get the real number to wait for
        // otherwise there are cases (branching etc. where quiescence also would be too quick)
        debug!(
            "{} We are waiting for {} Drained contraflow messages.",
            &self.ctx,
            self.started_sinks.len()
        );
        Ok(())
    }

    /// the source task
    ///
    /// handling control plane and data plane in a loop
    // TODO: data plane
    #[allow(clippy::too_many_lines)]
    async fn run(mut self, mut rx: UnboundedReceiver<source::Msg>) -> anyhow::Result<()> {
        loop {
            use futures::future::Either;

            while !self.should_pull_data() {
                match rx.recv().await {
                    Some(msg) => {
                        if self.handle_control_plane_msg(msg).await? == Control::Terminate {
                            debug!("{} Terminating source task...", self.ctx);
                            return Ok(());
                        }
                    }
                    None => {
                        debug!("{} Control Plane channel closed", self.ctx);
                    }
                }
            }

            let mut pull_id = self.pull_counter;
            let r = {
                // TODO: we could specialize for sources that just use a queue for pull_data
                //       and handle it the same as rx
                let src_future = self.source.pull_data(&mut pull_id, &self.ctx);
                match futures::future::select(Box::pin(rx.recv()), src_future).await {
                    Either::Left((msg, _o)) => Either::Left(
                        msg.ok_or_else(|| Error::ChannelEmpty(self.ctx.alias.clone()))?,
                    ),
                    Either::Right((data, _o)) => Either::Right(data),
                }
            };

            let ctrl = match r {
                Either::Left(msg) => self.handle_control_plane_msg(msg).await?,
                Either::Right(data) => {
                    if let Err(e) = self.handle_source_reply(data, pull_id).await {
                        error!("{} Error handling source reply: {}", self.ctx, e);
                    }
                    self.pull_counter += 1;
                    Control::Continue
                }
            };

            if ctrl == Control::Terminate {
                debug!("{} Terminating source task...", self.ctx);
                return Ok(());
            };
        }
    }
}

/// source manager functions moved out

/// build any number of `Event`s from a given Source Transport Unit (`data`)
/// preprocessor or codec errors are turned into events to the ERR port of the source/connector
#[allow(clippy::too_many_arguments)]
async fn build_events(
    alias: &alias::Connector,
    stream_state: &mut StreamState,
    ingest_ns: &mut u64,
    pull_id: u64,
    origin_uri: &EventOriginUri,
    port: Option<&Port<'static>>,
    data: Vec<u8>,
    meta: Value<'static>,
    is_transactional: bool,
) -> Vec<(Port<'static>, Event)> {
    match preprocess(
        stream_state.preprocessors.as_mut_slice(),
        ingest_ns,
        data,
        meta.clone(),
        alias,
    ) {
        Ok(processed) => {
            let mut res = Vec::with_capacity(processed.len());

            for (chunk, meta) in processed {
                let line_value = EventPayload::from_codec(
                    chunk,
                    meta.clone(),
                    ingest_ns,
                    &mut stream_state.codec,
                )
                .await;
                let (port, payload) = match line_value {
                    Ok(Some(decoded)) => (port.unwrap_or(&OUT).clone(), decoded),
                    Ok(None) => continue,
                    Err(e) => (
                        ERR,
                        make_error(alias, &e.into(), stream_state.stream_id, pull_id, meta),
                    ),
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
            let err_payload = make_error(alias, &e, stream_state.stream_id, pull_id, meta);
            let event = build_event(
                stream_state,
                pull_id,
                *ingest_ns,
                err_payload,
                origin_uri.clone(),
                is_transactional,
            );
            vec![(ERR, event)]
        }
    }
}

/// build any number of `Event`s from a given Source Transport Unit (`data`)
/// preprocessor or codec errors are turned into events to the ERR port of the source/connector
#[allow(clippy::too_many_arguments)]
async fn build_last_events(
    alias: &alias::Connector,
    stream_state: &mut StreamState,
    ingest_ns: &mut u64,
    pull_id: u64,
    origin_uri: &EventOriginUri,
    port: Option<&Port<'static>>,
    meta: Value<'static>,
    is_transactional: bool,
) -> Vec<(Port<'static>, Event)> {
    match finish(stream_state.preprocessors.as_mut_slice(), alias) {
        Ok(processed) => {
            let mut res = Vec::with_capacity(processed.len());
            for (chunk, meta) in processed {
                let line_value = EventPayload::from_codec(
                    chunk,
                    meta.clone(),
                    ingest_ns,
                    &mut stream_state.codec,
                )
                .await;
                let (port, payload) = match line_value {
                    Ok(Some(decoded)) => (port.unwrap_or(&OUT).clone(), decoded),
                    Ok(None) => continue,
                    Err(e) => (
                        ERR,
                        make_error(alias, &e.into(), stream_state.stream_id, pull_id, meta),
                    ),
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
            let err_payload = make_error(alias, &e, stream_state.stream_id, pull_id, meta);
            let event = build_event(
                stream_state,
                pull_id,
                *ingest_ns,
                err_payload,
                origin_uri.clone(),
                is_transactional,
            );
            vec![(ERR, event)]
        }
    }
}

/// create an error payload
fn make_error(
    connector_alias: &alias::Connector,
    error: &anyhow::Error,
    stream_id: u64,
    pull_id: u64,
    mut meta: Value<'static>,
) -> EventPayload {
    let e_string = error.to_string();
    let data = literal!({
        "error": e_string.clone(),
        "source": connector_alias.to_string(),
        "stream_id": stream_id,
        "pull_id": pull_id
    });
    meta.try_insert("error", e_string);
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
