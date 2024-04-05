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

/// Providing a `Sink` implementation for connectors handling multiple Streams
pub mod channel_sink;
/// Utility for limiting concurrency (by sending `CB::Close` messages when a maximum concurrency value is reached)
pub mod concurrency_cap;

use super::{metrics::SinkReporter, ConnectionLostNotifier, Msg, QuiescenceBeacon};
use crate::{
    channel::{unbounded, UnboundedReceiver, UnboundedSender},
    config::Connector as ConnectorConfig,
    pipeline,
    prelude::*,
};
use futures::StreamExt; // for .next() on PriorityMerge
use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap, HashSet},
    fmt::Display,
    sync::Arc,
};
use tokio::task;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tremor_codec::{self as codec, Codec};
use tremor_common::{
    ids::{SinkId, SourceId},
    primerge::PriorityMerge,
    time::nanotime,
};
use tremor_interceptor::postprocessor::{
    self, finish, make_postprocessors, postprocess, Postprocessors,
};
use tremor_script::ast::DeployEndpoint;
use tremor_system::{connector::sink, dataplane::SignalKind};

pub(crate) type ReplySender = UnboundedSender<AsyncSinkReply>;

/// Result for a sink function that may provide insights or response.
///
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
#[derive(Clone, Debug, Default, Copy, PartialEq)]
pub struct SinkReply {
    /// guaranteed delivery response - did we sent the event successfully `SinkAck::Ack` or did it fail `SinkAck::Fail`
    pub(crate) ack: SinkAck,
    /// circuit breaker action
    pub(crate) cb: CbAction,
}

impl SinkReply {
    /// Acknowledges
    pub const ACK: SinkReply = SinkReply {
        ack: SinkAck::Ack,
        cb: CbAction::None,
    };
    /// Fails
    pub const FAIL: SinkReply = SinkReply {
        ack: SinkAck::Fail,
        cb: CbAction::None,
    };
    /// None
    pub const NONE: SinkReply = SinkReply {
        ack: SinkAck::None,
        cb: CbAction::None,
    };

    /// Decide according to the given flag if we return a fail or a none
    #[must_use]
    pub fn fail_or_none(needs_fail: bool) -> Self {
        if needs_fail {
            Self::FAIL
        } else {
            Self::NONE
        }
    }

    /// Decide according to the given flag if we return a ack or a none
    #[must_use]
    pub fn ack_or_none(needs_ack: bool) -> Self {
        if needs_ack {
            Self::ACK
        } else {
            Self::NONE
        }
    }
}

/// stuff a sink replies back upon an event or a signal
/// to the calling sink/connector manager
#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) enum SinkAck {
    /// no reply - maybe no reply yet, maybe replies come asynchronously...
    None,
    /// everything went smoothly, chill
    Ack,
    /// shit hit the fan, but only for this event, nothing big
    Fail,
}

impl Default for SinkAck {
    fn default() -> Self {
        Self::None
    }
}

/// Possible replies from asynchronous sinks via `reply_channel` from event or signal handling
#[derive(Debug)]
pub enum AsyncSinkReply {
    /// success
    Ack(ContraflowData, u64),
    /// failure
    Fail(ContraflowData),
    /// circuitbreaker shit
    /// TODO: do we actually need ContraflowData here?
    CB(ContraflowData, CbAction),
}

/// connector sink - receiving events
#[async_trait::async_trait]
pub trait Sink: Send {
    /// called when receiving an event
    async fn on_event(
        &mut self,
        input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> anyhow::Result<SinkReply>;
    /// called when receiving a signal
    async fn on_signal(
        &mut self,
        _signal: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
    ) -> anyhow::Result<SinkReply> {
        Ok(SinkReply::default())
    }

    /// Pull metrics from the sink
    ///
    /// The expected format is:
    ///
    /// ```js
    /// {
    ///     "measurement": <name>,
    ///     "tags": {
    ///         "connector": <connector-url>,
    ///         ...
    ///     },
    ///     "fields": {
    ///         "name": <measurement-value>,
    ///         ...
    ///     },
    ///     "timestamp": <timestamp in ns>
    /// }
    /// ```
    ///
    async fn metrics(&mut self, _timestamp: u64, _ctx: &SinkContext) -> Vec<EventPayload> {
        vec![]
    }

    // lifecycle stuff
    /// called when started
    async fn on_start(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// Connect to the external thingy.
    /// This function is called definitely after `on_start` has been called.
    ///
    /// This function might be called multiple times, check the `attempt` where you are at.
    /// The intended result of this function is to re-establish a connection. It might reuse a working connection.
    ///
    /// Return `Ok(true)` if the connection could be successfully established.
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        Ok(true)
    }

    /// called when paused
    async fn on_pause(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// called when resumed
    async fn on_resume(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// called when stopped
    async fn on_stop(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        Ok(())
    }

    // connectivity stuff
    /// called when sink lost connectivity
    async fn on_connection_lost(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        Ok(())
    }
    /// called when sink re-established connectivity
    async fn on_connection_established(&mut self, _ctx: &SinkContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// if `true` events are acknowledged/failed automatically by the sink manager.
    /// Such sinks should return SinkReply::None from on_event or SinkReply::Fail if they fail immediately.
    ///
    /// if `false` events need to be acked/failed manually by the sink impl
    fn auto_ack(&self) -> bool;

    /// if true events are sent asynchronously, not necessarily when `on_event` returns.
    /// if false events can be considered delivered once `on_event` returns.
    fn asynchronous(&self) -> bool {
        false
    }
}

/// handles writing to 1 stream (e.g. file or TCP connection)
#[async_trait::async_trait]
pub trait StreamWriter: Send + Sync {
    /// write the given data out to the stream
    async fn write(&mut self, data: Vec<Vec<u8>>, meta: Option<&Value>) -> anyhow::Result<()>;
    /// handle the stream being done, by error or regular end of stream
    /// This controls the reaction of the runtime:
    /// Should the connector be considered disconnected now? Or is this just one stream amongst many?
    async fn on_done(&mut self, _stream: u64) -> anyhow::Result<StreamDone> {
        Ok(StreamDone::StreamClosed)
    }
}

#[async_trait::async_trait]
pub(crate) trait SinkRuntime: Send + Sync {
    async fn unregister_stream_writer(&mut self, stream: u64) -> anyhow::Result<()>;
}
/// context for the connector sink
#[derive(Clone)]
pub(crate) struct SinkContextInner {
    /// the connector unique identifier
    pub(crate) uid: SinkId,
    /// the connector alias
    pub(crate) alias: alias::Connector,
    /// the connector type
    pub(crate) connector_type: ConnectorType,

    /// check if we are paused or should stop reading/writing
    pub(crate) quiescence_beacon: QuiescenceBeacon,

    /// notifier the connector runtime if we lost a connection
    pub(crate) notifier: ConnectionLostNotifier,
}

/// context for the connector sink
#[derive(Clone)]
pub struct SinkContext(Arc<SinkContextInner>);
impl SinkContext {
    /// get the unique identifier for the sink
    #[must_use]
    pub fn uid(&self) -> SinkId {
        self.0.uid
    }
    /// get the connection lost notifier
    #[must_use]
    pub fn notifier(&self) -> &ConnectionLostNotifier {
        &self.0.notifier
    }
    pub(crate) fn new(
        uid: SinkId,
        alias: alias::Connector,
        connector_type: ConnectorType,
        quiescence_beacon: QuiescenceBeacon,
        notifier: ConnectionLostNotifier,
    ) -> SinkContext {
        Self(Arc::new(SinkContextInner {
            uid,
            alias,
            connector_type,
            quiescence_beacon,
            notifier,
        }))
    }
}

impl Display for SinkContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Sink::{}]", &self.0.alias)
    }
}

impl Context for SinkContext {
    // fn uid(&self) -> &SinkId {
    //     &self.0.uid
    // }
    fn alias(&self) -> &alias::Connector {
        &self.0.alias
    }

    fn quiescence_beacon(&self) -> &QuiescenceBeacon {
        &self.0.quiescence_beacon
    }

    fn notifier(&self) -> &ConnectionLostNotifier {
        &self.0.notifier
    }

    fn connector_type(&self) -> &ConnectorType {
        &self.0.connector_type
    }
}

/// Wrapper around all possible sink messages
/// handled in the Sink task
#[allow(clippy::large_enum_variant)] // TODO: should we box SinkMsg here?
#[derive(Debug)]
enum SinkMsgWrapper {
    FromSink(AsyncSinkReply),
    ToSink(sink::Msg),
}

/// Builder for the sink manager
pub struct SinkManagerBuilder {
    serializer: EventSerializer,
    reply_tx: ReplySender,
    reply_rx: UnboundedReceiver<AsyncSinkReply>,
    metrics_reporter: SinkReporter,
}

impl SinkManagerBuilder {
    /// Get yourself a sender to send replies back from your concrete sink.
    ///
    /// This is especially useful if your sink handles events asynchronously
    /// and you can't reply immediately.
    #[must_use]
    pub fn reply_tx(&self) -> ReplySender {
        self.reply_tx.clone()
    }

    /// spawn your specific sink
    pub fn spawn<S>(self, sink: S, ctx: SinkContext) -> sink::Addr
    where
        S: Sink + Send + 'static,
    {
        let (sink_tx, sink_rx) = bounded(qsize());
        let manager = SinkManager::new(sink, ctx, self, sink_rx);
        task::spawn(manager.run());

        sink::Addr::new(sink_tx)
    }
}

/// create a builder for a `SinkManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SinkManager`.
pub(crate) fn builder(
    config: &ConnectorConfig,
    connector_codec_requirement: CodecReq,
    alias: &alias::Connector,

    metrics_reporter: SinkReporter,
) -> anyhow::Result<SinkManagerBuilder> {
    // resolve codec and processors
    let postprocessor_configs = config.postprocessors.clone().unwrap_or_default();
    let serializer = EventSerializer::new(
        config.codec.clone(),
        connector_codec_requirement,
        postprocessor_configs,
        &config.connector_type,
        alias,
    )?;
    // the incoming channels for events are all bounded, so we can safely be unbounded here
    // TODO: actually we could have lots of CB events not bound to events here
    let (reply_tx, reply_rx) = unbounded();
    Ok(SinkManagerBuilder {
        serializer,
        reply_tx,
        reply_rx,
        metrics_reporter,
    })
}

/// Helper for serializing events within sinks
///
/// Keeps track of codec/postprocessors for seach stream
/// Attention: Take care to clear out data for streams that are not used
pub struct EventSerializer {
    alias: String,
    // default stream handling
    pub(crate) codec: Box<dyn Codec>,
    postprocessors: Postprocessors,
    // creation templates for stream handling
    codec_config: tremor_codec::Config,
    postprocessor_configs: Vec<postprocessor::Config>,
    // stream data
    // TODO: clear out state from codec, postprocessors and enable reuse
    streams: BTreeMap<u64, (Box<dyn Codec>, Postprocessors)>,
}

impl EventSerializer {
    /// create a new event serializer with the given codec and postprocessors
    /// # Errors
    ///  * if codec resolution fails
    ///  * if postprocessor resolution fails
    pub fn new(
        codec_config: Option<tremor_codec::Config>,
        default_codec: CodecReq,
        postprocessor_configs: Vec<postprocessor::Config>,
        _connector_type: &ConnectorType,
        alias: &alias::Connector,
    ) -> anyhow::Result<Self> {
        let codec_config = match default_codec {
            CodecReq::Structured => {
                if codec_config.is_some() {
                    return Err(Error::UnsupportedCodec(alias.clone()).into());
                }
                tremor_codec::Config::from("null")
            }
            CodecReq::Required => codec_config.ok_or_else(|| Error::MissingCodec(alias.clone()))?,
            CodecReq::Optional(opt) => {
                codec_config.unwrap_or_else(|| tremor_codec::Config::from(opt))
            }
        };

        let codec = codec::resolve(&codec_config)?;
        let postprocessors = make_postprocessors(postprocessor_configs.as_slice())?;
        Ok(Self {
            alias: alias.to_string(),
            codec,
            postprocessors,
            codec_config,
            postprocessor_configs,
            streams: BTreeMap::new(),
        })
    }

    /// drop a stream
    pub fn drop_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    /// clear out all streams - this can lead to data loss
    /// only use when you are sure, all the streams are gone
    pub fn clear(&mut self) {
        self.streams.clear();
    }

    /// serialize event for the default stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    pub async fn serialize<'v>(
        &mut self,
        value: &Value<'v>,
        meta: &Value<'v>,
        ingest_ns: u64,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        self.serialize_for_stream(value, meta, ingest_ns, DEFAULT_STREAM_ID)
            .await
    }

    /// serialize event for a certain stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    pub async fn serialize_for_stream<'v>(
        &mut self,
        value: &Value<'v>,
        meta: &Value<'v>,

        ingest_ns: u64,
        stream_id: u64,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        self.serialize_for_stream_with_codec(value, meta, ingest_ns, stream_id, None)
            .await
    }

    /// Serialize an event for a certain stream with the possibility to overwrite the configured codec.
    ///
    /// The `codec_overwrite` is only considered when creating new streams, not for existing streams.
    /// It is also ignored when for the `DEFAULT_STREAM_ID`, which is `0`. Beware!
    ///
    /// # Errors
    ///   * if serialization fails (codec or postprocessors)
    pub async fn serialize_for_stream_with_codec<'v>(
        &mut self,
        value: &Value<'v>,
        meta: &Value<'v>,
        ingest_ns: u64,
        stream_id: u64,
        codec_overwrite: Option<&NameWithConfig>,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        if stream_id == DEFAULT_STREAM_ID {
            // no codec_overwrite for the default stream
            Ok(postprocess(
                &mut self.postprocessors,
                ingest_ns,
                self.codec.encode(value, meta).await?,
                &self.alias,
            )?)
        } else {
            match self.streams.entry(stream_id) {
                Entry::Occupied(mut entry) => {
                    let (codec, pps) = entry.get_mut();
                    Ok(postprocess(
                        pps,
                        ingest_ns,
                        codec.encode(value, meta).await?,
                        &self.alias,
                    )?)
                }
                Entry::Vacant(entry) => {
                    // codec overwrite only considered for new streams
                    let codec = match codec_overwrite {
                        Some(codec) => codec::resolve(codec),
                        None => codec::resolve(&self.codec_config),
                    }?;
                    let pps = make_postprocessors(self.postprocessor_configs.as_slice())?;
                    // insert data for a new stream
                    let (c, pps2) = entry.insert((codec, pps));
                    Ok(postprocess(
                        pps2,
                        ingest_ns,
                        c.encode(value, meta).await?,
                        &self.alias,
                    )?)
                }
            }
        }
    }

    /// remove and flush out any pending data from the stream identified by the given `stream_id`
    /// # Errors
    ///  * if serialization failed (codec or postprocessors)
    pub fn finish_stream(&mut self, stream_id: u64) -> anyhow::Result<Vec<Vec<u8>>> {
        if let Some((mut _codec, mut postprocessors)) = self.streams.remove(&stream_id) {
            Ok(finish(&mut postprocessors, &self.alias)?)
        } else {
            Ok(vec![])
        }
    }
}

#[derive(Debug, PartialEq)]
enum SinkState {
    Initialized,
    Running,
    Paused,
    Draining,
    Drained,
    Stopped,
}

impl std::fmt::Display for SinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub(crate) struct SinkManager<S>
where
    S: Sink,
{
    sink: S,
    ctx: SinkContext,
    rx: ReceiverStream<sink::Msg>,
    reply_rx: UnboundedReceiverStream<AsyncSinkReply>,
    serializer: EventSerializer,
    metrics_reporter: SinkReporter,
    /// tracking which operators incoming events visited
    merged_operator_meta: OpMeta,
    // pipelines connected to IN port
    pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
    // set of source ids we received start signals from
    starts_received: HashSet<SourceId>,
    // set of connector ids we received drain signals from
    drains_received: HashSet<SourceId>, // TODO: use a bitset for both?
    drain_channel: Option<Sender<Msg>>,
    state: SinkState,
}

impl<S> SinkManager<S>
where
    S: Sink,
{
    fn new(
        sink: S,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
        rx: Receiver<sink::Msg>,
    ) -> Self {
        let SinkManagerBuilder {
            serializer,
            reply_rx,
            metrics_reporter,
            ..
        } = builder;
        Self {
            sink,
            ctx,
            rx: ReceiverStream::new(rx),
            reply_rx: UnboundedReceiverStream::new(reply_rx),
            serializer,
            metrics_reporter,
            merged_operator_meta: OpMeta::default(),
            pipelines: Vec::with_capacity(1), // by default 1 connected to "in" port
            starts_received: HashSet::new(),
            drains_received: HashSet::new(),
            drain_channel: None,
            state: SinkState::Initialized,
        }
    }
    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<(), Error> {
        use SinkState::{Drained, Draining, Initialized, Paused, Running, Stopped};
        let from_sink = self.reply_rx.map(SinkMsgWrapper::FromSink);
        let to_sink = self.rx.map(SinkMsgWrapper::ToSink);
        let mut from_and_to_sink_channel = PriorityMerge::new(to_sink, from_sink);
        while let Some(msg_wrapper) = from_and_to_sink_channel.next().await {
            match msg_wrapper {
                SinkMsgWrapper::ToSink(sink_msg) => {
                    match sink_msg {
                        sink::Msg::Link { mut pipelines } => {
                            self.pipelines.append(&mut pipelines);
                        }
                        sink::Msg::Start if self.state == Initialized => {
                            self.state = Running;
                            self.ctx.swallow_err(
                                self.sink.on_start(&self.ctx).await,
                                "Error during on_start",
                            );
                            let cf = Event {
                                ingest_ns: nanotime(),
                                cb: CbAction::SinkStart(self.ctx.uid()),
                                ..Event::default()
                            };
                            // send CB start to all pipes
                            send_contraflow(&self.pipelines, &self.ctx, cf);
                        }
                        sink::Msg::Connect(sender, attempt) => {
                            info!("{} Connecting...", &self.ctx);
                            let connect_result = self.sink.connect(&self.ctx, &attempt).await;
                            if let Ok(true) = connect_result {
                                info!("{} Sink connected.", &self.ctx);
                            }
                            self.ctx.swallow_err(
                                sender.send(connect_result).await,
                                "Error sending sink connect result",
                            );
                        }
                        sink::Msg::Resume if self.state == Paused => {
                            self.state = Running;
                            self.ctx.swallow_err(
                                self.sink.on_resume(&self.ctx).await,
                                "Error during on_resume",
                            );
                        }
                        sink::Msg::Pause if self.state == Running => {
                            self.state = Paused;
                            self.ctx.swallow_err(
                                self.sink.on_pause(&self.ctx).await,
                                "Error during on_pause",
                            );
                        }
                        sink::Msg::Stop(sender) => {
                            info!("{} Stopping...", &self.ctx);
                            self.state = Stopped;
                            self.ctx.swallow_err(
                                sender.send(self.sink.on_stop(&self.ctx).await).await,
                                "Error sending Stop reply",
                            );
                            // exit control plane
                            break;
                        }
                        sink::Msg::Drain(_sender) if self.state == Draining => {
                            info!(
                                "{} Ignoring Drain message in {} state",
                                self.ctx, self.state
                            );
                        }

                        sink::Msg::Drain(sender) if self.state == Drained => {
                            debug!("{} Sink already Drained.", self.ctx);
                            self.ctx.swallow_err(
                                sender.send(Msg::SinkDrained).await,
                                "Error sending SinkDrained message",
                            );
                        }
                        sink::Msg::Drain(sender) => {
                            // send message back if we already received Drain signal from all input pipelines
                            debug!("{} Draining...", self.ctx);
                            self.state = Draining;
                            self.drain_channel = Some(sender);
                            if self.drains_received.is_superset(&self.starts_received) {
                                // we are all drained
                                debug!("{} Sink Drained.", self.ctx);
                                self.state = Drained;
                                if let Some(sender) = self.drain_channel.take() {
                                    self.ctx.swallow_err(
                                        sender.send(Msg::SinkDrained).await,
                                        "Error sending SinkDrained message",
                                    );
                                }
                            } else {
                                debug!(
                                    "{} Not all drains received yet, waiting for drains from: {}",
                                    self.ctx,
                                    self.starts_received
                                        .difference(&self.drains_received)
                                        .map(ToString::to_string)
                                        .collect::<Vec<String>>()
                                        .join(",")
                                );
                            }
                        }
                        sink::Msg::ConnectionEstablished => {
                            debug!("{} Connection established", self.ctx);
                            self.ctx.swallow_err(
                                self.sink.on_connection_established(&self.ctx).await,
                                "Error during on_connection_established",
                            );
                            let cf = Event::cb_open(nanotime(), self.merged_operator_meta.clone());
                            // send CB restore to all pipes
                            send_contraflow(&self.pipelines, &self.ctx, cf);
                        }
                        sink::Msg::ConnectionLost => {
                            // clean out all pending stream data from EventSerializer - we assume all streams closed at this point
                            self.serializer.clear();
                            self.ctx.swallow_err(
                                self.sink.on_connection_lost(&self.ctx).await,
                                "Error during on_connection_lost",
                            );
                            // send CB trigger to all pipes
                            let cf = Event::cb_close(nanotime(), self.merged_operator_meta.clone());
                            send_contraflow(&self.pipelines, &self.ctx, cf);
                        }
                        sink::Msg::Event { event, port } => {
                            let cf_builder = ContraflowData::from(&event);

                            self.metrics_reporter.increment_in();
                            if let Some(t) = self.metrics_reporter.periodic_flush(event.ingest_ns) {
                                self.metrics_reporter
                                    .send_sink_metrics(self.sink.metrics(t, &self.ctx).await);
                            }

                            // TODO: fix additional clones here for merge
                            //       (hg) - I don't think we can do this w/o a clone since we need
                            //              them here and in the on_event
                            self.merged_operator_meta.merge(event.op_meta.clone());
                            let transactional = event.transactional;
                            let start = nanotime();

                            let res = self
                                .sink
                                .on_event(
                                    port.borrow(),
                                    event,
                                    &self.ctx,
                                    &mut self.serializer,
                                    start,
                                )
                                .await;

                            let duration = nanotime() - start;
                            match res {
                                Ok(replies) => {
                                    // TODO: send metric for duration
                                    handle_replies(
                                        replies,
                                        duration,
                                        cf_builder,
                                        &self.pipelines,
                                        &self.ctx,
                                        transactional && self.sink.auto_ack(),
                                    );
                                }
                                Err(e) => {
                                    // sink error that is not signalled via SinkReply::Fail (not handled)
                                    // This could fill the logs quickly.
                                    // TODO: Rather emit a metrics event with the logging info?
                                    error!("{} Error: {e}", &self.ctx);
                                    if transactional {
                                        let cf = cf_builder.into_fail();
                                        send_contraflow(&self.pipelines, &self.ctx, cf);
                                    }
                                }
                            };
                        }
                        sink::Msg::Signal { signal } => {
                            // special treatment
                            match signal.kind {
                                Some(SignalKind::Drain(source_uid)) => {
                                    debug!("{} Drain signal received from {source_uid}", self.ctx);
                                    // account for all received drains per source
                                    self.drains_received.insert(source_uid);
                                    // check if all "reachable sources" did send a `Drain` signal
                                    if self.drains_received.is_superset(&self.starts_received) {
                                        debug!("{} Sink Drained.", self.ctx);
                                        self.state = Drained;
                                        if let Some(sender) = self.drain_channel.take() {
                                            self.ctx.swallow_err(
                                                sender.send(Msg::SinkDrained).await,
                                                "Error sending SinkDrained CF",
                                            );
                                        }
                                    }

                                    // send a cb Drained contraflow message back
                                    let cf = ContraflowData::from(&signal)
                                        .into_cb(CbAction::Drained(source_uid, self.ctx.uid()));
                                    send_contraflow(&self.pipelines, &self.ctx, cf);
                                }
                                Some(SignalKind::Start(source_uid)) => {
                                    debug!("{} Received Start signal from {source_uid}", self.ctx);
                                    self.starts_received.insert(source_uid);
                                }
                                _ => {} // ignore
                            }
                            // hand it over to the sink impl
                            let cf_builder = ContraflowData::from(&signal);
                            let start = nanotime();
                            let res = self
                                .sink
                                .on_signal(signal, &self.ctx, &mut self.serializer)
                                .await;
                            let duration = nanotime() - start;
                            match res {
                                Ok(replies) => {
                                    handle_replies(
                                        replies,
                                        duration,
                                        cf_builder,
                                        &self.pipelines,
                                        &self.ctx,
                                        false,
                                    );
                                }
                                Err(e) => {
                                    // logging here is ok, as this is mostly limited to ticks (every 100ms)
                                    error!("{} Error handling signal: {e}", self.ctx);
                                }
                            }
                        }
                        st @ (sink::Msg::Start | sink::Msg::Resume | sink::Msg::Pause) => {
                            info!("{} Ignoring {st:?} message in {}", self.ctx, self.state);
                        }
                    }
                }
                SinkMsgWrapper::FromSink(reply) => {
                    // handle asynchronous sink replies
                    let cf = match reply {
                        AsyncSinkReply::Ack(data, duration) => Event::cb_ack_with_timing(
                            data.ingest_ns,
                            data.event_id,
                            data.op_meta,
                            duration,
                        ),
                        AsyncSinkReply::Fail(data) => {
                            Event::cb_fail(data.ingest_ns, data.event_id, data.op_meta)
                        }
                        AsyncSinkReply::CB(data, cb) => {
                            Event::insight(cb, data.event_id, data.ingest_ns, data.op_meta)
                        }
                    };
                    send_contraflow(&self.pipelines, &self.ctx, cf);
                }
            }
        }
        // sink has been stopped
        info!("{} Terminating Sink Task.", &self.ctx);
        Ok(())
    }
}

#[derive(Clone, Debug)]
/// basic data to build contraflow messages
pub struct ContraflowData {
    event_id: EventId,
    ingest_ns: u64,
    op_meta: OpMeta,
}

impl ContraflowData {
    /// create a new `ContraflowData` instance
    #[must_use]
    pub fn new(event_id: EventId, ingest_ns: u64, op_meta: OpMeta) -> Self {
        Self {
            event_id,
            ingest_ns,
            op_meta,
        }
    }
    pub(crate) fn into_ack(self, duration: u64) -> Event {
        Event::cb_ack_with_timing(self.ingest_ns, self.event_id, self.op_meta, duration)
    }
    pub(crate) fn into_fail(self) -> Event {
        Event::cb_fail(self.ingest_ns, self.event_id, self.op_meta)
    }
    fn cb(&self, cb: CbAction) -> Event {
        Event::insight(
            cb,
            self.event_id.clone(),
            self.ingest_ns,
            self.op_meta.clone(),
        )
    }
    fn into_cb(self, cb: CbAction) -> Event {
        Event::insight(cb, self.event_id, self.ingest_ns, self.op_meta)
    }

    /// Fetches the correlating event
    #[must_use]
    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }
}

impl From<&Event> for ContraflowData {
    fn from(event: &Event) -> Self {
        ContraflowData {
            event_id: event.id.clone(),
            ingest_ns: event.ingest_ns,
            op_meta: event.op_meta.clone(),
        }
    }
}

impl From<Event> for ContraflowData {
    fn from(event: Event) -> Self {
        ContraflowData {
            event_id: event.id,
            ingest_ns: event.ingest_ns,
            op_meta: event.op_meta,
        }
    }
}

/// send contraflow back to pipelines
fn send_contraflow(
    pipelines: &[(DeployEndpoint, pipeline::Addr)],
    connector_ctx: &impl Context,
    contraflow: Event,
) {
    if let Some(((last_url, last_addr), rest)) = pipelines.split_last() {
        for (url, addr) in rest {
            if let Err(e) = addr.send_insight(contraflow.clone()) {
                error!("{connector_ctx} Error sending contraflow to {url}: {e}",);
            }
        }
        if let Err(e) = last_addr.send_insight(contraflow) {
            error!("{connector_ctx} Error sending contraflow to {last_url}: {e}",);
        }
    }
}

fn handle_replies(
    reply: SinkReply,
    duration: u64,
    cf_builder: ContraflowData,
    pipelines: &[(DeployEndpoint, pipeline::Addr)],
    ctx: &impl Context,
    send_auto_ack: bool,
) {
    let ps = pipelines;
    if reply.cb != CbAction::None {
        // we do not maintain a merged op_meta here, to avoid the cost
        // the downside is, only operators which this event passed get to know this CB event
        // but worst case is, 1 or 2 more events are lost - totally worth it
        send_contraflow(ps, ctx, cf_builder.cb(reply.cb));
    }
    match reply.ack {
        SinkAck::Ack => send_contraflow(ps, ctx, cf_builder.into_ack(duration)),
        SinkAck::Fail => send_contraflow(ps, ctx, cf_builder.into_fail()),
        SinkAck::None if send_auto_ack => {
            send_contraflow(ps, ctx, cf_builder.into_ack(duration));
        }
        SinkAck::None => (),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn sink_reply_constructors() {
        assert_eq!(SinkReply::fail_or_none(true), SinkReply::FAIL);
        assert_eq!(SinkReply::fail_or_none(false), SinkReply::NONE);
        assert_eq!(SinkReply::ack_or_none(true), SinkReply::ACK);
        assert_eq!(SinkReply::ack_or_none(false), SinkReply::NONE);
    }
}
