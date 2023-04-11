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
pub(crate) mod channel_sink;
/// Utility for limiting concurrency (by sending `CB::Close` messages when a maximum concurrency value is reached)
pub(crate) mod concurrency_cap;

use crate::config::{
    Codec as CodecConfig, Connector as ConnectorConfig, Postprocessor as PostprocessorConfig,
};
use crate::connectors::{utils::metrics::SinkReporter, CodecReq};
use crate::connectors::{Alias, ConnectorType, Context, Msg, QuiescenceBeacon, StreamDone};
use crate::errors::Result;
use crate::pipeline;
use crate::postprocessor::{finish, make_postprocessors, postprocess, Postprocessors};
use crate::primerge::PriorityMerge;
use crate::{
    channel::{bounded, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender},
    qsize,
};
use crate::{
    codec::{self, Codec},
    config::NameWithConfig,
};
use crate::{
    connectors::utils::reconnect::{Attempt, ConnectionLostNotifier},
    raft,
};
use futures::StreamExt; // for .next() on PriorityMerge
use std::collections::{btree_map::Entry, BTreeMap, HashSet};
use std::fmt::Display;
use std::{borrow::Borrow, sync::Arc};
use tokio::task;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tremor_common::time::nanotime;
use tremor_common::{
    ports::Port,
    uids::{SinkUId, SourceUId},
};
use tremor_pipeline::{CbAction, Event, EventId, OpMeta, SignalKind, DEFAULT_STREAM_ID};
use tremor_script::{ast::DeployEndpoint, EventPayload};
use tremor_value::Value;

pub(crate) type ReplySender = UnboundedSender<AsyncSinkReply>;

/// Result for a sink function that may provide insights or response.
///
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
#[derive(Clone, Debug, Default, Copy, PartialEq)]
pub(crate) struct SinkReply {
    /// guaranteed delivery response - did we sent the event successfully `SinkAck::Ack` or did it fail `SinkAck::Fail`
    pub(crate) ack: SinkAck,
    /// circuit breaker action
    pub(crate) cb: CbAction,
}

impl SinkReply {
    /// Acknowledges
    pub(crate) const ACK: SinkReply = SinkReply {
        ack: SinkAck::Ack,
        cb: CbAction::None,
    };
    /// Fails
    pub(crate) const FAIL: SinkReply = SinkReply {
        ack: SinkAck::Fail,
        cb: CbAction::None,
    };
    /// None
    pub(crate) const NONE: SinkReply = SinkReply {
        ack: SinkAck::None,
        cb: CbAction::None,
    };

    /// Decide according to the given flag if we return a fail or a none
    #[must_use]
    pub(crate) fn fail_or_none(needs_fail: bool) -> Self {
        if needs_fail {
            Self::FAIL
        } else {
            Self::NONE
        }
    }

    /// Decide according to the given flag if we return a ack or a none
    #[must_use]
    pub(crate) fn ack_or_none(needs_ack: bool) -> Self {
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
pub(crate) enum AsyncSinkReply {
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
pub(crate) trait Sink: Send {
    /// called when receiving an event
    async fn on_event(
        &mut self,
        input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply>;
    /// called when receiving a signal
    async fn on_signal(
        &mut self,
        _signal: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
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
    async fn on_start(&mut self, _ctx: &SinkContext) -> Result<()> {
        Ok(())
    }

    /// Connect to the external thingy.
    /// This function is called definitely after `on_start` has been called.
    ///
    /// This function might be called multiple times, check the `attempt` where you are at.
    /// The intended result of this function is to re-establish a connection. It might reuse a working connection.
    ///
    /// Return `Ok(true)` if the connection could be successfully established.
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    /// called when paused
    async fn on_pause(&mut self, _ctx: &SinkContext) -> Result<()> {
        Ok(())
    }
    /// called when resumed
    async fn on_resume(&mut self, _ctx: &SinkContext) -> Result<()> {
        Ok(())
    }
    /// called when stopped
    async fn on_stop(&mut self, _ctx: &SinkContext) -> Result<()> {
        Ok(())
    }

    // connectivity stuff
    /// called when sink lost connectivity
    async fn on_connection_lost(&mut self, _ctx: &SinkContext) -> Result<()> {
        Ok(())
    }
    /// called when sink re-established connectivity
    async fn on_connection_established(&mut self, _ctx: &SinkContext) -> Result<()> {
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
pub(crate) trait StreamWriter: Send + Sync {
    /// write the given data out to the stream
    async fn write(&mut self, data: Vec<Vec<u8>>, meta: Option<&Value>) -> Result<()>;
    /// handle the stream being done, by error or regular end of stream
    /// This controls the reaction of the runtime:
    /// Should the connector be considered disconnected now? Or is this just one stream amongst many?
    async fn on_done(&mut self, _stream: u64) -> Result<StreamDone> {
        Ok(StreamDone::StreamClosed)
    }
}

#[async_trait::async_trait]
pub(crate) trait SinkRuntime: Send + Sync {
    async fn unregister_stream_writer(&mut self, stream: u64) -> Result<()>;
}
/// context for the connector sink
#[derive(Clone)]
pub(crate) struct SinkContextInner {
    pub(crate) node_id: openraft::NodeId,
    /// the connector unique identifier
    pub(crate) uid: SinkUId,
    /// the connector alias
    pub(crate) alias: Alias,
    /// the connector type
    pub(crate) connector_type: ConnectorType,

    /// check if we are paused or should stop reading/writing
    pub(crate) quiescence_beacon: QuiescenceBeacon,

    /// notifier the connector runtime if we lost a connection
    pub(crate) notifier: ConnectionLostNotifier,

    /// sender for raft requests
    pub(crate) raft: raft::Manager,
}
#[derive(Clone)]
pub(crate) struct SinkContext(Arc<SinkContextInner>);
impl SinkContext {
    pub(crate) fn uid(&self) -> SinkUId {
        self.0.uid
    }
    pub(crate) fn notifier(&self) -> &ConnectionLostNotifier {
        &self.0.notifier
    }
    pub(crate) fn new(
        node_id: openraft::NodeId,
        uid: SinkUId,
        alias: Alias,
        connector_type: ConnectorType,
        quiescence_beacon: QuiescenceBeacon,
        notifier: ConnectionLostNotifier,
        raft_api_tx: raft::Manager,
    ) -> SinkContext {
        Self(Arc::new(SinkContextInner {
            node_id,
            uid,
            alias,
            connector_type,
            quiescence_beacon,
            notifier,
            raft: raft_api_tx,
        }))
    }
}

impl Display for SinkContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Node::{}][Sink::{}]", self.0.node_id, &self.0.alias)
    }
}

impl Context for SinkContext {
    // fn uid(&self) -> &SinkId {
    //     &self.0.uid
    // }
    fn alias(&self) -> &Alias {
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

    fn raft(&self) -> &raft::Manager {
        &self.0.raft
    }
}

/// messages a sink can receive
#[derive(Debug)]
pub(crate) enum SinkMsg {
    /// receive an event to handle
    Event {
        /// the event
        event: Event,
        /// the port through which it came
        port: Port<'static>,
    },
    /// receive a signal
    Signal {
        /// the signal event
        signal: Event,
    },
    /// link some pipelines to the give port
    Link {
        /// the pipelines
        pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
    },
    /// Connect to the outside world and send the result back
    Connect(Sender<Result<bool>>, Attempt),
    /// the connection to the outside world wasl ost
    ConnectionLost,
    /// connection established
    ConnectionEstablished,
    // TODO: fill those
    /// start the sink
    Start,
    /// pause the sink
    Pause,
    /// resume the sink
    Resume,
    /// stop the sink
    Stop(Sender<Result<()>>),
    /// drain this sink and notify the connector via the provided sender
    Drain(Sender<Msg>),
}

/// Wrapper around all possible sink messages
/// handled in the Sink task
#[allow(clippy::large_enum_variant)] // TODO: should we box SinkMsg here?
#[derive(Debug)]
enum SinkMsgWrapper {
    FromSink(AsyncSinkReply),
    ToSink(SinkMsg),
}

/// address of a connector sink
#[derive(Clone, Debug)]
pub(crate) struct SinkAddr {
    /// the actual sender
    pub(crate) addr: Sender<SinkMsg>,
}

/// Builder for the sink manager
pub(crate) struct SinkManagerBuilder {
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
    pub(crate) fn reply_tx(&self) -> ReplySender {
        self.reply_tx.clone()
    }

    /// spawn your specific sink
    pub(crate) fn spawn<S>(self, sink: S, ctx: SinkContext) -> SinkAddr
    where
        S: Sink + Send + 'static,
    {
        let (sink_tx, sink_rx) = bounded(qsize());
        let manager = SinkManager::new(sink, ctx, self, sink_rx);
        task::spawn(manager.run());

        SinkAddr { addr: sink_tx }
    }
}

/// create a builder for a `SinkManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SinkManager`.
pub(crate) fn builder(
    config: &ConnectorConfig,
    connector_codec_requirement: CodecReq,
    alias: &Alias,

    metrics_reporter: SinkReporter,
) -> Result<SinkManagerBuilder> {
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
pub(crate) struct EventSerializer {
    alias: String,
    // default stream handling
    pub(crate) codec: Box<dyn Codec>,
    postprocessors: Postprocessors,
    // creation templates for stream handling
    codec_config: CodecConfig,
    postprocessor_configs: Vec<PostprocessorConfig>,
    // stream data
    // TODO: clear out state from codec, postprocessors and enable reuse
    streams: BTreeMap<u64, (Box<dyn Codec>, Postprocessors)>,
}

impl EventSerializer {
    pub(crate) fn new(
        codec_config: Option<CodecConfig>,
        default_codec: CodecReq,
        postprocessor_configs: Vec<PostprocessorConfig>,
        connector_type: &ConnectorType,
        alias: &Alias,
    ) -> Result<Self> {
        let codec_config = match default_codec {
            CodecReq::Structured => {
                if codec_config.is_some() {
                    return Err(format!(
                        "The {connector_type} connector {alias} can not be configured with a codec.",
                    )
                    .into());
                }
                CodecConfig::from("null")
            }
            CodecReq::Required => codec_config
                .ok_or_else(|| format!("Missing codec for {connector_type} connector {alias}"))?,
            CodecReq::Optional(opt) => codec_config.unwrap_or_else(|| CodecConfig::from(opt)),
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
    pub(crate) fn drop_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    /// clear out all streams - this can lead to data loss
    /// only use when you are sure, all the streams are gone
    pub(crate) fn clear(&mut self) {
        self.streams.clear();
    }

    /// serialize event for the default stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    pub(crate) fn serialize(&mut self, value: &Value, ingest_ns: u64) -> Result<Vec<Vec<u8>>> {
        self.serialize_for_stream(value, ingest_ns, DEFAULT_STREAM_ID)
    }

    /// serialize event for a certain stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    pub(crate) fn serialize_for_stream(
        &mut self,
        value: &Value,
        ingest_ns: u64,
        stream_id: u64,
    ) -> Result<Vec<Vec<u8>>> {
        self.serialize_for_stream_with_codec(value, ingest_ns, stream_id, None)
    }

    /// Serialize an event for a certain stream with the possibility to overwrite the configured codec.
    ///
    /// The `codec_overwrite` is only considered when creating new streams, not for existing streams.
    /// It is also ignored when for the `DEFAULT_STREAM_ID`, which is `0`. Beware!
    ///
    /// # Errors
    ///   * if serialization fails (codec or postprocessors)
    pub(crate) fn serialize_for_stream_with_codec(
        &mut self,
        value: &Value,
        ingest_ns: u64,
        stream_id: u64,
        codec_overwrite: Option<&NameWithConfig>,
    ) -> Result<Vec<Vec<u8>>> {
        if stream_id == DEFAULT_STREAM_ID {
            // no codec_overwrite for the default stream
            postprocess(
                &mut self.postprocessors,
                ingest_ns,
                self.codec.encode(value)?,
                &self.alias,
            )
        } else {
            match self.streams.entry(stream_id) {
                Entry::Occupied(mut entry) => {
                    let (codec, pps) = entry.get_mut();
                    postprocess(pps, ingest_ns, codec.encode(value)?, &self.alias)
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
                    postprocess(pps2, ingest_ns, c.encode(value)?, &self.alias)
                }
            }
        }
    }

    /// remove and flush out any pending data from the stream identified by the given `stream_id`
    pub(crate) fn finish_stream(&mut self, stream_id: u64) -> Result<Vec<Vec<u8>>> {
        if let Some((mut _codec, mut postprocessors)) = self.streams.remove(&stream_id) {
            finish(&mut postprocessors, &self.alias)
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
    rx: ReceiverStream<SinkMsg>,
    reply_rx: UnboundedReceiverStream<AsyncSinkReply>,
    serializer: EventSerializer,
    metrics_reporter: SinkReporter,
    /// tracking which operators incoming events visited
    merged_operator_meta: OpMeta,
    // pipelines connected to IN port
    pipelines: Vec<(DeployEndpoint, pipeline::Addr)>,
    // set of source ids we received start signals from
    starts_received: HashSet<SourceUId>,
    // set of connector ids we received drain signals from
    drains_received: HashSet<SourceUId>, // TODO: use a bitset for both?
    drain_channel: Option<Sender<Msg>>,
    state: SinkState,
}

impl<S> SinkManager<S>
where
    S: Sink,
{
    fn new(sink: S, ctx: SinkContext, builder: SinkManagerBuilder, rx: Receiver<SinkMsg>) -> Self {
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
    async fn run(mut self) -> Result<()> {
        use SinkState::{Drained, Draining, Initialized, Paused, Running, Stopped};
        let from_sink = self.reply_rx.map(SinkMsgWrapper::FromSink);
        let to_sink = self.rx.map(SinkMsgWrapper::ToSink);
        let mut from_and_to_sink_channel = PriorityMerge::new(to_sink, from_sink);
        while let Some(msg_wrapper) = from_and_to_sink_channel.next().await {
            match msg_wrapper {
                SinkMsgWrapper::ToSink(sink_msg) => {
                    match sink_msg {
                        SinkMsg::Link { mut pipelines } => {
                            self.pipelines.append(&mut pipelines);
                        }
                        SinkMsg::Start if self.state == Initialized => {
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
                        SinkMsg::Connect(sender, attempt) => {
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
                        SinkMsg::Resume if self.state == Paused => {
                            self.state = Running;
                            self.ctx.swallow_err(
                                self.sink.on_resume(&self.ctx).await,
                                "Error during on_resume",
                            );
                        }
                        SinkMsg::Pause if self.state == Running => {
                            self.state = Paused;
                            self.ctx.swallow_err(
                                self.sink.on_pause(&self.ctx).await,
                                "Error during on_pause",
                            );
                        }
                        SinkMsg::Stop(sender) => {
                            info!("{} Stopping...", &self.ctx);
                            self.state = Stopped;
                            self.ctx.swallow_err(
                                sender.send(self.sink.on_stop(&self.ctx).await).await,
                                "Error sending Stop reply",
                            );
                            // exit control plane
                            break;
                        }
                        SinkMsg::Drain(_sender) if self.state == Draining => {
                            info!(
                                "{} Ignoring Drain message in {} state",
                                self.ctx, self.state
                            );
                        }

                        SinkMsg::Drain(sender) if self.state == Drained => {
                            debug!("{} Sink already Drained.", self.ctx);
                            self.ctx.swallow_err(
                                sender.send(Msg::SinkDrained).await,
                                "Error sending SinkDrained message",
                            );
                        }
                        SinkMsg::Drain(sender) => {
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
                        SinkMsg::ConnectionEstablished => {
                            debug!("{} Connection established", self.ctx);
                            self.ctx.swallow_err(
                                self.sink.on_connection_established(&self.ctx).await,
                                "Error during on_connection_established",
                            );
                            let cf = Event::cb_open(nanotime(), self.merged_operator_meta.clone());
                            // send CB restore to all pipes
                            send_contraflow(&self.pipelines, &self.ctx, cf);
                        }
                        SinkMsg::ConnectionLost => {
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
                        SinkMsg::Event { event, port } => {
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
                        SinkMsg::Signal { signal } => {
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
                        st @ (SinkMsg::Start | SinkMsg::Resume | SinkMsg::Pause) => {
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
pub(crate) struct ContraflowData {
    event_id: EventId,
    ingest_ns: u64,
    op_meta: OpMeta,
}

impl ContraflowData {
    pub(crate) fn new(event_id: EventId, ingest_ns: u64, op_meta: OpMeta) -> Self {
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

    #[cfg(test)]
    pub(crate) fn event_id(&self) -> &EventId {
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
