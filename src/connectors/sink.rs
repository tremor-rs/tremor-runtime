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
/// Providing a `Sink` implementation for connectors handling only a single Stream
pub(crate) mod single_stream_sink;

pub(crate) use self::channel_sink::SinkMeta;
use super::{utils::metrics::SinkReporter, CodecReq};
use crate::codec::{self, Codec};
use crate::config::{
    Codec as CodecConfig, Connector as ConnectorConfig, Postprocessor as PostprocessorConfig,
};
use crate::connectors::utils::reconnect::{Attempt, ConnectionLostNotifier};
use crate::connectors::{ConnectorType, Context, Msg, QuiescenceBeacon, StreamDone};
use crate::errors::Result;
use crate::pipeline;
use crate::postprocessor::{finish, make_postprocessors, postprocess, Postprocessors};
use crate::primerge::PriorityMerge;
use async_std::channel::{bounded, unbounded, Receiver, Sender};
use async_std::stream::StreamExt; // for .next() on PriorityMerge
use async_std::task;
use beef::Cow;
pub(crate) use channel_sink::{ChannelSink, ChannelSinkRuntime};
pub(crate) use single_stream_sink::{SingleStreamSink, SingleStreamSinkRuntime};
use std::borrow::Borrow;
use std::collections::{btree_map::Entry, BTreeMap, HashSet};
use std::fmt::Display;
use tremor_common::ids::{SinkId, SourceId};
use tremor_common::time::nanotime;
use tremor_pipeline::{CbAction, Event, EventId, OpMeta, SignalKind, DEFAULT_STREAM_ID};
use tremor_script::{ast::DeployEndpoint, EventPayload};
use tremor_value::Value;

use crate::{connectors::prelude::*, errors::Error};
use abi_stable::{
    rvec,
    std_types::{
        RBox,
        ROption::{self, RNone, RSome},
        RResult::ROk,
        RStr, RString, RVec, SendRBoxError,
    },
    type_level::downcasting::TD_Opaque,
    RMut, StableAbi,
};
use async_ffi::{BorrowingFfiFuture, FutureExt as _};
use std::future;
use tremor_common::{
    pdk::{RError, RResult},
    ttry,
};

/// Result for a sink function that may provide insights or response.
///
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
#[repr(C)]
#[derive(Clone, Debug, Default, Copy, PartialEq, StableAbi)]
pub struct SinkReply {
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
#[repr(C)]
#[derive(Clone, Debug, Copy, PartialEq, StableAbi)]
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
#[repr(C)]
#[derive(Debug, StableAbi)]
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
#[abi_stable::sabi_trait]
pub trait RawSink: Send {
    /// called when receiving an event
    fn on_event<'a>(
        &'a mut self,
        input: RStr<'a>,
        event: Event,
        ctx: &'a SinkContext,
        serializer: &'a mut MutEventSerializer,
        start: u64,
    ) -> BorrowingFfiFuture<'a, RResult<SinkReply>>;
    /// called when receiving a signal
    fn on_signal<'a>(
        &'a mut self,
        _signal: Event,
        _ctx: &'a SinkContext,
        _serializer: &'a mut MutEventSerializer,
    ) -> BorrowingFfiFuture<'a, RResult<SinkReply>> {
        future::ready(ROk(SinkReply::default())).into_ffi()
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
    fn metrics<'a>(
        &'a mut self,
        _timestamp: u64,
        _ctx: &'a SinkContext,
    ) -> BorrowingFfiFuture<'a, RVec<EventPayload>> {
        future::ready(rvec![]).into_ffi()
    }

    // lifecycle stuff
    /// called when started
    fn on_start<'a>(&'a mut self, _ctx: &'a SinkContext) -> BorrowingFfiFuture<'a, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    /// Connect to the external thingy.
    /// This function is called definitely after `on_start` has been called.
    ///
    /// This function might be called multiple times, check the `attempt` where you are at.
    /// The intended result of this function is to re-establish a connection. It might reuse a working connection.
    ///
    /// Return `Ok(true)` if the connection could be successfully established.
    fn connect<'a>(
        &'a mut self,
        _ctx: &'a SinkContext,
        _attempt: &'a Attempt,
    ) -> BorrowingFfiFuture<'a, RResult<bool>> {
        future::ready(ROk(true)).into_ffi()
    }

    /// called when paused
    fn on_pause<'a>(&'a mut self, _ctx: &'a SinkContext) -> BorrowingFfiFuture<'a, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }
    /// called when resumed
    fn on_resume<'a>(&'a mut self, _ctx: &'a SinkContext) -> BorrowingFfiFuture<'a, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }
    /// called when stopped
    fn on_stop<'a>(&'a mut self, _ctx: &'a SinkContext) -> BorrowingFfiFuture<'a, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }

    // connectivity stuff
    /// called when sink lost connectivity
    fn on_connection_lost<'a>(
        &'a mut self,
        _ctx: &'a SinkContext,
    ) -> BorrowingFfiFuture<'a, RResult<()>> {
        future::ready(ROk(())).into_ffi()
    }
    /// called when sink re-established connectivity
    fn on_connection_established<'a>(
        &'a mut self,
        _ctx: &'a SinkContext,
    ) -> BorrowingFfiFuture<'a, RResult<()>> {
        future::ready(ROk(())).into_ffi()
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

/// Alias for the FFI-safe dynamic source type
pub type BoxedRawSink = RawSink_TO<'static, RBox<()>>;

/// Sink part of a connector.
///
/// Just like `Connector`, this wraps the FFI dynamic sink with `abi_stable`
/// types so that it's easier to use with `std`.
pub(crate) struct Sink(BoxedRawSink);
impl Sink {
    #[inline]
    pub async fn on_event(
        &mut self,
        input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        // TODO: this should take the boxed serializer instead for performance (?)
        let mut serializer = MutEventSerializer::from_ptr(serializer, TD_Opaque);
        self.0
            .on_event(input.into(), event, ctx, &mut serializer, start)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }
    #[inline]
    pub async fn on_signal(
        &mut self,
        signal: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
        // TODO: this should take the boxed serializer instead for performance (?)
        let mut serializer = MutEventSerializer::from_ptr(serializer, TD_Opaque);
        self.0
            .on_signal(signal, ctx, &mut serializer)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn metrics(&mut self, timestamp: u64, ctx: &SinkContext) -> Vec<EventPayload> {
        self.0.metrics(timestamp, ctx).await.into()
    }

    #[inline]
    pub async fn on_start(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_start(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn connect(&mut self, ctx: &SinkContext, attempt: &Attempt) -> Result<bool> {
        self.0
            .connect(ctx, attempt)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_pause(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_pause(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }
    #[inline]
    pub async fn on_resume(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_resume(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }
    #[inline]
    pub async fn on_stop(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_stop(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }

    #[inline]
    pub async fn on_connection_lost(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_connection_lost(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }
    #[inline]
    pub async fn on_connection_established(&mut self, ctx: &SinkContext) -> Result<()> {
        self.0
            .on_connection_established(ctx)
            .await
            .map_err(Into::into) // RBoxError -> Error::PluginError
            .into() // RResult -> Result
    }

    #[inline]
    pub fn auto_ack(&self) -> bool {
        self.0.auto_ack()
    }

    #[inline]
    pub fn asynchronous(&self) -> bool {
        self.0.asynchronous()
    }
}

/// handles writing to 1 stream (e.g. file or TCP connection)
#[async_trait::async_trait]
pub(crate) trait StreamWriter: Send + Sync {
    /// write the given data out to the stream
    async fn write(&mut self, data: Vec<Vec<u8>>, meta: Option<SinkMeta>) -> Result<()>;
    /// handle the stream being done, by error or regular end of stream
    /// This controls the reaction of the runtime:
    /// Should the connector be considered disconnected now? Or is this just one stream amongst many?
    async fn on_done(&mut self, _stream: u64) -> Result<StreamDone> {
        Ok(StreamDone::StreamClosed)
    }
}

#[async_trait::async_trait]
pub(crate) trait SinkRuntime: Send + Sync {
    async fn unregister_stream_writer(&self, stream: u64) -> Result<()>;
}
/// context for the connector sink
#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct SinkContext {
    /// the connector unique identifier
    pub(crate) uid: SinkId,
    /// the connector url
    pub(crate) alias: RString,
    /// the connector type
    pub(crate) connector_type: ConnectorType,

    /// check if we are paused or should stop reading/writing
    pub(crate) quiescence_beacon: BoxedQuiescenceBeacon,

    /// notifier the connector runtime if we lost a connection
    pub(crate) notifier: BoxedConnectionLostNotifier,
}

impl Display for SinkContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Sink::{}]", &self.alias)
    }
}

impl Context for SinkContext {
    fn alias(&self) -> &str {
        &self.alias
    }

    fn quiescence_beacon(&self) -> &BoxedQuiescenceBeacon {
        &self.quiescence_beacon
    }

    fn notifier(&self) -> &BoxedConnectionLostNotifier {
        &self.notifier
    }

    fn connector_type(&self) -> &ConnectorType {
        &self.connector_type
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
        port: Cow<'static, str>,
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
    qsize: usize,
    serializer: EventSerializer,
    reply_channel: (Sender<AsyncSinkReply>, Receiver<AsyncSinkReply>),
    metrics_reporter: SinkReporter,
}

impl SinkManagerBuilder {
    /// globally configured queue size
    #[must_use]
    pub(crate) fn qsize(&self) -> usize {
        self.qsize
    }

    /// Get yourself a sender to send replies back from your concrete sink.
    ///
    /// This is especially useful if your sink handles events asynchronously
    /// and you can't reply immediately.
    #[must_use]
    pub(crate) fn reply_tx(&self) -> Sender<AsyncSinkReply> {
        self.reply_channel.0.clone()
    }

    /// spawn your specific sink
    pub(crate) fn spawn(self, sink: BoxedRawSink, ctx: SinkContext) -> Result<SinkAddr> {
        let qsize = self.qsize;
        let name = format!("{}-sink", ctx.alias);
        let (sink_tx, sink_rx) = bounded(qsize);
        let manager = SinkManager::new(sink, ctx, self, sink_rx);
        // spawn manager task
        task::Builder::new().name(name).spawn(manager.run())?;

        Ok(SinkAddr { addr: sink_tx })
    }
}

#[abi_stable::sabi_trait]
pub trait ContraflowSenderOpaque: Send + Sync + Clone {
    /// Send a contraflow message to the runtime
    fn send(&self, reply: AsyncSinkReply) -> BorrowingFfiFuture<'_, RResult<()>>;
}
impl ContraflowSenderOpaque for Sender<AsyncSinkReply> {
    fn send(&self, reply: AsyncSinkReply) -> BorrowingFfiFuture<'_, RResult<()>> {
        async move { self.send(reply).await.map_err(RError::new).into() }.into_ffi()
    }
}
/// Alias for the FFI-safe contraflow sender, boxed
pub type BoxedContraflowSender = ContraflowSenderOpaque_TO<'static, RBox<()>>;

// TODO: not quite sure what to debug here, but the implementation is necessary
impl std::fmt::Debug for BoxedContraflowSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "boxed contraflow sender")
    }
}

/// create a builder for a `SinkManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SinkManager`.
pub(crate) fn builder(
    config: &ConnectorConfig,
    connector_codec_requirement: CodecReq,
    alias: &str,
    qsize: usize,
    metrics_reporter: SinkReporter,
) -> Result<SinkManagerBuilder> {
    // resolve codec and processors
    let postprocessor_configs = config.postprocessors.clone().unwrap_or_default();
    let serializer = EventSerializer::new(
        config.codec.clone().into(),
        connector_codec_requirement,
        postprocessor_configs.into(),
        &config.connector_type,
        alias,
    )?;
    // the incoming channels for events are all bounded, so we can safely be unbounded here
    // TODO: actually we could have lots of CB events not bound to events here
    let reply_channel = unbounded();
    Ok(SinkManagerBuilder {
        qsize,
        serializer,
        reply_channel,
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
        alias: &str,
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
            CodecReq::Optional(opt) => {
                codec_config.unwrap_or_else(|| CodecConfig::from(opt.as_str()))
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
}

/// Note that since `EventSerializer` is used for the plugin system, it
/// must be `#[repr(C)]` in order to interact with it. However, since it uses
/// complex types and it's easier to just make it available as an opaque type
/// instead, with the help of `sabi_trait`.
#[abi_stable::sabi_trait]
pub trait EventSerializerOpaque: Send {
    /// drop a stream
    fn drop_stream(&mut self, stream_id: u64);

    /// clear out all streams - this can lead to data loss
    /// only use when you are sure, all the streams are gone
    fn clear(&mut self);

    /// serialize event for the default stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    fn serialize(&mut self, value: &Value, ingest_ns: u64) -> RResult<RVec<RVec<u8>>> {
        self.serialize_for_stream(value, ingest_ns, DEFAULT_STREAM_ID)
    }

    /// serialize event for a certain stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    fn serialize_for_stream(
        &mut self,
        value: &Value,
        ingest_ns: u64,
        stream_id: u64,
    ) -> RResult<RVec<RVec<u8>>> {
        self.serialize_for_stream_with_codec(value, ingest_ns, stream_id, RNone)
    }

    /// Serialize an event for a certain stream with the possibility to overwrite the configured codec.
    ///
    /// The `codec_overwrite` is only considered when creating new streams, not for existing streams.
    /// It is also ignored when for the `DEFAULT_STREAM_ID`, which is `0`. Beware!
    ///
    /// # Errors
    ///   * if serialization fails (codec or postprocessors)
    fn serialize_for_stream_with_codec(
        &mut self,
        value: &Value,
        ingest_ns: u64,
        stream_id: u64,
        codec_overwrite: ROption<&RString>,
    ) -> RResult<RVec<RVec<u8>>>;

    /// remove and flush out any pending data from the stream identified by the given `stream_id`
    fn finish_stream(&mut self, stream_id: u64) -> RResult<RVec<RVec<u8>>>;
}

impl EventSerializerOpaque for EventSerializer {
    fn drop_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    fn clear(&mut self) {
        self.streams.clear();
    }

    fn serialize_for_stream_with_codec(
        &mut self,
        value: &Value,
        ingest_ns: u64,
        stream_id: u64,
        codec_overwrite: ROption<&RString>,
    ) -> RResult<RVec<RVec<u8>>> {
        let data = if stream_id == DEFAULT_STREAM_ID {
            // no codec_overwrite for the default stream
            ttry!(postprocess(
                &mut self.postprocessors,
                ingest_ns,
                ttry!(self.codec.encode(value)),
                &self.alias,
            ))
        } else {
            match self.streams.entry(stream_id) {
                Entry::Occupied(mut entry) => {
                    let (codec, pps) = entry.get_mut();
                    ttry!(postprocess(
                        pps,
                        ingest_ns,
                        ttry!(codec.encode(value)),
                        &self.alias
                    ))
                }
                Entry::Vacant(entry) => {
                    // codec overwrite only considered for new streams
                    let codec = ttry!(match codec_overwrite {
                        // TODO: add conversion RString -> Codec
                        RSome(codec) => codec::resolve(&codec.as_str().into()),
                        RNone => codec::resolve(&self.codec_config),
                    });
                    let pps = ttry!(make_postprocessors(self.postprocessor_configs.as_slice()));
                    // insert data for a new stream
                    let (c, pps2) = entry.insert((codec, pps));
                    ttry!(postprocess(
                        pps2,
                        ingest_ns,
                        ttry!(c.encode(value)),
                        &self.alias
                    ))
                }
            }
        };
        // TODO: avoid this conversion
        ROk(data.into_iter().map(RVec::from).collect())
    }

    fn finish_stream(&mut self, stream_id: u64) -> RResult<RVec<RVec<u8>>> {
        if let Some((mut _codec, mut postprocessors)) = self.streams.remove(&stream_id) {
            // TODO: avoid this conversion
            let data = ttry!(finish(&mut postprocessors, &self.alias));
            ROk(data.into_iter().map(RVec::from).collect())
        } else {
            ROk(rvec![])
        }
    }
}

/// Alias for the type used in FFI, as a box
pub type BoxedEventSerializer = EventSerializerOpaque_TO<'static, RBox<()>>;
/// Alias for the type used in FFI, as a mutable reference
pub type MutEventSerializer<'a> = EventSerializerOpaque_TO<'static, RMut<'a, ()>>;

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

pub(crate) struct SinkManager {
    sink: Sink,
    ctx: SinkContext,
    rx: Receiver<SinkMsg>,
    reply_rx: Receiver<AsyncSinkReply>,
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

impl SinkManager {
    fn new(
        sink: BoxedRawSink,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
        rx: Receiver<SinkMsg>,
    ) -> Self {
        let SinkManagerBuilder {
            serializer,
            reply_channel,
            metrics_reporter,
            ..
        } = builder;
        Self {
            sink: Sink(sink),
            ctx,
            rx,
            reply_rx: reply_channel.1,
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
        let mut from_and_to_sink_channel = PriorityMerge::new(from_sink, to_sink);
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
                                cb: CbAction::SinkStart(self.ctx.uid),
                                ..Event::default()
                            };
                            // send CB start to all pipes
                            send_contraflow(&self.pipelines, &self.ctx.alias, cf).await;
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
                            send_contraflow(&self.pipelines, &self.ctx.alias, cf).await;
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
                            send_contraflow(&self.pipelines, &self.ctx.alias, cf).await;
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
                                        &self.ctx.alias,
                                        transactional && self.sink.auto_ack(),
                                    )
                                    .await;
                                }
                                Err(_e) => {
                                    // sink error that is not signalled via SinkReply::Fail (not handled)
                                    // TODO: error logging? This could fill the logs quickly. Rather emit a metrics event with the logging info?
                                    if transactional {
                                        let cf = cf_builder.into_fail();
                                        send_contraflow(&self.pipelines, &self.ctx.alias, cf).await;
                                    }
                                }
                            };
                        }
                        SinkMsg::Signal { signal } => {
                            // special treatment
                            match signal.kind {
                                RSome(SignalKind::Drain(source_uid)) => {
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
                                        .into_cb(CbAction::Drained(source_uid, self.ctx.uid));
                                    send_contraflow(&self.pipelines, &self.ctx.alias, cf).await;
                                }
                                RSome(SignalKind::Start(source_uid)) => {
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
                                        &self.ctx.alias,
                                        false,
                                    )
                                    .await;
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
                    send_contraflow(&self.pipelines, &self.ctx.alias, cf).await;
                }
            }
        }
        // sink has been stopped
        info!("[Sink::{}] Terminating Sink Task.", &self.ctx.alias);
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Debug, StableAbi)]
/// basic data to build contraflow messages
pub struct ContraflowData {
    event_id: EventId,
    ingest_ns: u64,
    op_meta: OpMeta,
}

impl ContraflowData {
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
async fn send_contraflow(
    pipelines: &[(DeployEndpoint, pipeline::Addr)],
    connector_url: &str,
    contraflow: Event,
) {
    if let Some(((last_url, last_addr), rest)) = pipelines.split_last() {
        for (url, addr) in rest {
            if let Err(e) = addr.send_insight(contraflow.clone()).await {
                error!(
                    "[Connector::{}] Error sending contraflow to {}: {}",
                    &connector_url, url, e
                );
            }
        }
        if let Err(e) = last_addr.send_insight(contraflow).await {
            error!(
                "[Connector::{}] Error sending contraflow to {}: {}",
                &connector_url, last_url, e
            );
        }
    }
}

async fn handle_replies(
    reply: SinkReply,
    duration: u64,
    cf_builder: ContraflowData,
    pipelines: &[(DeployEndpoint, pipeline::Addr)],
    connector_url: &str,
    send_auto_ack: bool,
) {
    let ps = pipelines;
    let url = connector_url;
    if reply.cb != CbAction::None {
        // we do not maintain a merged op_meta here, to avoid the cost
        // the downside is, only operators which this event passed get to know this CB event
        // but worst case is, 1 or 2 more events are lost - totally worth it
        send_contraflow(ps, url, cf_builder.cb(reply.cb)).await;
    }
    match reply.ack {
        SinkAck::Ack => send_contraflow(ps, url, cf_builder.into_ack(duration)).await,
        SinkAck::Fail => send_contraflow(ps, url, cf_builder.into_fail()).await,
        SinkAck::None if send_auto_ack => {
            send_contraflow(ps, url, cf_builder.into_ack(duration)).await;
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
