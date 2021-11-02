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

use async_std::future::timeout;
use async_std::task;
use either::Either;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::time::Duration;
use tremor_common::time::nanotime;
use tremor_script::{EventPayload, ValueAndMeta};

use crate::config::{Codec as CodecConfig, Connector as ConnectorConfig};
use crate::connectors::Msg;
use crate::errors::{Error, Result};
use crate::pipeline;
use crate::preprocessor::{make_preprocessors, preprocess, Preprocessors};
use crate::url::ports::{ERR, OUT};
use crate::url::TremorUrl;
use crate::{
    codec::{self, Codec},
    pipeline::ConnectInputTarget,
};
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use beef::Cow;
use tremor_pipeline::{
    CbAction, Event, EventId, EventIdGenerator, EventOriginUri, DEFAULT_STREAM_ID,
};
use tremor_value::{literal, Value};
use value_trait::Builder;

use super::metrics::SourceReporter;
use super::quiescence::QuiescenceBeacon;
use super::{ConnectorContext, StreamDone};

/// The default poll interval for `try_recv` on channels in connectors
pub const DEFAULT_POLL_INTERVAL: u64 = 10;

#[derive(Debug)]
/// Messages a Source can receive
pub enum SourceMsg {
    /// connect a pipeline
    Link {
        /// port
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    },
    /// disconnect a pipeline from a port
    Unlink {
        /// port
        port: Cow<'static, str>,
        /// url of the pipeline
        id: TremorUrl,
    },
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
    Stop,
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
    // an already structured event payload
    Structured {
        origin_uri: EventOriginUri,
        payload: EventPayload,
        stream: u64,
        /// Port to send to, defaults to `out`
        port: Option<Cow<'static, str>>,
    },
    // a bunch of separated `Vec<u8>` with optional metadata
    // for when the source knows where boundaries are, maybe because it receives chunks already
    BatchData {
        origin_uri: EventOriginUri,
        batch_data: Vec<(Vec<u8>, Option<Value<'static>>)>,
        /// Port to send to, defaults to `out`
        port: Option<Cow<'static, str>>,
        stream: u64,
    },
    /// A stream is opened
    StartStream(u64),
    /// A stream is closed
    EndStream(u64),
    /// no new data/event, wait for the given ms
    Empty(u64),
}

// sender for source reply
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
    async fn on_start(&mut self, _ctx: &mut SourceContext) {}
    /// called when the source is explicitly paused as result of a user/operator interaction
    /// in contrast to `on_cb_close` which happens automatically depending on downstream pipeline or sink connector logic.
    async fn on_pause(&mut self, _ctx: &mut SourceContext) {}
    /// called when the source is explicitly resumed from being paused
    async fn on_resume(&mut self, _ctx: &mut SourceContext) {}
    /// called when the source is stopped. This happens only once in the whole source lifecycle, as the very last callback
    async fn on_stop(&mut self, _ctx: &mut SourceContext) {}

    // circuit breaker callbacks
    /// called when we receive a `close` Circuit breaker event from any connected pipeline
    /// Expected reaction is to pause receiving messages, which is handled automatically by the runtime
    /// Source implementations might want to close connections or signal a pause to the upstream entity it connects to if not done in the connector (the default)
    // TODO: add info of Cb event origin (port, origin_uri)?
    async fn on_cb_close(&mut self, _ctx: &mut SourceContext) {}
    /// Called when we receive a `open` Circuit breaker event from any connected pipeline
    /// This means we can start/continue polling this source for messages
    /// Source implementations might want to start establishing connections if not done in the connector (the default)
    async fn on_cb_open(&mut self, _ctx: &mut SourceContext) {}

    // guaranteed delivery callbacks
    /// an event has been acknowledged and can be considered delivered
    /// multiple acks for the same set of ids are always possible
    async fn ack(&mut self, _stream_id: u64, _pull_id: u64) {}
    /// an event has failed along its way and can be considered failed
    /// multiple fails for the same set of ids are always possible
    async fn fail(&mut self, _stream_id: u64, _pull_id: u64) {}

    // connectivity stuff
    /// called when connector lost connectivity
    async fn on_connection_lost(&mut self, _ctx: &mut SourceContext) {}
    /// called when connector re-established connectivity
    async fn on_connection_established(&mut self, _ctx: &mut SourceContext) {}

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
    ctx: SourceContext,
}

impl ChannelSource {
    /// constructor
    pub fn new(ctx: SourceContext, qsize: usize) -> Self {
        let (tx, rx) = bounded(qsize);
        Self { rx, tx, ctx }
    }

    /// get the sender for the source
    /// FIXME: change the name
    pub fn runtime(&self) -> ChannelSourceRuntime {
        ChannelSourceRuntime {
            sender: self.tx.clone(),
            ctx: self.ctx.clone(),
        }
    }
}

///
#[async_trait::async_trait]
pub trait StreamReader: Send {
    /// reads from the source reader
    async fn read(&mut self, stream: u64) -> Result<SourceReply>;
    async fn on_done(&mut self, _stream: u64) -> StreamDone {
        StreamDone::StreamClosed
    }
}

/// FIXME: this needs renaming and docs
#[derive(Clone)]
pub struct ChannelSourceRuntime {
    sender: Sender<SourceReply>,
    ctx: SourceContext,
}

impl ChannelSourceRuntime {
    const READ_TIMEOUT_MS: Duration = Duration::from_millis(100);
    pub(crate) fn register_stream_reader<R>(
        &self,
        stream: u64,
        ctx: &ConnectorContext,
        mut reader: R,
    ) where
        R: StreamReader + 'static + std::marker::Sync,
    {
        let ctx = ctx.clone();
        let tx = self.sender.clone();
        task::spawn(async move {
            if tx.send(SourceReply::StartStream(stream)).await.is_err() {
                error!("[Connector::{}] Failed to start stream", ctx.url);
                return;
            };

            while ctx.quiescence_beacon.continue_reading().await {
                let sc_data = timeout(Self::READ_TIMEOUT_MS, reader.read(stream)).await;

                let sc_data = match sc_data {
                    Err(_) => continue,
                    Ok(Ok(d)) => d,
                    Ok(Err(e)) => {
                        error!("[Connector::{}] reader error: {}", ctx.url, e);
                        break;
                    }
                };
                let last = matches!(&sc_data, SourceReply::EndStream(_));
                if tx.send(sc_data).await.is_err() || last {
                    break;
                };
            }
            if reader.on_done(stream).await == StreamDone::ConnectorClosed {
                if let Err(e) = ctx.notifier.notify().await {
                    error!("[Connector::{}] Failed to notify connector: {}", ctx.url, e);
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
    /// connector url
    pub url: TremorUrl,
    /// The Quiescence Beacon
    pub quiescence_beacon: QuiescenceBeacon,
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

#[allow(clippy::module_name_repetitions)]
pub struct SourceManagerBuilder {
    qsize: usize,
    streams: Streams,
    source_metrics_reporter: SourceReporter,
}

impl SourceManagerBuilder {
    pub fn qsize(&self) -> usize {
        self.qsize
    }

    pub fn spawn<S>(self, source: S, ctx: SourceContext) -> Result<SourceAddr>
    where
        S: Source + Send + 'static,
    {
        let qsize = self.qsize;
        let name = ctx.url.short_id("c-src"); // connector source
        let (source_tx, source_rx) = bounded(qsize);
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
    let preprocessor_names = config.preprocessors.clone().unwrap_or_else(Vec::new);
    let codec_config = config
        .codec
        .clone()
        .unwrap_or_else(|| Either::Left(connector_default_codec.to_string()));
    let streams = Streams::new(connector_uid, codec_config, preprocessor_names)?;

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
    codec_config: Either<String, CodecConfig>,
    preprocessor_names: Vec<String>,
    default: StreamState,
    states: BTreeMap<u64, StreamState>,
}

impl Streams {
    fn new(
        uid: u64,
        codec_config: Either<String, CodecConfig>,
        preprocessor_names: Vec<String>,
    ) -> Result<Self> {
        let default = Self::build_stream(
            uid,
            DEFAULT_STREAM_ID,
            &codec_config,
            preprocessor_names.as_slice(),
        )?;
        Ok(Self {
            uid,
            codec_config,
            preprocessor_names,
            default,
            states: BTreeMap::new(),
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
                self.preprocessor_names.as_slice(),
            )?;
            e.insert(state);
        }
        Ok(())
    }

    fn end_stream(&mut self, stream_id: u64) {
        self.states.remove(&stream_id);
    }

    fn get_or_create_stream(&mut self, stream_id: u64) -> Result<&mut StreamState> {
        let state = if stream_id == DEFAULT_STREAM_ID {
            &mut self.default
        } else {
            match self.states.entry(stream_id) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    let state = Self::build_stream(
                        self.uid,
                        stream_id,
                        &self.codec_config,
                        &self.preprocessor_names,
                    )?;
                    e.insert(state)
                }
            }
        };
        Ok(state)
    }

    fn build_stream(
        connector_uid: u64,
        stream_id: u64,
        codec_config: &Either<String, CodecConfig>,
        preprocessor_names: &[String],
    ) -> Result<StreamState> {
        let codec = codec::resolve(codec_config)?;
        let preprocessors = make_preprocessors(preprocessor_names)?;
        let idgen = EventIdGenerator::with_stream(connector_uid, stream_id);
        Ok(StreamState {
            stream_id,
            idgen,
            codec,
            preprocessors,
        })
    }
}

struct StreamState {
    stream_id: u64,
    idgen: EventIdGenerator,
    codec: Box<dyn Codec>,
    preprocessors: Preprocessors,
}

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
    pipelines_out: Vec<(TremorUrl, pipeline::Addr)>,
    pipelines_err: Vec<(TremorUrl, pipeline::Addr)>,
    streams: Streams,
    metrics_reporter: SourceReporter,
    // `Paused` is used for both explicitly pausing and CB close/open
    // this way we can explicitly resume a Cb triggered source if need be
    // but also an explicitly paused source might receive a Cb open and continue sending data :scream:
    state: SourceState,
    is_transactional: bool,
    connector_channel: Option<Sender<Msg>>,
    expected_drained: usize,
}

impl<S> SourceManager<S>
where
    S: Source,
{
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
            is_transactional,
            connector_channel: None,
            expected_drained: 0,
        }
    }

    /// we wait for control plane messages iff
    ///
    /// - we are paused
    /// - we have some control plane messages (here we don't need to wait)
    /// - if we have no pipelines connected
    fn needs_control_plane_msg(&self) -> bool {
        self.state == SourceState::Paused || !self.rx.is_empty() || self.pipelines_out.is_empty()
    }

    /// returns `Ok(true)` if this source should be terminated
    // FIXME: return meaningful enum
    #[allow(clippy::too_many_lines)]
    async fn control_plane(&mut self) -> Result<bool> {
        use SourceState::{Drained, Draining, Initialized, Paused, Running, Stopped};
        loop {
            if !self.needs_control_plane_msg() {
                return Ok(false);
            }
            if let Ok(source_msg) = self.rx.recv().await {
                match source_msg {
                    SourceMsg::Link {
                        port,
                        mut pipelines,
                    } => {
                        let pipes = if port.eq_ignore_ascii_case(OUT.as_ref()) {
                            &mut self.pipelines_out
                        } else if port.eq_ignore_ascii_case(ERR.as_ref()) {
                            &mut self.pipelines_err
                        } else {
                            error!(
                                "[Source::{}] Tried to connect to invalid port: {}",
                                &self.ctx.url, &port
                            );
                            continue;
                        };
                        for (_, p) in &pipelines {
                            p.send_mgmt(pipeline::MgmtMsg::ConnectInput {
                                input_url: self.ctx.url.clone(),
                                target: ConnectInputTarget::Source(self.addr.clone()),
                                transactional: self.is_transactional,
                            })
                            .await?;
                        }

                        pipes.append(&mut pipelines);
                    }
                    SourceMsg::Unlink { id, port } => {
                        let pipelines = if port.eq_ignore_ascii_case(OUT.as_ref()) {
                            &mut self.pipelines_out
                        } else if port.eq_ignore_ascii_case(ERR.as_ref()) {
                            &mut self.pipelines_err
                        } else {
                            error!(
                                "[Source::{}] Tried to disconnect from an invalid port: {}",
                                &self.ctx.url, &port
                            );
                            continue;
                        };
                        pipelines.retain(|(url, _)| url == &id);
                        if self.pipelines_out.is_empty() && self.pipelines_err.is_empty() {
                            self.source.on_stop(&mut self.ctx).await;
                            return Ok(true);
                        }
                    }
                    SourceMsg::Start if self.state == Initialized => {
                        self.state = Running;
                        self.source.on_start(&mut self.ctx).await;

                        if let Err(e) = self.send_signal(Event::signal_start(self.ctx.uid)).await {
                            error!(
                                "[Source::{}] Error sending start signal: {}",
                                &self.ctx.url, e
                            );
                        }
                    }
                    SourceMsg::Start => {
                        info!(
                            "[Source::{}] Ignoring Start msg in {:?} state",
                            &self.ctx.url, &self.state
                        );
                    }
                    SourceMsg::Resume if self.state == Paused => {
                        self.state = Running;
                        self.source.on_resume(&mut self.ctx).await;
                    }
                    SourceMsg::Resume => {
                        info!(
                            "[Source::{}] Ignoring Resume msg in {:?} state",
                            &self.ctx.url, &self.state
                        );
                    }
                    SourceMsg::Pause if self.state == Running => {
                        // TODO: execute pause strategy chosen by source / connector / configured by user
                        self.state = Paused;
                        self.source.on_pause(&mut self.ctx).await;
                    }
                    SourceMsg::Pause => {
                        info!(
                            "[Source::{}] Ignoring Pause msg in {:?} state",
                            &self.ctx.url, &self.state
                        );
                    }
                    SourceMsg::Stop => {
                        self.state = Stopped;
                        self.source.on_stop(&mut self.ctx).await;
                        return Ok(true);
                    }
                    SourceMsg::Drain(_sender) if self.state == Draining => {
                        info!(
                            "[Source::{}] Ignoring incoming Drain message in {:?} state",
                            &self.ctx.url, &self.state
                        );
                    }
                    SourceMsg::Drain(sender) if self.state == Drained => {
                        if sender.send(Msg::SourceDrained).await.is_err() {
                            error!(
                                "[Source::{}] Error sending SourceDrained message",
                                &self.ctx.url
                            );
                        }
                    }
                    SourceMsg::Drain(drained_sender) => {
                        // At this point the connector has advised all reading from external connections to stop via the `QuiescenceBeacon`
                        // We change the source state to `Draining` and wait for `SourceReply::Empty` as we drain out everything that might be in flight
                        // when reached the `Empty` point, we emit the `Drain` signal and wait for the CB answer (one for each connected pipeline)
                        self.connector_channel = Some(drained_sender);
                        self.state = Draining;
                    }
                    SourceMsg::ConnectionLost => {
                        self.source.on_connection_lost(&mut self.ctx).await;
                    }
                    SourceMsg::ConnectionEstablished => {
                        self.source.on_connection_established(&mut self.ctx).await;
                    }
                    SourceMsg::Cb(CbAction::Fail, id) => {
                        if let Some((stream_id, id)) = id.get_min_by_source(self.ctx.uid) {
                            self.source.fail(stream_id, id).await;
                        }
                    }
                    SourceMsg::Cb(CbAction::Ack, id) => {
                        if let Some((stream_id, id)) = id.get_max_by_source(self.ctx.uid) {
                            self.source.ack(stream_id, id).await;
                        }
                    }
                    SourceMsg::Cb(CbAction::Close, _id) => {
                        // TODO: execute pause strategy chosen by source / connector / configured by user
                        self.source.on_cb_close(&mut self.ctx).await;
                        self.state = Paused;
                    }
                    SourceMsg::Cb(CbAction::Open, _id) => {
                        self.source.on_cb_open(&mut self.ctx).await;
                        self.state = Running;
                    }
                    SourceMsg::Cb(CbAction::Drained(uid), _id) => {
                        // only account for Drained CF which we caused
                        // as CF is sent back the DAG to all destinations
                        if uid == self.ctx.uid {
                            self.expected_drained -= 1;
                            if self.expected_drained == 0 {
                                // we received 1 drain CB event per connected pipeline (hopefully)
                                if let Some(connector_channel) = self.connector_channel.as_ref() {
                                    if connector_channel.send(Msg::SourceDrained).await.is_err() {
                                        error!("[Source::{}] Error sending SourceDrained message to Connector", &self.ctx.url);
                                    }
                                }
                            }
                        }
                    }
                    SourceMsg::Cb(CbAction::None, _id) => {}
                }
            }
        }
    }

    /// send a signal to all connected pipelines
    async fn send_signal(&mut self, signal: Event) -> Result<()> {
        for (_url, addr) in self.pipelines_out.iter().chain(self.pipelines_err.iter()) {
            addr.send(pipeline::Msg::Signal(signal.clone())).await?;
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
                    &self.ctx.url, &port
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
                    if let Some(input) = pipe_url.instance_port() {
                        if let Err(e) = addr
                            .send(pipeline::Msg::Event {
                                input: input.to_string().into(),
                                event: event.clone(),
                            })
                            .await
                        {
                            error!(
                                "[Source::{}] Failed to send event {} to pipeline {}: {}",
                                &self.ctx.url, &event.id, &pipe_url, e
                            );
                            send_error = true;
                        }
                    } else {
                        // INVALID pipeline URL - this should not happen
                        error!(
                            "[Source::{}] Cannot send event to invalid Pipeline URL: {}",
                            &self.ctx.url, &pipe_url
                        );
                        send_error = true;
                    }
                }

                if let Some(input) = last.0.instance_port() {
                    if let Err(e) = last
                        .1
                        .send(pipeline::Msg::Event {
                            input: input.to_string().into(),
                            event,
                        })
                        .await
                    {
                        error!(
                            "[Source::{}] Failed to send event to pipeline {}: {}",
                            &self.ctx.url, &last.0, e
                        );
                        send_error = true;
                    }
                } else {
                    // INVALID pipeline URL - this should not happen
                    error!(
                        "[Source::{}] Cannot send event to invalid Pipeline URL: {}",
                        &self.ctx.url, &last.0
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

    /// the source task
    ///
    /// handling control plane and data plane in a loop
    // TODO: data plane
    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<()> {
        // this one serves as simple counter for our pulls from the source
        // we expect 1 source transport unit (stu) per pull, so this counter is equivalent to a stu counter
        // it is not unique per stream only, but per source
        let mut pull_counter: u64 = 0;
        loop {
            // FIXME: change reply from true/false to something descriptive like enum {Stop Continue} or the rust controlflow thingy
            if self.control_plane().await? {
                // source has been stopped, lets stop running here
                return Ok(());
            }

            if self.state.should_pull_data() && !self.pipelines_out.is_empty() {
                match self.source.pull_data(pull_counter, &self.ctx).await {
                    Ok(SourceReply::Data {
                        origin_uri,
                        data,
                        meta,
                        stream,
                        port,
                    }) => {
                        let mut ingest_ns = nanotime();
                        let stream_state = self.streams.get_or_create_stream(stream)?; // we fail if we cannot create a stream (due to misconfigured codec, preprocessors, ...) (should not happen)
                        let results = build_events(
                            &self.ctx.url,
                            stream_state,
                            &mut ingest_ns,
                            pull_counter,
                            origin_uri,
                            port.as_ref(),
                            data,
                            &meta.unwrap_or_else(Value::object),
                            self.is_transactional,
                        );
                        if results.is_empty() {
                            if let Err(e) = self
                                .source
                                .on_no_events(pull_counter, stream, &self.ctx)
                                .await
                            {
                                error!(
                                    "[Source::{}] Error on no events callback: {}",
                                    &self.ctx.url, e
                                );
                            }
                        } else {
                            let error = self.route_events(results).await;
                            if error {
                                self.source.fail(stream, pull_counter).await;
                            }
                        }
                    }
                    Ok(SourceReply::BatchData {
                        origin_uri,
                        batch_data,
                        stream,
                        port,
                    }) => {
                        let mut ingest_ns = nanotime();
                        let stream_state = self.streams.get_or_create_stream(stream)?; // we only error here due to misconfigured codec etc
                        let connector_url = &self.ctx.url;

                        let mut results = Vec::with_capacity(batch_data.len()); // assuming 1:1 mapping
                        for (data, meta) in batch_data {
                            let mut events = build_events(
                                connector_url,
                                stream_state,
                                &mut ingest_ns,
                                pull_counter,
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
                                .on_no_events(pull_counter, stream, &self.ctx)
                                .await
                            {
                                error!(
                                    "[Source::{}] Error on no events callback: {}",
                                    &self.ctx.url, e
                                );
                            }
                        } else {
                            let error = self.route_events(results).await;
                            if error {
                                self.source.fail(stream, pull_counter).await;
                            }
                        }
                    }
                    Ok(SourceReply::Structured {
                        origin_uri,
                        payload,
                        stream,
                        port,
                    }) => {
                        let ingest_ns = nanotime();
                        let stream_state = self.streams.get_or_create_stream(stream)?;
                        let event = build_event(
                            stream_state,
                            pull_counter,
                            ingest_ns,
                            payload,
                            origin_uri,
                            self.is_transactional,
                        );
                        let error = self.route_events(vec![(port.unwrap_or(OUT), event)]).await;
                        if error {
                            self.source.fail(stream, pull_counter).await;
                        }
                    }
                    Ok(SourceReply::StartStream(stream_id)) => {
                        self.streams.start_stream(stream_id)?;
                    } // failing here only due to misconfig, in that case, bail out, #yolo
                    Ok(SourceReply::EndStream(stream_id)) => self.streams.end_stream(stream_id),
                    Ok(SourceReply::Empty(wait_ms)) => {
                        if self.state == SourceState::Draining {
                            // this source has been fully drained
                            self.state = SourceState::Drained;
                            // send Drain signal
                            let signal = Event::signal_drain(self.ctx.uid);
                            if let Err(e) = self.send_signal(signal).await {
                                error!(
                                    "[Source::{}] Error sending DRAIN signal: {}",
                                    &self.ctx.url, e
                                );
                            }
                            // if we get a disconnect in between we might never receive every drain CB
                            // but we will stop everything forcefully after a certain timeout at some point anyways
                            // TODO: account for all connector uids that sent some Cb to us upon startup or so, to get the real number to wait for
                            // otherwise there are cases (branching etc. where quiescence also would be too quick)
                            self.expected_drained =
                                self.pipelines_err.len() + self.pipelines_out.len();
                        } else {
                            // wait for the given ms
                            task::sleep(Duration::from_millis(wait_ms)).await;
                        }
                    }
                    Err(e) => {
                        warn!("[Source::{}] Error pulling data: {}", &self.ctx.url, e);
                        // TODO: increment error metric
                        // FIXME: emit event to err port
                    }
                }
                pull_counter += 1;
            }
        }
    }
}

/// source manager functions moved out

/// build any number of `Event`s from a given Source Transport Unit (`data`)
/// preprocessor or codec errors are turned into events to the ERR port of the source/connector
#[allow(clippy::too_many_arguments)]
fn build_events(
    url: &TremorUrl,
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
        url,
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
                    Err(Some(e)) => (ERR, make_error(url, &e, stream_state.stream_id, pull_id)),
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
            let err_payload = make_error(url, &e, stream_state.stream_id, pull_id);
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

fn make_error(
    connector_url: &TremorUrl,
    error: &Error,
    stream_id: u64,
    pull_id: u64,
) -> EventPayload {
    let e_string = error.to_string();
    let data = literal!({
        "error": e_string.clone(),
        "source": connector_url.to_string(),
        "stream_id": stream_id,
        "pull_id": pull_id
    });
    let meta = literal!({ "error": e_string });
    EventPayload::from(ValueAndMeta::from_parts(data, meta))
}

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
