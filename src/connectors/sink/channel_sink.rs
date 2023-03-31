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

//! Sink implementation that keeps track of multiple streams and keeps channels to send to each stream

use crate::channel::{bounded, Receiver, Sender};
use crate::{
    connectors::{prelude::*, Context, StreamDone},
    errors::Result,
};
use bimap::BiMap;
use either::Either;
use hashbrown::HashMap;
use std::{
    hash::Hash,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    task::{self, JoinHandle},
    time::timeout,
};
use tremor_common::{ids::Id, time::nanotime};
use tremor_pipeline::{CbAction, Event, SignalKind};
use tremor_value::Value;
use value_trait::ValueAccess;

/// Behavioral trait for defining if a Channel Sink needs metadata or not
pub(crate) trait SinkMetaBehaviour: Send + Sync {
    /// Does this channel sink need metadata
    const NEEDS_META: bool;
}

/// Marker that this sink doesn't need any metadata
pub(crate) struct NoMeta {}
impl SinkMetaBehaviour for NoMeta {
    const NEEDS_META: bool = false;
}

/// Marker that this sink needs metadata - and thus requires a clone
pub(crate) struct WithMeta {}
impl SinkMetaBehaviour for WithMeta {
    const NEEDS_META: bool = true;
}

#[derive(Debug)]
/// messages a channel sink can receive
pub(crate) enum ChannelSinkMsg<M>
where
    M: Hash + Eq + Send + 'static,
{
    /// add a new stream
    NewStream {
        /// the id of the stream
        stream_id: u64,
        /// stream metadata used for resolving a stream
        meta: Option<M>,
        /// sender to the actual stream handling data
        sender: Sender<SinkData>,
    },
    /// remove the stream
    RemoveStream(u64),
}

/// Metadata for a sink message
pub(crate) type SinkMeta = Value<'static>;

/// some data for a `ChannelSink` stream
#[derive(Clone, Debug)]
pub(crate) struct SinkData {
    /// data to send
    pub(crate) data: Vec<Vec<u8>>,
    /// async reply utils (if required)
    pub(crate) contraflow: Option<(ContraflowData, ReplySender)>,
    /// Metadata for this request
    pub(crate) meta: Option<SinkMeta>,
    /// timestamp of processing start
    pub(crate) start: u64,
}

/// tracking 1 channel per stream
pub(crate) struct ChannelSink<M, F, B>
where
    M: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<M>,
    B: SinkMetaBehaviour,
{
    _b: PhantomData<B>,
    streams_meta: BiMap<M, u64>,
    streams: HashMap<u64, Sender<SinkData>>,
    resolver: F,
    tx: Sender<ChannelSinkMsg<M>>,
    rx: Receiver<ChannelSinkMsg<M>>,
    reply_tx: ReplySender,
    sink_is_connected: Arc<AtomicBool>,
}

impl<T, F> ChannelSink<T, F, NoMeta>
where
    T: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<T>,
{
    /// Construct a new instance that redacts metadata with prepared `rx` and `tx`
    pub(crate) fn from_channel_no_meta(
        resolver: F,
        reply_tx: ReplySender,
        tx: Sender<ChannelSinkMsg<T>>,
        rx: Receiver<ChannelSinkMsg<T>>,
        sink_is_connected: Arc<AtomicBool>,
    ) -> Self {
        ChannelSink::new(resolver, reply_tx, tx, rx, sink_is_connected)
    }
}

impl<T, F> ChannelSink<T, F, WithMeta>
where
    T: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<T>,
{
    /// Construct a new instance of a channel sink with metadata support
    pub(crate) fn new_with_meta(
        resolver: F,
        reply_tx: ReplySender,
        sink_is_connected: Arc<AtomicBool>,
    ) -> Self {
        let (tx, rx) = bounded(qsize());
        ChannelSink::new(resolver, reply_tx, tx, rx, sink_is_connected)
    }
}

impl<T, F, B> ChannelSink<T, F, B>
where
    T: Hash + Eq + Send + 'static,
    F: Fn(&Value<'_>) -> Option<T>,
    B: SinkMetaBehaviour,
{
    /// constructor of a `ChannelSink` that is sending the event metadata to the `StreamWriter`
    /// in case it needs it in the write.
    /// This costs a clone.
    pub(crate) fn new(
        resolver: F,
        reply_tx: ReplySender,
        tx: Sender<ChannelSinkMsg<T>>,
        rx: Receiver<ChannelSinkMsg<T>>,
        sink_is_connected: Arc<AtomicBool>,
    ) -> Self {
        let streams = HashMap::with_capacity(8);
        let streams_meta = BiMap::with_capacity(8);
        Self {
            streams_meta,
            streams,
            resolver,
            tx,
            rx,
            reply_tx,
            _b: PhantomData::default(),
            sink_is_connected,
        }
    }

    /// hand out a `ChannelSinkRuntime` instance in order to register stream writers
    pub(crate) fn runtime(&self) -> ChannelSinkRuntime<T> {
        ChannelSinkRuntime {
            tx: self.tx.clone(),
        }
    }

    /// returns true, if there are no more channels to send stuff to
    fn handle_channels(
        &mut self,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        clean_closed_streams: bool,
    ) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                ChannelSinkMsg::NewStream {
                    stream_id,
                    meta,
                    sender,
                } => {
                    trace!("{ctx} started new stream {stream_id}");
                    self.streams.insert(stream_id, sender);
                    if let Some(meta) = meta {
                        self.streams_meta.insert(meta, stream_id);
                    }
                }
                ChannelSinkMsg::RemoveStream(stream_id) => {
                    self.remove_stream(stream_id);
                    serializer.drop_stream(stream_id);
                }
            }
        }
        // clean out closed streams
        if clean_closed_streams {
            for (stream_id, _) in self.streams.drain_filter(|_k, v| v.is_closed()) {
                self.streams_meta.remove_by_right(&stream_id);
                serializer.drop_stream(stream_id);
            }
        }
        self.streams.is_empty()
    }

    fn remove_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
        self.streams_meta.remove_by_right(&stream_id);
    }

    fn resolve_stream_from_meta(
        &self,
        meta: &Value,
        ctx: &SinkContext,
    ) -> Option<(&u64, &Sender<SinkData>)> {
        let sink_meta = get_sink_meta(meta, ctx)?;
        let stream_meta = (self.resolver)(sink_meta)?;
        let stream_id = self.streams_meta.get_by_left(&stream_meta)?;
        let sender = self.streams.get(stream_id)?;
        Some((stream_id, sender))
    }
}

/// The runtime driving the part of the sink that is receiving data and writing it out
#[derive(Clone)]
pub(crate) struct ChannelSinkRuntime<T>
where
    T: Hash + Eq + Send + 'static,
{
    tx: Sender<ChannelSinkMsg<T>>,
}

#[async_trait::async_trait()]
impl<T> SinkRuntime for ChannelSinkRuntime<T>
where
    T: Hash + Eq + Send + 'static,
{
    /// This will cause the writer to stop as its receiving channel will be closed.
    /// The writer should receive an error when the channel is empty, so we safely drain all messages.
    /// We will get a double `RemoveStream` message, but this is fine
    async fn unregister_stream_writer(&mut self, stream: u64) -> Result<()> {
        Ok(self.tx.send(ChannelSinkMsg::RemoveStream(stream)).await?)
    }
}

impl<T> ChannelSinkRuntime<T>
where
    T: Hash + Eq + Send + 'static,
{
    const RECV_TIMEOUT: Duration = Duration::from_millis(1000);

    pub(crate) fn new(tx: Sender<ChannelSinkMsg<T>>) -> Self {
        Self { tx }
    }

    pub(crate) async fn register_stream_writer<W, C>(
        &self,
        stream: u64,
        connection_meta: Option<T>,
        ctx: &C,
        mut writer: W,
    ) -> JoinHandle<Result<()>>
    where
        W: StreamWriter + 'static,
        C: Context + Send + Sync + 'static,
    {
        let (stream_tx, mut stream_rx) = bounded::<SinkData>(qsize());
        let stream_sink_tx = self.tx.clone();
        let ctx = ctx.clone();
        let tx = self.tx.clone();
        ctx.swallow_err(
            tx.send(ChannelSinkMsg::NewStream {
                stream_id: stream,
                meta: connection_meta,
                sender: stream_tx,
            })
            .await,
            "Error sending NewStream msg to ChannelSink",
        );
        task::spawn(async move {
            // receive loop from channel sink
            while let (true, sinkdata) = (
                ctx.quiescence_beacon().continue_writing().await,
                // we timeout to not hang here but to check the beacon from time to time
                timeout(Self::RECV_TIMEOUT, stream_rx.recv()).await,
            ) {
                match sinkdata {
                    Err(_) => {
                        // timeout, just continue
                        continue;
                    }
                    Ok(Some(SinkData {
                        data,
                        meta,
                        contraflow,
                        start,
                    })) => {
                        let failed = writer.write(data, meta.as_ref()).await.is_err();

                        // send async contraflow insights if requested (only if event.transactional)
                        if let Some((cf_data, sender)) = contraflow {
                            let reply = if failed {
                                AsyncSinkReply::Fail(cf_data)
                            } else {
                                AsyncSinkReply::Ack(cf_data, nanotime() - start)
                            };
                            if let Err(e) = sender.send(reply) {
                                error!("{ctx} Error sending async sink reply: {e}");
                            }
                        }
                        if failed {
                            break;
                        }
                    }
                    Ok(None) => {
                        warn!("{ctx} Error receiving data from ChannelSink");
                        break;
                    }
                }
            }
            let error = match writer.on_done(stream).await {
                Err(e) => Some(e),
                Ok(StreamDone::ConnectorClosed) => ctx.notifier().connection_lost().await.err(),
                Ok(_) => None,
            };
            if let Some(e) = error {
                error!("{ctx} Error shutting down write half of stream {stream}: {e}");
            }
            stream_sink_tx
                .send(ChannelSinkMsg::RemoveStream(stream))
                .await?;
            Result::Ok(())
        })
    }
}

/// Extract sink specific metadata from event metadata
///
/// The general path is `$<CONNECTOR_TYPE>`
/// Example: `$tcp_server`
fn get_sink_meta<'lt, 'value>(
    meta: &'lt Value<'value>,
    ctx: &SinkContext,
) -> Option<&'lt Value<'value>> {
    meta.get(ctx.connector_type().to_string().as_str())
}

#[async_trait::async_trait()]
impl<T, F, B> Sink for ChannelSink<T, F, B>
where
    T: Hash + Eq + Send + Sync,
    F: (Fn(&Value<'_>) -> Option<T>) + Send + Sync,
    B: SinkMetaBehaviour + Send + Sync,
{
    /// Receives events and tries to route them to the correct connection/channel/stream
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        // clean up
        // make sure channels for the given event are added to avoid stupid errors
        // due to channels not yet handled
        let empty = self.handle_channels(ctx, serializer, false);
        if empty {
            // no streams available :sob:
            return Ok(SinkReply {
                ack: SinkAck::Fail,
                cb: CbAction::Trigger,
            });
        }

        let ingest_ns = event.ingest_ns;
        let stream_ids = event.id.get_streams(ctx.uid().id());
        trace!("{ctx} on_event stream_ids: {stream_ids:?}");

        let contraflow_utils = if event.transactional {
            Some((ContraflowData::from(&event), self.reply_tx.clone()))
        } else {
            None
        };

        let mut remove_streams = vec![];
        let mut reply = SinkReply::default();
        for (value, meta) in event.value_meta_iter() {
            let mut errored = false;
            let mut found = false;
            // route based on stream id present in event metadata or in event id (trackign the event origin)
            // resolve by checking meta for sink specific metadata
            // fallback: get all tracked stream_ids for the current connector uid
            let streams = self.resolve_stream_from_meta(meta, ctx).map_or_else(
                || {
                    Either::Right(
                        stream_ids
                            .iter()
                            .filter_map(|sid| self.streams.get(sid).map(|sender| (sid, sender))),
                    )
                },
                |stream| Either::Left(std::iter::once(stream)),
            );

            for (stream_id, sender) in streams {
                trace!("{ctx} Send to stream {stream_id}.");
                let data = serializer.serialize_for_stream(value, ingest_ns, *stream_id)?;
                let meta = if B::NEEDS_META {
                    Some(meta.clone_static())
                } else {
                    None
                };
                let sink_data = SinkData {
                    meta,
                    data,
                    contraflow: contraflow_utils.clone(),
                    start,
                };
                found = true;
                if sender.send(sink_data).await.is_err() {
                    error!("{ctx} Error sending to closed stream {stream_id}.",);
                    remove_streams.push(*stream_id);
                    errored = true;
                }
            }
            if errored || !found {
                debug!("{ctx} No stream found for event: {}", &event.id);
                reply = SinkReply::FAIL;
            }
        }
        for stream_id in remove_streams {
            trace!("{ctx} Removing stream {stream_id}");
            self.remove_stream(stream_id);
            serializer.drop_stream(stream_id);
        }
        Ok(reply) // empty vec in case of success
    }

    async fn on_signal(
        &mut self,
        signal: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
        match signal.kind.as_ref() {
            Some(SignalKind::Tick) => {
                self.handle_channels(ctx, serializer, true);
            }
            Some(SignalKind::Start(_)) => {
                // store that fact that there is something connected to this sink
                self.sink_is_connected.store(true, Ordering::Release);
            }
            _ => {}
        }
        Ok(SinkReply::default())
    }

    fn asynchronous(&self) -> bool {
        // events are delivered asynchronously on their stream tasks
        true
    }

    fn auto_ack(&self) -> bool {
        // we handle ack/fail in the asynchronous streams
        false
    }
}
