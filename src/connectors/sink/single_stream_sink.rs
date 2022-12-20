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

//! Simple Sink implementation for handling a single stream
//!
//! With some shenanigans removed, compared to `ChannelSink`.

use super::channel_sink::{SinkMetaBehaviour, WithMeta};
use crate::{connectors::prelude::*, errors::already_created_error};
use std::marker::PhantomData;
use tokio::task::{self, JoinHandle};
use tremor_common::time::nanotime;

/// simple Sink implementation that is handling only a single stream
pub(crate) struct SingleStreamSink<B>
where
    B: SinkMetaBehaviour + Send + Sync,
{
    _b: PhantomData<B>,
    tx: Sender<SinkData>,
    rx: Option<Receiver<SinkData>>,
    reply_tx: ReplySender,
}

impl SingleStreamSink<WithMeta> {
    /// Constructs a new single stream sink with metadata support enabled
    pub(crate) fn new_with_meta(reply_tx: ReplySender) -> Self {
        SingleStreamSink::new(reply_tx)
    }
}

impl<B> SingleStreamSink<B>
where
    B: SinkMetaBehaviour + Send + Sync,
{
    /// constructs a sink that requires metadata
    pub(crate) fn new(reply_tx: ReplySender) -> Self {
        let (tx, rx) = bounded(qsize());
        Self {
            tx,
            rx: Some(rx),
            reply_tx,
            _b: PhantomData::default(),
        }
    }
    /// hand out a `ChannelSinkRuntime` instance in order to register stream writers
    pub(crate) fn runtime(&mut self) -> Result<SingleStreamSinkRuntime> {
        Ok(SingleStreamSinkRuntime {
            rx: Some(self.rx.take().ok_or_else(already_created_error)?),
            reply_tx: self.reply_tx.clone(),
        })
    }
}

pub(crate) struct SinkData {
    data: Vec<Vec<u8>>,
    meta: Option<SinkMeta>,
    contraflow: Option<ContraflowData>,
    start: u64,
}

/// The runtime receiving and writing data out
pub(crate) struct SingleStreamSinkRuntime {
    rx: Option<Receiver<SinkData>>,
    reply_tx: ReplySender,
}

#[async_trait::async_trait()]
impl SinkRuntime for SingleStreamSinkRuntime {
    async fn unregister_stream_writer(&mut self, _stream: u64) -> Result<()> {
        if let Some(mut rx) = self.rx.take() {
            rx.close();
        }
        Ok(())
    }
}

impl SingleStreamSinkRuntime {
    pub(crate) fn register_stream_writer<W>(
        &mut self,
        stream: u64,
        ctx: &ConnectorContext,
        mut writer: W,
    ) -> Result<JoinHandle<Result<()>>>
    where
        W: StreamWriter + 'static,
    {
        let ctx = ctx.clone();
        let mut rx = self.rx.take().ok_or_else(already_created_error)?;
        let reply_tx = self.reply_tx.clone();
        Ok(task::spawn(async move {
            while let (
                true,
                Some(SinkData {
                    data,
                    meta,
                    contraflow,
                    start,
                }),
            ) = (
                ctx.quiescence_beacon.continue_writing().await,
                rx.recv().await,
            ) {
                let failed = writer.write(data, meta).await.is_err();

                if let Some(cf_data) = contraflow {
                    let reply = if failed {
                        AsyncSinkReply::Fail(cf_data)
                    } else {
                        AsyncSinkReply::Ack(cf_data, nanotime() - start)
                    };
                    if let Err(e) = reply_tx.send(reply) {
                        error!(
                            "[Connector::{}] Error sending async sink reply: {}",
                            ctx.alias, e
                        );
                    }
                };
            }
            let error = match writer.on_done(stream).await {
                Err(e) => Some(e),
                Ok(StreamDone::ConnectorClosed) => ctx.notifier.connection_lost().await.err(),
                Ok(_) => None,
            };
            if let Some(e) = error {
                error!(
                    "[Connector::{}] Error shutting down write half of stream {}: {}",
                    ctx.alias, stream, e
                );
            }
            Result::Ok(())
        }))
    }
}

#[async_trait::async_trait()]
impl<B> Sink for SingleStreamSink<B>
where
    B: SinkMetaBehaviour + Send + Sync,
{
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        let ingest_ns = event.ingest_ns;
        let contraflow = if event.transactional {
            Some(ContraflowData::from(&event))
        } else {
            None
        };
        if let Some(((last_value, last_meta), value_meta_iter)) =
            event.value_meta_iter().split_last()
        {
            // handle first couple of items (if batched)
            for (value, meta) in value_meta_iter {
                let data = serializer.serialize(value, ingest_ns)?;
                let meta = if B::NEEDS_META {
                    Some(meta.clone_static())
                } else {
                    None
                };
                let sink_data = SinkData {
                    data,
                    meta,
                    contraflow: contraflow.clone(), // :scream:
                    start,
                };
                if self.tx.send(sink_data).await.is_err() {
                    error!("{ctx} Error sending to closed stream: 0");
                    return Ok(SinkReply::FAIL);
                }
            }
            // handle last item
            let data = serializer.serialize(last_value, ingest_ns)?;
            let meta = if B::NEEDS_META {
                Some(last_meta.clone_static())
            } else {
                None
            };
            let sink_data = SinkData {
                data,
                meta,
                contraflow,
                start,
            };
            if self.tx.send(sink_data).await.is_err() {
                error!("{ctx} Error sending to closed stream: 0");
                Ok(SinkReply::FAIL)
            } else {
                Ok(SinkReply::NONE)
            }
        } else {
            // no values inside the event, let's ack it, as we never go asynchronous
            Ok(SinkReply::ACK)
        }
    }

    fn asynchronous(&self) -> bool {
        // events are delivered asynchronously on their stream task
        true
    }

    fn auto_ack(&self) -> bool {
        // we handle ack/fail in the asynchronous stream
        false
    }
}
