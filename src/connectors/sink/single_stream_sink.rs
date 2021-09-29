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

use crate::connectors::{sink::SinkReply, ConnectorContext, StreamDone};
use crate::errors::Result;
use async_std::{
    channel::{bounded, Receiver, Sender},
    task,
};
use tremor_common::time::nanotime;

use super::{AsyncSinkReply, EventCfData, Sink, StreamWriter};

pub struct SingleStreamSink {
    tx: Sender<SinkData>,
    rx: Receiver<SinkData>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl SingleStreamSink {
    pub fn new(qsize: usize, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let (tx, rx) = bounded(qsize);
        Self { tx, rx, reply_tx }
    }
    /// hand out a `ChannelSinkRuntime` instance in order to register stream writers
    pub fn runtime(&self) -> SingleStreamSinkRuntime {
        SingleStreamSinkRuntime {
            rx: self.rx.clone(),
            reply_tx: self.reply_tx.clone(),
        }
    }
}

pub(crate) struct SinkData {
    data: Vec<Vec<u8>>,
    contraflow: Option<EventCfData>,
    start: u64,
}

#[derive(Clone)]
pub struct SingleStreamSinkRuntime {
    rx: Receiver<SinkData>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl SingleStreamSinkRuntime {
    pub(crate) fn register_stream_writer<W>(
        &self,
        stream: u64,
        ctx: &ConnectorContext,
        mut writer: W,
    ) where
        W: StreamWriter + 'static,
    {
        let ctx = ctx.clone();
        let rx = self.rx.clone();
        let reply_tx = self.reply_tx.clone();
        task::spawn(async move {
            while let (
                true,
                Ok(SinkData {
                    data,
                    contraflow,
                    start,
                }),
            ) = (
                ctx.quiescence_beacon.continue_writing().await,
                rx.recv().await,
            ) {
                let failed = writer.write(data).await.is_err();

                if let Some(cf_data) = contraflow {
                    let reply = if failed {
                        AsyncSinkReply::Fail(cf_data)
                    } else {
                        AsyncSinkReply::Ack(cf_data, nanotime() - start)
                    };
                    if let Err(e) = reply_tx.send(reply).await {
                        error!(
                            "[Connector::{}] Error sending async sink reply: {}",
                            ctx.url, e
                        );
                    }
                };
            }
            let error = match writer.on_done(stream).await {
                Err(e) => Some(e),
                Ok(StreamDone::ConnectorClosed) => ctx.notifier.notify().await.err(),
                Ok(_) => None,
            };
            if let Some(e) = error {
                error!(
                    "[Connector::{}] Error shutting down write half of stream {}: {}",
                    ctx.url, stream, e
                );
            }
            Result::Ok(())
        });
    }
}
#[async_trait::async_trait()]
impl Sink for SingleStreamSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        ctx: &super::SinkContext,
        serializer: &mut super::EventSerializer,
        start: u64,
    ) -> super::ResultVec {
        let ingest_ns = event.ingest_ns;
        let contraflow = if event.transactional {
            Some(EventCfData::from(&event))
        } else {
            None
        };
        let mut res = Vec::with_capacity(event.len());
        for value in event.value_iter() {
            let data = serializer.serialize(value, ingest_ns)?;
            let sink_data = SinkData {
                data,
                contraflow: contraflow.clone(), // :scream:
                start,
            };
            res.push(if self.tx.send(sink_data).await.is_err() {
                error!("[Sink::{}] Error sending to closed stream: 0", &ctx.url);
                SinkReply::Fail
            } else {
                SinkReply::None
            });
        }
        Ok(res)
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
