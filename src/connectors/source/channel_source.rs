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

use crate::connectors::source::{
    Source, SourceContext, SourceReply, SourceReplySender, StreamDone, StreamReader,
    DEFAULT_POLL_INTERVAL,
};
use crate::connectors::Context;
use crate::errors::Result;
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use async_std::prelude::*;
use async_std::task;
use std::time::Duration;
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
    #[must_use]
    pub fn new(qsize: usize) -> Self {
        let (tx, rx) = bounded(qsize);
        Self::from_channel(tx, rx)
    }

    /// construct a channel source from a given channel
    #[must_use]
    pub fn from_channel(tx: Sender<SourceReply>, rx: Receiver<SourceReply>) -> Self {
        Self { rx, tx }
    }

    /// get the runtime for the source

    #[must_use]
    pub fn runtime(&self) -> ChannelSourceRuntime {
        ChannelSourceRuntime {
            sender: self.tx.clone(),
        }
    }
}

/// The runtime driving the `ChannelSource`
#[derive(Clone)]
pub struct ChannelSourceRuntime {
    sender: Sender<SourceReply>,
}

impl ChannelSourceRuntime {
    pub(crate) fn new(source_tx: Sender<SourceReply>) -> Self {
        Self { sender: source_tx }
    }
}

impl ChannelSourceRuntime {
    const READ_TIMEOUT_MS: Duration = Duration::from_millis(100);
    pub(crate) fn register_stream_reader<R, C>(&self, stream: u64, ctx: &C, mut reader: R)
    where
        R: StreamReader + Send + Sync + 'static,
        C: Context + Send + Sync + 'static,
    {
        let ctx = ctx.clone();
        let tx = self.sender.clone();
        task::spawn(async move {
            while ctx.quiescence_beacon().continue_reading().await {
                let sc_data = reader.read(stream).timeout(Self::READ_TIMEOUT_MS).await;

                let sc_data = match sc_data {
                    Err(_) => continue,
                    Ok(Ok(d)) => d,
                    Ok(Err(e)) => {
                        error!("{} Stream {} error: {}", &ctx, &stream, e);
                        ctx.log_err(
                            tx.send(SourceReply::StreamFail(stream)).await,
                            "Error Sending StreamFail Message",
                        );
                        break;
                    }
                };
                let last = matches!(&sc_data, SourceReply::EndStream { .. });
                if tx.send(sc_data).await.is_err() || last {
                    break;
                };
            }
            if reader.on_done(stream).await == StreamDone::ConnectorClosed {
                ctx.log_err(ctx.notifier().notify().await, "Failed to notify connector");
            }
        });
    }
}

#[async_trait::async_trait()]
impl Source for ChannelSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
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

    fn asynchronous(&self) -> bool {
        true
    }
}
