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

use crate::{
    channel::{bounded, Receiver, Sender},
    errors::empty_error,
    qsize,
};
use crate::{
    errors::Result,
    source::{Source, SourceContext, SourceReply, SourceReplySender, StreamDone, StreamReader},
    Context,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::{task, time::timeout};
/// A source that receives `SourceReply` messages via a channel.
/// It does not handle acks/fails.
///
/// Connector implementations handling their stuff in a separate task can use the
/// channel obtained by `ChannelSource::sender()` to send `SourceReply`s to the
/// runtime.
pub(crate) struct ChannelSource {
    rx: Receiver<SourceReply>,
    tx: SourceReplySender,
    is_connected: Arc<AtomicBool>,
}

impl ChannelSource {
    /// constructor
    #[must_use]
    pub fn new(is_connected: Arc<AtomicBool>) -> Self {
        let (tx, rx) = bounded(qsize());
        Self::from_channel(tx, rx, is_connected)
    }

    /// construct a channel source from a given channel
    #[must_use]
    pub fn from_channel(
        tx: Sender<SourceReply>,
        rx: Receiver<SourceReply>,
        is_connected: Arc<AtomicBool>,
    ) -> Self {
        Self {
            rx,
            tx,
            is_connected,
        }
    }

    /// get the runtime for the source

    #[must_use]
    pub fn runtime(&self) -> ChannelSourceRuntime {
        ChannelSourceRuntime {
            sender: self.tx.clone(),
        }
    }
}

#[async_trait::async_trait()]
impl Source for ChannelSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        Ok(self.rx.recv().await.ok_or_else(empty_error)?)
    }

    /// this source is not handling acks/fails
    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }

    async fn on_cb_restore(&mut self, _ctx: &SourceContext) -> Result<()> {
        // we will only know if we are connected to some pipelines if we receive a CBAction::Restore contraflow event
        // we will not send responses to out/err if we are not connected and this is determined by this variable
        self.is_connected.store(true, Ordering::Release);
        Ok(())
    }
}

/// The runtime driving the `ChannelSource`
#[derive(Clone)]
pub(crate) struct ChannelSourceRuntime {
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
            loop {
                if !ctx.quiescence_beacon().continue_reading().await {
                    debug!("{ctx} quiescing stream {stream}");
                    if let Some(sc_data) = reader.quiesce(stream).await {
                        ctx.swallow_err(tx.send(sc_data).await, "Error Sending StreamFail Message");
                        break;
                    }
                };
                let sc_data = timeout(Self::READ_TIMEOUT_MS, reader.read(stream)).await;

                let sc_data = match sc_data {
                    Err(_) => {
                        continue; // timeout
                    }
                    Ok(Ok(d)) => d,
                    Ok(Err(e)) => {
                        error!("{ctx} Stream {stream} error: {e}");
                        ctx.swallow_err(
                            tx.send(SourceReply::StreamFail(stream)).await,
                            "Error Sending StreamFail Message",
                        );
                        break;
                    }
                };

                let last = matches!(
                    sc_data,
                    SourceReply::EndStream { .. }
                        | SourceReply::Finished
                        | SourceReply::StreamFail(_)
                );

                if tx.send(sc_data).await.is_err() || last {
                    break;
                };
            }

            if reader.on_done(stream).await == StreamDone::ConnectorClosed {
                ctx.swallow_err(
                    ctx.notifier().connection_lost().await,
                    "Failed to notify connector",
                );
            }
        });
    }
}
