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

use crate::connectors::sink::{AsyncSinkReply, ContraflowData};
use crate::errors::Result;
use async_std::channel::Sender;
use async_std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tremor_pipeline::{CbAction, Event};

/// Utility for limiting concurrency in a sink to a certain `cap` value
/// Issueing CB::Close message when the `cap` value is reached and CB::Open message when we fall back below it
#[derive(Debug, Clone)]
pub(crate) struct ConcurrencyCap {
    cap: usize,
    reply_tx: Sender<AsyncSinkReply>,
    counter: Arc<AtomicUsize>,
}

impl ConcurrencyCap {
    pub(crate) fn new(max: usize, reply_tx: Sender<AsyncSinkReply>) -> Self {
        Self {
            cap: max,
            reply_tx,
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// increment the counter and return a guard for safely counting down
    /// wrapped inside an enum to check whether we exceeded the maximum or not
    pub(crate) async fn inc_for(&self, event: &Event) -> Result<CounterGuard> {
        let num = self.counter.fetch_add(1, Ordering::AcqRel);
        let guard = CounterGuard(num, self.clone(), ContraflowData::from(event));
        if num == self.cap {
            // we crossed max - send a close
            self.reply_tx
                .send(AsyncSinkReply::CB(
                    ContraflowData::from(event),
                    CbAction::Close,
                ))
                .await?;
        }
        Ok(guard)
    }

    async fn dec_with(&self, cf_data: &ContraflowData) -> Result<()> {
        if self.counter.fetch_sub(1, Ordering::AcqRel) == self.cap {
            // we crossed max - send an open
            self.reply_tx
                .send(AsyncSinkReply::CB(cf_data.clone(), CbAction::Open))
                .await?
        }
        Ok(())
    }
}

/// ensures that we subtract 1 from the counter once this drops
pub(crate) struct CounterGuard(usize, ConcurrencyCap, ContraflowData);

impl CounterGuard {
    pub(crate) fn num(&self) -> usize {
        self.0
    }
}
impl Drop for CounterGuard {
    fn drop(&mut self) {
        // TODO: move this out of drop here
        if async_std::task::block_on(self.1.dec_with(&self.2)).is_err() {
            // TODO: add a ctx log here
            error!("Error sending a CB Open.");
        }
    }
}
