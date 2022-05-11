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
/// Issueing `CB::Close` message when the `cap` value is reached and `CB::Open` message when we fall back below it
#[derive(Debug, Clone)]
pub(crate) struct ConcurrencyCap {
    cap: usize,
    reply_tx: Sender<AsyncSinkReply>,
    counter: Arc<AtomicUsize>,
}

impl ConcurrencyCap {
    pub(crate) fn new(cap: usize, reply_tx: Sender<AsyncSinkReply>) -> Self {
        Self {
            cap,
            reply_tx,
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[cfg(test)]
    fn get_counter(&self) -> usize {
        self.counter.load(Ordering::Acquire)
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
                .await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::channel::bounded;

    #[async_std::test]
    async fn concurrency_cap() -> Result<()> {
        let (tx, rx) = bounded(64);
        let cap = ConcurrencyCap::new(2, tx);
        let event1 = Event::default();
        let guard1 = cap.inc_for(&event1).await?;
        assert_eq!(1, cap.get_counter());
        assert!(rx.is_empty());
        let guard2 = cap.inc_for(&event1).await?;
        assert_eq!(2, cap.get_counter());
        assert!(rx.is_empty());
        // if we exceed the maximum concurrency, we issue a CB Close
        let guard3 = cap.inc_for(&event1).await?;
        assert_eq!(3, cap.get_counter());
        let reply = rx.try_recv()?;
        assert!(matches!(reply, AsyncSinkReply::CB(_, CbAction::Close)));
        // one more will not send another Close
        let guard4 = cap.inc_for(&event1).await?;
        assert_eq!(4, cap.get_counter());
        assert!(rx.is_empty());

        // concurrent tasks are finished
        drop(guard4);
        assert_eq!(3, cap.get_counter());
        assert!(rx.is_empty());
        drop(guard3);
        assert_eq!(2, cap.get_counter());
        assert!(rx.is_empty());
        // when we are below the configured threshold, we issue an Open
        drop(guard2);
        assert_eq!(1, cap.get_counter());
        let reply = rx.try_recv()?;
        assert!(matches!(reply, AsyncSinkReply::CB(_, CbAction::Open)));
        drop(guard1);
        assert_eq!(0, cap.get_counter());
        assert!(rx.is_empty());
        Ok(())
    }
}
