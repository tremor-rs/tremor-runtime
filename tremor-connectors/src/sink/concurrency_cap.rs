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

use tremor_system::{controlplane::CbAction, event::Event};

use super::{AsyncSinkReply, ReplySender};
use crate::sink::ContraflowData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Utility for limiting concurrency in a sink to a certain `cap` value
/// Issueing `CB::Close` message when the `cap` value is reached and `CB::Open` message when we fall back below it
#[derive(Debug, Clone)]
pub struct ConcurrencyCap {
    cap: usize,
    reply_tx: ReplySender,
    counter: Arc<AtomicUsize>,
}

impl ConcurrencyCap {
    /// creates a new `ConcurrencyCap` with a given `cap` value and a `reply_tx` to send CB messages
    #[must_use]
    pub fn new(cap: usize, reply_tx: ReplySender) -> Self {
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
    /// # Errors
    /// if we fail to send a CB message
    pub fn inc_for(&self, event: &Event) -> anyhow::Result<CounterGuard> {
        let num = self.counter.fetch_add(1, Ordering::AcqRel);
        let guard = CounterGuard(num, self.clone(), ContraflowData::from(event));
        if num == self.cap {
            // we crossed max - send a close
            self.reply_tx.send(AsyncSinkReply::CB(
                ContraflowData::from(event),
                CbAction::Trigger,
            ))?;
        }
        Ok(guard)
    }

    fn dec_with(&self, cf_data: &ContraflowData) -> anyhow::Result<()> {
        let num = self.counter.fetch_sub(1, Ordering::AcqRel);
        if num == self.cap {
            // we crossed max - send an open
            self.reply_tx
                .send(AsyncSinkReply::CB(cf_data.clone(), CbAction::Restore))?;
        }
        Ok(())
    }
}

/// ensures that we subtract 1 from the counter once this drops
pub struct CounterGuard(usize, ConcurrencyCap, ContraflowData);

impl Drop for CounterGuard {
    fn drop(&mut self) {
        // TODO: move this out of drop here
        if self.1.dec_with(&self.2).is_err() {
            error!("Error sending a CB Open.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::unbounded;

    #[test]
    fn concurrency_cap() -> anyhow::Result<()> {
        let (tx, mut rx) = unbounded();
        let cap = ConcurrencyCap::new(2, tx);
        let event1 = Event::default();
        let guard1 = cap.inc_for(&event1)?;
        assert_eq!(1, cap.get_counter());
        let guard2 = cap.inc_for(&event1)?;
        assert_eq!(2, cap.get_counter());
        // if we exceed the maximum concurrency, we issue a CB Close
        let guard3 = cap.inc_for(&event1)?;
        assert_eq!(3, cap.get_counter());
        let reply = rx.try_recv()?;
        assert!(matches!(reply, AsyncSinkReply::CB(_, CbAction::Trigger)));
        // one more will not send another Close
        let guard4 = cap.inc_for(&event1)?;
        assert_eq!(4, cap.get_counter());

        // concurrent tasks are finished
        drop(guard4);
        assert_eq!(3, cap.get_counter());
        drop(guard3);
        assert_eq!(2, cap.get_counter());
        // when we are below the configured threshold, we issue an Open
        drop(guard2);
        assert_eq!(1, cap.get_counter());
        let reply = rx.try_recv()?;
        assert!(matches!(reply, AsyncSinkReply::CB(_, CbAction::Restore)));
        drop(guard1);
        assert_eq!(0, cap.get_counter());
        Ok(())
    }
}
