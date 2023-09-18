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

use crate::errors::{Error, Result};
use event_listener::Event;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct Inner {
    state: AtomicU32,
    resume_event: Event,
}
impl Inner {
    // different states of the beacon
    const RUNNING: u32 = 0x0;
    const PAUSED: u32 = 0x1;
    const STOP_READING: u32 = 0x2;
    const STOP_ALL: u32 = 0x4;
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            state: AtomicU32::new(Self::RUNNING),
            resume_event: Event::new(),
        }
    }
}
/// use this beacon to check if tasks reading or writing from external connections should stop
#[derive(Debug, Clone, Default)]
#[allow(clippy::module_name_repetitions)]
pub(crate) struct QuiescenceBeacon(Arc<Inner>);

impl QuiescenceBeacon {
    // we have max 2 listeners at a time, checking this beacon
    // the connector itself, the sink and the source of the connector
    const MAX_LISTENERS: usize = 3;

    /// returns `true` if consumers should continue reading
    /// If the connector is paused, it awaits until it is resumed.
    ///
    /// Use this function in asynchronous tasks consuming from external resources to check
    /// whether it should still read from the external resource. This will also pause external consumption if the
    /// connector is paused.
    pub async fn continue_reading(&self) -> bool {
        loop {
            match self.0.state.load(Ordering::Acquire) {
                Inner::RUNNING => break true,
                Inner::PAUSED => {
                    // we wait to be notified
                    // if so, we re-enter the loop to check the new state
                    self.0.resume_event.listen().await;
                }
                _ => break false, // STOP_ALL | STOP_READING | _
            }
        }
    }

    /// Returns `true` if consumers should continue writing.
    /// If the connector is paused, it awaits until it is resumed.
    pub async fn continue_writing(&self) -> bool {
        loop {
            match self.0.state.load(Ordering::Acquire) {
                Inner::RUNNING | Inner::STOP_READING => break true,
                Inner::PAUSED => {
                    self.0.resume_event.listen().await;
                }
                _ => break false, // STOP_ALL | _
            }
        }
    }

    /// notify consumers of this beacon that reading should be stopped
    pub fn stop_reading(&mut self) {
        self.0.state.store(Inner::STOP_READING, Ordering::Release);
        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
    }

    /// pause both reading and writing
    pub fn pause(&mut self) -> Result<()> {
        match self.0.state.compare_exchange(
            Inner::RUNNING,
            Inner::PAUSED,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) | Err(Inner::PAUSED | Inner::RUNNING | Inner::STOP_ALL | Inner::STOP_READING) => {
                Ok(())
            }
            // TODO: should we error on those?
            // Err(Inner::STOP_ALL) => Err(Error::from("failed to pause from state STOP_ALL")),
            // Err(Inner::STOP_READING) => Err(Error::from("failed to pause from state STOP_READING")),
            Err(e) => Err(Error::from(format!("Invalid state {e}"))),
        }?;
        Ok(())
    }

    /// Resume both reading and writing.
    ///
    /// Has no effect if not currently paused.
    pub fn resume(&mut self) -> Result<()> {
        match self.0.state.compare_exchange(
            Inner::PAUSED,
            Inner::RUNNING,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) | Err(Inner::PAUSED | Inner::RUNNING | Inner::STOP_ALL | Inner::STOP_READING) => {
                Ok(())
            }
            // TODO: should we error on those?
            // Err(Inner::STOP_READING) => Err(Error::from("Can't resume from STOP_READING")),
            // Err(Inner::STOP_ALL) => Err(Error::from("Can't resume from STOP_ALL")),
            Err(e) => Err(Error::from(format!("Invalid state {e}"))),
        }?;

        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
        Ok(())
    }

    /// notify consumers of this beacon that reading and writing should be stopped
    pub fn full_stop(&mut self) {
        self.0.state.store(Inner::STOP_ALL, Ordering::Release);
        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Result;
    use futures::pin_mut;
    use std::{task::Poll, time::Duration};
    use tokio::time::timeout;

    #[tokio::test(flavor = "multi_thread")]
    async fn quiescence_pause_resume() -> Result<()> {
        let beacon = QuiescenceBeacon::default();
        let mut ctrl_beacon = beacon.clone();
        assert!(beacon.continue_reading().await);
        assert!(beacon.continue_writing().await);

        ctrl_beacon.pause()?;

        let timeout_ms = Duration::from_millis(50);
        let read_future = beacon.continue_reading();
        let write_future = beacon.continue_writing();
        pin_mut!(read_future);
        pin_mut!(write_future);
        // no progress for reading while being paused
        assert_eq!(futures::poll!(read_future.as_mut()), Poll::Pending);
        // no progress for writing while being paused
        assert_eq!(futures::poll!(write_future.as_mut()), Poll::Pending);

        ctrl_beacon.resume()?;

        // future created during pause will be picked up and completed after resume only
        assert_eq!(futures::poll!(read_future.as_mut()), Poll::Ready(true));
        assert_eq!(futures::poll!(write_future.as_mut()), Poll::Ready(true));

        ctrl_beacon.stop_reading();

        // don't continue reading when stopped reading
        assert_eq!(false, timeout(timeout_ms, beacon.continue_reading()).await?);
        // writing is fine
        assert!(timeout(timeout_ms, beacon.continue_writing()).await?);

        // a resume after stopping reading has no effect
        ctrl_beacon.resume()?;
        // don't continue reading when stopped reading
        assert!(!timeout(timeout_ms, beacon.continue_reading()).await?);
        // writing is still fine
        assert!(timeout(timeout_ms, beacon.continue_writing()).await?);

        ctrl_beacon.full_stop();
        // no reading upon full stop
        assert!(!timeout(timeout_ms, beacon.continue_reading()).await?);
        // no writing upon full stop
        assert!(!timeout(timeout_ms, beacon.continue_writing()).await?);

        // a resume after a full stop has no effect
        ctrl_beacon.resume()?;
        // no reading upon full stop
        assert!(!timeout(timeout_ms, beacon.continue_reading()).await?);
        // no writing upon full stop
        assert!(!timeout(timeout_ms, beacon.continue_writing()).await?);
        Ok(())
    }
}
