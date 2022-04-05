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

use event_listener::Event;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use abi_stable::std_types::RBox;
use async_ffi::{BorrowingFfiFuture, FutureExt};

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
pub struct QuiescenceBeacon(Arc<Inner>);

/// `QuiescenceBeacon` is used for the plugin system, so it must be `#[repr(C)]`
/// in order to interact with it. However, since it uses complex types
/// internally, it's easier to just make it available as an opaque type instead,
/// with the help of `sabi_trait`.
#[abi_stable::sabi_trait]
pub trait QuiescenceBeaconOpaque: fmt::Debug + Clone + Send + Sync {
    /// returns `true` if consumers should continue reading
    /// If the connector is paused, it awaits until it is resumed.
    ///
    /// Use this function in asynchronous tasks consuming from external resources to check
    /// whether it should still read from the external resource. This will also pause external consumption if the
    /// connector is paused.
    fn continue_reading(&self) -> BorrowingFfiFuture<'_, bool>;

    /// Returns `true` if consumers should continue writing.
    /// If the connector is paused, it awaits until it is resumed.
    fn continue_writing(&self) -> BorrowingFfiFuture<'_, bool>;

    /// notify consumers of this beacon that reading should be stopped
    fn stop_reading(&mut self);

    /// pause both reading and writing
    fn pause(&mut self);

    /// Resume both reading and writing.
    ///
    /// Has no effect if not currently paused.
    fn resume(&mut self);

    /// notify consumers of this beacon that reading and writing should be stopped
    fn full_stop(&mut self);
}
impl QuiescenceBeacon {
    // we have max 2 listeners at a time, checking this beacon
    // the sink and the source of the connector
    const MAX_LISTENERS: usize = 2;

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
    pub fn pause(&mut self) {
        if self
            .0
            .state
            .compare_exchange(
                Inner::RUNNING,
                Inner::PAUSED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_err()
        {}
    }

    /// Resume both reading and writing.
    ///
    /// Has no effect if not currently paused.
    pub fn resume(&mut self) {
        if self
            .0
            .state
            .compare_exchange(
                Inner::PAUSED,
                Inner::RUNNING,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_err()
        {}
        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
    }

    /// notify consumers of this beacon that reading and writing should be stopped
    pub fn full_stop(&mut self) {
        self.0.state.store(Inner::STOP_ALL, Ordering::Release);
        self.0.resume_event.notify(Self::MAX_LISTENERS); // we might have been paused, so notify here
    }
}
/// Alias for the FFI-safe beacon, boxed
pub type BoxedQuiescenceBeacon = QuiescenceBeaconOpaque_TO<'static, RBox<()>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Result;
    use async_std::prelude::FutureExt as AsyncFutureExt;
    use std::time::Duration;

    #[async_std::test]
    async fn quiescence_pause_resume() -> Result<()> {
        let mut beacon = QuiescenceBeacon::default();
        assert!(beacon.continue_reading().await);
        assert!(beacon.continue_writing().await);

        beacon.pause();

        let timeout_ms = Duration::from_millis(50);
        // no progress for reading while being paused
        assert!(beacon.continue_reading().timeout(timeout_ms).await.is_err());
        // no progress for writing while being paused
        assert!(beacon.continue_writing().timeout(timeout_ms).await.is_err());

        beacon.resume();
        assert!(beacon.continue_reading().timeout(timeout_ms).await?);
        assert!(beacon.continue_writing().timeout(timeout_ms).await?);

        beacon.stop_reading();
        // don't continue reading when stopped reading
        assert_eq!(false, beacon.continue_reading().timeout(timeout_ms).await?);
        // writing is fine
        assert!(beacon.continue_writing().timeout(timeout_ms).await?);

        // a resume after stopping reading has no effect
        beacon.resume();
        // don't continue reading when stopped reading
        assert!(!beacon.continue_reading().timeout(timeout_ms).await?);
        // writing is still fine
        assert!(beacon.continue_writing().timeout(timeout_ms).await?);

        beacon.full_stop();
        // no reading upon full stop
        assert!(!beacon.continue_reading().timeout(timeout_ms).await?);
        // no writing upon full stop
        assert!(!beacon.continue_writing().timeout(timeout_ms).await?);

        // a resume after a full stop has no effect
        beacon.resume();
        // no reading upon full stop
        assert!(!beacon.continue_reading().timeout(timeout_ms).await?);
        // no writing upon full stop
        assert!(!beacon.continue_writing().timeout(timeout_ms).await?);
        Ok(())
    }
}
