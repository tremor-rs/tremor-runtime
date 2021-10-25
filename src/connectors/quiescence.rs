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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use abi_stable::{StableAbi, std_types::RArc};

/// use this beacon to check if tasks reading or writing from external connections should stop
#[repr(C)]
#[derive(Clone, Debug, StableAbi)]
#[allow(clippy::module_name_repetitions)]
pub struct QuiescenceBeacon {
    read: RArc<AtomicBool>,
    write: RArc<AtomicBool>,
}

impl Default for QuiescenceBeacon {
    fn default() -> Self {
        Self {
            read: RArc::new(AtomicBool::new(true)),
            write: RArc::new(AtomicBool::new(true)),
        }
    }
}

impl QuiescenceBeacon {
    /// returns `true` if consumers should continue reading
    /// doesn't return untill the beacon is unpaused
    pub async fn continue_reading(&self) -> bool {
        // FIXME: implement pausing block via a notifier so
        // that continue_reading allows for pausing the reader
        self.read.load(Ordering::Relaxed)
    }

    /// returns `true` if consumers should continue writing
    /// doesn't return untill the beacon is unpaused
    pub async fn continue_writing(&self) -> bool {
        // FIXME: implement pausing block via a notifier so
        // that continue_reading allows for pausing the reader
        self.write.load(Ordering::Relaxed)
    }

    /// notify consumers of this beacon that reading should be stopped
    pub fn stop_reading(&mut self) {
        self.read.store(false, Ordering::Relaxed);
    }

    /// notify consumers of this beacon that reading and writing should be stopped
    pub fn full_stop(&mut self) {
        self.read.store(false, Ordering::Relaxed);
        self.write.store(false, Ordering::Relaxed);
    }
}
