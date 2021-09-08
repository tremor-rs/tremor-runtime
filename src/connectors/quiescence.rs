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

/// use this beacon to check if tasks reading or writing from external connections should stop
#[derive(Clone, Debug, Default)]
pub struct QuiescenceBeacon {
    read: Arc<AtomicBool>,
    write: Arc<AtomicBool>,
}

impl QuiescenceBeacon {
    pub fn new() -> Self {
        Self {
            read: Arc::new(AtomicBool::new(true)),
            write: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn continue_reading(&self) -> bool {
        self.read.load(Ordering::Relaxed)
    }

    pub fn continue_writing(&self) -> bool {
        self.write.load(Ordering::Relaxed)
    }

    pub fn close_reading(&mut self) {
        self.read.store(false, Ordering::Relaxed)
    }

    pub fn full_close(&mut self) {
        self.read.store(false, Ordering::Relaxed);
        self.write.store(false, Ordering::Relaxed);
    }
}
