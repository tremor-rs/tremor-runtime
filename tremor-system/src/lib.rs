// Copyright 2020-2024, The Tremor Team
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

//! Tremor system definiton

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

/// control plane
pub mod controlplane;
/// dataplane
pub mod dataplane;

/// The Event that carries data through the system
pub mod event;
/// Instance management
pub mod instance;
/// Killswitch
pub mod killswitch;

/// connector messages
pub mod connector;
/// pipelines
pub mod pipeline;

/// default graceful shutdown timeout
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Metrics instance name
pub static mut INSTANCE: &str = "tremor";

/// Default Q Size
static QSIZE: AtomicUsize = AtomicUsize::new(128);

/// Default Q Size
pub fn qsize() -> usize {
    QSIZE.load(Ordering::Relaxed)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_qsize() {
        assert_eq!(qsize(), QSIZE.load(Ordering::Relaxed));
    }
}
