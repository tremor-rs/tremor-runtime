// Copyright 2018-2019, Wayfair GmbH
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

use chrono::{Timelike, Utc};
use std::time::Duration;

pub fn duration_to_millis(at: Duration) -> u64 {
    (at.as_secs() as u64 * 1_000) + (u64::from(at.subsec_nanos()) / 1_000_000)
}

#[allow(clippy::cast_sign_loss)]
pub fn nanotime() -> u64 {
    let now = Utc::now();
    let seconds: u64 = now.timestamp() as u64;
    let nanoseconds: u64 = u64::from(now.nanosecond());

    (seconds * 1_000_000_000) + nanoseconds
}
