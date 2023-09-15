// Copyright 2020-2021, The Tremor Team
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
use std::time::SystemTime;

/// Get a nanosecond timestamp
///
/// # Panics
/// if we travled to the past
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn nanotime() -> u64 {
    // TODO we want to turn this into u128 eventually
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        // ALLOW: If this happens, now() is BEFORE the unix epoch, this is so bad panicing is the least of our problems
        .expect("Our time was before the unix epoc, this is really bad!")
        .as_nanos() as u64
}
