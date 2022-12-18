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

pub mod backpressure;
pub mod percentile;
pub mod roundrobin;

pub use backpressure::BackpressureFactory;
pub use percentile::PercentileFactory;
pub use roundrobin::RoundRobinFactory;

use crate::op::prelude::*;
use tremor_script::prelude::*;

/// Returns true if the given `insight` signals a downstream error
///
/// This is the case when:
/// * the event metadata has an `error` field OR
/// * we have a event delivery failure OR
/// * we have a message triggering a circuit breaker OR
/// * the event metadata has a `time` field that exceeds the given `timeout`
fn is_error_insight(insight: &Event, timeout: u64) -> bool {
    let meta = insight.data.suffix().meta();
    meta.get("error").is_some()
        || insight.cb == CbAction::Fail
        || insight.cb == CbAction::Trigger
        || meta.get_u64("time").map_or(false, |v| v > timeout)
}
