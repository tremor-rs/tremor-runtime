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

//! Keeps track of the number of events as they come and emits the current count out alongside the event. The output is a record of the form `{"count": n, "event": event}`, where `n` is the current count and `event` is the original event.
//!
//! The counter starts when the first event comes through and begins from 1.
//!
//! This operator preserves event metadata.
//!
//! **Outputs**:
//!
//! - `out`
//!
//! **Example**:
//!
//! ```tremor
//! define operator counter from generic::counter;
//! ```

use crate::op::prelude::*;
use tremor_script::prelude::*;

#[derive(Debug, Clone)]
// TODO add seed value and field name as config items
struct Counter {}

op!(CounterFactory(_uid, _node) {
    Ok(Box::new(Counter{}))
});

impl Operator for Counter {
    fn on_event(
        &mut self,
        _uid: OperatorUId,
        _port: &Port<'static>,
        state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        if state.is_null() {
            *state = Value::from(1_u64);
        } else {
            *state = Value::from(state.as_u64().ok_or("Expected number for state")? + 1);
        }

        event.data.rent_mut(|data| {
            let (v, _) = data.parts_mut();
            let mut h = Value::object_with_capacity(2);
            std::mem::swap(&mut h, v);
            v.try_insert("count", state.clone());
            v.try_insert("event", h);
        });
        Ok(event.into())
    }
}
