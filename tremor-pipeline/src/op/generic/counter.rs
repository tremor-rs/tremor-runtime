// Copyright 2020, The Tremor Team
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

use crate::op::prelude::*;
use tremor_script::prelude::*;

#[derive(Debug, Clone)]
// TODO add seed value and field name as config items
pub struct Counter {}

op!(CounterFactory(_node) {
    Ok(Box::new(Counter{}))
});

impl Operator for Counter {
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        if state.is_null() {
            *state = Value::from(1 as u64);
        } else {
            *state = Value::from(
                state
                    .as_u64()
                    .ok_or_else(|| Error::from("Expected Some not None for state"))?
                    + 1,
            );
        }

        let (value, _) = event.data.parts();

        *value = Value::from(hashmap! {
            "count".into() => state.clone(),
            // TODO build data.map() functionality with rentals to avoid the clone here
            "event".into() => value.clone_static(),
        });

        Ok(vec![(OUT, event)].into())
    }
}
