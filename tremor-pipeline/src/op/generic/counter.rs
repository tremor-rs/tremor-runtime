// Copyright 2018-2020, Wayfair GmbH
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
pub struct Counter {}

op!(CounterFactory(_node) {
    Ok(Box::new(Counter{}))
});

#[allow(unused_mut)]
impl Operator for Counter {
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(Cow<'static, str>, Event)>> {
        Ok(vec![("out".into(), event)])
    }

    // TODO replace on_event with this
    fn on_event2(
        &mut self,
        _port: &str,
        state: &mut StateObject,
        event: Event,
    ) -> Result<Vec<(Cow<'static, str>, Event)>> {
        let mut count_tracker = Value::from(1 as u64);
        // TODO remove unwrap
        if let Some(count) = state.as_object_mut().unwrap().get_mut("count") {
            if let Some(n) = count.as_u64() {
                count_tracker = Value::from(n + 1);
                *count = count_tracker.clone();
            }
        } else {
            state
                .as_object_mut()
                .unwrap() // TODO remove
                .insert("count".into(), count_tracker.clone());
        }

        //dbg!(&state);

        let (value, _) = event.data.parts();
        // TODO remove
        //dbg!(&value);

        // TODO build data.map() functionality with rentals to avoid the clone here
        *value = Value::from(hashmap! {
            "count".into() => count_tracker,
            "event".into() => value.clone_static(),
        });

        Ok(vec![("out".into(), event)])
    }
}
