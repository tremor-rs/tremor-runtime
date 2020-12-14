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

#[derive(Debug, Clone, Hash)]
pub struct Passthrough {}

op!(PassthroughFactory (_uid, _node) {
    Ok(Box::new(Passthrough{}))
});

impl Operator for Passthrough {
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        Ok(event.into())
    }
    // this is just returning true
    #[cfg(not(tarpaulin_include))]
    fn skippable(&self) -> bool {
        // ALLOW: This is Ok
        let _ = self;
        true
    }
}
