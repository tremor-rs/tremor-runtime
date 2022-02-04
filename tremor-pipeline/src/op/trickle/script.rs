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

use crate::op::prelude::*;
use std::mem;
use tremor_script::{
    highlighter::{self, Highlighter},
    prelude::*,
};

#[derive(Debug)]
pub struct Script {
    pub id: String,
    pub script: tremor_script::Script,
}

impl Operator for Script {
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let context = EventContext::new(event.ingest_ns, event.origin_uri.as_ref());

        let port = event.data.rent_mut(|data| {
            let (unwind_event, event_meta): (&mut Value, &mut Value) = data.parts_mut();

            let value = self.script.run(
                &context,
                AggrType::Emit,
                unwind_event, // event
                state,        // state
                event_meta,   // $
            );

            match value {
                Ok(Return::EmitEvent { port }) => Some(port.map_or(OUT, Cow::from)),

                Ok(Return::Emit { value, port }) => {
                    *unwind_event = value;
                    Some(port.map_or(OUT, Cow::from))
                }
                Ok(Return::Drop) => None,
                Err(e) => {
                    let mut h = highlighter::Dumb::default();
                    h.format_error(&e).ok()?;
                    let s = h.to_string();

                    let mut o = literal!({ "error": s });
                    mem::swap(&mut o, unwind_event);
                    if let Some(error) = unwind_event.as_object_mut() {
                        error.insert("event".into(), o);
                    };
                    Some(ERR)
                }
            }
        });

        Ok(port.map_or_else(EventAndInsights::default, |port| vec![(port, event)].into()))
    }
}
