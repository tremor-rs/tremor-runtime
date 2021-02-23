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

use simd_json_derive::Serialize;

use crate::sink::prelude::*;
pub struct CB {}

///
/// CB event provoking offramp for testing sources and operators for their handling of CB events
///
/// Put a single string or an array of strings under the `cb` key into the meta or value to trigger the corresponding events.
///
/// Examples: `{"cb": "ack"}` or `{"cb": ["fail", "close"]}`
///
impl offramp::Impl for CB {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(SinkManager::new_box(Self {}))
    }
}

#[async_trait::async_trait]
impl Sink for CB {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &dyn Codec,
        _codec_map: &halfbrown::HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let mut res = Vec::with_capacity(event.len());
        for (value, meta) in event.value_meta_iter() {
            info!(
                "[Sink::CB] {} {}",
                event.id.event_id(),
                value.json_string()?
            );
            if let Some(cb) = meta.get("cb").or_else(|| value.get("cb")) {
                let cb_cmds = if let Some(array) = cb.as_array() {
                    array
                        .iter()
                        .filter_map(|v| v.as_str().map(ToString::to_string))
                        .collect()
                } else if let Some(str) = cb.as_str() {
                    vec![str.to_string()]
                } else {
                    vec![]
                };
                if cb_cmds.contains(&"ack".to_string()) {
                    let mut ack = Event::cb_ack(event.ingest_ns, event.id.clone());
                    ack.origin_uri = event.origin_uri.clone();
                    ack.op_meta = event.op_meta.clone();
                    res.push(sink::Reply::Insight(ack));
                } else if cb_cmds.contains(&"fail".to_string()) {
                    let mut fail = Event::cb_fail(event.ingest_ns, event.id.clone());
                    fail.origin_uri = event.origin_uri.clone();
                    fail.op_meta = event.op_meta.clone();
                    res.push(sink::Reply::Insight(fail));
                }

                if cb_cmds.contains(&"close".to_string())
                    || cb_cmds.contains(&"trigger".to_string())
                {
                    let mut close = Event::cb_trigger(event.ingest_ns);
                    close.origin_uri = event.origin_uri.clone();
                    close.op_meta = event.op_meta.clone();
                    res.push(sink::Reply::Insight(close));
                } else if cb_cmds.contains(&"open".to_string())
                    || cb_cmds.contains(&"restore".to_string())
                {
                    let mut open = Event::cb_restore(event.ingest_ns);
                    open.origin_uri = event.origin_uri.clone();
                    open.op_meta = event.op_meta.clone();
                    res.push(sink::Reply::Insight(open));
                }
            }
        }
        Ok(Some(res))
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        // TODO: add signal reaction via config
        Ok(None)
    }

    #[allow(clippy::clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorURL,
        _codec: &dyn Codec,
        _codec_map: &halfbrown::HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<Reply>,
    ) -> Result<()> {
        Ok(())
    }

    fn is_active(&self) -> bool {
        true
    }

    fn auto_ack(&self) -> bool {
        false
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
