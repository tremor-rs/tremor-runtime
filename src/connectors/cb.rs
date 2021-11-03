use tremor_pipeline::CbAction;

// Copyright 2021, The Tremor Team
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
use crate::connectors::prelude::*;
use value_trait::ValueAccess;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// path to file to load data from
    path: Option<String>,

    // timeout in millis
    #[serde(default = "default_timeout")]
    timeout: u64,

    // only expect the latest event to be acked, the earliest to be failed
    #[serde(default = "default_expect_batched")]
    expect_batched: bool,
}

/// 10 seconds
fn default_timeout() -> u64 {
    10_000
}

fn default_expect_batched() -> bool {
    false
}

/// Testing connector for verifying correct CB Ack/Fail behaviour of the whole downstream pipeline/connectors
/// and for triggering custom cb (circuit breaker open/close) or gd (guaranteed delivery ack/fail) contraflow events.
///
/// Source: takes events from a file and expects at least one (or exactly one) ack or fail for each event.
/// Sink: expects a `"cb"` array or string in the event payload or metadata and reacts with the given event
///       (possible values: "ack", "fail", "open", "close", "trigger", "restore")
///
/// ### Notes:
///
/// * In case the connected pipeline drops events no ack or fail is received with the current runtime.
/// * In case the pipeline branches off, it copies the event and it reaches two offramps, we might receive more than 1 ack or fail for an event with the current runtime.
pub struct Cb {}

#[async_trait::async_trait()]
impl Connector for Cb {
    fn default_codec(&self) -> &str {
        "json"
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    async fn create_source(
        &mut self,
        _source_context: SourceContext,
        _builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        Ok(None)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = CbSink {};
        let sink_addr = builder.spawn(sink, sink_context)?;
        Ok(Some(sink_addr))
    }
}

pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        _id: &TremorUrl,
        _config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(Cb {}))
    }
}

struct CbSink {}

impl CbSink {
    const CB: &'static str = "cb";
}

#[async_trait::async_trait()]
impl Sink for CbSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (value, meta) in event.value_meta_iter() {
            if let Some(cb) = meta.get(Self::CB).or_else(|| value.get(Self::CB)) {
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

                // Acknowledgement tracking
                let ack = if cb_cmds.contains(&"ack".to_string()) {
                    SinkAck::Ack
                } else if cb_cmds.contains(&"fail".to_string()) {
                    SinkAck::Fail
                } else {
                    SinkAck::None
                };

                // Circuit breaker tracking
                let cb = if cb_cmds.contains(&"close".to_string())
                    || cb_cmds.contains(&"trigger".to_string())
                {
                    CbAction::Close
                } else if cb_cmds.contains(&"open".to_string())
                    || cb_cmds.contains(&"restore".to_string())
                {
                    CbAction::Open
                } else {
                    CbAction::None
                };
                return Ok(SinkReply { ack, cb });
            }
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

struct CbSource {}

#[async_trait::async_trait()]
impl Source for CbSource {
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        // FIXME: implement
        todo!()
    }

    fn is_transactional(&self) -> bool {
        true
    }
}
