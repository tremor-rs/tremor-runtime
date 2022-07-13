// Copyright 2022, The Tremor Team
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

//! A connector that never sends any events through its source and swallows all events with its sink.
//!
//! This connector is only used for testing purposes.

use crate::connectors::prelude::*;

#[derive(Default, Debug)]
pub(crate) struct Builder {}
#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "null".into()
    }

    async fn build(
        &self,
        _alias: &ConnectorAlias,
        _config: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        Ok(Box::new(Null {}))
    }
}

struct Null {}
#[async_trait::async_trait]
impl Connector for Null {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = NullSource {};
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = NullSink {};
        builder.spawn(sink, sink_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional("json")
    }
}

struct NullSource {}
#[async_trait::async_trait]
impl Source for NullSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        Ok(SourceReply::Finished)
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

struct NullSink {}
#[async_trait::async_trait]
impl Sink for NullSink {
    async fn on_event(
        &mut self,
        _input: &str,
        _event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}
