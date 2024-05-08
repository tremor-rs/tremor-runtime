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
#![allow(clippy::doc_markdown)]
//! :::info
//!
//! This connector is not intended for production use, but for testing the Tremor runtime itself. To enable it pass `--debug-connectors` to tremor.
//!
//! :::
//!
//!
//! The `null` connector is a connector that is used during development.
//!
//! Events sent to its source are discarded on receive.
//!
//! Events sent to its sink are discarded on receive.
//!
//! ## Configuration
//!
//! There is no specific configuration required for this connector
//!
//! ```tremor title="example.troy"
//!   define connector nil from `null`;
//! ```
//!
//! ## A `null` integration test
//!
//! Although designed for tests written in the rust programming language
//! rather than the integration test framework, it could be used in an
//! integration test as follows:
//!
//! ```tremor title="config.troy"
//! define flow main
//! flow
//!   use integration;
//!
//!   define connector nil from `null`;
//!   create connector nil;
//!
//!   create connector exit from integration::exit;
//!   create connector out from integration::write_file;
//!   create connector in from integration::read_file;
//!   create pipeline main from integration::out_or_exit;
//!
//!   connect /connector/in to /pipeline/main;
//!   connect /pipeline/main to /connector/out;
//!   connect /pipeline/main to /connector/nil;
//!   connect /pipeline/main/exit to /connector/nil;
//!   connect /pipeline/main/exit to /connector/exit;
//!   
//! end;
//! deploy flow main;
//! ```
//!
//! We can then execute the test as follows
//! ```bash
//! $ export TREMOR_PATH=/path/to/tremor-runtime/tremor-script/lib:/path/to/tremor-runtime/tremor-cli/tests/lib
//! $ tremor test integration .
//! ```
//!
//! ## Constraints
//!
//! It is an error to attempt to run any deployment using the `null` connector
//! via the regular server execution command in the `tremor` command line interface
//!
//! ```bash
//! ➜  null-exit git:(main) ✗ tremor server run config.troy
//! tremor version: 0.12
//! tremor instance: tremor
//! rd_kafka version: 0x000002ff, 1.8.2
//! allocator: snmalloc
//! [2022-04-12T15:28:23Z ERROR tremor_runtime::system] Error starting deployment of flow main: Unknown connector type null
//! Error: An error occurred while loading the file `config.troy`: Error deploying Flow main: Unknown connector type null
//! [2022-04-12T15:28:23Z ERROR tremor::server] Error: An error occurred while loading the file `config.troy`: Error deploying Flow main: Unknown connector type null
//! We are SHUTTING DOWN due to errors during initialization!
//! [2022-04-12T15:28:23Z ERROR tremor::server] We are SHUTTING DOWN due to errors during initialization!
//! ```

use crate::{sink::prelude::*, source::prelude::*};

/// The `null` connector is a connector that deletes all incoming data
#[derive(Default, Debug)]
pub struct Builder {}
#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "null".into()
    }

    async fn build(
        &self,
        _alias: &alias::Connector,
        _config: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        Ok(Box::new(Null {}))
    }
}

struct Null {}
#[async_trait::async_trait]
impl Connector for Null {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let source = NullSource {};
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = NullSink {};
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional("json")
    }
}

struct NullSource {}
#[async_trait::async_trait]
impl Source for NullSource {
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
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
    ) -> anyhow::Result<SinkReply> {
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}
