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

//! The metronome connector generates a periodic flow of events at a configured interval.
//!
//! The connector is particularly useful when using transient or persistent state via the
//! [`kv`](./kv) connector or via the state mechanism to evict and remove `stale` data
//! that is no longer current or relevant to the running system.
//!
//! The connector is also sueful for driving periodic events when used in concert with
//! other connectors or pipelines to periodically poll to retrieve meta data updates to
//! merge with in-memory state in the current solution.
//!
//! ## Configuration
//!
//! An `interval` in nanoseconds is required to use the metronome
//!
//! ```tremor
//!   use std::time::nanos;
//!   define connector every_second from metronome
//!   with
//!      config = {"interval": nanos::from_seconds(1) }
//!   end;
//! ```
//!
//! ## Illustrative example
//!
//! A simple tremor application that issues a single metronome event and uses it to
//! trigger an exit of the tremor runtime - a common quiescence pattern in our tests
//! driven by the metronome in this illustration.
//!
//! A high level summary of this flow's logic:
//!
//! ```mermaid
//! graph LR
//!     A{Metronome} -->|every second| B(triggered events)
//!     B -->|passthrough| C{Standard Output}
//!     B -->|stop server| D{Exit Runtime}
//! ```
//!
//! The application source for this example:
//!
//! ```tremor
//! define flow main
//! flow
//!   use tremor::connectors;
//!   use integration;
//!   use std::time::nanos;
//!
//!   # Define our periodic source of events
//!   define connector every_second from metronome
//!   with
//!     config = {"interval": nanos::from_seconds(1) }
//!   end;
//!  
//!   # Triggered for every periodic event from our metronome
//!   define pipeline main
//!   pipeline
//!     select "triggered" from in into out;
//!   end;
//!
//!   # An `exit` event used in integration tests to stop tremor
//!   define pipeline exit
//!   pipeline    
//!     select { "exit": 0, } from in into out;
//!   end;
//!
//!   create connector exit from connectors::exit;
//!   create connector file from integration::write_file;
//!   create connector metronome;
//!
//!   create pipeline main;
//!   create pipeline exit;
//!
//!   connect /connector/every_second to /pipeline/main;
//!   connect /connector/every_second to /pipeline/exit;
//!   connect /pipeline/main to /connector/file;
//!   connect /pipeline/exit to /connector/exit;
//! end;
//!
//! deploy flow main;
//! ```
//!
//! ## Exercises
//!
//! To further explore the `metronome`:
//!
//! * Modify the application to stop after N events by introducing a counter
//! * Modify the application to trigger a HTTP `GET` request and serialize the response to a log file
//! * Replace the `metronome` with a `crononome` for periodic scheduled calendar driven events

use crate::connectors::prelude::*;
use std::time::Duration;
use tremor_common::time::nanotime;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Interval in nanoseconds
    pub interval: u64,
}

impl tremor_config::Impl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "metronome".into()
    }

    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(raw)?;
        let origin_uri = EventOriginUri {
            scheme: "tremor-metronome".to_string(),
            host: hostname(),
            port: None,
            path: vec![config.interval.to_string()],
        };

        Ok(Box::new(Metronome {
            interval: config.interval,
            origin_uri,
        }))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Metronome {
    interval: u64,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait()]
impl Connector for Metronome {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = MetronomeSource::new(self.interval, self.origin_uri.clone());
        Ok(Some(builder.spawn(source, ctx)))
    }
}

struct MetronomeSource {
    interval_ns: u64,
    next: u64,
    origin_uri: EventOriginUri,
    id: u64,
}

impl MetronomeSource {
    fn new(interval_ns: u64, origin_uri: EventOriginUri) -> Self {
        Self {
            interval_ns,
            next: nanotime() + interval_ns, // dummy placeholer
            origin_uri,
            id: 0,
        }
    }
}

#[async_trait::async_trait()]
impl Source for MetronomeSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        self.next = nanotime() + self.interval_ns;
        Ok(true)
    }
    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let now = nanotime();
        // we need to wait here before we continue to fulfill the interval conditions
        if now < self.next {
            tokio::time::sleep(Duration::from_nanos(self.next - now)).await;
        }
        self.next += self.interval_ns;
        *pull_id = self.id;
        self.id += 1;
        let data = literal!({
            "connector": "metronome",
            "ingest_ns": now,
            "id": *pull_id
        });
        Ok(SourceReply::Structured {
            origin_uri: self.origin_uri.clone(),
            payload: data.into(),
            stream: DEFAULT_STREAM_ID,
            port: None,
        })
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {

    use crate::{config::Reconnect, connectors::prelude::*};

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_config() -> Result<()> {
        let alias = alias::Connector::new("flow", "connector");
        let builder = super::Builder::default();
        let connector_config = super::ConnectorConfig {
            connector_type: builder.connector_type(),
            codec: None,
            config: None,
            preprocessors: None,
            postprocessors: None,
            reconnect: Reconnect::None,
            metrics_interval_s: Some(5),
        };
        let kill_switch = KillSwitch::dummy();
        assert!(matches!(
            builder
                .build(&alias, &connector_config, &kill_switch)
                .await
                .err(),
            Some(Error(ErrorKind::MissingConfiguration(_), _))
        ));
        Ok(())
    }
}
