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

//! The crononome connector generates a periodic flow of events at a configured periodicity
//! based on the modern Gregorian calendar system.
//!
//! As the name suggests - the connector is configured using `cron` like schedules and
//! a user configurable data event provider by authors with the business event which
//! can be used to trigger downstream processing.
//!
//! The connector is particularly useful when using transient or persistent state via the
//! [`kv`](./kv) connector or via the state mechanism to evict and remove `stale` data
//! that is no longer current or relevant to the running system.
//!
//! The connector is also useful for driving periodic events when used in concert with
//! other connectors or pipelines to periodically poll to retrieve meta data updates to
//! merge with in-memory state in the current solution.
//!
//! ## Configuration
//!
//! One or many `cron` like specifications for the event schedule and a user defined data
//! payload.
//!
//! ```tremor title="config.troy"
//! define connector crononome from crononome
//! with
//!   config = {
//!     "entries": [{
//!       "name": "test",                # A symbolic name for this schedule
//!       "expr": "* * * * * *",         # A cron schedule
//!       "payload": {"snot": "badger"}  # A user configurable payload value
//!     }]
//!   }
//! end;
//! ```
//!
//! ## Illustrative example
//!
//! A simple tremor application that issues a single crononome event and uses it to
//! trigger an exit of the tremor runtime - a common quiescence pattern in our tests
//! driven by the crononome in this illustration.
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
//!   define connector crononome from crononome
//!   with
//!     config = {
//!       "entries": [{
//!         "name": "test",
//!         "expr": "* * * * * *",
//!         "payload": {"snot": "badger"}
//!       }]
//!     }
//!   end;
//!
//!   define pipeline main
//!   pipeline
//!     select event.trigger from in into out;
//!   end;
//!
//!   define pipeline exit
//!   pipeline
//!     select {
//!       "exit": 0,
//!       "delay": 1
//!     } from in into out;
//!   end;
//!
//!   create connector file from integration::write_file;
//!   create connector crononome;
//!   create connector exit from connectors::exit;
//!   create pipeline main;
//!   create pipeline exit;
//!
//!   connect /connector/crononome to /pipeline/main;
//!   connect /connector/crononome to /pipeline/exit;
//!   connect /pipeline/main to /connector/file;
//!   connect /pipeline/exit to /connector/exit;
//! end;
//! deploy flow main;
//! ```
//!
//! ## Exercises
//!
//! To further explore the `crononome`:
//!
//! * Modify the application to stop after N events by introducing a counter
//! * Modify the application to trigger a HTTP `GET` request and serialize the response to a log file
//! * Replace the `crononome` with a `crononome` for periodic scheduled calendar driven events

mod handler;

use crate::{connectors::prelude::*, errors::err_connector_def, system::KillSwitch};
use handler::{ChronomicQueue, CronEntryInt};
use serde_yaml::Value as YamlValue;
use tremor_common::time::nanotime;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Cron entries
    pub(crate) entries: Option<YamlValue>,
}

impl tremor_config::Impl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "crononome".into()
    }

    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let raw = Config::new(raw)?;
        let payload = if let Some(yaml_entries) = raw.entries {
            let mut entries = simd_json::to_vec(&yaml_entries)?;
            let tremor_entries = tremor_value::parse_to_value(&mut entries)?;
            Some(tremor_entries.into_static())
        } else {
            None
        };

        let entries = if let Some(Value::Array(entries)) = &payload {
            entries
                .iter()
                .cloned()
                .map(CronEntryInt::try_from)
                .collect::<Result<Vec<CronEntryInt>>>()?
        } else {
            return Err(err_connector_def(id, "missing `entries` array"));
        };

        Ok(Box::new(Crononome { entries }))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Crononome {
    entries: Vec<CronEntryInt>,
}

#[async_trait::async_trait()]
impl Connector for Crononome {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = CrononomeSource::new(self.entries.clone());
        Ok(Some(builder.spawn(source, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}
struct CrononomeSource {
    entries: Vec<CronEntryInt>,
    cq: ChronomicQueue,
    origin_uri: EventOriginUri,
}

impl CrononomeSource {
    fn new(entries: Vec<CronEntryInt>) -> Self {
        Self {
            entries,
            cq: ChronomicQueue::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-crononome".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
        }
    }
}
#[async_trait::async_trait()]
impl Source for CrononomeSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        self.cq = ChronomicQueue::default(); // create a new queue to avoid duplication in case of reconnect
        for entry in &self.entries {
            self.cq.enqueue(entry);
        }
        Ok(true)
    }
    async fn pull_data(&mut self, pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        let mut origin_uri = self.origin_uri.clone();
        if let Some(trigger) = self.cq.wait_for_next().await {
            origin_uri.path.push(trigger.0.clone());

            let mut tr: Value<'static> = Value::object_with_capacity(2);
            tr.try_insert("name", trigger.0.clone());
            if let Some(payload) = trigger.1 {
                tr.try_insert("payload", payload);
            }
            let data = literal!({
                "connector": "crononome",
                "ingest_ns": nanotime(),
                "id": *pull_id,
                "trigger": tr
            });
            let meta = ctx.meta(literal!({
                "trigger": trigger.0
            }));
            Ok(SourceReply::Structured {
                origin_uri,
                payload: (data, meta).into(),
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        } else {
            Ok(SourceReply::Finished)
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
