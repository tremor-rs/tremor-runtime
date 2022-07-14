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
#![allow(clippy::doc_markdown)]
//! :::info
//!
//! This connector is not intended for production use, but for testing the Tremor runtime itself. To enable it pass `--debug-connectors` to tremor.
//!
//! :::
//!
//! The `cb` connector is for testing circuit breaker internals.
//!
//! The connector expects a circuit breaker event for each event propagated
//! and then, the latest after a configurable `timeout` has been exceeded,
//! it halts or exits the tremor process and reports on the validation result via stdout.
//!
//! Most of the time it is used to test the behaviour of the whole runtime when it comes to the circuit breaker and guaranteed delivery (ack/fail) mechanisms. To that end use the connector as source and sink of events by connecting your tremor pipeline application with the `in` and `out` port of this connector.
//! It can also be used to test the behaviour of connectors acting as event sinks, checking if they handle all events correctly, in this case connect the `in` port of the connector to test against to the `out` port of the `cb` connector via a pipeline of your choice.
//!
//!
//! ## Ports
//!
//! - `in`: This Connector is expecting events that trigger certain Circuit Breaker and/or Event acknowledge/fail behaviour. See [Expected Input Data](#expected-input-data).
//! - `out`: When configured with [`paths`](#configuration) this connector will emit events on the `out` port.
//!
//! ## Configuration
//!
//! | Config Option    | Description                                                                                                                                                                                         | Possible Values  | Required / Optional                | Default Value                 |
//! |------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|------------------------------------|-------------------------------|
//! | `paths`          | A list of paths to files to load data from. Each file will be split into lines and each line will form one event. Only required for the source part.                                                | array of strings | Only required for the source part. |                               |
//! | `timeout`        | Timeout in nanoseconds to wait for circuit breaker messages after all events from the `paths` files have been sent. After this timeout is expired the Tremor process will be stopped.               | positive integer | optional                           | `10_000_000_000` (10 seconds) |
//! | `expect_batched` | When set to `true` changes verifcation mode for testing applications involving [batching](../operators/batch.md). That means only not all events are required to be acknowledged, only all batches. | boolean          | optional                           | `false`                       |
//!
//! ### Example
//!
//! ```tremor title="cb_example.troy"
//! define flow cb_example
//! flow
//!   use tremor::pipelines;
//!   use std::time::nanos;
//!
//!   define connector my_cb from cb
//!   with
//!     codec = "json",         # Events are line delimited JSON
//!     config = {
//!       "paths": ["in.json"], # Files from which to replay events
//!       "timeout": nanos::from_seconds(1)
//!     }
//!   end;
//!   create connector my_cb;
//!
//!   create pipeline passthrough from pipelines::passthrough;
//!
//!   # connect both the `out` and the `in` port of the connector
//!   # via a simple passthrough pipeline.
//!   # (The ports can also be omitted for brevity)
//!   connect /connector/my_cb/out to /pipeline/passthrough/in;
//!   connect /pipeline/passthrough/out to /connector/my_cb/in;
//! end;
//!
//! deploy flow cb_example;
//! ```
//!
//! ## Expected Data Format
//!
//! This connector expects events that can trigger certain Circuit Breaker and Event acknowledgement behaviour via its `in` port.
//!
//! The event can contain the `cb` specific payload in either the event metadata or the event payload.
//!
//! The expected is a JSON document with a string or array beneath the key `"cb"`.
//!
//! The `"cb"` values can be one of the following (or multiple):
//!
//! | Value           | Description                                                                                                             |
//! |-----------------|-------------------------------------------------------------------------------------------------------------------------|
//! | ack             | Send a runtime-internal message upstream in order to **acknowledge the event**.                                         |
//! | fail            | Send a runtime-internal message upstream in order to **fail the event**.                                                |
//! | close / trigger | *Close* or *trigger* the circuit breaker, so that all upstream connectors stop sending events until it is opened again. |
//! | open / restore  | *Open* or *restore* the circuit breaker, so that all upstream connectors start sending events again.                    |
//!
//! :::info
//!
//! Only 1 value of the pairs `ack` - `fail` and `open` / `restore` and `close` / `trigger` will be considered.
//!
//! :::
//!
//! ### Examples
//!
//! ```js
//! { "cb": "ack" }    // this will acknowledge the event
//! { "cb": "open" }   // this will open the circuit breaker
//! { "cb": ["fail", "trigger"] } // this will fail the event and close the circuit breaker
//! ```
//!
//! ## Example usage
//!
//! | Name                                                                                                                                         | Description                                                              |
//! |----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
//! | [Validate dead ends are dropped](https://github.com/tremor-rs/tremor-runtime/tree/main/tremor-cli/tests/integration/cb-drop-dead-ends)       | All required circuit breaker events are received and processed correctly |
//! | [Validate pipeline to pipeline](https://github.com/tremor-rs/tremor-runtime/tree/main/tremor-cli/tests/integration/cb-pipeline-to-pipeline)  | All required circuit breaker events are received and processed correctly |
//! | [Validate auto-acknowledged sinks](https://github.com/tremor-rs/tremor-runtime/tree/main/tremor-cli/tests/integration/cb-with-auto-ack-sink) | All required circuit breaker events are received and processed correctly |

use crate::connectors::prelude::*;
use crate::system::{KillSwitch, ShutdownMode};
use halfbrown::HashMap;
use std::{path::PathBuf, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader, Lines},
};
use tremor_common::asy::file::open;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// paths to files to load data from
    #[serde(default = "Default::default")]
    paths: Vec<PathBuf>,
    // timeout in nanoseconds
    #[serde(default = "default_timeout")]
    timeout: u64,
    // only expect the latest event to be acked, the earliest to be failed
    #[serde(default = "default_false")]
    expect_batched: bool,
}

/// 10 seconds
fn default_timeout() -> u64 {
    10_000_000_000
}

impl tremor_config::Impl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "cb".into()
    }

    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        raw: &Value,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(raw)?;
        Ok(Box::new(Cb { config }))
    }
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
pub(crate) struct Cb {
    config: Config,
}

#[async_trait::async_trait()]
impl Connector for Cb {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional("json")
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        if self.config.paths.is_empty() {
            return Err(ErrorKind::InvalidConfiguration(
                ctx.alias().to_string(),
                "\"paths\" config missing".to_string(),
            )
            .into());
        }
        let source = CbSource::new(&self.config, ctx.alias(), ctx.killswitch()).await?;
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = CbSink {};
        Ok(Some(builder.spawn(sink, ctx)))
    }
}

struct CbSink {}

#[async_trait::async_trait()]
impl Sink for CbSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        for (value, meta) in event.value_meta_iter() {
            if let Some(cb) = ctx.extract_meta(meta).or_else(|| ctx.extract_meta(value)) {
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
                    CbAction::Trigger
                } else if cb_cmds.contains(&"open".to_string())
                    || cb_cmds.contains(&"restore".to_string())
                {
                    CbAction::Restore
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

#[derive(Default, Debug)]
struct ReceivedCbs {
    ack: HashMap<u64, Vec<u64>>,  // collect ids of acks
    fail: HashMap<u64, Vec<u64>>, // collect ids of fails
    trigger: u64,                 // counter
    restore: u64,                 // counter
}

impl ReceivedCbs {
    fn count(&self) -> usize {
        self.ack.values().map(Vec::len).sum::<usize>()
            + self.fail.values().map(Vec::len).sum::<usize>()
    }

    fn max(&self) -> HashMap<u64, u64> {
        let mut acks = self
            .ack
            .iter()
            .filter_map(|(stream_id, pull_ids)| pull_ids.iter().max().map(|max| (*stream_id, *max)))
            .collect::<HashMap<_, _>>();
        let fails = self.fail.iter().filter_map(|(stream_id, pull_ids)| {
            pull_ids.iter().max().map(|max| (*stream_id, *max))
        });
        // gather max pull_ids from fails and overwrite if bigger
        for (k, v) in fails {
            if let Some(ack) = acks.get_mut(&k) {
                *ack = (*ack).max(v);
            } else {
                acks.insert(k, v);
            }
        }
        acks
    }
}

#[derive(Debug)]
struct CbSource {
    files: Vec<Lines<BufReader<File>>>,
    num_sent: usize,
    last_sent: HashMap<u64, u64>,
    received_cbs: ReceivedCbs,
    finished: bool,
    config: Config,
    origin_uri: EventOriginUri,
    kill_switch: KillSwitch,
}

impl CbSource {
    fn did_receive_all(&self) -> bool {
        let all_received = if self.config.expect_batched {
            let mut received = 0;
            for (stream, max_pull_id) in self.received_cbs.max() {
                if let Some(last_sent_pull_id) = self.last_sent.get(&stream) {
                    received += usize::from(*last_sent_pull_id == max_pull_id);
                }
            }
            received == self.last_sent.len()
        } else {
            self.received_cbs.count() == self.num_sent
        };
        self.finished && all_received
    }
    async fn new(
        config: &Config,
        _alias: &alias::Connector,
        kill_switch: KillSwitch,
    ) -> Result<Self> {
        let mut files = vec![];
        for path in &config.paths {
            let file = open(path).await?;
            files.push(BufReader::new(file).lines());
        }

        Ok(Self {
            files,
            num_sent: 0,
            last_sent: HashMap::new(),
            received_cbs: ReceivedCbs::default(),
            finished: false,
            config: config.clone(),
            origin_uri: EventOriginUri {
                scheme: String::from("tremor-cb"),
                host: hostname(),
                ..EventOriginUri::default()
            },
            kill_switch,
        })
    }
}

#[async_trait::async_trait()]
impl Source for CbSource {
    async fn pull_data(&mut self, pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        if self.files.is_empty() {
            info!("{ctx} finished.");
            self.finished = true;
            let kill_switch = self.kill_switch.clone();

            if self.config.timeout > 0 && !self.did_receive_all() {
                tokio::time::sleep(Duration::from_nanos(self.config.timeout)).await;
            }

            if self.did_receive_all() {
                eprintln!("All required CB events received.");
                eprintln!("Got acks: {:?}", self.received_cbs.ack);
                eprintln!("Got fails: {:?}", self.received_cbs.fail);
            } else {
                // report failures to stderr and exit with 1
                eprintln!("Expected CB events up to id {:?}.", self.last_sent);
                eprintln!("Got acks: {:?}", self.received_cbs.ack);
                eprintln!("Got fails: {:?}", self.received_cbs.fail);
            }
            tokio::task::spawn(async move {
                kill_switch.stop(ShutdownMode::Graceful).await?;
                Result::Ok(())
            });

            Ok(SourceReply::Finished)
        } else {
            let idx: usize = self.num_sent % self.files.len();
            let file = self
                .files
                .get_mut(idx) // this is safe as we do the module above
                .ok_or(ErrorKind::ClientNotAvailable("cb", "No file available"))?;
            let res = if let Some(line) = file.next_line().await? {
                self.num_sent += 1;
                self.last_sent
                    .entry(idx as u64)
                    .and_modify(|last_sent| *last_sent = *last_sent.max(pull_id))
                    .or_insert(*pull_id);

                SourceReply::Data {
                    data: line.into_bytes(),
                    meta: None,
                    stream: Some(idx as u64),
                    port: None,
                    origin_uri: self.origin_uri.clone(),
                    codec_overwrite: None,
                }
            } else {
                // file is exhausted, remove it from our list
                // lines claims to require being used but there are no lines left
                drop(self.files.remove(idx));

                SourceReply::EndStream {
                    stream: idx as u64,
                    origin_uri: self.origin_uri.clone(),
                    meta: None,
                }
            };
            Ok(res)
        }
    }

    async fn on_cb_trigger(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.received_cbs.trigger += 1;
        Ok(())
    }
    async fn on_cb_restore(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.received_cbs.restore += 1;
        Ok(())
    }

    async fn ack(&mut self, stream_id: u64, pull_id: u64, _ctx: &SourceContext) -> Result<()> {
        self.received_cbs
            .ack
            .entry(stream_id)
            .and_modify(|pull_ids| pull_ids.push(pull_id))
            .or_insert_with(|| vec![pull_id]);
        Ok(())
    }

    async fn fail(&mut self, stream_id: u64, pull_id: u64, _ctx: &SourceContext) -> Result<()> {
        self.received_cbs
            .fail
            .entry(stream_id)
            .and_modify(|pull_ids| pull_ids.push(pull_id))
            .or_insert_with(|| vec![pull_id]);
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
