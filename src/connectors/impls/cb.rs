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
use async_std::fs::File;
use async_std::io::ReadExt;
use tremor_common::asy::file::open;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// path to file to load data from
    path: Option<String>,

    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    // timeout in millis
    #[serde(default = "default_timeout")]
    timeout: u64,

    // only expect the latest event to be acked, the earliest to be failed
    #[serde(default = "default_expect_batched")]
    expect_batched: bool,
}

fn default_chunk_size() -> usize {
    // equals default chunk size for BufReader
    8 * 1024
}

/// 10 seconds
fn default_timeout() -> u64 {
    10_000
}

fn default_expect_batched() -> bool {
    false
}

impl ConfigImpl for Config {}

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
pub struct Cb {
    config: Config,
}

#[async_trait::async_trait()]
impl Connector for Cb {
    fn default_codec(&self) -> &str {
        "json"
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = CbSource::from_config(&self.config, source_context.alias()).await?;
        let source_addr = builder.spawn(source, source_context)?;
        Ok(Some(source_addr))
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

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "cb".into()
    }

    async fn from_config(
        &self,
        alias: &str,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw) = config {
            let config = Config::new(raw)?;
            Ok(Box::new(Cb { config }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
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

#[derive(Default, Debug)]
struct ReceivedCbs {
    ack: Vec<u64>,  // collect ids of acks
    fail: Vec<u64>, // collect ids of fails
    trigger: u64,   // counter
    restore: u64,   // counter
}

impl ReceivedCbs {
    fn count(&self) -> usize {
        self.ack.len() + self.fail.len()
    }

    fn max(&self) -> Option<u64> {
        self.ack
            .iter()
            .copied()
            .max()
            .max(self.fail.iter().copied().max())
    }
}

struct CbSource {
    file: File,
    buf: Vec<u8>,
    num_sent: usize,
    last_sent: u64,
    received_cbs: ReceivedCbs,
    finished: bool,
    config: Config,
    origin_uri: EventOriginUri,
}

impl CbSource {
    async fn from_config(config: &Config, alias: &str) -> Result<Self> {
        if let Some(path) = config.path.as_ref() {
            let file = open(path).await?;
            Ok(Self {
                file,
                buf: vec![0; config.chunk_size],
                num_sent: 0,
                last_sent: 0,
                received_cbs: ReceivedCbs::default(),
                finished: false,
                config: config.clone(),
                origin_uri: EventOriginUri {
                    scheme: String::from("tremor-cb"),
                    host: hostname(),
                    ..EventOriginUri::default()
                },
            })
        } else {
            Err(ErrorKind::InvalidConfiguration(
                alias.to_string(),
                String::from("Missing path key."),
            )
            .into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for CbSource {
    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let bytes_read = self.file.read(&mut self.buf).await?;
        if bytes_read == 0 {
            let wait = 100_u64;
            if self.finished {
                if self.config.timeout == 0 {
                    // timeout reached
                    let max_cb_received = self.received_cbs.max().unwrap_or_default();
                    let cbs_missing = if self.config.expect_batched {
                        max_cb_received < self.last_sent
                    } else {
                        self.received_cbs.count() < self.num_sent
                    };
                    let status = if cbs_missing {
                        // report failures to stderr and exit with 1
                        eprintln!("Expected CB events up to id {}.", self.last_sent);
                        eprintln!("Got acks: {:?}", self.received_cbs.ack);
                        eprintln!("Got fails: {:?}", self.received_cbs.fail);
                        1
                    } else {
                        0
                    };
                    // TODO: do a proper graceful shutdown
                    // ALLOW: this is the supposed to exit
                    std::process::exit(status);
                } else {
                    self.config.timeout = self.config.timeout.saturating_sub(wait);
                }
                Ok(SourceReply::Empty(wait))
            } else {
                self.finished = true;
                Ok(SourceReply::EndStream {
                    stream: DEFAULT_STREAM_ID,
                    origin_uri: self.origin_uri.clone(),
                    meta: None,
                })
            }
        } else {
            self.num_sent += 1;
            self.last_sent = self.last_sent.max(*pull_id);
            let data = self.buf[0..bytes_read].to_vec();
            Ok(SourceReply::Data {
                data,
                meta: None,
                stream: DEFAULT_STREAM_ID,
                port: None,
                origin_uri: self.origin_uri.clone(),
            })
        }
    }

    async fn on_cb_close(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.received_cbs.trigger += 1;
        Ok(())
    }
    async fn on_cb_open(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.received_cbs.restore += 1;
        Ok(())
    }

    async fn ack(&mut self, _stream_id: u64, pull_id: u64) -> Result<()> {
        self.received_cbs.ack.push(pull_id);
        if self.finished && self.received_cbs.count() == self.num_sent
            || (self.config.expect_batched
                && self
                    .received_cbs
                    .max()
                    .map(|m| m == self.last_sent)
                    .unwrap_or_default())
        {
            eprintln!("All required CB events received.");
        }
        Ok(())
    }
    async fn fail(&mut self, _stream_id: u64, pull_id: u64) -> Result<()> {
        self.received_cbs.fail.push(pull_id);
        if self.finished && self.received_cbs.count() == self.num_sent
            || (self.config.expect_batched
                && self
                    .received_cbs
                    .max()
                    .map(|m| m == self.last_sent)
                    .unwrap_or_default())
        {
            eprintln!("All required CB events received.");
        }
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
