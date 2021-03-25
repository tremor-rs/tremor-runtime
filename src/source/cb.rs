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
#![cfg(not(tarpaulin_include))]

use crate::source::prelude::*;
use async_std::fs::File as FSFile;
use async_std::io::prelude::*;
use async_std::io::{BufReader, Lines};
use async_std::prelude::*;
use tremor_common::asy::file;

/// Testing source for verifying correct CB Ack/Fail behaviour of the whole downstream pipeline/offramps
///
/// takes events from a file and expects at least one (or exactly one) ack or fail for each event.
///
/// ### Notes:
///
/// * In case the connected pipeline drops events no ack or fail is received with the current runtime.
/// * In case the pipeline branches off, it copies the event and it reaches two offramps, we might receive more than 1 ack or fail for an event with the current runtime.
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// path to file to load data from
    source: String,

    // timeout in millis
    #[serde(default = "default_timeout")]
    timeout: u64,
}

/// 10 seconds
fn default_timeout() -> u64 {
    10_000
}

impl ConfigImpl for Config {}

pub struct Cb {
    onramp_id: TremorURL,
    origin_uri: EventOriginUri,
    config: Config,
}

impl onramp::Impl for Cb {
    fn from_config(id: &TremorURL, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                onramp_id: id.clone(),
                origin_uri: EventOriginUri {
                    uid: 0, // will be filled in start
                    scheme: "tremor-cb".to_owned(),
                    host: hostname(),
                    ..EventOriginUri::default()
                },
                config,
            }))
        } else {
            Err("Missing config for cb onramp".into())
        }
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

    fn missing_ids(&self) -> Vec<u64> {
        let mut all_ids: Vec<u64> = self.ack.iter().chain(self.fail.iter()).copied().collect();
        all_ids.sort_unstable();

        all_ids
            .windows(2)
            .filter_map(|window| match window {
                [low, hi] if hi - low > 1 => Some((low + 1)..*hi),
                _ => None,
            })
            .flatten()
            .collect()
    }
}

#[derive(Debug)]
struct Int {
    id: TremorURL,
    num_sent: usize,
    last_sent: u64, // we assume we have an increasing run of ids without gap, so we only store the latest
    received_cbs: ReceivedCbs,
    lines: Lines<BufReader<FSFile>>,
    timeout: u64,
    finished: bool,
}

impl Int {
    async fn from_config(_uid: u64, onramp_id: TremorURL, config: Config) -> Result<Self> {
        let f = file::open(&config.source).await?;
        let lines = BufReader::new(f).lines();
        Ok(Self {
            id: onramp_id,
            num_sent: 0,
            last_sent: 0,
            received_cbs: ReceivedCbs::default(),
            lines,
            timeout: config.timeout,
            finished: false,
        })
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        match self.lines.next().await {
            Some(Ok(line)) => {
                self.num_sent += 1;
                self.last_sent = self.last_sent.max(id);
                Ok(SourceReply::Data {
                    origin_uri: EventOriginUri {
                        uid: 0,
                        scheme: "tremor-cb".to_owned(),
                        host: hostname(),
                        ..EventOriginUri::default()
                    },
                    data: line.into_bytes(),
                    meta: None,
                    codec_override: None,
                    stream: 0,
                })
            }
            Some(Err(e)) => Err(e.into()),
            None => {
                let wait = 100;
                if self.finished {
                    if self.timeout == 0 {
                        // timeout reached check if we received all cb events
                        let status = if self.received_cbs.count() < self.num_sent {
                            // report failures to stderr and exit with 1
                            let missing_ids = self.received_cbs.missing_ids();
                            eprintln!("Missing CB insights for events: {:?}", missing_ids);
                            1
                        } else {
                            0
                        };

                        // ALLOW: this is the supposed to exit
                        std::process::exit(status);
                    } else {
                        self.timeout = self.timeout.saturating_sub(wait);
                    }
                } else {
                    self.finished = true;
                }

                Ok(SourceReply::Empty(wait))
            }
        }
    }

    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }

    fn id(&self) -> &TremorURL {
        &self.id
    }

    fn trigger_breaker(&mut self) {
        self.received_cbs.trigger += 1;
    }

    fn restore_breaker(&mut self) {
        self.received_cbs.restore += 1;
    }

    fn ack(&mut self, id: u64) {
        self.received_cbs.ack.push(id);
        if self.finished && self.received_cbs.count() == self.num_sent {
            eprintln!("All required CB events received.");
        }
    }

    fn fail(&mut self, id: u64) {
        self.received_cbs.fail.push(id);
        if self.finished && self.received_cbs.count() == self.num_sent {
            eprintln!("All required CB events received.");
        }
    }

    fn is_transactional(&self) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl Onramp for Cb {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        self.origin_uri.uid = config.onramp_uid;
        let source = Int::from_config(
            config.onramp_uid,
            self.onramp_id.clone(),
            self.config.clone(),
        )
        .await?;
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
