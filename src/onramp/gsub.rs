// Copyright 2018-2019, Wayfair GmbH
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

//! # Google Pub/Sub Onramp
//!
//! The `gsub` onramp allows receiving events from a GPS topic.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!

use crate::dflt;
use crate::errors::*;
use crate::onramp::prelude::*;

use crate::google::pubsub_api;
use base64;
use serde_yaml::Value;
use std::thread;

pub struct GSub {
    config: Config,
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub service_account: String,
    pub subscription: String,
    #[serde(default = "dflt::d_false")]
    pub sync: bool,
}
impl ConfigImpl for Config {}

impl onramp::Impl for GSub {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for gsub onramp".into())
        }
    }
}

fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();

    let mut id = 0;
    let subscription_name = config.subscription.clone();

    //    thread::spawn(move || {
    let hub = pubsub_api(&config.service_account.to_string())?;
    let projects = hub.projects();

    loop {
        let request = google_pubsub1::PullRequest {
            return_immediately: Some(false),
            max_messages: Some(10),
        };

        while pipelines.is_empty() {
            match rx.recv()? {
                onramp::Msg::Connect(mut ps) => pipelines.append(&mut ps),
                onramp::Msg::Disconnect { tx, .. } => {
                    tx.send(true)?;
                    return Ok(());
                }
            };
        }

        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(_e) => error!("Crossbream receive error"),
            Ok(onramp::Msg::Connect(mut ps)) => pipelines.append(&mut ps),
            Ok(onramp::Msg::Disconnect { id, tx }) => {
                pipelines.retain(|(pipeline, _)| pipeline != &id);
                if pipelines.is_empty() {
                    tx.send(true)?;
                    return Ok(());
                } else {
                    tx.send(false)?;
                }
            }
        };

        let subscription = projects
            .subscriptions_pull(request, &subscription_name)
            .doit();

        match subscription {
            Err(e) => warn!("Onramp error {:?}", e),
            Ok((_x, batch)) => {
                // TODO extract 'ack' logic as utility function
                let mut ingest_ns = nanotime();
                for message in batch.received_messages.unwrap_or_default() {
                    let ack_id = message.ack_id.unwrap_or_default();
                    let body = message.message.unwrap_or_default();

                    match base64::decode(&body.data.unwrap_or_default()) {
                        Ok(decoded) => {
                            send_event(
                                &pipelines,
                                &mut preprocessors,
                                &mut codec,
                                &mut ingest_ns,
                                // TODO proper origin uri here
                                None,
                                id,
                                decoded,
                            );
                            id += 1;
                        }
                        Err(e) => error!("base64 decoding error error: {:?}", e),
                    }

                    if ack_id != "" {
                        if let Err(e) = projects
                            .subscriptions_acknowledge(
                                google_pubsub1::AcknowledgeRequest {
                                    ack_ids: Some(vec![ack_id]),
                                },
                                &subscription_name,
                            )
                            .doit()
                        {
                            error!("Ack error: {:?}", e)
                        }
                    };
                }
            }
        }
    }
    //    })
}

impl Onramp for GSub {
    fn start(&mut self, codec: &str, preprocessors: &[String]) -> Result<onramp::Addr> {
        let (tx, rx) = bounded(0);
        let config = self.config.clone();
        let codec = codec::lookup(&codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;

        thread::Builder::new()
            .name(format!("onramp-gsub-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(&rx, &config, preprocessors, codec) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
