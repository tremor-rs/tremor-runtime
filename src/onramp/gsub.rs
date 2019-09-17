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

impl OnrampImpl for GSub {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(GSub { config }))
        } else {
            Err("Missing config for gsub onramp".into())
        }
    }
}

fn onramp_loop(
    rx: Receiver<OnrampMsg>,
    config: Config,
    preprocessors: Vec<String>,
    codec: String,
) -> Result<()> {
    let mut codec = codec::lookup(&codec)?;

    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut preprocessors = make_preprocessors(&preprocessors)?;

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
                OnrampMsg::Connect(mut ps) => pipelines.append(&mut ps),
                OnrampMsg::Disconnect { tx, .. } => {
                    let _ = tx.send(true);
                    return Ok(());
                }
            };
        }

        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(_e) => error!("Crossbream receive error"),
            Ok(OnrampMsg::Connect(mut ps)) => pipelines.append(&mut ps),
            Ok(OnrampMsg::Disconnect { id, tx }) => {
                pipelines.retain(|(pipeline, _)| pipeline != &id);
                if pipelines.is_empty() {
                    let _ = tx.send(true);
                    return Ok(());
                } else {
                    let _ = tx.send(false);
                }
            }
        };

        let sub = projects
            .subscriptions_pull(request, &subscription_name)
            .doit();

        match sub {
            Err(e) => warn!("Onramp error {:?}", e),
            Ok((_x, batch)) => {
                // TODO extract 'ack' logic as utility function
                let mut ingest_ns = nanotime();
                for message in batch.received_messages.unwrap_or_default() {
                    let ack_id = message.ack_id.unwrap_or_default();
                    let body = message.message.unwrap_or_default();

                    let decoded = base64::decode(&body.data.unwrap_or_default())
                        .expect("could not base64 decode");

                    send_event(
                        &pipelines,
                        &mut preprocessors,
                        &mut codec,
                        &mut ingest_ns,
                        id,
                        decoded,
                    );
                    id += 1;

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
                            println!("Ack error: {:?}", e)
                        }
                    };
                }
            }
        }
    }
    //    })
}

impl Onramp for GSub {
    fn start(&mut self, codec: String, preprocessors: Vec<String>) -> Result<OnrampAddr> {
        let (tx, rx) = bounded(0);
        let config = self.config.clone();

        thread::Builder::new()
            .name(format!("onramp-gsub-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(rx, config, preprocessors, codec) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
