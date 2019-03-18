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
use crate::google::{authenticate, pubsub_api};
use crate::onramp::{EnterReturn, Onramp as OnrampT, PipelineOnramp};
use crate::pipeline::prelude::*;
use crate::utils;
use base64;
use futures::prelude::*;
use futures::sync::mpsc::channel;
use google_pubsub1 as pubsub;
use serde_yaml;
use hashbrown::HashMap;
use std::thread;

pub struct Onramp {
    config: Config,
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub service_account: String,
    pub subscription: String,
    #[serde(default = "dflt::d_false")]
    pub sync: bool,
}

impl Onramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        Ok(Self { config })
    }
}

impl OnrampT for Onramp {
    fn enter_loop(&mut self, pipelines: PipelineOnramp) -> EnterReturn {
        let config = self.config.clone();
        let pipelines = pipelines.clone();
        let i = 0;
        let subscription_name = self.config.subscription.clone();

        thread::spawn(move || {
            let hub =
                pubsub_api(authenticate(&config.service_account.to_string()).unwrap()).unwrap();
            let projects = hub.projects();

            loop {
                let request = pubsub::PullRequest {
                    return_immediately: Some(false),
                    max_messages: Some(10),
                };

                let sub = projects
                    .subscriptions_pull(request, &subscription_name)
                    .doit();

                match sub {
                    Err(e) => warn!("Onramp error {:?}", e),
                    Ok((_x, batch)) => {
                        // TODO extract 'ack' logic as utility function
                        for message in batch.received_messages.unwrap_or_default() {
                            let ack_id = message.ack_id.unwrap_or_default();
                            let body = message.message.unwrap_or_default();
                            let vars = HashMap::new();

                            let decoded = base64::decode(&body.data.unwrap_or_default()).unwrap();
                            if config.sync {
                                let (tx, rx) = channel(0);
                                pipelines[i].do_send(OnData {
                                    reply_channel: Some(tx),
                                    data: EventValue::Raw(decoded),
                                    vars,
                                    ingest_ns: utils::nanotime(),
                                });
                                for _r in rx.wait() {}
                                if ack_id != "" {
                                    if let Err(e) = projects
                                        .subscriptions_acknowledge(
                                            pubsub::AcknowledgeRequest {
                                                ack_ids: Some(vec![ack_id]),
                                            },
                                            &subscription_name,
                                        )
                                        .doit()
                                    {
                                        println!("Ack error: {:?}", e)
                                    }
                                };
                            } else {
                                pipelines[i].do_send(OnData {
                                    reply_channel: None,
                                    data: EventValue::Raw(decoded),
                                    vars,
                                    ingest_ns: utils::nanotime(),
                                });

                                if ack_id != "" {
                                    if let Err(e) = projects
                                        .subscriptions_acknowledge(
                                            pubsub::AcknowledgeRequest {
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
            }
        })
    }
}
