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

//! # Google PubSub Publication Offramp
//!
//! The `Gpub` offramp writes events to a Google PubSub topic.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::errors::*;
use crate::google::{authenticate, pubsub_api, GpsHub};
use crate::pipeline::prelude::*;
use google_pubsub1 as pubsub;
use serde_yaml;
use hashbrown::HashMap;
use std::fmt;

// TODO batch+flush

#[derive(Clone, Deserialize)]
pub struct Config {
    /// Service Account secrets file ( json )
    pub service_account: String,
    /// topic to publish to
    pub topic: String,
}

/// An offramp that write to GCS
pub struct Offramp {
    config: Config,
    hub: GpsHub,
}

impl fmt::Debug for Offramp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(Gpub, topic: {})", self.config.topic)
    }
}

impl Offramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let hub = pubsub_api(authenticate(&config.service_account.to_string())?)?;
        Ok(Self { config, hub })
    }

    fn handle_return(r: &Result<Option<f64>>, returns: &HashMap<ReturnDest, Vec<u64>>) {
        match &r {
            Ok(r) => {
                for (dst, ids) in returns.iter() {
                    let ret = Return {
                        source: dst.source.to_owned(),
                        chain: dst.chain.to_owned(),
                        ids: ids.to_owned(),
                        v: Ok(*r),
                    };
                    ret.send();
                }
            }
            Err(e) => {
                for (dst, ids) in returns.iter() {
                    let ret = Return {
                        source: dst.source.to_owned(),
                        chain: dst.chain.to_owned(),
                        ids: ids.to_owned(),
                        v: Err(e.clone()),
                    };
                    ret.send();
                }
            }
        };
    }
}

impl Opable for Offramp {
    fn on_event(&mut self, event: EventData) -> EventResult {
        let methods = self.hub.projects();
        let topic_name = self.config.topic.clone();
        let (_ret, data) = event.make_return_and_value(Ok(None));
        let returns = HashMap::new();
        let request = match data {
            EventValue::Raw(raw) => {
                let message = pubsub::PubsubMessage {
                    data: Some(base64::encode(&raw)),
                    ..Default::default()
                };
                pubsub::PublishRequest {
                    messages: Some(vec![message]),
                }
            }
            EventValue::JSON(json) => {
                let message = pubsub::PubsubMessage {
                    data: Some(base64::encode(&serde_json::to_vec(&json).unwrap())),
                    ..Default::default()
                };
                pubsub::PublishRequest {
                    messages: Some(vec![message]),
                }
            }
        };

        let response = methods.topics_publish(request.clone(), &topic_name).doit();

        let res = match response {
            Err(_) => Err("Error publishing to topic".into()),
            Ok((_, _)) => Ok(None),
        };

        Self::handle_return(&res, &returns);
        EventResult::Done
    }

    opable_types!(ValueType::Any, ValueType::Any);
}
