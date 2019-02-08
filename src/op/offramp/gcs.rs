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

//! # GCS Offramp
//!
//! The `GCS` offramp writes events to a GCS object. This offramp writes
//! exactly one object and finishes it when the pipeline is shut down.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::dflt;
use crate::errors::*;
use crate::google::{self, authenticate, storage_api, GcsHub};
use crate::pipeline::prelude::*;
use google_storage1::Object;
use serde_yaml;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Service Account secrets file ( json )
    pub service_account: String,
    /// bucket to write to
    pub bucket: String,
    /// object name
    pub name: String,
    /// Content encoding
    pub content_encoding: String,
    /// number of events in each batch
    pub batch_size: usize,
    /// Timeout before a batch is always send
    #[serde(default = "dflt::d_0")]
    pub timeout: u64,
}

/// An offramp that write to GCS
pub struct Offramp {
    config: Config,
    hub: GcsHub,
    payload: VecDeque<EventData>,
    cnt: u64,
    i: u64,
}

impl std::fmt::Debug for Offramp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "(GcsHubOfframp, opaque)")
    }
}

impl Offramp {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let hub = storage_api(authenticate(&config.service_account.to_string())?)?;
        Ok(Self {
            cnt: 0,
            i: 0,
            payload: VecDeque::with_capacity(config.batch_size),
            config,
            hub,
        })
    }
    fn render_payload(&mut self) -> (Vec<u8>, HashMap<ReturnDest, Vec<u64>>) {
        let mut payload = Vec::new();
        let mut returns = HashMap::new();
        while let Some(event) = self.payload.pop_front() {
            let res = event.maybe_extract(|val| match val {
                EventValue::Raw(raw) => {
                    let raw = raw.to_vec();
                    Ok((raw, Ok(None)))
                }
                EventValue::JSON(json) => {
                    let raw = serde_json::to_vec(json).unwrap();
                    Ok((raw, Ok(None)))
                }
            });
            //let (ret, raw) = event.to_return_and_value(Ok(None));
            match res {
                Ok((mut raw, ret)) => {
                    let mut ids = ret.ids;
                    payload.append(&mut raw);
                    payload.push(10); // \n
                    match returns.entry(ReturnDest {
                        source: ret.source,
                        chain: ret.chain,
                    }) {
                        Vacant(entry) => {
                            entry.insert(ids);
                        }
                        Occupied(entry) => {
                            entry.into_mut().append(&mut ids);
                        }
                    }
                }
                Err(e) => error!("{:?}", e),
            };
        }
        (payload, returns)
    }
    fn handle_return(r: &Result<Option<f64>>, returns: HashMap<ReturnDest, Vec<u64>>) {
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
        self.i += 1;
        self.payload.push_back(event);
        if self.payload.len() >= self.config.batch_size {
            let (payload, returns) = self.render_payload();
            let req = Object::default();
            let r = google::verbose(
                self.hub
                    .objects()
                    .insert(req, &self.config.bucket)
                    .name(&format!("{}.{}", self.config.name, self.cnt))
                    .content_encoding(&self.config.content_encoding)
                    .upload(
                        Cursor::new(payload),
                        "application/octet-stream".parse().unwrap(),
                    ),
            );
            self.cnt += 1;
            let res = match r {
                Ok(_) => Ok(None),
                Err(_e) => Err("google cloud error".into()),
            };
            Self::handle_return(&res, returns);
        }

        if self.payload.len() == 1 {
            EventResult::Timeout {
                timeout_millis: self.config.timeout,
                result: Box::new(EventResult::Done),
            }
        } else {
            EventResult::Done
        }
    }

    fn shutdown(&mut self) {
        let (payload, returns) = self.render_payload();
        let req = Object::default();
        let r = google::verbose(
            self.hub
                .objects()
                .insert(req, &self.config.bucket)
                .name(&format!("{}.{}", self.config.name, self.cnt))
                .content_encoding(&self.config.content_encoding)
                .upload(
                    Cursor::new(payload),
                    "application/octet-stream".parse().unwrap(),
                ),
        );
        let res = match r {
            Ok(_) => Ok(None),
            Err(_e) => Err("google cloud error".into()),
        };
        Self::handle_return(&res, returns);
    }

    fn on_timeout(&mut self) -> EventResult {
        let (payload, returns) = self.render_payload();
        let req = Object::default();
        let r = google::verbose(
            self.hub
                .objects()
                .insert(req, &self.config.bucket)
                .name(&format!("{}.{}", self.config.name, self.cnt))
                .content_encoding(&self.config.content_encoding)
                .upload(
                    Cursor::new(payload),
                    "application/octet-stream".parse().unwrap(),
                ),
        );
        let res = match r {
            Ok(_) => Ok(None),
            Err(_e) => Err("google cloud error".into()),
        };
        Self::handle_return(&res, returns);
        EventResult::Done
    }

    opable_types!(ValueType::Any, ValueType::Any);
}
