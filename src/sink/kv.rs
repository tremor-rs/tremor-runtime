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

// TODO: Add correlation of reads and replies.

#![cfg(not(tarpaulin_include))]
use crate::sink::{prelude::*, Reply};
use crate::source::prelude::*;
use async_channel::Sender;
use halfbrown::HashMap;
use serde::Deserialize;
use sled::{CompareAndSwapError, IVec};
use std::boxed::Box;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    dir: String,
}
pub struct Kv {
    sink_url: TremorUrl,
    event_origin_uri: EventOriginUri,
    db: sled::Db,
}

fn decode(mut v: Option<IVec>, codec: &mut dyn Codec, ingest_ns: u64) -> Result<Value<'static>> {
    if let Some(v) = v.as_mut() {
        let data: &mut [u8] = v;
        // TODO: We could optimize this
        Ok(codec
            .decode(data, ingest_ns)?
            .unwrap_or_default()
            .into_static())
    } else {
        Ok(Value::null())
    }
}

fn ok(v: Value<'static>) -> Value<'static> {
    let mut reply = Value::object_with_capacity(1);
    if reply.insert("ok", v).is_ok() {
        reply
    } else {
        // ALLOW: We know this never can happen since `reply` is created as an object
        unreachable!()
    }
}

impl Kv {
    fn execute(
        &self,
        cmd: Command,
        codec: &mut dyn Codec,
        ingest_ns: u64,
    ) -> Result<Value<'static>> {
        match cmd {
            Command::Get { key } => decode(self.db.get(key)?, codec, ingest_ns).map(ok),
            Command::Put { key, value } => {
                decode(self.db.insert(key, codec.encode(value)?)?, codec, ingest_ns).map(ok)
            }
            Command::Delete { key } => decode(self.db.remove(key)?, codec, ingest_ns).map(ok),
            Command::Cas { key, old, new } => {
                if let Err(CompareAndSwapError { current, proposed }) = self.db.compare_and_swap(
                    key,
                    old.map(|v| codec.encode(v)).transpose()?,
                    new.map(|v| codec.encode(v)).transpose()?,
                )? {
                    let mut err = Value::object_with_capacity(2);
                    err.insert(
                        "current",
                        current.map(|v| {
                            let v: &[u8] = &v;
                            Value::Bytes(Cow::from(Vec::from(v)))
                        }),
                    )?;
                    err.insert(
                        "proposed",
                        proposed.map(|v| {
                            let v: &[u8] = &v;
                            Value::Bytes(Cow::from(Vec::from(v)))
                        }),
                    )?;
                    let mut reply = Value::object_with_capacity(1);
                    reply.insert("error", err)?;
                    Ok(reply)
                } else {
                    Ok(ok(Value::null()))
                }
            }
            Command::Scan { start, end } => {
                let i = match (start, end) {
                    (None, None) => self.db.range::<Vec<u8>, _>(..),
                    (Some(start), None) => self.db.range(start..),
                    (None, Some(end)) => self.db.range(..end),
                    (Some(start), Some(end)) => self.db.range(start..end),
                };
                let mut res = Vec::with_capacity(32);
                for e in i {
                    let mut kv = Value::object_with_capacity(2);

                    let (key, e) = e?;
                    let key: &[u8] = &key;
                    kv.insert("key", Value::Bytes(key.to_vec().into()))?;
                    kv.insert("value", decode(Some(e), codec, ingest_ns)?)?;
                    res.push(kv)
                }
                Ok(Value::from(res))
            }
        }
    }
}

impl offramp::Impl for Kv {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;

            let db = sled::open(&config.dir)?;
            let event_origin_uri = EventOriginUri {
                uid: 0,
                scheme: "tremor-kv".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: config.dir.split('/').map(ToString::to_string).collect(),
            };

            Ok(SinkManager::new_box(Kv {
                sink_url: TremorUrl::from_onramp_id("kv")?, // dummy value
                event_origin_uri,
                db,
            }))
        } else {
            Err("[KV Offramp] Offramp requires a config".into())
        }
    }
}

#[derive(Debug)]
enum Command<'v> {
    /// ```json
    /// {"get": {"key": "the-key"}}
    /// ```
    Get { key: Vec<u8> },
    /// ```json
    /// {"put": {"key": "the-key", "value": "the-value"}}
    /// ```
    Put { key: Vec<u8>, value: &'v Value<'v> },
    /// ```json
    /// {"delete": {"key": "the-key"}}
    /// ```
    Delete { key: Vec<u8> },
    /// ```json
    /// {"scan": {
    ///    "start": "key1",
    ///    "end": "key2",
    /// }
    /// ```
    Scan {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
    /// ```json
    /// {"cas": {
    ///    "key": "key"
    ///    "old": "<value|null|not-se>",
    ///    "new": "<value|null|not-set>",
    /// }
    /// ```
    Cas {
        key: Vec<u8>,
        old: Option<&'v Value<'v>>,
        new: Option<&'v Value<'v>>,
    },
}

#[async_trait::async_trait]
impl Sink for Kv {
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let mut r = Vec::with_capacity(10);
        let ingest_ns = tremor_common::time::nanotime();

        let cmds = event.value_iter().filter_map(|v| {
            if let Some(g) = v.get("get") {
                Some(Command::Get {
                    key: g.get_bytes("key")?.to_vec(),
                })
            } else if let Some(p) = v.get("put") {
                Some(Command::Put {
                    key: p.get_bytes("key")?.to_vec(),
                    value: p.get("value")?,
                })
            } else if let Some(c) = v.get("cas") {
                Some(Command::Cas {
                    key: c.get_bytes("key")?.to_vec(),
                    old: c.get("old"),
                    new: c.get("new"),
                })
            } else if let Some(d) = v.get("delete") {
                Some(Command::Delete {
                    key: d.get_bytes("key")?.to_vec(),
                })
            } else if let Some(s) = v.get("scan") {
                Some(Command::Scan {
                    start: s.get_bytes("start").map(|v| v.to_vec()),
                    end: s.get_bytes("end").map(|v| v.to_vec()),
                })
            } else {
                error!("failed to decode command: {}", v);
                None
            }
        });

        for c in cmds {
            let data = self.execute(c, codec, ingest_ns)?;
            let e = Event {
                data: data.into(),
                origin_uri: Some(self.event_origin_uri.clone()),
                ..Event::default()
            };
            r.push(Reply::Response(OUT, e))
        }
        Ok(Some(r))
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.event_origin_uri.uid = sink_uid;
        self.sink_url = sink_url.clone();
        Ok(())
    }

    fn is_active(&self) -> bool {
        true
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn terminate(&mut self) {}
}
