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

#![cfg(not(tarpaulin_include))]
use crate::{
    codec::{
        json::{Json, Sorted},
        Codec,
    },
    connectors::prelude::*,
};
use async_std::channel::{bounded, Receiver, Sender, TryRecvError};
use beef::Cow;
use serde::Deserialize;
use sled::{CompareAndSwapError, Db, IVec};
use std::{boxed::Box, convert::TryFrom};

#[derive(Debug)]
enum Command<'v> {
    /// Format:
    /// ```json
    /// {"get": "the-key"}
    /// ```
    ///
    /// Response: the value behing "the-key" or `null`
    Get { key: Vec<u8> },
    /// Format:
    /// ```json
    /// {"put": "the-key"}
    /// ```
    /// Event Payload: data to put here
    /// Response: the putted value if successful
    Put { key: Vec<u8> },
    /// Format:
    /// ```json
    /// {"swap": "the-key"}
    /// ```
    /// Event Payload: data to put here
    ///
    /// Response: the old value or `null` is there was no previous value for this key
    Swap { key: Vec<u8> },

    /// Format:
    /// ```json
    /// {"delete": "the-key"}
    /// ```
    ///
    /// Response: the old value
    Delete { key: Vec<u8> },
    /// Format:
    /// ```json
    /// {
    ///    "start": "key1",
    ///    "end": "key2",
    /// }
    /// ```
    ///
    /// Response: 1 event for each value in the scanned range
    Scan {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    },
    /// Format:
    ///  ```json
    /// {
    ///    "cas": "key",
    ///    "old": "<value|null|not-set>",
    /// }
    /// ```
    /// EventPayload: event payload
    ///
    /// Response: `null` if the operation succeeded, an event on `err` if it failed
    Cas {
        key: Vec<u8>,
        old: Option<&'v Value<'v>>,
    },
}

impl<'v> TryFrom<&'v Value<'v>> for Command<'v> {
    type Error = crate::Error;

    fn try_from(v: &'v Value<'v>) -> Result<Self> {
        let v = v.get("kv").ok_or("Missing `$kv` field for commands")?;
        if let Some(key) = v.get_bytes("get").map(|v| v.to_vec()) {
            Ok(Command::Get { key })
        } else if let Some(key) = v.get_bytes("put").map(|v| v.to_vec()) {
            Ok(Command::Put { key })
        } else if let Some(key) = v.get_bytes("swap").map(|v| v.to_vec()) {
            Ok(Command::Swap { key })
        } else if let Some(key) = v.get_bytes("cas").map(|v| v.to_vec()) {
            Ok(Command::Cas {
                key,
                old: v.get("old"),
            })
        } else if let Some(key) = v.get_bytes("delete").map(|v| v.to_vec()) {
            Ok(Command::Delete { key })
        } else if let Some(start) = v.get_bytes("scan").map(|v| v.to_vec()) {
            Ok(Command::Scan {
                start,
                end: v.get_bytes("end").map(|v| v.to_vec()),
            })
        } else {
            Err(format!("Invalid KV command: {}", v).into())
        }
    }
}

impl<'v> Command<'v> {
    fn op_name(&self) -> &'static str {
        match self {
            Command::Get { .. } => "get",
            Command::Put { .. } => "put",
            Command::Swap { .. } => "swap",
            Command::Delete { .. } => "delete",
            Command::Scan { .. } => "scan",
            Command::Cas { .. } => "cas",
        }
    }

    fn key(&self) -> Option<Vec<u8>> {
        match self {
            Command::Get { key, .. }
            | Command::Put { key, .. }
            | Command::Swap { key, .. }
            | Command::Delete { key }
            | Command::Cas { key, .. } => Some(key.clone()),
            Command::Scan { .. } => None,
        }
    }
}

fn ok(op_name: &'static str, k: Vec<u8>, v: Value<'static>) -> (Value<'static>, Value<'static>) {
    (
        v,
        literal!({
            "kv": {
                "op": op_name,
                "ok": Value::Bytes(k.into())
            }
        }),
    )
}
fn oks(
    op_name: &'static str,
    k: Vec<u8>,
    v: Value<'static>,
) -> Vec<(Value<'static>, Value<'static>)> {
    vec![ok(op_name, k, v)]
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    dir: String,
}

impl ConfigImpl for Config {}

pub struct Kv {
    sink_url: TremorUrl,
    event_origin_uri: EventOriginUri,
    config: Config,
    rx: Receiver<KvMesssage>,
    tx: Sender<KvMesssage>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "kv".into()
    }
    async fn from_config(
        &self,
        id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            let event_origin_uri = EventOriginUri {
                scheme: "tremor-kv".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: config.dir.split('/').map(ToString::to_string).collect(),
            };
            let (tx, rx) = bounded(128);
            Ok(Box::new(Kv {
                sink_url: id.clone(),
                event_origin_uri,
                config,
                tx,
                rx,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

type KvMesssage = (Cow<'static, str>, EventPayload);

struct KvSink {
    tx: Sender<KvMesssage>,
    db: Db,
    idgen: EventIdGenerator,
    codec: Json<Sorted>,
    url: TremorUrl,
}

struct KvSource {
    rx: Receiver<KvMesssage>,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait()]
impl Source for KvSource {
    fn is_transactional(&self) -> bool {
        false
    }
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok((port, payload)) => Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                stream: 0,
                payload,
                port: Some(port),
            }),
            Err(TryRecvError::Empty) => {
                // TODO: configure pull interval in connector config?
                Ok(SourceReply::Empty(DEFAULT_POLL_INTERVAL))
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl KvSink {
    fn decode(&mut self, mut v: Option<IVec>, ingest_ns: u64) -> Result<Value<'static>> {
        if let Some(v) = v.as_mut() {
            let data: &mut [u8] = v;
            // TODO: We could optimize this
            Ok(self
                .codec
                .decode(data, ingest_ns)?
                .unwrap_or_default()
                .into_static())
        } else {
            Ok(Value::null())
        }
    }
    fn encode(&self, v: &Value) -> Result<Vec<u8>> {
        self.codec.encode(v)
    }
    fn execute(
        &mut self,
        cmd: Command,
        op_name: &'static str,
        value: &Value,
        ingest_ns: u64,
    ) -> Result<Vec<(Value<'static>, Value<'static>)>> {
        match cmd {
            Command::Get { key } => self
                .decode(self.db.get(&key)?, ingest_ns)
                .map(|v| oks(op_name, key, v)),
            Command::Put { key } => self
                .decode(self.db.insert(&key, self.encode(value)?)?, ingest_ns)
                .map(|_old_value| oks(op_name, key, value.clone_static())), // return the new value
            Command::Swap { key } => self
                .decode(self.db.insert(&key, self.encode(value)?)?, ingest_ns)
                .map(|old_value| oks(op_name, key, old_value)), // return the old value
            Command::Delete { key } => self
                .decode(self.db.remove(&key)?, ingest_ns)
                .map(|v| oks(op_name, key, v)),
            Command::Cas { key, old } => {
                if let Err(CompareAndSwapError { current, proposed }) = self.db.compare_and_swap(
                    &key,
                    old.map(|v| self.encode(v)).transpose()?,
                    Some(self.encode(value)?),
                )? {
                    Err(format!(
                        "CAS error: expected {} but found {}.",
                        self.decode(proposed, ingest_ns)?,
                        self.decode(current, ingest_ns)?,
                    )
                    .into())
                } else {
                    Ok(oks(op_name, key, Value::null()))
                }
            }
            Command::Scan { start, end } => {
                let i = match end {
                    None => self.db.range(start..),
                    Some(end) => self.db.range(start..end),
                };
                let mut res = Vec::with_capacity(i.size_hint().0);
                for e in i {
                    let (key, e) = e?;
                    let key: &[u8] = &key;

                    res.push(ok(op_name, key.to_vec(), self.decode(Some(e), ingest_ns)?));
                }
                Ok(res)
            }
        }
    }
}

#[async_trait::async_trait]
impl Sink for KvSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let ingest_ns = tremor_common::time::nanotime();

        let mut r = SinkReply::ACK;
        for (v, m) in event.value_meta_iter() {
            let correlation = m.get("correlation");
            let executed = match Command::try_from(m) {
                Ok(cmd) => {
                    let name = cmd.op_name();
                    let key = cmd.key();
                    self.execute(cmd, name, v, ingest_ns)
                        .map_err(|e| (Some(name), key, e))
                }
                Err(e) => Err((None, None, e)),
            };
            match executed {
                Ok(res) => {
                    for (data, mut meta) in res {
                        let mut id = self.idgen.next_id();
                        id.track(&event.id);

                        if let Some(correlation) = correlation {
                            meta.try_insert("correlation", correlation.clone_static());
                        }

                        let e = (data, meta).into();
                        if let Err(e) = self.tx.send((OUT, e)).await {
                            error!("[Sink::{}], Faild to send to source: {}", self.url, e);
                        };
                    }
                }
                Err((op, key, e)) => {
                    // send ERR response and log err
                    let mut id = self.idgen.next_id();
                    id.track(&event.id);
                    let mut meta = literal!({
                        "error": e.to_string(),
                        "kv": op.map(|op| literal!({ "op": op, "key": key }))
                    });
                    if let Some(correlation) = correlation {
                        meta.try_insert("correlation", correlation.clone_static());
                    }
                    let e = ((), meta).into();
                    if let Err(e) = self.tx.send((ERR, e)).await {
                        error!("[Sink::{}], Faild to send to source: {}", self.url, e);
                    };

                    r = SinkReply::FAIL;
                }
            }
        }
        Ok(r)
    }
    fn auto_ack(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl Connector for Kv {
    fn is_structured(&self) -> bool {
        true
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let s = KvSource {
            rx: self.rx.clone(),
            origin_uri: self.event_origin_uri.clone(),
        };
        builder.spawn(s, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let db = sled::open(&self.config.dir)?;
        let idgen = EventIdGenerator::default();
        let codec = Json::default();
        let s = KvSink {
            db,
            tx: self.tx.clone(),
            idgen,
            codec,
            url: self.sink_url.clone(),
        };
        builder.spawn(s, sink_context).map(Some)
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json-sorted"
    }
}
