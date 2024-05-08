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
//! The `kv` connector provides a key value storate facility for tremor-based
//! applications allowing shared state across pipelines. The facility is useful
//! when in memory state via the `state` mechanism is not useful as the state is
//! not shared across pipeline instances or flows.
//!
//! The `kv` service allows state to be shared to multiple pipelines within a a
//! single flow definition or unit of deployment.
//!
//! The `kv` storage is persistent. A storage directory needs to be provided.
//!
//! :::warning
//! If not found, the storage directory will be auto-created, or an error will be propagated.
//! :::
//!
//! ## Configuration
//!
//! ```tremor
//! define my_kv from kv
//! with
//!   "path" = "state",
//! end;
//! ```
//!
//! ## How do I get values from the `kv` store?
//!
//! We use the `kv` `get` command for this purpose.
//!
//! ```tremor
//! let $kv = { "get": "snot" };
//! let event = null;
//! ```
//!
//! This will result in either `null` when  not found or the stored value
//!
//! ## How do I put values into the `kv` store?
//!
//! We use the `kv` `put` command for this purpose.
//!
//! ```tremor
//! let $kv = { "put": "snot",  };
//! let event = "badger";
//! ```
//!
//! This will result in either `null` when not found or the stored value
//!
//! ## How do I swap values in the `kv` store?
//!
//! We use the `kv` `cas` command for this purpose.
//!
//! ```tremor
//! let $kv = {"cas": "heinz", "old": "gies"}
//! let event = "ketchup";
//! ```
//!
//! This will result in either `null` on success, or an event on the `err` stream upon failure
//!
//! ## How do I scan values in the `kv` store?
//!
//! We use the `kv` `scan` command for this purpose.
//!
//! ```tremor
//! let $kv = {"scan": "", "end": "heinz"};
//! let event = 9;
//! ```
//!
//! This will return a value for each match in the specified scan range.
//!
//! ## How do I delete values in the `kv` store?
//!
//! We use the `kv` `delete` command for this purpose.
//!
//! ```tremor
//! let $kv = { "delete": "heinz" };
//! let event = null;
//! ```
//!
//! ## Application
//!
//! Assuming the `$kv` metadata and event are as above, the following query connected to
//! an instance of the kv connector should suffice:
//!
//! ```tremor
//! select event from in into out;
//! ```
//!
//! ## Correlation
//!
//! The `$correlation` metadata key can be set so that a request and response from the
//! `kv` facility can be traced:
//!
//! ```tremor
//! let $correlation = "some-correlating-unique-data";
//! ```
//!
//! ## Conditioning
//!
//! :::note
//! To avoid write errors when other streams are writing to the same kv store we provide the
//! old value as a comparand so that the swap only occurs if the value hasn't changed independently
//! in the intervening time since we last read from the store for this key.
//! :::

use crate::{
    errors::error_connector_def,
    sink::prelude::*,
    source::{channel_source::ChannelSource, prelude::*},
};
use serde::Deserialize;
use sled::{CompareAndSwapError, Db, IVec};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tremor_codec::{
    json::{Json, Sorted},
    Codec,
};
use tremor_common::ports::{ERR, OUT};
use tremor_system::event::DEFAULT_STREAM_ID;
use tremor_value::prelude::*;

/// KV Errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Missing $kv field
    #[error("Missing `$kv` field for commands")]
    MissingKvField,
    /// Invalid Command
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    /// Sled error
    #[error("{0}")]
    Sled(#[from] sled::Error),
    /// JSON error
    #[error("{0}")]
    Json(#[from] tremor_codec::Error),
    /// CAS Error
    #[error("CAS error: expected {0} but found {1}.")]
    Cas(Value<'static>, Value<'static>),
}

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
    type Error = Error;

    fn try_from(v: &'v Value<'v>) -> Result<Self, Error> {
        let v = v.get("kv").ok_or(Error::MissingKvField)?;
        if let Some(key) = v.get_bytes("get").map(<[u8]>::to_vec) {
            Ok(Command::Get { key })
        } else if let Some(key) = v.get_bytes("put").map(<[u8]>::to_vec) {
            Ok(Command::Put { key })
        } else if let Some(key) = v.get_bytes("swap").map(<[u8]>::to_vec) {
            Ok(Command::Swap { key })
        } else if let Some(key) = v.get_bytes("cas").map(<[u8]>::to_vec) {
            Ok(Command::Cas {
                key,
                old: v.get("old"),
            })
        } else if let Some(key) = v.get_bytes("delete").map(<[u8]>::to_vec) {
            Ok(Command::Delete { key })
        } else if let Some(start) = v.get_bytes("scan").map(<[u8]>::to_vec) {
            Ok(Command::Scan {
                start,
                end: v.get_bytes("end").map(<[u8]>::to_vec),
            })
        } else {
            Err(Error::InvalidCommand(v.to_string()))
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
pub(crate) struct Config {
    path: String,
}

impl tremor_config::Impl for Config {}

/// KV connector
#[derive(Debug, Default)]
pub struct Builder {}

impl Builder {
    const INVALID_DIR: &'static str = "Invalid `dir`. Not a directory or not accessible.";
}
#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "kv".into()
    }
    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config: Config = Config::new(config)?;
        if !PathBuf::from(&config.path).is_dir() {
            return Err(error_connector_def(id, Builder::INVALID_DIR).into());
        }

        let (tx, rx) = channel(qsize());
        Ok(Box::new(Kv {
            config,
            rx: Some(rx),
            tx,
            source_is_connected: Arc::default(),
        }))
    }
}

/// Key value store connector
///
/// Receiving commands via its sink and emitting responses to those commands via its source.
pub(crate) struct Kv {
    config: Config,
    rx: Option<Receiver<SourceReply>>,
    tx: Sender<SourceReply>,
    source_is_connected: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Connector for Kv {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let source = ChannelSource::from_channel(
            self.tx.clone(),
            self.rx
                .take()
                .ok_or(GenericImplementationError::AlreadyConnected)?,
            self.source_is_connected.clone(),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let db = sled::open(&self.config.path)?;
        let codec = Json::default();
        let origin_uri = EventOriginUri {
            scheme: "tremor-kv".to_string(),
            host: crate::utils::hostname(),
            port: None,
            path: self
                .config
                .path
                .split('/')
                .map(ToString::to_string)
                .collect(),
        };
        let sink = KvSink {
            db,
            tx: self.tx.clone(),
            codec,
            origin_uri,
            source_is_connected: self.source_is_connected.clone(),
        };
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }
}

struct KvSink {
    db: Db,
    tx: Sender<SourceReply>,
    codec: Json<Sorted>,
    origin_uri: EventOriginUri,
    source_is_connected: Arc<AtomicBool>,
}

impl KvSink {
    async fn decode(
        &mut self,
        mut v: Option<IVec>,
        ingest_ns: u64,
    ) -> anyhow::Result<Value<'static>> {
        if let Some(v) = v.as_mut() {
            let data: &mut [u8] = v;
            // TODO: We could optimize this
            Ok(self
                .codec
                .decode(data, ingest_ns, Value::const_null())
                .await?
                .unwrap_or_default()
                .0
                .into_static())
        } else {
            Ok(Value::const_null())
        }
    }
    async fn encode<'v>(&mut self, v: &Value<'v>) -> anyhow::Result<Vec<u8>> {
        Ok(self.codec.encode(v, &Value::const_null()).await?)
    }
    async fn execute<'v>(
        &mut self,
        cmd: Command<'v>,
        op_name: &'static str,
        value: &Value<'v>,
        ingest_ns: u64,
    ) -> anyhow::Result<Vec<(Value<'static>, Value<'static>)>> {
        match cmd {
            Command::Get { key } => self
                .decode(self.db.get(&key)?, ingest_ns)
                .await
                .map(|v| oks(op_name, key, v)),
            Command::Put { key } => {
                // return the new value
                let value_vec = self.encode(value).await?;
                let v = self.db.insert(&key, value_vec)?;
                self.decode(v, ingest_ns)
                    .await
                    .map(|_old_value| oks(op_name, key, value.clone_static()))
            }
            Command::Swap { key } => {
                // return the old value
                let value = self.encode(value).await?;
                let v = self.db.insert(&key, value)?;
                self.decode(v, ingest_ns)
                    .await
                    .map(|old_value| oks(op_name, key, old_value))
            }
            Command::Delete { key } => self
                .decode(self.db.remove(&key)?, ingest_ns)
                .await
                .map(|v| oks(op_name, key, v)),
            Command::Cas { key, old } => {
                let vec = self.encode(value).await?;
                let old = if let Some(v) = old {
                    Some(self.encode(v).await?)
                } else {
                    None
                };

                if let Err(CompareAndSwapError { current, proposed }) =
                    self.db.compare_and_swap(&key, old, Some(vec))?
                {
                    Err(Error::Cas(
                        self.decode(proposed, ingest_ns).await?,
                        self.decode(current, ingest_ns).await?,
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

                    res.push(ok(
                        op_name,
                        key.to_vec(),
                        self.decode(Some(e), ingest_ns).await?,
                    ));
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
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        let ingest_ns = tremor_common::time::nanotime();
        let send_replies = self.source_is_connected.load(Ordering::Acquire);

        let mut r = SinkReply::ACK;
        for (v, m) in event.value_meta_iter() {
            let correlation = m.get("correlation");
            let executed = match Command::try_from(m) {
                Ok(cmd) => {
                    let name = cmd.op_name();
                    let key = cmd.key();
                    self.execute(cmd, name, v, ingest_ns)
                        .await
                        .map_err(|e| (Some(name), key, e))
                }
                Err(e) => {
                    error!("{ctx} Invalid KV command: {e}");
                    Err((None, None, e.into()))
                }
            };
            match executed {
                Ok(res) => {
                    if send_replies {
                        for (data, mut meta) in res {
                            if let Some(correlation) = correlation {
                                meta.try_insert("correlation", correlation.clone_static());
                            }
                            let reply = SourceReply::Structured {
                                origin_uri: self.origin_uri.clone(),
                                payload: (data, meta).into(),
                                stream: DEFAULT_STREAM_ID,
                                port: Some(OUT),
                            };
                            if let Err(e) = self.tx.send(reply).await {
                                error!("{ctx}, Failed to send to source: {e}");
                            };
                        }
                    }
                }
                Err((op, key, e)) => {
                    error!("{ctx} Error: {e}");
                    if send_replies {
                        // send ERR response and log err
                        let mut meta = literal!({
                            "error": e.to_string(),
                            "kv": op.map(|op| literal!({ "op": op, "key": key }))
                        });
                        if let Some(correlation) = correlation {
                            meta.try_insert("correlation", correlation.clone_static());
                        }
                        let reply = SourceReply::Structured {
                            origin_uri: self.origin_uri.clone(),
                            payload: ((), meta).into(),
                            stream: DEFAULT_STREAM_ID,
                            port: Some(ERR),
                        };
                        ctx.swallow_err(self.tx.send(reply).await, "Failed to send to source");
                    }

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
