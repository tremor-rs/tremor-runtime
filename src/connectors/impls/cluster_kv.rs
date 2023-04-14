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

// #![cfg_attr(coverage, no_coverage)]
use crate::ids::GenericAlias;
use crate::{
    channel::{bounded, Receiver, Sender},
    errors::already_created_error,
    raft,
};
use crate::{
    codec::{
        json::{Json, Sorted},
        Codec,
    },
    connectors::prelude::*,
};
use serde::Deserialize;
use std::sync::Arc;
use std::{boxed::Box, convert::TryFrom, sync::atomic::AtomicBool};

#[derive(Debug)]
enum Command {
    /// Format:
    /// ```json
    /// {"get": "the-key", strict: true}
    /// ```
    ///
    /// Response: the value behing "the-key" or `null`
    Get { key: String, strict: bool },
    /// Format:
    /// ```json
    /// {"put": "the-key"}
    /// ```
    /// Event Payload: data to put here
    /// Response: the putted value if successful
    Put { key: String },
    // /// Format:
    // /// ```json
    // /// {"swap": "the-key"}
    // /// ```
    // /// Event Payload: data to put here
    // ///
    // /// Response: the old value or `null` is there was no previous value for this key
    // Swap { key: Vec<u8> },

    // /// Format:
    // /// ```json
    // /// {"delete": "the-key"}
    // /// ```
    // ///
    // /// Response: the old value
    // Delete { key: Vec<u8> },
    // /// Format:
    // /// ```json
    // /// {
    // ///    "start": "key1",
    // ///    "end": "key2",
    // /// }
    // /// ```
    // ///
    // /// Response: 1 event for each value in the scanned range
    // Scan {
    //     start: Vec<u8>,
    //     end: Option<Vec<u8>>,
    // },
    // /// Format:
    // ///  ```json
    // /// {
    // ///    "cas": "key",
    // ///    "old": "<value|null|not-set>",
    // /// }
    // /// ```
    // /// EventPayload: event payload
    // ///
    // /// Response: `null` if the operation succeeded, an event on `err` if it failed
    // Cas {
    //     key: Vec<u8>,
    //     old: Option<&'v Value<'v>>,
    // },
}

impl<'v> TryFrom<&'v Value<'v>> for Command {
    type Error = crate::Error;

    fn try_from(v: &'v Value<'v>) -> Result<Self> {
        let v = v.get("kv").ok_or("Missing `$kv` field for commands")?;
        if let Some(key) = v.get_str("get").map(ToString::to_string) {
            Ok(Command::Get {
                key,
                strict: v.get_bool("strict").unwrap_or(false),
            })
        } else if let Some(key) = v.get_str("put").map(ToString::to_string) {
            Ok(Command::Put { key })
        // } else if let Some(key) = v.get_bytes("swap").map(<[u8]>::to_vec) {
        //     Ok(Command::Swap { key })
        // } else if let Some(key) = v.get_bytes("cas").map(<[u8]>::to_vec) {
        //     Ok(Command::Cas {
        //         key,
        //         old: v.get("old"),
        //     })
        // } else if let Some(key) = v.get_bytes("delete").map(<[u8]>::to_vec) {
        //     Ok(Command::Delete { key })
        // } else if let Some(start) = v.get_bytes("scan").map(<[u8]>::to_vec) {
        //     Ok(Command::Scan {
        //         start,
        //         end: v.get_bytes("end").map(<[u8]>::to_vec),
        //     })
        } else {
            Err(format!("Invalid KV command: {v}").into())
        }
    }
}

impl Command {
    fn op_name(&self) -> &'static str {
        match self {
            Command::Get { .. } => "get",
            Command::Put { .. } => "put",
            // Command::Swap { .. } => "swap",
            // Command::Delete { .. } => "delete",
            // Command::Scan { .. } => "scan",
            // Command::Cas { .. } => "cas",
        }
    }

    fn key(&self) -> &str {
        match self {
            Command::Get { key, .. } | Command::Put { key, .. } => key,
            // | Command::Swap { key, .. }
            // | Command::Delete { key }
            // | Command::Cas { key, .. } => Some(key.clone()),
            // Command::Scan { .. } => None,
        }
    }
}

fn ok(op_name: &'static str, k: String, v: Value<'static>) -> (Value<'static>, Value<'static>) {
    (
        v,
        literal!({
            "kv": {
                "op": op_name,
                "ok": k
            }
        }),
    )
}
fn oks(
    op_name: &'static str,
    k: String,
    v: Value<'static>,
) -> Vec<(Value<'static>, Value<'static>)> {
    vec![ok(op_name, k, v)]
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

impl Builder {}
#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "cluster_kv".into()
    }
    async fn build(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let (tx, rx) = bounded(qsize());
        Ok(Box::new(Kv {
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
    ) -> Result<Option<SourceAddr>> {
        let source = ChannelSource::from_channel(
            self.tx.clone(),
            self.rx.take().ok_or_else(already_created_error)?,
            self.source_is_connected.clone(),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let codec = Json::default();
        let origin_uri = EventOriginUri {
            scheme: "tremor-cluster-kv".to_string(),
            host: hostname(),
            port: None,
            path: vec![],
        };
        let sink = KvSink {
            alias: ctx.alias().clone(),
            raft: ctx.raft().clone(),
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
    alias: Alias,
    raft: raft::Manager,
    tx: Sender<SourceReply>,
    codec: Json<Sorted>,
    origin_uri: EventOriginUri,
    source_is_connected: Arc<AtomicBool>,
}

impl KvSink {
    fn decode(&mut self, mut v: Option<Vec<u8>>, ingest_ns: u64) -> Result<Value<'static>> {
        if let Some(v) = v.as_mut() {
            let data: &mut [u8] = v.as_mut_slice();
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
    fn encode(&mut self, v: &Value) -> Result<Vec<u8>> {
        self.codec.encode(v)
    }
    async fn execute<'a>(
        &mut self,
        cmd: Command,
        op_name: &'static str,
        value: &Value<'a>,
        ingest_ns: u64,
    ) -> Result<Vec<(Value<'static>, Value<'static>)>> {
        match cmd {
            Command::Get { key, strict } => {
                let key_parts = vec![
                    self.alias.app_id().to_string(),
                    self.alias.app_instance().to_string(),
                    self.alias.alias().to_string(),
                    key.clone(),
                ];
                let combined_key = key_parts.join(".");

                self.decode(
                    if strict {
                        self.raft.kv_get(combined_key).await?
                    } else {
                        self.raft.kv_get_local(combined_key).await?
                    },
                    ingest_ns,
                )
                .map(|v| oks(op_name, key, v))
            }
            Command::Put { key } => {
                // return the new value
                let value_vec = self.encode(value)?;
                let key_parts = vec![
                    self.alias.app_id().to_string(),
                    self.alias.app_instance().to_string(),
                    self.alias.alias().to_string(),
                    key.clone(),
                ];
                let combined_key = key_parts.join(".");
                self.raft.kv_set(combined_key, value_vec).await?;
                Ok(oks(op_name, key, value.clone_static()))
            } // Command::Swap { key } => {
              //     // return the old value
              //     let value = self.encode(value)?;
              //     let v = self.db.insert(&key, value)?;
              //     self.decode(v, ingest_ns)
              //         .map(|old_value| oks(op_name, key, old_value))
              // }
              // Command::Delete { key } => self
              //     .decode(self.db.remove(&key)?, ingest_ns)
              //     .map(|v| oks(op_name, key, v)),
              // Command::Cas { key, old } => {
              //     let vec = self.encode(value)?;
              //     let old = old.map(|v| self.encode(v)).transpose()?;
              //     if let Err(CompareAndSwapError { current, proposed }) =
              //         self.db.compare_and_swap(&key, old, Some(vec))?
              //     {
              //         Err(format!(
              //             "CAS error: expected {} but found {}.",
              //             self.decode(proposed, ingest_ns)?,
              //             self.decode(current, ingest_ns)?,
              //         )
              //         .into())
              //     } else {
              //         Ok(oks(op_name, key, Value::null()))
              //     }
              // }
              // Command::Scan { start, end } => {
              //     let i = match end {
              //         None => self.db.range(start..),
              //         Some(end) => self.db.range(start..end),
              //     };
              //     let mut res = Vec::with_capacity(i.size_hint().0);
              //     for e in i {
              //         let (key, e) = e?;
              //         let key: &[u8] = &key;

              //         res.push(ok(op_name, key.to_vec(), self.decode(Some(e), ingest_ns)?));
              //     }
              //     Ok(res)
              // }
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
    ) -> Result<SinkReply> {
        let ingest_ns = tremor_common::time::nanotime();
        let send_replies = self.source_is_connected.load(Ordering::Acquire);

        let mut r = SinkReply::ACK;
        for (v, m) in event.value_meta_iter() {
            let correlation = m.get("correlation");
            let executed = match Command::try_from(m) {
                Ok(cmd) => {
                    let name = cmd.op_name();
                    let key = cmd.key().to_string();
                    self.execute(cmd, name, v, ingest_ns)
                        .await
                        .map_err(|e| (Some(name), Some(key), e))
                }
                Err(e) => {
                    error!("{ctx} Invalid KV command: {e}");
                    Err((None, None, e))
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
