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
use crate::{
    channel::{bounded, Receiver, Sender},
    raft::{self, ClusterError},
};
use crate::{connectors::prelude::*, system::flow::AppContext};
use serde::Deserialize;
use std::sync::Arc;
use std::{boxed::Box, convert::TryFrom, sync::atomic::AtomicBool};
use tremor_codec::{
    json::{Json, Sorted},
    Codec,
};
use tremor_common::alias::Generic;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Missing `$kv` field for commands")]
    MissingMeta,
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
}

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
}

impl<'v> TryFrom<&'v Value<'v>> for Command {
    type Error = crate::Error;

    fn try_from(v: &'v Value<'v>) -> Result<Self> {
        let v = v.get("kv").ok_or(Error::MissingMeta)?;
        if let Some(key) = v.get_str("get").map(ToString::to_string) {
            Ok(Command::Get {
                key,
                strict: v.get_bool("strict").unwrap_or(false),
            })
        } else if let Some(key) = v.get_str("put").map(ToString::to_string) {
            Ok(Command::Put { key })
        } else {
            Err(Error::InvalidCommand(v.to_string()).into())
        }
    }
}

impl Command {
    fn op_name(&self) -> &'static str {
        match self {
            Command::Get { .. } => "get",
            Command::Put { .. } => "put",
        }
    }

    fn key(&self) -> &str {
        match self {
            Command::Get { key, .. } | Command::Put { key, .. } => key,
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

impl tremor_config::Impl for Config {}

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
        _id: &alias::Connector,
        _: &ConnectorConfig,
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
            self.rx
                .take()
                .ok_or_else(|| ConnectorError::AlreadyCreated(ctx.alias().clone()))?,
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
            app_ctx: ctx.app_ctx().clone(),
            raft: ctx
                .raft()
                .map_or_else(|| Err(ClusterError::RaftNotRunning), |v| Ok(v.clone()))?,
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
    alias: alias::Connector,
    app_ctx: AppContext,
    raft: raft::Cluster,
    tx: Sender<SourceReply>,
    codec: Json<Sorted>,
    origin_uri: EventOriginUri,
    source_is_connected: Arc<AtomicBool>,
}

impl KvSink {
    async fn encode<'v>(&mut self, v: &Value<'v>) -> Result<Vec<u8>> {
        Ok(self.codec.encode(v, &Value::const_null()).await?)
    }
    async fn execute<'a>(
        &mut self,
        cmd: Command,
        op_name: &'static str,
        value: &Value<'a>,
        _ingest_ns: u64,
    ) -> Result<Vec<(Value<'static>, Value<'static>)>> {
        match cmd {
            Command::Get { key, strict } => {
                let key_parts = vec![
                    self.app_ctx.id().to_string(),
                    self.app_ctx.instance().to_string(),
                    self.alias.alias().to_string(),
                    key.clone(),
                ];
                let combined_key = key_parts.join(".");

                let value = if strict {
                    self.raft.kv_get(combined_key).await?
                } else {
                    self.raft.kv_get_local(combined_key).await?
                }
                .map_or(Value::const_null(), Value::from);
                Ok(oks(op_name, key, value))
            }
            Command::Put { key } => {
                // return the new value
                let value_vec = self.encode(value).await?;
                let key_parts = vec![
                    self.app_ctx.id().to_string(),
                    self.app_ctx.instance().to_string(),
                    self.alias.alias().to_string(),
                    key.clone(),
                ];
                let combined_key = key_parts.join(".");
                self.raft.kv_set(combined_key, value_vec).await?;
                Ok(oks(op_name, key, value.clone_static()))
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

#[cfg(test)]
mod test {

    use matches::assert_matches;
    use openraft::error::{CheckIsLeaderError, ForwardToLeader, RaftError};

    use crate::raft::{api::APIStoreReq, manager::IFRequest, node::Addr};

    use super::*;

    #[test]
    fn try_from_command() {
        let cmd = literal!({
            "kv": {
                "get": "the-key",
                "strict": true
            }
        });
        let cmd = Command::try_from(&cmd).expect("it's OK");
        assert_eq!(cmd.op_name(), "get");
        assert_eq!(cmd.key(), "the-key");

        let cmd = literal!({
            "kv": {
                "put": "the-key"
            }
        });
        let cmd = Command::try_from(&cmd).expect("it's OK");
        assert_eq!(cmd.op_name(), "put");
        assert_eq!(cmd.key(), "the-key");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_get() -> Result<()> {
        let (tx, mut rx) = bounded(1);

        let (store_tx, mut store_rx) = bounded(8);
        let (cluster_tx, _) = bounded(8);

        tokio::spawn(async move {
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::KVGet(_key, result_tx) => {
                    let _ = result_tx.send(Some(b"42".to_vec()));
                }
                _ => panic!("wrong request"),
            };
        });
        let alias = alias::Connector::new("cluster_kv");
        let raft = raft::Cluster::dummy(store_tx, cluster_tx);

        let app_ctx = AppContext {
            raft: Some(raft.clone()),
            ..AppContext::default()
        };
        let mut sink = KvSink {
            alias,
            app_ctx,
            raft,
            tx,
            codec: Json::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-cluster-kv".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
            source_is_connected: Arc::new(true.into()),
        };
        let ctx = SinkContext::dummy("cluster_kv");
        let mut serializer = EventSerializer::dummy(None)?;

        let event = Event {
            data: ((), literal!({"kv": { "get": "the-key" }})).into(),
            ..Event::default()
        };

        let reply = sink
            .on_event("in", event, &ctx, &mut serializer, 0)
            .await
            .expect("it's OK");

        assert_eq!(reply, SinkReply::ACK);
        let reply = rx.recv().await.expect("it's OK");
        let expected: EventPayload = (
            Value::from(42),
            literal!({ "kv": { "op": "get", "ok": "the-key" } }),
        )
            .into();
        assert_matches!(
            reply,
            SourceReply::Structured {
                payload,
                ..
            } if payload == expected
        );
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_get_strict_local() -> Result<()> {
        let (tx, mut rx) = bounded(1);

        let (store_tx, mut store_rx) = bounded(8);
        let (cluster_tx, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Ok(()));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
            match store_rx.recv().await.expect("rcv") {
                APIStoreReq::KVGet(_key, result_tx) => {
                    let _ = result_tx.send(Some(b"42".to_vec()));
                }
                _ => panic!("wrong request"),
            };
        });
        let alias = alias::Connector::new("cluster_kv");
        let raft = raft::Cluster::dummy(store_tx, cluster_tx);

        let app_ctx = AppContext {
            raft: Some(raft.clone()),
            ..AppContext::default()
        };
        let mut sink = KvSink {
            alias,
            app_ctx,
            raft,
            tx,
            codec: Json::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-cluster-kv".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
            source_is_connected: Arc::new(true.into()),
        };
        let ctx = SinkContext::dummy("cluster_kv");
        let mut serializer = EventSerializer::dummy(None)?;

        let event = Event {
            data: ((), literal!({"kv": { "get": "the-key", "strict": true }})).into(),
            ..Event::default()
        };

        let reply = sink
            .on_event("in", event, &ctx, &mut serializer, 0)
            .await
            .expect("it's OK");

        assert_eq!(reply, SinkReply::ACK);
        let reply = rx.recv().await.expect("it's OK");
        let expected: EventPayload = (
            Value::from(42),
            literal!({ "kv": { "op": "get", "ok": "the-key" } }),
        )
            .into();
        assert_matches!(
            reply,
            SourceReply::Structured {
                payload,
                ..
            } if payload == expected
        );
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_get_strict_remote() -> Result<()> {
        let (tx, mut rx) = bounded(1);

        let (store_tx, _) = bounded(8);
        let (cluster_tx, mut cluster_rx) = bounded(8);

        let mut api_server = mockito::Server::new();

        let api = api_server.host_with_port();
        let rpc = api.clone();

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Err(RaftError::APIError(
                        CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                            leader_node: Some(Addr::new(api, rpc)),
                            leader_id: Some(1),
                        }),
                    )));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });
        let alias = alias::Connector::new("cluster_kv");
        let raft = raft::Cluster::dummy(store_tx, cluster_tx);

        let app_ctx = AppContext {
            raft: Some(raft.clone()),
            ..AppContext::default()
        };
        let mut sink = KvSink {
            alias,
            app_ctx,
            raft,
            tx,
            codec: Json::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-cluster-kv".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
            source_is_connected: Arc::new(true.into()),
        };
        let ctx = SinkContext::dummy("cluster_kv");
        let mut serializer = EventSerializer::dummy(None)?;

        let event = Event {
            data: ((), literal!({"kv": { "get": "the-key", "strict": true }})).into(),
            ..Event::default()
        };

        let api_mock = api_server
            .mock("POST", "/v1/api/kv/consistent_read")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("42")
            .create();

        let reply = sink
            .on_event("in", event, &ctx, &mut serializer, 0)
            .await
            .expect("it's OK");

        assert_eq!(reply, SinkReply::ACK);
        let reply = rx.recv().await.expect("it's OK");
        let expected: EventPayload = (
            Value::from(42),
            literal!({ "kv": { "op": "get", "ok": "the-key" } }),
        )
            .into();
        assert_matches!(
            reply,
            SourceReply::Structured {
                payload,
                ..
            } if payload == expected
        );
        api_mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_put_local() -> Result<()> {
        let (tx, mut rx) = bounded(1);

        let (store_tx, _) = bounded(8);
        let (cluster_tx, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Ok(()));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::SetKeyLocal(_key, result_tx) => {
                    let _ = result_tx.send(Ok(b"42".to_vec()));
                }
                IFRequest::IsLeader(_) => panic!("wrong request"),
            };
        });
        let alias = alias::Connector::new("cluster_kv");
        let raft = raft::Cluster::dummy(store_tx, cluster_tx);

        let app_ctx = AppContext {
            raft: Some(raft.clone()),
            ..AppContext::default()
        };
        let mut sink = KvSink {
            alias,
            app_ctx,
            raft,
            tx,
            codec: Json::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-cluster-kv".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
            source_is_connected: Arc::new(true.into()),
        };
        let ctx = SinkContext::dummy("cluster_kv");
        let mut serializer = EventSerializer::dummy(None)?;

        let event = Event {
            data: (literal!(42), literal!({"kv": { "put": "the-key",  }})).into(),
            ..Event::default()
        };

        let reply = sink
            .on_event("in", event, &ctx, &mut serializer, 0)
            .await
            .expect("it's OK");

        assert_eq!(reply, SinkReply::ACK);
        let reply = rx.recv().await.expect("it's OK");
        let expected: EventPayload = (
            Value::from(42),
            literal!({ "kv": { "op": "put", "ok": "the-key" } }),
        )
            .into();
        assert_matches!(
            reply,
            SourceReply::Structured {
                payload,
                ..
            } if payload == expected
        );
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_put_remote() -> Result<()> {
        let (tx, mut rx) = bounded(1);

        let (store_tx, _) = bounded(8);
        let (cluster_tx, mut cluster_rx) = bounded(8);

        let mut api_server = mockito::Server::new();
        let api = api_server.host_with_port();
        let rpc = api.clone();

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Err(RaftError::APIError(
                        CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                            leader_node: Some(Addr::new(api, rpc)),
                            leader_id: Some(1),
                        }),
                    )));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
        });
        let alias = alias::Connector::new("cluster_kv");
        let raft = raft::Cluster::dummy(store_tx, cluster_tx);

        let app_ctx = AppContext {
            raft: Some(raft.clone()),
            ..AppContext::default()
        };
        let mut sink = KvSink {
            alias,
            app_ctx,
            raft,
            tx,
            codec: Json::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-cluster-kv".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
            source_is_connected: Arc::new(true.into()),
        };
        let ctx = SinkContext::dummy("cluster_kv");
        let mut serializer = EventSerializer::dummy(None)?;

        let event = Event {
            data: (literal!(42), literal!({"kv": { "put": "the-key",  }})).into(),
            ..Event::default()
        };

        let api_mock = api_server
            .mock("POST", "/v1/api/kv/write")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("42")
            .create();

        let reply = sink
            .on_event("in", event, &ctx, &mut serializer, 0)
            .await
            .expect("it's OK");

        assert_eq!(reply, SinkReply::ACK);
        let reply = rx.recv().await.expect("it's OK");
        let expected: EventPayload = (
            Value::from(42),
            literal!({ "kv": { "op": "put", "ok": "the-key" } }),
        )
            .into();
        assert_matches!(
            reply,
            SourceReply::Structured {
                payload,
                ..
            } if payload == expected
        );
        api_mock.assert();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bad_command() -> Result<()> {
        let (tx, mut rx) = bounded(1);

        let (store_tx, _) = bounded(8);
        let (cluster_tx, _) = bounded(8);

        let alias = alias::Connector::new("cluster_kv");
        let raft = raft::Cluster::dummy(store_tx, cluster_tx);

        let app_ctx = AppContext {
            raft: Some(raft.clone()),
            ..AppContext::default()
        };
        let mut sink = KvSink {
            alias,
            app_ctx,
            raft,
            tx,
            codec: Json::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-cluster-kv".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
            source_is_connected: Arc::new(true.into()),
        };
        let ctx = SinkContext::dummy("cluster_kv");
        let mut serializer = EventSerializer::dummy(None)?;

        let event = Event {
            data: ((), literal!({"kv": { "snot": "the-badger" }})).into(),
            ..Event::default()
        };

        let reply = sink
            .on_event("in", event, &ctx, &mut serializer, 0)
            .await
            .expect("it's OK");

        assert_eq!(reply, SinkReply::FAIL);
        let reply = rx.recv().await.expect("it's OK");
        let expected: EventPayload = (
            Value::null(),
            literal!({ "error": "Invalid command: {\"snot\": String(\"the-badger\")}", "kv": () }),
        )
            .into();
        assert_matches!(
            reply,
            SourceReply::Structured {
                payload,
                ..
            } if payload == expected
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_event_error() -> Result<()> {
        let (tx, mut rx) = bounded(1);

        let (store_tx, _) = bounded(8);
        let (cluster_tx, mut cluster_rx) = bounded(8);

        tokio::spawn(async move {
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::IsLeader(result_tx) => {
                    let _ = result_tx.send(Ok(()));
                }
                IFRequest::SetKeyLocal(_, _) => panic!("wrong request"),
            };
            match cluster_rx.recv().await.expect("rcv") {
                IFRequest::SetKeyLocal(_key, result_tx) => {
                    let _ = result_tx.send(Err(ClusterError::RaftNotRunning.into()));
                }
                IFRequest::IsLeader(_) => panic!("wrong request"),
            };
        });
        let alias = alias::Connector::new("cluster_kv");
        let raft = raft::Cluster::dummy(store_tx, cluster_tx);

        let app_ctx = AppContext {
            raft: Some(raft.clone()),
            ..AppContext::default()
        };
        let mut sink = KvSink {
            alias,
            app_ctx,
            raft,
            tx,
            codec: Json::default(),
            origin_uri: EventOriginUri {
                scheme: "tremor-cluster-kv".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            },
            source_is_connected: Arc::new(true.into()),
        };
        let ctx = SinkContext::dummy("cluster_kv");
        let mut serializer = EventSerializer::dummy(None)?;

        let event = Event {
            data: (literal!(42), literal!({"kv": { "put": "the-key",  }})).into(),
            ..Event::default()
        };

        let reply = sink
            .on_event("in", event, &ctx, &mut serializer, 0)
            .await
            .expect("it's OK");

        assert_eq!(reply, SinkReply::FAIL);
        let reply = rx.recv().await.expect("it's OK");
        let expected: EventPayload = (
            Value::null(),
            literal!({ "error": "The raft node isn't running", "kv": {"op": "put", "key": "the-key"}}),
        )
            .into();
        assert_matches!(
            reply,
            SourceReply::Structured {
                payload,
                ..
            } if payload == expected
        );
        Ok(())
    }
}
