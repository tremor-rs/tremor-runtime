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

use std::fmt::Display;

use crate::connectors::prelude::*;
use crate::connectors::sink::concurrency_cap::ConcurrencyCap;
use crate::errors::{Error, Kind as ErrorKind, Result};
use async_std::channel::{bounded, Receiver, Sender};
use elasticsearch::cluster::ClusterHealthParts;
use elasticsearch::http::response::Response;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::Url;
use elasticsearch::params::Refresh;
use elasticsearch::{BulkDeleteOperation, BulkOperation, BulkOperations, BulkParts, Elasticsearch};
use tremor_common::time::nanotime;
use tremor_script::utils::sorted_serialize;
use tremor_value::value::StaticValue;
use value_trait::Mutable;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// list of elasticsearch cluster nodes
    pub nodes: Vec<String>,

    /// index to write events to, can be overwritten by metadata `$elastic["_index"]`
    pub index: Option<String>,

    /// maximum number of parallel in-flight requests before this connector is considered fully saturated
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// if true, ES success and error responses will contain the whole event payload they are based upon
    #[serde(default = "Default::default")]
    pub include_payload_in_response: bool,
}
impl ConfigImpl for Config {}

const DEFAULT_CONCURRENCY: usize = 4;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

#[derive(Default, Debug)]
pub(crate) struct Builder {}
#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "elastic".into()
    }

    async fn from_config(&self, id: &str, config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = &config.config {
            let config = Config::new(raw_config)?;
            if config.nodes.is_empty() {
                Err(
                    ErrorKind::InvalidConfiguration(id.to_string(), "empty nodes provided".into())
                        .into(),
                )
            } else {
                let node_urls = config
                    .nodes
                    .iter()
                    .map(|s| {
                        Url::parse(s.as_str()).map_err(|e| {
                            ErrorKind::InvalidConfiguration(id.to_string(), e.to_string()).into()
                        })
                    })
                    .collect::<Result<Vec<Url>>>()?;
                let (response_tx, response_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
                Ok(Box::new(Elastic {
                    node_urls,
                    config,
                    response_tx,
                    response_rx,
                }))
            }
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}

/// the elasticsearch connector - for sending stuff to elasticsearch
struct Elastic {
    node_urls: Vec<Url>,
    config: Config,
    response_tx: Sender<SourceReply>,
    response_rx: Receiver<SourceReply>,
}

#[async_trait::async_trait()]
impl Connector for Elastic {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Structured
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source =
            ChannelSource::from_channel(self.response_tx.clone(), self.response_rx.clone());
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ElasticSink::new(
            self.node_urls.clone(),
            self.response_tx.clone(),
            builder.reply_tx(),
            &self.config,
        );
        builder.spawn(sink, sink_context).map(Some)
    }
}

#[derive(Clone, Debug)]
struct ElasticClient {
    client: Elasticsearch,
    url: Url,
    cluster_name: String,
}

impl ElasticClient {
    fn new(client: Elasticsearch, url: Url, cluster_name: String) -> Self {
        Self {
            client,
            url,
            cluster_name,
        }
    }
}

impl Display for ElasticClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.url)
    }
}

/// round robin over the clients
///
/// TODO: behave like the round robin operator
struct ElasticClients {
    clients: Vec<ElasticClient>,
    idx: usize,
}

impl ElasticClients {
    fn new(clients: Vec<ElasticClient>) -> Self {
        Self { clients, idx: 0 }
    }

    /// return a freshly cloned client and its address.
    ///
    /// Sending a request tracks the transport-lifetime
    /// so we need to clone it, so we can handle it in a separate task.
    /// Cloning the client should be cheap
    fn next(&mut self) -> Option<ElasticClient> {
        let len = self.clients.len();
        if len > 0 {
            let idx = self.idx % len;
            self.idx += 1;
            self.clients.get(idx).cloned()
        } else {
            None
        }
    }
}

struct ElasticSink {
    node_urls: Vec<Url>,
    clients: ElasticClients,
    response_tx: Sender<SourceReply>,
    reply_tx: Sender<AsyncSinkReply>,
    concurrency_cap: ConcurrencyCap,
    include_payload: bool,
    default_index: Option<String>,
    origin_uri: EventOriginUri,
}

impl ElasticSink {
    // ALLOW: this is a string
    const MISSING_ID: &'static str = "Missing field `$elastic[\"_id\"]`";
    fn new(
        node_urls: Vec<Url>,
        response_tx: Sender<SourceReply>,
        reply_tx: Sender<AsyncSinkReply>,
        config: &Config,
    ) -> Self {
        Self {
            node_urls,
            clients: ElasticClients::new(vec![]),
            response_tx,
            reply_tx: reply_tx.clone(),
            concurrency_cap: ConcurrencyCap::new(config.concurrency, reply_tx),
            include_payload: config.include_payload_in_response,
            default_index: config.index.clone(),
            origin_uri: EventOriginUri {
                scheme: String::from("elastic"),
                host: String::from("dummy"), // will be replaced in `on_event`
                port: None,
                path: vec![],
            },
        }
    }
}

#[async_trait::async_trait()]
impl Sink for ElasticSink {
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let mut clients = Vec::with_capacity(self.node_urls.len());
        for node in &self.node_urls {
            let conn_pool = SingleNodeConnectionPool::new(node.clone());
            let transport = TransportBuilder::new(conn_pool).build()?;
            let client = Elasticsearch::new(transport);
            // we use the cluster health endpoint, as the ping endpoint is not reliable
            let res = client
                .cluster()
                .health(ClusterHealthParts::None)
                .send()
                .await?;
            let json = res.json::<StaticValue>().await?.into_value();
            let cluster_name = json.get_str("cluster_name").unwrap_or("").to_string();
            info!(
                "{} Connected to Elasticsearch cluster: {} via node: {}",
                ctx, cluster_name, node
            );
            let es_client = ElasticClient::new(client, node.clone(), cluster_name);
            clients.push(es_client);
        }
        self.clients = ElasticClients::new(clients);
        Ok(true)
    }

    async fn metrics(&mut self, _timestamp: u64, _ctx: &SinkContext) -> Vec<EventPayload> {
        // TODO: use the /_cluster/stats/nodes/ or /<index>/_stats/_all and expose them here
        // TODO: which are the important metrics to expose?
        vec![]
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        if event.is_empty() {
            debug!("{ctx} Received empty event. Won't send it to ES");
            return Ok(SinkReply::NONE);
        }
        // if we exceed the maximum concurrency here, we issue a CB close, but carry on anyhow
        let guard = self.concurrency_cap.inc_for(&event).await?;

        if let Some(client) = self.clients.next() {
            trace!("{} sending event [{}] to {}", ctx, event.id, client.url);

            // create task for awaiting the sending and handling the response
            let response_tx = self.response_tx.clone();
            let reply_tx = self.reply_tx.clone();
            let include_payload = self.include_payload;
            let mut origin_uri = self.origin_uri.clone();
            origin_uri.host = client.cluster_name;
            let default_index = self.default_index.clone();
            async_std::task::Builder::new()
                .name(format!(
                    "Elasticsearch Connector {}#{}",
                    ctx.alias(),
                    guard.num()
                ))
                .spawn::<_, Result<()>>(async move {
                    // build bulk request (we can't do that in a separate function)
                    let mut ops = BulkOperations::new();
                    // per request options - extract from event metadata (ignoring batched)
                    let event_es_meta = ESMeta::new(event.data.suffix().meta());
                    let index = event_es_meta.get_index().or(default_index.as_deref());
                    let doc_type = event_es_meta.get_type();
                    let routing = event_es_meta.get_routing();
                    let refresh = event_es_meta.get_refresh();
                    let timeout = event_es_meta.get_timeout();
                    let pipeline = event_es_meta.get_pipeline();
                    for (data, meta) in event.value_meta_iter() {
                        // item metadata
                        let es_meta = ESMeta::new(meta);
                        match es_meta.get_action() {
                            Some("index") | None => {
                                // index is the default action
                                let mut op = BulkOperation::index(data);
                                if let Some(index) = es_meta.get_index() {
                                    op = op.index(index);
                                }
                                if let Some(id) = es_meta.get_id() {
                                    op = op.id(id);
                                }
                                ops.push(op)?;
                            }
                            Some("delete") => {
                                let mut op: BulkDeleteOperation<()> =
                                    if let Some(id) = es_meta.get_id() {
                                        BulkOperation::delete(id)
                                    } else {
                                        let e = Error::from(Self::MISSING_ID);
                                        return handle_error(
                                            e,
                                            event,
                                            &origin_uri,
                                            &response_tx,
                                            &reply_tx,
                                            include_payload,
                                        )
                                        .await;
                                    };
                                if let Some(index) = es_meta.get_index() {
                                    op = op.index(index);
                                }
                                ops.push(op)?;
                            }
                            Some("create") => {
                                // create requires an `_id` here, which is not according to spec
                                let mut op = if let Some(id) = es_meta.get_id() {
                                    BulkOperation::create(id, data)
                                } else {
                                    // Actually `_id` should be completely optional here
                                    // See: https://github.com/elastic/elasticsearch-rs/issues/190
                                    // ALLOW: this is a string
                                    let e = Error::from(Self::MISSING_ID);
                                    return handle_error(
                                        e,
                                        event,
                                        &origin_uri,
                                        &response_tx,
                                        &reply_tx,
                                        include_payload,
                                    )
                                    .await;
                                };
                                if let Some(index) = es_meta.get_index() {
                                    op = op.index(index);
                                }
                                ops.push(op)?;
                            }
                            Some("update") => {
                                let mut op = if let Some(id) = es_meta.get_id() {
                                    BulkOperation::update(
                                        id,
                                        literal!({ "doc": data.clone_static() }), // TODO: find a way to not .clone_static()
                                    )
                                } else {
                                    let e = Error::from(Self::MISSING_ID);
                                    return handle_error(
                                        e,
                                        event,
                                        &origin_uri,
                                        &response_tx,
                                        &reply_tx,
                                        include_payload,
                                    )
                                    .await;
                                };
                                if let Some(index) = es_meta.get_index() {
                                    op = op.index(index);
                                }
                                ops.push(op)?;
                            }
                            Some(other) => {
                                // FIXME: send error response
                                let e = Error::from(format!("Invalid `$elastic.action` {}", other));
                                return handle_error(
                                    e,
                                    event,
                                    &origin_uri,
                                    &response_tx,
                                    &reply_tx,
                                    include_payload,
                                )
                                .await;
                            }
                        }
                    }

                    let parts = match (index, doc_type) {
                        (Some(index), Some(doc_type)) => BulkParts::IndexType(index, doc_type),
                        (Some(index), None) => BulkParts::Index(index),
                        _ => BulkParts::None,
                    };
                    let mut bulk = client.client.bulk(parts).body(vec![ops]);
                    // apply request scoped options
                    if let Some(routing) = routing {
                        bulk = bulk.routing(routing);
                    }
                    if let Some(refresh) = refresh {
                        let refresh = match refresh.as_str() {
                            "wait_for" => Refresh::WaitFor,
                            "false" => Refresh::False,
                            "true" => Refresh::True,
                            other => {
                                return handle_error(
                                    Error::from(format!(
                                        "Invalid value for `$elastic.refresh`: {}",
                                        other
                                    )),
                                    event,
                                    &origin_uri,
                                    &response_tx,
                                    &reply_tx,
                                    include_payload,
                                )
                                .await;
                            }
                        };
                        bulk = bulk.refresh(refresh);
                    }
                    if let Some(timeout) = timeout {
                        bulk = bulk.timeout(timeout);
                    }
                    if let Some(doc_type) = doc_type {
                        bulk = bulk.ty(doc_type);
                    }
                    if let Some(pipeline) = pipeline {
                        bulk = bulk.pipeline(pipeline);
                    }
                    match bulk.send().await.and_then(Response::error_for_status_code) {
                        Ok(response) => {
                            match response.json::<StaticValue>().await {
                                Ok(value) => {
                                    let v = value.into_value();
                                    // build responses - one for every item
                                    handle_response(
                                        v,
                                        event,
                                        &origin_uri,
                                        response_tx,
                                        reply_tx,
                                        include_payload,
                                        start,
                                    )
                                    .await?;
                                }
                                Err(e) => {
                                    // handle response deserialization error
                                    return handle_error(
                                        e,
                                        event,
                                        &origin_uri,
                                        &response_tx,
                                        &reply_tx,
                                        include_payload,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            return handle_error(
                                e,
                                event,
                                &origin_uri,
                                &response_tx,
                                &reply_tx,
                                include_payload,
                            )
                            .await;
                        }
                    }

                    drop(guard);

                    Ok(())
                })?;
            Ok(SinkReply::NONE)
        } else {
            error!("{} No elasticsearch client available.", &ctx);
            handle_error(
                Error::from("No elasticsearch client available."),
                event,
                &self.origin_uri,
                &self.response_tx,
                &self.reply_tx,
                self.include_payload,
            )
            .await?;
            Ok(SinkReply::NONE)
        }
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

async fn handle_response(
    mut response: Value<'static>,
    event: Event,
    elastic_origin_uri: &EventOriginUri,
    response_tx: Sender<SourceReply>,
    reply_tx: Sender<AsyncSinkReply>,
    include_payload: bool,
    start: u64,
) -> Result<()> {
    let correlation_values = event.correlation_metas();
    let payload_iter = event.value_iter();
    if let Some(items) = response.get_mut("items").and_then(Mutable::as_array_mut) {
        for ((mut item, correlation), payload) in items
            .drain(..)
            .zip(correlation_values.into_iter())
            .zip(payload_iter)
        {
            let (action, action_item) = if let Some(item_object) = item.as_object_mut() {
                if let Some((key, v)) = item_object.drain().next() {
                    (key, v)
                } else {
                    debug!("Skipping invalid action item: empty.");
                    continue;
                }
            } else {
                debug!("Skipping invalid action item: not an object.");
                continue;
            };
            let (data, meta, port) = if let Some(_error) = action_item.get("error") {
                // item failed
                let mut meta = literal!({
                    "elastic": {
                        "_id": action_item.get("_id").map(Value::clone_static),
                        "_index": action_item.get("_index").map(Value::clone_static),
                        "_type": action_item.get("_type").map(Value::clone_static),
                        "action": action.clone(),
                        "success": false
                    }
                });
                if let Some(correlation) = correlation {
                    meta.try_insert("correlation", correlation);
                }
                let mut data = literal!({ action: action_item });
                if include_payload {
                    data.try_insert("payload", payload.clone_static());
                }
                (data, meta, ERR)
            } else {
                // item succeeded
                let mut meta = literal!({
                    "elastic": {
                        "_id": action_item.get("_id").map(Value::clone_static),
                        "_index": action_item.get("_index").map(Value::clone_static),
                        "_type": action_item.get("_type").map(Value::clone_static),
                        "version": action_item.get("_version").map(Value::clone_static),
                        "action": action.clone(),
                        "success": true
                    }
                });
                if let Some(correlation) = correlation {
                    meta.try_insert("correlation", correlation);
                }
                let mut data = literal!({ action: action_item });
                if include_payload {
                    data.try_insert("payload", payload.clone_static());
                }
                (data, meta, OUT)
            };
            let event_payload: EventPayload = (data, meta).into();
            let source_reply = SourceReply::Structured {
                origin_uri: elastic_origin_uri.clone(),
                payload: event_payload,
                stream: DEFAULT_STREAM_ID,
                port: Some(port),
            };
            response_tx.send(source_reply).await?;
        }
    } else {
        return Err(Error::from(format!(
            "Invalid Response from ES: No \"items\" or not an array: {}",
            sorted_serialize(&response)?
        )));
    }
    // ack the event
    let duration = nanotime() - start;
    if event.transactional {
        reply_tx
            .send(AsyncSinkReply::Ack(ContraflowData::from(event), duration))
            .await?;
    }
    Ok(())
}

/// handle an error for the whole event
async fn handle_error<E>(
    e: E,
    event: Event,
    elastic_origin_uri: &EventOriginUri,
    response_tx: &Sender<SourceReply>,
    reply_tx: &Sender<AsyncSinkReply>,
    include_payload: bool,
) -> Result<()>
where
    E: std::error::Error,
{
    let e_str = e.to_string();
    let mut meta = literal!({
        "elastic": {
            "success": false
        },
        "error": e_str.clone()
    });
    if let Some(correlation) = event.correlation_meta() {
        meta.try_insert("correlation", correlation);
    }

    let mut data = Value::object_with_capacity(1);
    if include_payload {
        data.try_insert("payload", event.data.suffix().value().clone_static());
    }
    let event_payload: EventPayload = (data, meta).into();
    let source_reply = SourceReply::Structured {
        origin_uri: elastic_origin_uri.clone(),
        payload: event_payload,
        stream: DEFAULT_STREAM_ID,
        port: Some(ERR),
    };
    response_tx.send(source_reply).await?;
    if event.transactional {
        reply_tx
            .send(AsyncSinkReply::Fail(ContraflowData::from(event)))
            .await?;
    }
    Ok(())
}

struct ESMeta<'a, 'value> {
    meta: Option<&'a Value<'value>>,
}

impl<'a, 'value> ESMeta<'a, 'value> {
    fn new(meta: &'a Value<'value>) -> Self {
        Self {
            meta: if let Some(elastic_meta) = meta.get("elastic") {
                Some(elastic_meta)
            } else {
                None
            },
        }
    }

    fn get_id(&self) -> Option<&str> {
        self.meta.get_str("_id")
    }
    fn get_index(&self) -> Option<&str> {
        self.meta.get_str("_index")
    }

    fn get_type(&self) -> Option<&str> {
        self.meta.get_str("_type")
    }
    fn get_routing(&self) -> Option<&str> {
        self.meta.get_str("routing")
    }
    fn get_timeout(&self) -> Option<&str> {
        self.meta.get_str("timeout")
    }
    fn get_pipeline(&self) -> Option<&str> {
        self.meta.get_str("pipeline")
    }

    fn get_action(&self) -> Option<&str> {
        self.meta.get_str("action")
    }

    /// supported values: `true`, `false`, `"true"`, `"false"`, `"wait_for"`
    /// FIXME: Should we return a type here?
    fn get_refresh(&self) -> Option<String> {
        self.meta
            .get_bool("refresh")
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| self.meta.get_str("refresh").map(ToString::to_string))
    }
}
