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
use std::time::Duration;

use crate::{
    connectors::{
        impls::http::utils::Header, prelude::*, sink::concurrency_cap::ConcurrencyCap,
        utils::tls::TLSClientConfig,
    },
    errors::{Error, Kind as ErrorKind, Result},
};
use async_std::channel::{bounded, Receiver, Sender};
use either::Either;
use elasticsearch::{
    cert::{Certificate, CertificateValidation},
    cluster::ClusterHealthParts,
    http::{
        response::Response,
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    params::Refresh,
    Bulk, BulkDeleteOperation, BulkOperation, BulkOperations, BulkParts, Elasticsearch,
};
use halfbrown::HashMap;
use tremor_common::time::nanotime;
use tremor_script::utils::sorted_serialize;
use tremor_value::value::StaticValue;
use value_trait::Mutable;

use super::http::auth::Auth;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// list of elasticsearch cluster nodes
    nodes: Vec<String>,

    /// index to write events to, can be overwritten by metadata `$elastic["_index"]`
    index: Option<String>,

    /// maximum number of parallel in-flight requests before this connector is considered fully saturated
    #[serde(default = "default_concurrency")]
    concurrency: usize,

    /// if true, ES success and error responses will contain the whole event payload they are based upon
    #[serde(default = "default_false")]
    include_payload_in_response: bool,

    #[serde(default = "Default::default")]
    /// custom headers to add to each request to elastic
    headers: HashMap<String, Header>,

    /// means for authenticating towards elastic
    #[serde(default = "default_auth")]
    auth: Auth,

    tls: Option<TLSClientConfig>,

    /// request timeout in nanoseconds for each request against elasticsearch
    #[serde(default = "Default::default")]
    timeout: Option<u64>,
}
impl ConfigImpl for Config {}

const DEFAULT_CONCURRENCY: usize = 4;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_auth() -> Auth {
    Auth::None
}

#[derive(Default, Debug)]
pub(crate) struct Builder {}
#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "elastic".into()
    }

    async fn build(&self, id: &str, config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = &config.config {
            let config = Config::new(raw_config)?;
            if config.nodes.is_empty() {
                Err(ErrorKind::InvalidConnectorDefinition(
                    id.to_string(),
                    "empty nodes provided".into(),
                )
                .into())
            } else {
                let node_urls = config
                    .nodes
                    .iter()
                    .map(|s| {
                        Url::parse(s.as_str()).map_err(|e| {
                            ErrorKind::InvalidConnectorDefinition(id.to_string(), e.to_string())
                                .into()
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
    config: Config,
    origin_uri: EventOriginUri,
}

impl ElasticSink {
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
            config: config.clone(),
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
            let mut transport_builder = TransportBuilder::new(conn_pool).enable_meta_header(false); // no meta header, that's just overhead
            if let Some(timeout_ns) = self.config.timeout.as_ref() {
                let duration = Duration::from_nanos(*timeout_ns);
                transport_builder = transport_builder.timeout(duration);
            }
            if !self.config.headers.is_empty() {
                let mut headermap = reqwest::header::HeaderMap::new();
                for (k, v) in &self.config.headers {
                    match v {
                        Header(Either::Left(values)) => {
                            for value in values {
                                headermap.append(
                                    reqwest::header::HeaderName::from_bytes(k.as_bytes())?,
                                    reqwest::header::HeaderValue::from_str(value.as_str())?,
                                );
                            }
                        }
                        Header(Either::Right(value)) => {
                            headermap.append(
                                reqwest::header::HeaderName::from_bytes(k.as_bytes())?,
                                reqwest::header::HeaderValue::from_str(value.as_str())?,
                            );
                        }
                    }
                }
                transport_builder = transport_builder.headers(headermap);
            }

            if let Ok(Some(credentials)) = self.config.auth.as_elastic_credentials() {
                transport_builder = transport_builder.auth(credentials);
            }
            let cert_validation = if let Some(cafile) = self
                .config
                .tls
                .as_ref()
                .and_then(|tls_config| tls_config.cafile.as_ref())
            {
                let file = async_std::fs::read(cafile).await?;
                let cert = Certificate::from_pem(file.as_slice())
                    .or_else(|_| Certificate::from_der(file.as_slice()))?;
                CertificateValidation::Full(cert)
            } else {
                CertificateValidation::Default
            };
            transport_builder = transport_builder.cert_validation(cert_validation);

            let client = Elasticsearch::new(transport_builder.build()?);
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
            trace!("{ctx} sending event [{}] to {}", event.id, client.url);

            // create task for awaiting the sending and handling the response
            let response_tx = self.response_tx.clone();
            let reply_tx = self.reply_tx.clone();
            let include_payload = self.config.include_payload_in_response;
            let mut origin_uri = self.origin_uri.clone();
            origin_uri.host = client.cluster_name;
            let default_index = self.config.index.clone();
            let task_ctx = ctx.clone();
            async_std::task::Builder::new()
                .name(format!(
                    "Elasticsearch Connector {}#{}",
                    ctx.alias(),
                    guard.num()
                ))
                .spawn::<_, Result<()>>(async move {
                    let r: Result<Value> = (|| async {
                        // build bulk request (we can't do that in a separate function)
                        let mut ops = BulkOperations::new();
                        // per request options - extract from event metadata (ignoring batched)
                        let event_es_meta = ESMeta::new(event.data.suffix().meta());

                        for (data, meta) in event.value_meta_iter() {
                            ESMeta::new(meta).insert_op(data, &mut ops)?;
                        }

                        let parts = event_es_meta.parts(default_index.as_deref());
                        let bulk =
                            event_es_meta.apply_to(client.client.bulk(parts).body(vec![ops]))?;
                        // apply request scoped options

                        let response = bulk
                            .send()
                            .await
                            .and_then(Response::error_for_status_code)?;
                        let value = response.json::<StaticValue>().await?;
                        Ok(value.into_value())
                    })()
                    .await;
                    match r {
                        Err(e) => {
                            debug!("{task_ctx} Error sending Elasticsearch Bulk Request: {e}");
                            task_ctx.swallow_err(
                                handle_error(
                                    e,
                                    event,
                                    &origin_uri,
                                    &response_tx,
                                    &reply_tx,
                                    include_payload,
                                )
                                .await,
                                "Error handling ES error",
                            );
                        }
                        Ok(v) => {
                            task_ctx.swallow_err(
                                handle_response(
                                    v,
                                    event,
                                    &origin_uri,
                                    response_tx,
                                    reply_tx,
                                    include_payload,
                                    start,
                                )
                                .await,
                                "Error handling ES response",
                            );
                        }
                    }
                    drop(guard);

                    Ok(())
                })?;
        } else {
            error!("{} No elasticsearch client available.", &ctx);
            handle_error(
                Error::from("No elasticsearch client available."),
                event,
                &self.origin_uri,
                &self.response_tx,
                &self.reply_tx,
                self.config.include_payload_in_response,
            )
            .await?;
        }
        Ok(SinkReply::NONE)
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
    // ALLOW: this is a string
    const MISSING_ID: &'static str = "Missing field `$elastic[\"_id\"]`";

    fn new(meta: &'a Value<'value>) -> Self {
        Self {
            meta: if let Some(elastic_meta) = meta.get("elastic") {
                Some(elastic_meta)
            } else {
                None
            },
        }
    }

    fn insert_op(&self, data: &Value, ops: &mut BulkOperations) -> Result<()> {
        // index is the default action
        match self.get_action().unwrap_or("index") {
            "index" => {
                let mut op = BulkOperation::index(data);
                if let Some(id) = self.get_id() {
                    op = op.id(id);
                }
                if let Some(index) = self.get_index() {
                    op = op.index(index);
                }
                ops.push(op).map_err(Into::into)
            }
            "delete" => {
                let mut op: BulkDeleteOperation<()> = self
                    .get_id()
                    .map(BulkOperation::delete)
                    .ok_or_else(|| Error::from(Self::MISSING_ID))?;
                if let Some(index) = self.get_index() {
                    op = op.index(index);
                }
                ops.push(op).map_err(Into::into)
            }

            "create" => {
                // create requires an `_id` here, which is not according to spec
                // Actually `_id` should be completely optional here
                // See: https://github.com/elastic/elasticsearch-rs/issues/190
                let mut op = self
                    .get_id()
                    .map(|id| BulkOperation::create(id, data))
                    .ok_or_else(|| Error::from(Self::MISSING_ID))?;
                if let Some(index) = self.get_index() {
                    op = op.index(index);
                }
                ops.push(op).map_err(Into::into)
            }
            "update" => {
                let mut op = self
                    .get_id()
                    .map(|id| {
                        // TODO: find a way to not .clone_static()
                        let src = if self.get_raw_payload() {
                            data.clone_static()
                        } else {
                            literal!({ "doc": data.clone_static() })
                        };
                        BulkOperation::update(id, src)
                    })
                    .ok_or_else(|| Error::from(Self::MISSING_ID))?;
                if let Some(index) = self.get_index() {
                    op = op.index(index);
                }
                ops.push(op).map_err(Into::into)
            }
            other => Err(Error::from(format!("Invalid `$elastic.action` {}", other))),
        }
    }

    fn parts<'blk>(&'blk self, default_index: Option<&'blk str>) -> BulkParts<'blk> {
        match (self.get_index().or(default_index), self.get_type()) {
            (Some(index), Some(doc_type)) => BulkParts::IndexType(index, doc_type),
            (Some(index), None) => BulkParts::Index(index),
            _ => BulkParts::None,
        }
    }

    fn apply_to<'bulk, 'meta, T>(
        &'meta self,
        mut bulk: Bulk<'bulk, 'meta, T>,
    ) -> Result<Bulk<'bulk, 'meta, T>>
    where
        T: elasticsearch::http::request::Body,
    {
        if let Some(routing) = self.get_routing() {
            bulk = bulk.routing(routing);
        }
        if let Some(refresh) = self.get_refresh()? {
            bulk = bulk.refresh(refresh);
        }
        if let Some(timeout) = self.get_timeout() {
            bulk = bulk.timeout(timeout);
        }
        if let Some(doc_type) = self.get_type() {
            bulk = bulk.ty(doc_type);
        }
        if let Some(pipeline) = self.get_pipeline() {
            bulk = bulk.pipeline(pipeline);
        }
        Ok(bulk)
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

    fn get_raw_payload(&self) -> bool {
        self.meta.get_bool("raw_payload").unwrap_or_default()
    }

    /// supported values: `true`, `false`, `"wait_for"`
    fn get_refresh(&self) -> Result<Option<Refresh>> {
        let refresh = self.meta.get("refresh");
        if let Some(b) = refresh.as_bool() {
            Ok(Some(if b { Refresh::True } else { Refresh::False }))
        } else if refresh.as_str() == Some("wait_for") {
            Ok(Some(Refresh::WaitFor))
        } else if let Some(other) = refresh {
            Err(Error::from(format!(
                "Invalid value for `$elastic.refresh`: {}",
                other
            )))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Connector as ConnectorConfig;

    #[async_std::test]
    async fn connector_builder_empty_nodes() -> Result<()> {
        let config = literal!({
            "config": {
                "nodes": []
            }
        });
        let id = "my_elastic";
        let builder = super::Builder::default();
        let connector_config = ConnectorConfig::from_config(id, builder.connector_type(), &config)?;
        assert_eq!(
            String::from("Invalid Definition for connector \"my_elastic\": empty nodes provided"),
            builder
                .build("my_elastic", &connector_config)
                .await
                .err()
                .unwrap()
                .to_string()
        );
        Ok(())
    }

    #[async_std::test]
    async fn connector_builder_invalid_url() -> Result<()> {
        let config = literal!({
            "config": {
                "nodes": [
                    "http://localhost:12345/foo/bar/baz",
                    ":::////*^%$"
                ]
            }
        });
        let id = "my_elastic";
        let builder = super::Builder::default();
        let connector_config = ConnectorConfig::from_config(id, builder.connector_type(), &config)?;
        assert_eq!(
            String::from(
                "Invalid Definition for connector \"my_elastic\": relative URL without a base"
            ),
            builder
                .build("my_elastic", &connector_config)
                .await
                .err()
                .unwrap()
                .to_string()
        );
        Ok(())
    }
}
