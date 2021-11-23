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

use std::time::Duration;

use crate::connectors::prelude::*;
use crate::connectors::sink::concurrency_cap::ConcurrencyCap;
use crate::errors::{Error, ErrorKind, Result};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::prelude::*;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::Url;
use elasticsearch::params::Refresh;
use elasticsearch::{BulkOperation, BulkOperations, BulkParts, Elasticsearch};
use tremor_common::time::nanotime;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// list of elasticsearch cluster nodes
    pub nodes: Vec<String>,

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

/// the elasticsearch connector - for sending stuff to elasticsearch
struct Elastic {
    node_urls: Vec<Url>,
    max_concurrency: usize,
    include_payload: bool,
    response_tx: Sender<SourceReply>,
    response_rx: Receiver<SourceReply>,
    clients_tx: Sender<Vec<ElasticClient>>,
    clients_rx: Receiver<Vec<ElasticClient>>,
}

/// avoiding lifetime issues with generics
/// See: https://github.com/rust-lang/rust/issues/64552
struct StaticValue(tremor_value::Value<'static>);
impl<'de> serde::de::Deserialize<'de> for StaticValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        tremor_value::Value::deserialize(deserializer).map(|value| StaticValue(value.into_static()))
    }
}

#[async_trait::async_trait()]
impl Connector for Elastic {
    fn is_structured(&self) -> bool {
        true
    }
    fn default_codec(&self) -> &str {
        "json"
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = ElasticSource {
            rx: self.response_rx.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ElasticSink::new(
            self.clients_rx.clone(),
            self.response_tx.clone(),
            builder.reply_tx(),
            self.max_concurrency,
            self.include_payload,
        );
        builder.spawn(sink, sink_context).map(Some)
    }

    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let mut clients = Vec::with_capacity(self.node_urls.len());
        for node in &self.node_urls {
            let conn_pool = SingleNodeConnectionPool::new(node.clone());
            let transport = TransportBuilder::new(conn_pool).build()?;
            let client = Elasticsearch::new(transport);
            let res = client.ping().send().await?;
            let json = res.json::<StaticValue>().await?.0;
            let cluster_name = json.get_str("cluster_name").unwrap_or("").to_string();
            info!(
                "{} Connected to Elasticsearch cluster: {} via node: {}",
                ctx, cluster_name, node
            );
            let es_client = ElasticClient::new(client, node.clone(), cluster_name);
            clients.push(es_client);
        }
        self.clients_tx.send(clients).await?;
        Ok(true)
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
        let idx = self.idx % self.clients.len();
        self.idx += 1;
        self.clients.get(idx).cloned()
    }
}

struct ElasticSink {
    clients: ElasticClients,
    clients_rx: Receiver<Vec<ElasticClient>>,
    response_tx: Sender<SourceReply>,
    reply_tx: Sender<AsyncSinkReply>,
    concurrency_cap: ConcurrencyCap,
    include_payload: bool,
    origin_uri: EventOriginUri,
}

impl ElasticSink {
    fn new(
        clients_rx: Receiver<Vec<ElasticClient>>,
        response_tx: Sender<SourceReply>,
        reply_tx: Sender<AsyncSinkReply>,
        max_in_flight_requests: usize,
        include_payload: bool,
    ) -> Self {
        Self {
            clients: ElasticClients::new(vec![]),
            clients_rx,
            response_tx,
            reply_tx: reply_tx.clone(),
            concurrency_cap: ConcurrencyCap::new(max_in_flight_requests, reply_tx),
            include_payload,
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
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        start: u64,
    ) -> Result<SinkReply> {
        if let Ok(new_clients) = self.clients_rx.try_recv() {
            self.clients = ElasticClients::new(new_clients);
        }
        // if we exceed the maximum concurrency here, we issue a CB close, but carry on anyhow
        let guard = self.concurrency_cap.inc_for(&event).await?;

        if let Some(client) = self.clients.next() {
            debug!("{} sending event {} to {}", ctx, client.url, event.id);

            // create task for awaiting the sending and handling the response
            let response_tx = self.response_tx.clone();
            let reply_tx = self.reply_tx.clone();
            let include_payload = self.include_payload;
            let mut origin_uri = self.origin_uri.clone();
            origin_uri.host = client.cluster_name;
            async_std::task::Builder::new()
                .name(format!("Elasticsearch Connector #{}", guard.num()))
                .spawn::<_, Result<()>>(async move {
                    // build bulk request (we can't do that in a separate function)
                    let mut ops = BulkOperations::new();
                    // per request options
                    let mut index = None;
                    let mut doc_type = None;
                    let mut routing = None;
                    let mut refresh: Option<String> = None;
                    let mut timeout = None;
                    let mut pipeline = None;
                    for (data, meta) in event.value_meta_iter() {
                        if let Some(connector_meta) = meta.get("connector") {
                            if let Some(elastic_meta) = connector_meta.get("elastic") {
                                if index.is_none() {
                                    index = elastic_meta.get_str("index");
                                }
                                if doc_type.is_none() {
                                    doc_type = elastic_meta.get_str("_type");
                                }
                                if routing.is_none() {
                                    routing = elastic_meta.get_str("routing");
                                }
                                if refresh.is_none() {
                                    // supported values: true, false, "true", "false", "wait_for"
                                    refresh = elastic_meta
                                        .get_bool("refresh")
                                        .as_ref()
                                        .map(ToString::to_string)
                                        .or_else(|| {
                                            elastic_meta.get_str("refresh").map(ToString::to_string)
                                        });
                                }
                                if timeout.is_none() {
                                    timeout = elastic_meta.get_str("timeout");
                                }
                                if pipeline.is_none() {
                                    pipeline = elastic_meta.get_str("pipeline");
                                }

                                match elastic_meta.get_str("action") {
                                    Some("index") | None => {
                                        let mut op = BulkOperation::index(data);
                                        if let Some(index) = elastic_meta.get_str("_index") {
                                            op = op.index(index);
                                        }
                                        if let Some(id) = elastic_meta.get_str("_id") {
                                            op = op.id(id);
                                        }
                                        ops.push(op)?;
                                    } // FIXME: add others
                                    Some(other) => {
                                        // FIXME: send error response
                                        let e = Error::from(format!(
                                            "Invalid `$connector.elastic.action` {}",
                                            other
                                        ));
                                        return handle_error(
                                            e,
                                            event,
                                            &origin_uri,
                                            response_tx,
                                            reply_tx,
                                            include_payload,
                                        )
                                        .await;
                                    }
                                }
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
                                        "Invalid value for `$connector.elastic.refresh`: {}",
                                        other
                                    )),
                                    event,
                                    &origin_uri,
                                    response_tx,
                                    reply_tx,
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
                    if let Some(pipeline) = pipeline {
                        bulk = bulk.pipeline(pipeline);
                    }
                    match bulk.send().await {
                        Ok(response) => {
                            match response.json::<StaticValue>().await {
                                Ok(value) => {
                                    // build responses - one for every item
                                    handle_response(
                                        value.0,
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
                                        response_tx,
                                        reply_tx,
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
                                response_tx,
                                reply_tx,
                                include_payload,
                            )
                            .await;
                        }
                    }
                    Ok(())
                })?;
            Ok(SinkReply::NONE)
        } else {
            error!("{} No elasticsearch client available.", &ctx);
            // FIXME: build and send error response
            Ok(SinkReply::FAIL)
        }
    }

    async fn on_signal(
        &mut self,
        _signal: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
    ) -> Result<SinkReply> {
        // check for a client refresh
        if let Ok(new_clients) = self.clients_rx.try_recv() {
            debug!("{} Received new clients", ctx);
            self.clients = ElasticClients::new(new_clients);
        }
        Ok(SinkReply::default())
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

async fn handle_response(
    response: Value<'static>,
    event: Event,
    elastic_origin_uri: &EventOriginUri,
    response_tx: Sender<SourceReply>,
    reply_tx: Sender<AsyncSinkReply>,
    include_payload: bool,
    start: u64,
) -> Result<()> {
    let correlation_values = event.correlation_metas();
    let payload_iter = event.value_iter();
    if let Some(items) = response.get_array("items") {
        for ((item, correlation), payload) in items
            .into_iter()
            .zip(correlation_values.into_iter())
            .zip(payload_iter)
        {
            let (data, meta, port) = if let Some(_error) = item.get("error") {
                // item failed
                let mut meta = literal!({
                    "connector": {
                        "elastic": {
                            "_id": item.get("_id").map(Value::clone_static),
                            "_index": item.get("_index").map(Value::clone_static),
                            "_type": item.get("_type").map(Value::clone_static),
                        }
                    }
                });
                if let Some(correlation) = correlation {
                    meta.try_insert("correlation", correlation);
                }
                let mut data = literal!({
                    "success": false,
                    "item": item.clone_static()
                });
                if include_payload {
                    data.try_insert("payload", payload.clone_static());
                }
                (data, meta, ERR)
            } else {
                // item succeeded
                let mut meta = literal!({
                    "connector": {
                        "elastic": {
                            "_id": item.get("_id").map(Value::clone_static),
                            "_index": item.get("_index").map(Value::clone_static),
                            "_type": item.get("_type").map(Value::clone_static),
                            "version": item.get("_version").map(Value::clone_static)
                        }
                    }
                });
                if let Some(correlation) = correlation {
                    meta.try_insert("correlation", correlation);
                }
                let mut data = literal!({
                    "success": true,
                    "item": item.clone_static()
                });
                if include_payload {
                    data.try_insert("payload", payload.clone_static());
                }
                (data, meta, OUT)
            };
            let event_payload: EventPayload = (data, meta).into();
            let source_reply = SourceReply::Structured {
                origin_uri: elastic_origin_uri.clone(),
                payload: event_payload,
                stream: DEFAULT_STREAM_ID, //TODO: assign each bulk request a stream id?
                port: Some(port),
            };
            response_tx.send(source_reply).await?;
        }
    }
    // ack the event
    let duration = nanotime() - start;
    reply_tx
        .send(AsyncSinkReply::Ack(ContraflowData::from(event), duration))
        .await?;
    Ok(())
}

/// handle an error for the whole event
async fn handle_error<E>(
    e: E,
    event: Event,
    elastic_origin_uri: &EventOriginUri,
    response_tx: Sender<SourceReply>,
    reply_tx: Sender<AsyncSinkReply>,
    include_payload: bool,
) -> Result<()>
where
    E: std::error::Error,
{
    let e_str = e.to_string();
    let mut meta = Value::object_with_capacity(1);
    if let Some(correlation) = event.correlation_meta() {
        meta.try_insert("correlation", correlation);
    }

    let mut data = literal!({
        "success": false,
        "error": e_str,
        "source": {
            "event_id": event.id.to_string(),
            "origin_uri": event.origin_uri.as_ref().map(|uri| uri.to_string())
        }
    });
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
    reply_tx
        .send(AsyncSinkReply::Fail(ContraflowData::from(event)))
        .await?;
    Ok(())
}

/// time to wait for an answer before handing control back to the source manager
const SOURCE_RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Very simple source that just listens to a channel for new `SourceReply` messages.
/// Fed by the sink
struct ElasticSource {
    rx: Receiver<SourceReply>,
}

#[async_trait::async_trait()]
impl Source for ElasticSource {
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        match self.rx.recv().timeout(SOURCE_RECV_TIMEOUT).await {
            Ok(Ok(source_reply)) => Ok(source_reply),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Ok(SourceReply::Empty(10)),
        }
    }
    fn is_transactional(&self) -> bool {
        // we just send out responses or errors via the source, so no transactionality needed
        false
    }
}

struct Builder {}
#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "elastic".into()
    }

    async fn from_config(
        &self,
        id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;
            if config.nodes.is_empty() {
                Err(
                    ErrorKind::InvalidConfiguration(id.to_string(), "empty nodes provided".into())
                        .into(),
                )
            } else {
                let node_urls = config
                    .nodes
                    .into_iter()
                    .map(|s| {
                        Url::parse(s.as_str()).map_err(|e| {
                            ErrorKind::InvalidConfiguration(id.to_string(), e.to_string()).into()
                        })
                    })
                    .collect::<Result<Vec<Url>>>()?;
                let (clients_tx, clients_rx) = bounded(128);
                let (response_tx, response_rx) = bounded(128);
                Ok(Box::new(Elastic {
                    node_urls,
                    max_concurrency: config.concurrency,
                    include_payload: config.include_payload_in_response,
                    clients_tx,
                    clients_rx,
                    response_tx,
                    response_rx,
                }))
            }
        } else {
            Err(ErrorKind::MissingConfiguration(id.to_string()).into())
        }
    }
}
