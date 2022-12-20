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
use std::{fmt::Display, sync::atomic::AtomicBool};

use crate::system::KillSwitch;
use crate::{
    connectors::{
        impls::http::utils::Header, prelude::*, sink::concurrency_cap::ConcurrencyCap,
        utils::tls::TLSClientConfig,
    },
    errors::{err_connector_def, Error, Result},
};
use async_std::{
    channel::{bounded, Receiver, Sender},
    sync::Arc,
};
use either::Either;
use elasticsearch::{
    auth::{ClientCertificate, Credentials},
    cert::{Certificate, CertificateValidation},
    cluster::ClusterHealthParts,
    http::{
        response::Response,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    params::{Refresh, VersionType},
    Bulk, BulkDeleteOperation, BulkOperation, BulkOperations, BulkParts, BulkUpdateOperation,
    Elasticsearch,
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
    nodes: Vec<Url>,

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
    #[serde(default = "Default::default")]
    auth: Auth,

    /// optional tls client config
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    tls: Option<Either<TLSClientConfig, bool>>,

    /// request timeout in nanoseconds for each request against elasticsearch
    #[serde(default = "Default::default")]
    timeout: Option<u64>,
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

    async fn build_cfg(
        &self,
        id: &Alias,
        _: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(raw_config)?;
        if config.nodes.is_empty() {
            Err(err_connector_def(id, "empty nodes provided"))
        } else {
            let tls_config = match config.tls.as_ref() {
                Some(Either::Left(tls_config)) => Some(tls_config.clone()),
                Some(Either::Right(true)) => Some(TLSClientConfig::default()),
                Some(Either::Right(false)) | None => None,
            };
            if tls_config.is_some() {
                for node_url in &config.nodes {
                    if node_url.scheme() != "https" {
                        let e = format!("Node URL '{node_url}' needs 'https' scheme with tls.");
                        return Err(err_connector_def(id, &e));
                    }
                }
            }
            let credentials = if let Some((certfile, keyfile)) = tls_config
                .as_ref()
                .and_then(|tls| tls.cert.as_ref().zip(tls.key.as_ref()))
            {
                let mut cert_chain = async_std::fs::read(certfile).await?;
                let mut key = async_std::fs::read(keyfile).await?;
                key.append(&mut cert_chain);
                let client_certificate = ClientCertificate::Pem(key);
                Some(Credentials::Certificate(client_certificate))
            } else {
                match &config.auth {
                    Auth::Basic { username, password } => {
                        Some(Credentials::Basic(username.clone(), password.clone()))
                    }
                    Auth::Bearer(token) => Some(Credentials::Bearer(token.clone())),
                    Auth::ElasticsearchApiKey { id, api_key } => {
                        Some(Credentials::ApiKey(id.clone(), api_key.clone()))
                    }
                    // Gcp Auth is handled in sink connect
                    Auth::Gcp | Auth::None => None,
                }
            };
            let cert_validation =
                if let Some(cafile) = tls_config.as_ref().and_then(|tls| tls.cafile.as_ref()) {
                    let file = async_std::fs::read(cafile).await?;
                    CertValidation::Full(file)
                } else if tls_config.is_some() {
                    CertValidation::Default
                } else {
                    CertValidation::None
                };
            let (response_tx, response_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
            let source_is_connected = Arc::new(AtomicBool::new(false));
            Ok(Box::new(Elastic {
                config,
                cert_validation,
                credentials,
                response_tx,
                response_rx,
                source_is_connected,
            }))
        }
    }
}

/// the elasticsearch connector - for sending stuff to elasticsearch
struct Elastic {
    config: Config,
    cert_validation: CertValidation,
    credentials: Option<Credentials>,
    response_tx: Sender<SourceReply>,
    response_rx: Receiver<SourceReply>,
    source_is_connected: Arc<AtomicBool>,
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
        let source = ElasticSource {
            source_is_connected: self.source_is_connected.clone(),
            response_rx: self.response_rx.clone(),
        };
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ElasticSink::new(
            self.response_tx.clone(),
            builder.reply_tx(),
            self.source_is_connected.clone(),
            &self.config,
            self.credentials.clone(),
            self.cert_validation.clone(),
        );
        builder.spawn(sink, sink_context).map(Some)
    }
}

struct ElasticSource {
    source_is_connected: Arc<AtomicBool>,
    response_rx: Receiver<SourceReply>,
}

#[async_trait::async_trait]
impl Source for ElasticSource {
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        Ok(self.response_rx.recv().await?)
    }

    async fn on_cb_open(&mut self, _ctx: &SourceContext) -> Result<()> {
        // we will only know if we are connected to some pipelines if we receive a CBAction::Restore contraflow event
        // we will not send responses to out/err if we are not connected and this is determined by this variable
        self.source_is_connected.store(true, Ordering::Release);
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
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
    clients: ElasticClients,
    response_tx: Sender<SourceReply>,
    reply_tx: Sender<AsyncSinkReply>,
    concurrency_cap: ConcurrencyCap,
    source_is_connected: Arc<AtomicBool>,
    config: Config,
    es_credentials: Option<Credentials>,
    cert_validation: CertValidation,
    origin_uri: EventOriginUri,
}

impl ElasticSink {
    fn new(
        response_tx: Sender<SourceReply>,
        reply_tx: Sender<AsyncSinkReply>,
        source_is_connected: Arc<AtomicBool>,
        config: &Config,
        es_credentials: Option<Credentials>,
        cert_validation: CertValidation,
    ) -> Self {
        Self {
            clients: ElasticClients::new(vec![]),
            response_tx,
            reply_tx: reply_tx.clone(),
            concurrency_cap: ConcurrencyCap::new(config.concurrency, reply_tx),
            source_is_connected,
            config: config.clone(),
            es_credentials,
            cert_validation,
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
        let mut clients = Vec::with_capacity(self.config.nodes.len());
        for node in &self.config.nodes {
            let conn_pool = SingleNodeConnectionPool::new(node.url().clone());
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
            // client auth credentials
            if let Some(credentials) = self.es_credentials.as_ref() {
                transport_builder = transport_builder.auth(credentials.clone());
            }
            // server certificate validation
            if let Some(cert_validation) = self.cert_validation.as_certificate_validation()? {
                transport_builder = transport_builder.cert_validation(cert_validation);
            }

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

    #[allow(clippy::too_many_lines)]
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
        // we need to check if the source is actually connected
        // we should not send response events if it isn't
        // as this would fill the response_tx channel and block the send tasks
        // and keep them from completing
        // The result is a complete hang and no progress. :(
        let source_is_connected = self.source_is_connected.load(Ordering::Relaxed);
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

                        // apply request scoped options
                        let bulk =
                            event_es_meta.apply_to(client.client.bulk(parts).body(vec![ops]))?;

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
                            if source_is_connected {
                                task_ctx.swallow_err(
                                    handle_error(
                                        e,
                                        &event,
                                        &origin_uri,
                                        &response_tx,
                                        include_payload,
                                    )
                                    .await,
                                    "Error handling ES error",
                                );
                            }
                            task_ctx.swallow_err(
                                send_fail(event, &reply_tx).await,
                                "Error sending fail CB",
                            );
                        }
                        Ok(v) => {
                            if source_is_connected {
                                task_ctx.swallow_err(
                                    handle_response(
                                        v,
                                        &event,
                                        &origin_uri,
                                        response_tx,
                                        include_payload,
                                    )
                                    .await,
                                    "Error handling ES response",
                                );
                            }

                            task_ctx.swallow_err(
                                send_ack(event, start, &reply_tx).await,
                                "Error sending ack CB",
                            );
                        }
                    }
                    drop(guard);

                    Ok(())
                })?;
            Ok(SinkReply::NONE)
        } else {
            // shouldn't happen actually
            error!("{} No elasticsearch client available.", &ctx);
            handle_error(
                Error::from(ErrorKind::ClientNotAvailable(
                    "elastic",
                    "No elasticsearch client available.",
                )),
                &event,
                &self.origin_uri,
                &self.response_tx,
                self.config.include_payload_in_response,
            )
            .await?;
            ctx.swallow_err(
                ctx.notifier().connection_lost().await,
                "Error notifying about lost connection",
            );
            Ok(SinkReply::FAIL)
        }
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

/// Handle successful response from ES
///
/// send event to OUT
async fn handle_response(
    mut response: Value<'static>,
    event: &Event,
    elastic_origin_uri: &EventOriginUri,
    response_tx: Sender<SourceReply>,
    include_payload: bool,
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
    Ok(())
}

/// handle an error for the whole event
///
/// send event to err port
async fn handle_error<E>(
    e: E,
    event: &Event,
    elastic_origin_uri: &EventOriginUri,
    response_tx: &Sender<SourceReply>,
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
    Ok(())
}

async fn send_ack(event: Event, start: u64, reply_tx: &Sender<AsyncSinkReply>) -> Result<()> {
    if event.transactional {
        reply_tx
            .send(AsyncSinkReply::Ack(
                ContraflowData::from(event),
                nanotime() - start,
            ))
            .await?;
    }
    Ok(())
}

async fn send_fail(event: Event, reply_tx: &Sender<AsyncSinkReply>) -> Result<()> {
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
            "index" => self.insert_index_op(data, ops),
            "delete" => self.insert_delete_op(ops),

            "create" => self.insert_create_op(data, ops),
            "update" => {
                if self.get_raw_payload() {
                    self.insert_raw_update_op(data, ops)
                } else {
                    self.insert_update_op(data, ops)
                }
            }
            other => Err(Error::from(format!("Invalid `$elastic.action` {other}"))),
        }
    }

    fn insert_index_op(&self, data: &Value, ops: &mut BulkOperations) -> Result<()> {
        let mut op = BulkOperation::index(data);
        if let Some(id) = self.get_id() {
            op = op.id(id);
        }
        if let Some(index) = self.get_index() {
            op = op.index(index);
        }
        if let Some(version_type) = self.get_version_type() {
            op = op.version_type(version_type);
        }
        if let Some(version) = self.get_version() {
            op = op.version(version);
        }
        if let Some(if_primary_term) = self.get_if_primary_term() {
            op = op.if_primary_term(if_primary_term);
        }
        if let Some(if_seq_no) = self.get_if_seq_no() {
            op = op.if_seq_no(if_seq_no);
        }
        if let Some(routing) = self.get_routing() {
            op = op.routing(routing);
        }
        if let Some(pipeline) = self.get_pipeline() {
            op = op.pipeline(pipeline);
        }
        ops.push(op).map_err(Error::from)?;
        Ok(())
    }

    fn insert_delete_op(&self, ops: &mut BulkOperations) -> Result<()> {
        let mut op: BulkDeleteOperation<()> = self
            .get_id()
            .map(BulkOperation::delete)
            .ok_or_else(|| Error::from(Self::MISSING_ID))?;
        if let Some(index) = self.get_index() {
            op = op.index(index);
        }
        if let Some(version_type) = self.get_version_type() {
            op = op.version_type(version_type);
        }
        if let Some(version) = self.get_version() {
            op = op.version(version);
        }
        if let Some(if_primary_term) = self.get_if_primary_term() {
            op = op.if_primary_term(if_primary_term);
        }
        if let Some(if_seq_no) = self.get_if_seq_no() {
            op = op.if_seq_no(if_seq_no);
        }
        if let Some(routing) = self.get_routing() {
            op = op.routing(routing);
        }

        ops.push(op).map_err(Error::from)?;
        Ok(())
    }

    fn insert_create_op(&self, data: &Value, ops: &mut BulkOperations) -> Result<()> {
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
        if let Some(pipeline) = self.get_pipeline() {
            op = op.pipeline(pipeline);
        }
        if let Some(routing) = self.get_routing() {
            op = op.routing(routing);
        }
        ops.push(op).map_err(Error::from)?;
        Ok(())
    }

    // avoid a `.clone_static()` for the `raw` case
    fn insert_raw_update_op(&self, data: &Value, ops: &mut BulkOperations) -> Result<()> {
        let mut op = self
            .get_id()
            .map(|id| BulkOperation::update(id, data))
            .ok_or_else(|| Error::from(Self::MISSING_ID))?;

        op = self.apply_update_params(op);
        ops.push(op).map_err(Error::from)?;
        Ok(())
    }

    fn insert_update_op(&self, data: &Value, ops: &mut BulkOperations) -> Result<()> {
        let mut op = self
            .get_id()
            .map(|id| {
                // we are sure that this BulkUpdateOperation lives only as long as the event
                // so it is fine to only do a `.clone()` here instead of a `.clone_static()`
                let src = literal!({ "doc": data.clone() });
                BulkOperation::update(id, src)
            })
            .ok_or_else(|| Error::from(Self::MISSING_ID))?;

        op = self.apply_update_params(op);
        ops.push(op).map_err(Error::from)?;
        Ok(())
    }

    fn apply_update_params<T: serde::Serialize>(
        &self,
        mut op: BulkUpdateOperation<T>,
    ) -> BulkUpdateOperation<T> {
        if let Some(index) = self.get_index() {
            op = op.index(index);
        }
        if let Some(version_type) = self.get_version_type() {
            op = op.version_type(version_type);
        }
        if let Some(version) = self.get_version() {
            op = op.version(version);
        }
        if let Some(if_primary_term) = self.get_if_primary_term() {
            op = op.if_primary_term(if_primary_term);
        }
        if let Some(if_seq_no) = self.get_if_seq_no() {
            op = op.if_seq_no(if_seq_no);
        }
        if let Some(retry_on_conflict) = self.get_retry_on_conflict() {
            op = op.retry_on_conflict(retry_on_conflict);
        }
        if let Some(routing) = self.get_routing() {
            op = op.routing(routing);
        }
        op
    }

    fn parts<'blk>(&'blk self, default_index: Option<&'blk str>) -> BulkParts<'blk> {
        if let Some(index) = self.get_index().or(default_index) {
            BulkParts::Index(index)
        } else {
            BulkParts::None
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

    fn get_version(&self) -> Option<i64> {
        self.meta.get_i64("version")
    }

    fn get_version_type(&self) -> Option<VersionType> {
        self.meta
            .get_str("version_type")
            // didn't want to manually match on the `VersionType` enum
            // maybe there is a better way (without including serde_yaml)
            .and_then(|s| serde_yaml::from_str::<VersionType>(s).ok())
    }

    fn get_if_primary_term(&self) -> Option<i64> {
        self.meta.get_i64("if_primary_term")
    }

    fn get_if_seq_no(&self) -> Option<i64> {
        self.meta.get_i64("if_seq_no")
    }

    fn get_retry_on_conflict(&self) -> Option<i32> {
        self.meta.get_i32("retry_on_conflict")
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

/// stupid hack around non-clonability of `CertificateValidation`
#[derive(Debug, Clone)]
enum CertValidation {
    Default,
    Full(Vec<u8>),
    None,
}

impl CertValidation {
    fn as_certificate_validation(&self) -> Result<Option<CertificateValidation>> {
        let res = match self {
            Self::Default => Some(CertificateValidation::Default),
            Self::Full(data) => {
                let cert = Certificate::from_pem(data.as_slice())
                    .or_else(|_| Certificate::from_der(data.as_slice()))?;
                Some(CertificateValidation::Full(cert))
            }
            Self::None => None,
        };
        Ok(res)
    }
}
#[cfg(test)]
mod tests {
    use elasticsearch::http::request::Body;

    use super::*;
    use crate::config::Connector as ConnectorConfig;

    #[async_std::test]
    async fn connector_builder_empty_nodes() -> Result<()> {
        let config = literal!({
            "config": {
                "nodes": []
            }
        });
        let alias = Alias::new("flow", "my_elastic");
        let builder = super::Builder::default();
        let connector_config =
            ConnectorConfig::from_config(&alias, builder.connector_type(), &config)?;
        let kill_switch = KillSwitch::dummy();
        assert_eq!(
            String::from(
                "Invalid Definition for connector \"flow::my_elastic\": empty nodes provided"
            ),
            builder
                .build(&alias, &connector_config, &kill_switch)
                .await
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
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
        let alias = Alias::new("snot", "my_elastic");
        let builder = super::Builder::default();
        let connector_config =
            ConnectorConfig::from_config(&alias, builder.connector_type(), &config)?;
        let kill_switch = KillSwitch::dummy();
        assert_eq!(
            String::from("empty host"),
            builder
                .build(&alias, &connector_config, &kill_switch)
                .await
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
        );
        Ok(())
    }

    #[test]
    fn es_meta() -> Result<()> {
        let meta = literal!({
            "elastic": {
                "action": "index",
                "_id": "abcdef",
                "_index": "snot",
                "pipeline": "pipeline",
                "refresh": "wait_for",
                "routing": "routing",
                "timeout": "10s",
                "_type": "ttt",
                "version": 12,
                "version_type": "external"
            }
        });
        let es_meta = ESMeta::new(&meta);
        assert_eq!(Some("index"), es_meta.get_action());
        assert_eq!(None, es_meta.get_retry_on_conflict());
        assert_eq!(Some("abcdef"), es_meta.get_id());
        assert_eq!(Some("snot"), es_meta.get_index());
        assert_eq!(Some("pipeline"), es_meta.get_pipeline());
        assert_eq!(false, es_meta.get_raw_payload());
        assert_eq!(None, es_meta.get_if_primary_term());
        assert_eq!(None, es_meta.get_if_seq_no());
        assert_eq!(Ok(Some(Refresh::WaitFor)), es_meta.get_refresh());
        assert_eq!(Some("routing"), es_meta.get_routing());
        assert_eq!(Some("10s"), es_meta.get_timeout());
        assert_eq!(Some("ttt"), es_meta.get_type());
        assert_eq!(Some(12), es_meta.get_version());
        assert_eq!(Some(VersionType::External), es_meta.get_version_type());
        let data = literal!({});
        let mut ops = BulkOperations::new();
        es_meta.insert_op(&data, &mut ops)?;
        assert_eq!(
            Some(String::from(
                r#"{"index":{"_index":"snot","_id":"abcdef","version":12,"version_type":"external"}}
{}
"#
            )),
            ops.bytes().map(|b| String::from_utf8_lossy(&b).to_string())
        );

        let meta = literal!({
            "elastic": {
                "action": "update",
                "_id": "1234",
                "retry_on_conflict": 42,
                "if_seq_no": 123,
                "if_primary_term": 456
            }
        });
        let es_meta = ESMeta::new(&meta);
        assert_eq!(Some(42), es_meta.get_retry_on_conflict());
        assert_eq!(Some(123), es_meta.get_if_seq_no());
        assert_eq!(Some(456), es_meta.get_if_primary_term());
        let data = literal!({});
        let mut ops = BulkOperations::new();
        es_meta.insert_op(&data, &mut ops)?;
        assert_eq!(
            Some(String::from(
                r#"{"update":{"_id":"1234","if_seq_no":123,"if_primary_term":456,"retry_on_conflict":42}}
{"doc":{}}
"#
            )),
            ops.bytes().map(|b| String::from_utf8_lossy(&b).to_string())
        );
        Ok(())
    }
}
