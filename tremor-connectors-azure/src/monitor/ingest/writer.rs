// Copyright 2024, The Tremor Team
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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::auth::Config as AuthConfig;
use crate::rest::RequestId;
use azure_core::Body;
use azure_core::Method;
use serde::Deserialize;
use source::AmiSource;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tremor_common::alias;
use tremor_config::Impl;
use tremor_connectors::config;
use tremor_connectors::errors::GenericImplementationError;
use tremor_connectors::sink::prelude::SinkAddr;
use tremor_connectors::sink::EventSerializer;
use tremor_connectors::sink::SinkContext;
use tremor_connectors::sink::SinkManagerBuilder;
use tremor_connectors::source::prelude::SourceAddr;
use tremor_connectors::source::prelude::Value;
use tremor_connectors::source::SourceContext;
use tremor_connectors::source::SourceManagerBuilder;
use tremor_connectors::source::SourceReply;
use tremor_connectors::Connector;
use tremor_connectors::ConnectorBuilder;
use tremor_connectors::ConnectorType;
use tremor_system::killswitch::KillSwitch;
use tremor_system::qsize;

mod sink;
mod source;

// The Azure Monitor Ingestion API requires the following scope for default permissions
// to be available for the DCR based ingestion endpoints to be used.
//
const AZURE_MONITOR_AUTH_SCOPE: &str = "https://monitor.azure.com/.default";

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Azure authentication configuration. Required. The provided scope determines
    /// the permissions available to the token.
    pub auth: AuthConfig,

    /// The Azure Data Collection Rule Base URL. Required.
    pub dce_base_url: String,

    /// The immutable Data Collection Rule ID. Required.
    pub dcr: String,

    /// The stream name to publish to, from the Data Collection Rule. Required.
    pub stream: String,

    /// Api Version. Default value is "2023-01-01".
    #[serde(default = "default_api_version")]
    pub api_version: String,

    /// Concurrency capacity limits ( in flight requests )
    #[serde(default = "default_concurrency")]
    pub(super) concurrency: usize,

    /// Timeout for the request in milliseconds. Optional.
    timeout: Option<u64>,
}

const DEFAULT_CONCURRENCY: usize = 4;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_api_version() -> String {
    "2023-01-01".to_string()
}

impl tremor_config::Impl for Config {}

struct RequestBuilder {
    id: RequestId,
    rest_endpoint: azure_core::Url,
    auth_token: Option<String>,
    data: Vec<Vec<u8>>,
}

impl RequestBuilder {
    async fn new(
        id: RequestId,
        _meta: Option<&Value<'_>>,
        config: &Config,
    ) -> anyhow::Result<Self> {
        let rest_endpoint = format!(
            "{dce_base_url}/dataCollectionRules/{dcr}/streams/{stream}?api-version={api_version}",
            dce_base_url = config.dce_base_url,
            dcr = config.dcr,
            stream = config.stream,
            api_version = config.api_version
        );
        let azure_core_url = azure_core::Url::parse(&rest_endpoint)?;

        Ok(Self {
            id,
            rest_endpoint: azure_core_url,
            auth_token: Some(config.auth.get_token().await?),
            data: vec![],
        })
    }

    pub(super) async fn append<'event>(
        &mut self,
        value: &'event Value<'event>,
        meta: &'event Value<'event>,
        ingest_ns: u64,
        serializer: &mut EventSerializer,
    ) -> anyhow::Result<()> {
        let chunks = serializer
            .serialize_for_stream_with_codec(value, meta, ingest_ns, self.id.get(), None)
            .await?;
        self.append_data(chunks);
        Ok(())
    }

    fn append_data(&mut self, chunks: Vec<Vec<u8>>) {
        for chunk in chunks {
            self.data.push(chunk);
        }
    }

    fn take_request(&mut self) -> azure_core::Request {
        let endpoint = self.rest_endpoint.clone();
        let mut request = azure_core::Request::new(endpoint, Method::Post);
        request.insert_header("content-type", "application/json");
        if let Some(auth_token) = &self.auth_token {
            request.insert_header("authorization", &format!("Bearer {auth_token}"));
        }

        let mut bytes = vec![];
        bytes.extend_from_slice(&self.data.concat());
        request.set_body(Body::from(bytes));

        request
    }

    /// Finalize and send the response.
    /// In the chunked case we have already sent it before.
    ///
    /// After calling this function this instance shouldn't be used anymore
    pub(crate) fn finalize(&mut self, serializer: &mut EventSerializer) -> anyhow::Result<()> {
        // finalize the stream
        let rest = serializer.finish_stream(self.id.get())?;
        self.append_data(rest);
        Ok(())
    }
}

/// Connector Builder for Microsoft Azure Monitor Log Analytics Ingestion API
#[derive(Debug, Default)]
pub struct Builder {}

struct Ami {
    response_tx: Sender<SourceReply>,
    response_rx: Option<Receiver<SourceReply>>,
    config: Config,
    source_is_connected: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Connector for Ami {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let source = AmiSource {
            source_is_connected: self.source_is_connected.clone(),
            rx: self
                .response_rx
                .take()
                .ok_or(GenericImplementationError::AlreadyConnected)?,
        };
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = sink::AmiSink::new(
            self.response_tx.clone(),
            builder.reply_tx(),
            self.config.clone(),
            self.source_is_connected.clone(),
        );

        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> tremor_connectors::CodecReq {
        tremor_connectors::CodecReq::Structured // NOTE Always json
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "azure_monitor_ingest_writer".into()
    }

    async fn build_cfg(
        &self,
        _id: &alias::Connector,
        _connector_config: &config::Connector,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let (response_tx, response_rx) = channel(qsize());
        let mut config = Config::new(config)?;
        // NOTE We limit the scope to the Azure Monitor scope explicitly
        config.auth.scope = AZURE_MONITOR_AUTH_SCOPE.to_string();

        Ok(Box::new(Ami {
            response_tx,
            response_rx: Some(response_rx),
            config,
            source_is_connected: Arc::new(AtomicBool::new(false)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::mock_auth_server;
    use tremor_common::ids::Id;
    use tremor_common::ids::SinkId;
    use tremor_config::NameWithConfig;
    use tremor_connectors::{
        utils::{
            metrics::SinkReporter, quiescence::QuiescenceBeacon, reconnect::ConnectionLostNotifier,
        },
        CodecReq,
    };
    use tremor_value::literal;

    use super::*;

    // NOTE we dont care about dead code paths in tests, let it hang wet
    #[allow(clippy::match_wildcard_for_single_variants)]
    fn azure_body_to_string(body: &Body) -> String {
        match body {
            Body::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
            _ => "not supported".to_string(), // don't care
        }
    }

    fn basic_json_serializer() -> anyhow::Result<EventSerializer> {
        let serializer = EventSerializer::new(
            Some(NameWithConfig {
                name: "json".to_string(),
                config: None,
            }),
            CodecReq::Required,
            vec![],
            &ConnectorType::from("snot"),
            &tremor_common::alias::Connector::new("flow_alias", "badger"),
        )?;
        Ok(serializer)
    }

    async fn mock_monitor_ingest_api_server() -> mockito::ServerGuard {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock(
                "POST",
                "/dataCollectionRules/dcr-uuidv4/streams/stream_name?api-version=2023-01-01",
            )
            .with_status(204)
            .with_header("content-type", "application/json")
            .with_body("")
            .create();

        server
    }

    #[test]
    fn deserialize_for_coverage() -> anyhow::Result<()> {
        let config = r#"{
            "auth": {
                "base_url": "https://login.microsoftonline.com",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
                "scope": "scope"
            },
            "dce_base_url": "https://dce_base_url",
            "dcr": "dcr-uuidv4",
            "stream": "stream_name"
        }"#;

        let config = serde_json::from_str::<Config>(config)?;
        assert_eq!(config.auth.base_url, "https://login.microsoftonline.com");
        assert_eq!(config.auth.client_id, "client_id");
        assert_eq!(config.auth.client_secret, "client_secret");
        assert_eq!(config.auth.tenant_id, "tenant_id");
        assert_eq!(config.auth.scope, "scope");
        assert_eq!(config.dce_base_url, "https://dce_base_url");
        assert_eq!(config.dcr, "dcr-uuidv4");
        assert_eq!(config.stream, "stream_name");
        assert_eq!(config.api_version, "2023-01-01");
        assert_eq!(config.concurrency, 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_request_builder_single_request() -> anyhow::Result<()> {
        let auth_server = mock_auth_server().await;
        let ingest_server = mock_monitor_ingest_api_server().await;

        let config = Config {
            auth: AuthConfig::new_mock(&auth_server),
            dce_base_url: ingest_server.url(),
            dcr: "dcr-uuidv4".to_string(),
            stream: "stream_name".to_string(),
            api_version: "2023-01-01".to_string(),
            concurrency: 1,
            timeout: None,
        };

        let request_id = RequestId::new(123);

        let meta_none: Option<&Value> = None;
        let mut rb = RequestBuilder::new(request_id, meta_none, &config).await?;

        let serializer = &mut basic_json_serializer()?;
        rb.append(
            &literal!([{"foo": "bar"}]),
            &literal!([{"timestamp": 1_234_567_890 }]),
            1,
            serializer,
        )
        .await?;

        rb.finalize(serializer)?;

        let request = rb.take_request();
        let body = request.body();
        assert!(!body.is_empty());
        assert_eq!(r#"[{"foo":"bar"}]"#, &azure_body_to_string(body));

        Ok(())
    }

    #[tokio::test]
    async fn create_sink() -> anyhow::Result<()> {
        let builder = Builder::default();

        assert_eq!(
            builder.connector_type(),
            "azure_monitor_ingest_writer".into()
        );

        let config = literal!({
            "auth": {
                "base_url": "https://login.microsoftonline.com",
                "client_id": "client_id",
                "client_secret": "client_secret",
                "tenant_id": "tenant_id",
                "scope": "scope"
            },
            "dce_base_url": "https://dce_base_url",
            "dcr": "dcr-uuidv4",
            "stream": "stream_name",
        });

        let id = alias::Connector::new("test", "test");

        let connector_config =
            config::Connector::from_config(&id, builder.connector_type(), &config)?;

        let mut connector = builder
            .build_cfg(&id, &connector_config, &config, &KillSwitch::dummy())
            .await?;

        assert_eq!(connector.codec_requirements(), CodecReq::Structured);

        let sink_id = SinkId::new(1);
        let (lost_tx, _lost_rx) = tokio::sync::mpsc::channel(1);
        let notifier = ConnectionLostNotifier::new(&id, lost_tx);
        let beacon = QuiescenceBeacon::default();
        let ctx = SinkContext::new(
            sink_id,
            id.clone(),
            builder.connector_type(),
            beacon,
            notifier,
        );

        let (metrics_tx, _metrics_rx) = tokio::sync::broadcast::channel(1);

        let metrics_reporter = SinkReporter::new(id.clone(), metrics_tx, None);
        let sink_mgr_builder = tremor_connectors::sink::builder(
            &connector_config,
            CodecReq::Structured,
            &id,
            metrics_reporter,
        )?;
        let _sink = connector.create_sink(ctx, sink_mgr_builder).await?;

        // NOTE For coverage, we punt on sink testing as we have no live or CI capable Azure env

        Ok(())
    }
}
