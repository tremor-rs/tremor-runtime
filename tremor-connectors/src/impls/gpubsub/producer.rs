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

use crate::{
    prelude::*,
    utils::google::{AuthInterceptor, TokenSrc},
};
use googapis::google::pubsub::v1::{
    publisher_client::PublisherClient, PublishRequest, PubsubMessage,
};
use std::{collections::HashMap, time::Duration};
use tokio::time::timeout;
use tonic::{
    codegen::InterceptedService,
    transport::{Certificate, Channel, ClientTlsConfig},
    Code,
};

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default = "crate::impls::gpubsub::default_connect_timeout")]
    pub connect_timeout: u64,
    #[serde(default = "crate::impls::gpubsub::default_request_timeout")]
    pub request_timeout: u64,
    pub token: TokenSrc,
    #[serde(default = "crate::impls::gpubsub::default_endpoint")]
    pub url: Url<HttpsDefaults>,
    pub topic: String,
}

impl tremor_config::Impl for Config {}

/// PubSub Producer connector
#[derive(Default, Debug)]
pub struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("gpubsub_producer".to_string())
    }

    async fn build_cfg(
        &self,
        _alias: &alias::Connector,
        _config: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(raw_config)?;

        Ok(Box::new(GpubConnector { config }))
    }
}

struct GpubConnector {
    config: Config,
}

#[async_trait::async_trait()]
impl Connector for GpubConnector {
    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> anyhow::Result<Option<SinkAddr>> {
        let sink = GpubSink {
            config: self.config.clone(),
            hostname: self
                .config
                .url
                .host_str()
                .ok_or_else(|| {
                    Error::InvalidConfiguration(ctx.alias().clone(), "Missing hostname")
                })?
                .to_string(),
            client: None,
        };
        Ok(Some(builder.spawn(sink, ctx)))
    }

    async fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        _attempt: &Attempt,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct GpubSink {
    config: Config,
    hostname: String,
    client: Option<PublisherClient<InterceptedService<Channel, AuthInterceptor>>>,
}

#[async_trait::async_trait()]
impl Sink for GpubSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let mut channel = Channel::from_shared(self.config.url.to_string())?
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout));
        if self.config.url.scheme() == "https" {
            let tls_config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
                .domain_name(self.hostname.clone());

            channel = channel.tls_config(tls_config)?;
        }

        let channel = channel.connect().await?;

        self.client = Some(PublisherClient::with_interceptor(
            channel,
            self.config.token.clone().into(),
        ));

        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        let client = self
            .client
            .as_mut()
            .ok_or(GenericImplementationError::ClientNotAvailable("PubSub"))?;

        let mut messages = Vec::with_capacity(event.len());

        for (value, meta) in event.value_meta_iter() {
            for payload in serializer.serialize(value, meta, event.ingest_ns).await? {
                let ordering_key = ctx
                    .extract_meta(meta)
                    .get("ordering_key")
                    .as_str()
                    .map_or_else(String::new, ToString::to_string);

                messages.push(PubsubMessage {
                    data: payload,
                    attributes: HashMap::new(),
                    // publish_time and message_id will be ignored in the request and set by server
                    message_id: String::new(),
                    publish_time: None,

                    // from the metadata
                    ordering_key,
                });
            }
        }

        if let Ok(inner_result) = timeout(
            Duration::from_nanos(self.config.request_timeout),
            client.publish(PublishRequest {
                topic: self.config.topic.clone(),
                messages,
            }),
        )
        .await
        {
            if let Err(error) = inner_result {
                error!("{ctx} Failed to publish a message: {}", error);

                if matches!(
                    error.code(),
                    Code::Aborted
                        | Code::Cancelled
                        | Code::DataLoss
                        | Code::DeadlineExceeded
                        | Code::Internal
                        | Code::ResourceExhausted
                        | Code::Unavailable
                        | Code::Unknown
                ) {
                    ctx.swallow_err(
                        ctx.notifier().connection_lost().await,
                        "Failed to notify about PubSub connection loss",
                    );
                }

                Ok(SinkReply::fail_or_none(event.transactional))
            } else {
                Ok(SinkReply::ack_or_none(event.transactional))
            }
        } else {
            Ok(SinkReply::fail_or_none(event.transactional))
        }
    }

    fn auto_ack(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
