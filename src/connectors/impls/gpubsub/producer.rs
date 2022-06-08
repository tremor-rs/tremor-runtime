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

use crate::connectors::google::AuthInterceptor;
use crate::connectors::prelude::{
    Attempt, ErrorKind, EventSerializer, SinkAddr, SinkContext, SinkManagerBuilder, SinkReply, Url,
};
use crate::connectors::sink::Sink;
use crate::connectors::utils::url::HttpsDefaults;
use crate::connectors::{
    CodecReq, Connector, ConnectorBuilder, ConnectorConfig, ConnectorContext, ConnectorType,
    Context,
};
use crate::errors::Result;
use async_std::prelude::FutureExt;
use googapis::google::pubsub::v1::publisher_client::PublisherClient;
use googapis::google::pubsub::v1::{PublishRequest, PubsubMessage};
use gouth::Token;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Status;
use tremor_pipeline::{ConfigImpl, Event};
use value_trait::ValueAccess;

#[derive(Deserialize, Clone)]
pub struct Config {
    #[serde(default = "crate::connectors::impls::gpubsub::default_connect_timeout")]
    pub connect_timeout: u64,
    #[serde(default = "crate::connectors::impls::gpubsub::default_request_timeout")]
    pub request_timeout: u64,
    #[serde(default = "crate::connectors::impls::gpubsub::default_endpoint")]
    pub endpoint: String,
    pub topic: String,
    #[cfg(test)]
    #[serde(default = "crate::connectors::impls::gpubsub::default_skip_authentication")]
    pub skip_authentication: bool,
}

impl ConfigImpl for Config {}

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("gpubsub_producer".to_string())
    }

    async fn build(&self, alias: &str, config: &ConnectorConfig) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = &config.config {
            let config = Config::new(raw_config)?;

            Ok(Box::new(GpubConnector { config }))
        } else {
            Err(ErrorKind::MissingConfiguration(alias.to_string()).into())
        }
    }
}

struct GpubConnector {
    config: Config,
}

#[async_trait::async_trait()]
impl Connector for GpubConnector {
    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let url = Url::parse(&self.config.endpoint)?;
        let sink = GpubSink {
            config: self.config.clone(),
            url: url.clone(),
            hostname: url
                .host_str()
                .ok_or_else(|| {
                    ErrorKind::InvalidConfiguration(
                        "gpubsub-publisher".to_string(),
                        "Missing hostname".to_string(),
                    )
                })?
                .to_string(),
            client: None,
        };
        builder.spawn(sink, sink_context).map(Some)
    }

    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}

struct GpubSink {
    config: Config,
    url: Url<HttpsDefaults>,
    hostname: String,

    client: Option<PublisherClient<InterceptedService<Channel, AuthInterceptor>>>,
}

#[async_trait::async_trait()]
impl Sink for GpubSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let mut channel = Channel::from_shared(self.config.endpoint.clone())?
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout));
        if self.url.scheme() == "https" {
            let tls_config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
                .domain_name(self.hostname.clone());

            channel = channel.tls_config(tls_config)?;
        }

        let channel = channel.connect().await?;
        #[allow(unused_assignments, unused_mut)]
        let mut skip_authentication = false;
        #[cfg(test)]
        {
            skip_authentication = self.config.skip_authentication;
        }

        let client = if skip_authentication {
            info!("Skipping auth...");

            PublisherClient::with_interceptor(
                channel,
                AuthInterceptor {
                    token: Box::new(|| Ok(Arc::new(String::new()))),
                },
            )
        } else {
            let token = Token::new()?;

            PublisherClient::with_interceptor(
                channel,
                AuthInterceptor {
                    token: Box::new(move || {
                        token.header_value().map_err(|_| {
                            Status::unavailable("Failed to retrieve authentication token.")
                        })
                    }),
                },
            )
        };

        self.client = Some(client);

        Ok(true)
    }

    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "PubSub",
            "The publisher is not connected",
        ))?;

        let mut messages = Vec::with_capacity(event.len());

        for (value, meta) in event.value_meta_iter() {
            for payload in serializer.serialize(value, event.ingest_ns)? {
                let ordering_key = meta
                    .get("gpubsub_producer")
                    .get("ordering_key")
                    .as_str()
                    .map_or_else(|| "".to_string(), ToString::to_string);

                messages.push(PubsubMessage {
                    data: payload,
                    attributes: HashMap::new(),
                    // publish_time and message_id will be ignored in the request and set by server
                    message_id: "".to_string(),
                    publish_time: None,

                    // fixme get from the metadata
                    ordering_key,
                });
            }
        }

        if let Ok(inner_result) = client
            .publish(PublishRequest {
                topic: self.config.topic.clone(),
                messages,
            })
            .timeout(Duration::from_nanos(self.config.request_timeout))
            .await
        {
            if let Err(error) = inner_result {
                error!("Failed to publish a message: {}", error);
                ctx.swallow_err(
                    ctx.notifier.connection_lost().await,
                    "Failed to notify about PubSub connection loss",
                );

                Ok(SinkReply::FAIL)
            } else {
                Ok(SinkReply::ACK)
            }
        } else {
            ctx.swallow_err(
                ctx.notifier.connection_lost().await,
                "Failed to notify about PubSub connection loss",
            );

            Ok(SinkReply::FAIL)
        }
    }

    fn auto_ack(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}