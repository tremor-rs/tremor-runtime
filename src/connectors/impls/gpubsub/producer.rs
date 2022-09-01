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
    Alias, Attempt, ErrorKind, EventSerializer, KillSwitch, SinkAddr, SinkContext,
    SinkManagerBuilder, SinkReply, Url,
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
use std::collections::HashMap;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Code;
use tremor_pipeline::{ConfigImpl, Event};
use tremor_value::Value;
use value_trait::ValueAccess;

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default = "crate::connectors::impls::gpubsub::default_connect_timeout")]
    pub connect_timeout: u64,
    #[serde(default = "crate::connectors::impls::gpubsub::default_request_timeout")]
    pub request_timeout: u64,
    #[serde(default = "crate::connectors::impls::gpubsub::default_endpoint")]
    pub url: Url<HttpsDefaults>,
    pub topic: String,
}

impl ConfigImpl for Config {}

#[derive(Default, Debug)]
pub(crate) struct Builder {}

#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        ConnectorType("gpubsub_producer".to_string())
    }

    async fn build_cfg(
        &self,
        _alias: &Alias,
        _config: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
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
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = GpubSink {
            config: self.config.clone(),
            hostname: self
                .config
                .url
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
    hostname: String,

    client: Option<PublisherClient<InterceptedService<Channel, AuthInterceptor>>>,
}

#[cfg(not(test))]
fn create_publisher_client(
    channel: Channel,
) -> Result<PublisherClient<InterceptedService<Channel, AuthInterceptor>>> {
    use gouth::Token;
    use tonic::Status;

    let token = Token::new()?;

    Ok(PublisherClient::with_interceptor(
        channel,
        AuthInterceptor {
            token: Box::new(move || {
                token
                    .header_value()
                    .map_err(|_| Status::unavailable("Failed to retrieve authentication token."))
            }),
        },
    ))
}

#[cfg(test)]
fn create_publisher_client(
    channel: Channel,
) -> Result<PublisherClient<InterceptedService<Channel, AuthInterceptor>>> {
    use std::sync::Arc;

    Ok(PublisherClient::with_interceptor(
        channel,
        AuthInterceptor {
            token: Box::new(|| Ok(Arc::new(String::new()))),
        },
    ))
}

#[async_trait::async_trait()]
impl Sink for GpubSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let mut channel = Channel::from_shared(self.config.url.to_string())?
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout));
        if self.config.url.scheme() == "https" {
            let tls_config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
                .domain_name(self.hostname.clone());

            channel = channel.tls_config(tls_config)?;
        }

        let channel = channel.connect().await?;

        self.client = Some(create_publisher_client(channel)?);

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
                let ordering_key = ctx
                    .extract_meta(meta)
                    .get("ordering_key")
                    .as_str()
                    .map_or_else(|| "".to_string(), ToString::to_string);

                messages.push(PubsubMessage {
                    data: payload,
                    attributes: HashMap::new(),
                    // publish_time and message_id will be ignored in the request and set by server
                    message_id: "".to_string(),
                    publish_time: None,

                    // from the metadata
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
                        ctx.notifier.connection_lost().await,
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
mod tests {
    use super::*;

    #[test]
    pub fn is_not_auto_ack() {
        let sink = GpubSink {
            config: Config {
                connect_timeout: 0,
                request_timeout: 0,
                url: Default::default(),
                topic: "".to_string(),
            },
            hostname: "".to_string(),
            client: None,
        };

        assert!(!sink.auto_ack());
    }

    #[test]
    pub fn is_async() {
        let sink = GpubSink {
            config: Config {
                connect_timeout: 0,
                request_timeout: 0,
                url: Default::default(),
                topic: "".to_string(),
            },
            hostname: "".to_string(),
            client: None,
        };

        assert!(sink.asynchronous());
    }
}
