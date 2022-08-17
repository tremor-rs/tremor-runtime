// Copyright 2022, The Tremor Team
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
use crate::connectors::prelude::*;
use crate::connectors::utils::url::HttpsDefaults;
use async_std::channel::{Receiver, Sender};
use async_std::stream::StreamExt;
use async_std::sync::RwLock;
use async_std::task;
use async_std::task::JoinHandle;
use beef::generic::Cow;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use googapis::google::pubsub::v1::{PubsubMessage, ReceivedMessage, StreamingPullRequest};
use gouth::Token;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Code, Status};
use tremor_common::blue_green_hashmap::BlueGreenHashMap;
use tremor_pipeline::ConfigImpl;

// controlling retries upon gpubsub returning `Unavailable` from StreamingPull
// this in on purpose not exposed via config as this should remain an internal thing
const RETRY_WAIT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RETRIES: u64 = 5;

#[derive(Deserialize, Clone)]
struct Config {
    #[serde(default = "crate::connectors::impls::gpubsub::default_connect_timeout")]
    pub connect_timeout: u64,
    #[serde(default = "default_ack_deadline")]
    pub ack_deadline: u64,
    pub subscription_id: String,
    #[serde(default = "crate::connectors::impls::gpubsub::default_endpoint")]
    pub url: Url<HttpsDefaults>,
    #[cfg(test)]
    #[serde(default = "crate::connectors::impls::gpubsub::default_skip_authentication")]
    pub skip_authentication: bool,
}
impl ConfigImpl for Config {}

fn default_ack_deadline() -> u64 {
    10_000_000_000u64 // 10 seconds
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "gpubsub_consumer".into()
    }

    async fn build_cfg(
        &self,
        alias: &Alias,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(raw)?;
        let url = Url::<HttpsDefaults>::parse(config.url.as_str())?;
        let client_id = format!("tremor-{}-{}-{:?}", hostname(), alias, task::current().id());

        Ok(Box::new(GSub {
            config,
            url,
            client_id,
        }))
    }
}

struct GSub {
    config: Config,
    url: Url<HttpsDefaults>,
    client_id: String,
}

type PubSubClient = SubscriberClient<InterceptedService<Channel, AuthInterceptor>>;
type AsyncTaskMessage = Result<(u64, PubsubMessage)>;

struct GSubSource {
    config: Config,
    client: Option<PubSubClient>,
    receiver: Option<Receiver<AsyncTaskMessage>>,
    ack_sender: Option<Sender<u64>>,
    task_handle: Option<JoinHandle<()>>,
    url: Url<HttpsDefaults>,
    client_id: String,
}

impl GSubSource {
    pub fn new(config: Config, url: Url<HttpsDefaults>, client_id: String) -> Self {
        GSubSource {
            config,
            url,
            client_id,
            client: None,
            receiver: None,
            task_handle: None,
            ack_sender: None,
        }
    }
}

async fn consumer_task(
    mut client: PubSubClient,
    ctx: SourceContext,
    client_id: String,
    sender: Sender<AsyncTaskMessage>,
    subscription_id: String,
    ack_deadline: Duration,
    ack_receiver: Receiver<u64>,
) -> Result<()> {
    let mut ack_counter = 0;

    let ack_ids = Arc::new(RwLock::new(BlueGreenHashMap::new(
        ack_deadline,
        SystemTime::now(),
    )));
    let initial_request = StreamingPullRequest {
        subscription: subscription_id.clone(),
        ack_ids: vec![],
        modify_deadline_seconds: vec![],
        modify_deadline_ack_ids: vec![],
        stream_ack_deadline_seconds: i32::try_from(ack_deadline.as_secs()).unwrap_or(10),
        client_id: client_id.clone(),
        max_outstanding_messages: i64::try_from(QSIZE.load(Ordering::Relaxed)).unwrap_or(128),
        max_outstanding_bytes: 0,
    };

    let ack_ids_c = ack_ids.clone();
    let mut retry_wait_interval = RETRY_WAIT_INTERVAL;
    let mut attempt: u64 = 0;

    'retry: loop {
        // yeah, i know, my name is george cloney
        let initial_req = initial_request.clone();
        let ack_recvr = ack_receiver.clone();
        let ack_ids_cc = ack_ids_c.clone();
        let client_id_c = client_id.clone();
        let request_stream = async_stream::stream! {
            yield initial_req;

            while let Ok(pull_id) = ack_recvr.recv().await {
                if let (Some(ack_id)) = ack_ids_cc.write().await.remove(&pull_id) {
                    yield StreamingPullRequest {
                        subscription: "".to_string(),
                        ack_ids: vec![ack_id],
                        modify_deadline_seconds: vec![],
                        modify_deadline_ack_ids: vec![],
                        stream_ack_deadline_seconds: 0,
                        client_id: client_id_c.clone(),
                        max_outstanding_messages: 0,
                        max_outstanding_bytes: 0
                    };
                } else {
                    warn!("Did not find an ACK ID for pull_id: {pull_id}");
                }
            }
        };

        let mut stream = client.streaming_pull(request_stream).await?.into_inner();

        while let Some(response) = stream.next().await {
            let response = match response {
                Ok(res) => {
                    // reset the wait interval and attempts
                    retry_wait_interval = RETRY_WAIT_INTERVAL;
                    attempt = 0;
                    res
                }
                Err(status) if status.code() == Code::Unavailable => {
                    // keep track of the numbers of retries
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        info!("{ctx} Got `Unavailable` for {MAX_RETRIES} times. Bailing out!");
                        return Err(Error::from(status));
                    }
                    info!(
                        "{ctx} ERROR: {status}. Waiting for {}s for a retry...",
                        retry_wait_interval.as_secs_f32()
                    );
                    async_std::task::sleep(retry_wait_interval).await;
                    // exponential backoff
                    retry_wait_interval *= 2;

                    continue 'retry;
                }
                Err(s) => {
                    return Err(Error::from(s));
                }
            };

            for message in response.received_messages {
                let ReceivedMessage {
                    ack_id,
                    message: msg,
                    ..
                } = message;

                if let Some(pubsub_message) = msg {
                    ack_counter += 1;
                    ack_ids
                        .write()
                        .await
                        .insert(ack_counter, ack_id, SystemTime::now());

                    sender.send(Ok((ack_counter, pubsub_message))).await?;
                }
            }
        }
        return Ok(());
    }
}

fn pubsub_metadata(
    id: String,
    ordering_key: String,
    publish_time: Option<Duration>,
    attributes: HashMap<String, String>,
) -> Value<'static> {
    let mut attributes_value = Value::object_with_capacity(attributes.len());
    for (name, value) in attributes {
        attributes_value
            .as_object_mut()
            .map(|x| x.insert(Cow::from(name), Value::from(value)));
    }
    literal!({
        "gpubsub_consumer": {
            "message_id": id,
            "ordering_key": ordering_key,
            "publish_time": publish_time.map(|x| u64::try_from(x.as_nanos()).unwrap_or(0)),
            "attributes": attributes_value
        }
    })
}

#[async_trait::async_trait]
impl Source for GSubSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        let mut channel = Channel::from_shared(self.config.url.to_string())?
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout));
        if self.url.scheme() == "https" {
            let tls_config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
                .domain_name(
                    self.url
                        .host_str()
                        .ok_or_else(|| Status::unavailable("The endpoint is missing a hostname"))?
                        .to_string(),
                );

            channel = channel.tls_config(tls_config)?;
        }

        let channel = channel.connect().await?;

        #[cfg(test)]
        let skip_authentication = self.config.skip_authentication;

        let connect_to_pubsub = move || -> Result<PubSubClient> {
            #[cfg(test)]
            if skip_authentication {
                info!("Skipping auth...");
                return Ok(SubscriberClient::with_interceptor(
                    channel.clone(),
                    AuthInterceptor {
                        token: Box::new(|| Ok(Arc::new(String::new()))),
                    },
                ));
            }

            let token = Token::new()?;

            Ok(SubscriberClient::with_interceptor(
                channel.clone(),
                AuthInterceptor {
                    token: Box::new(move || {
                        token.header_value().map_err(|_| {
                            Status::unavailable("Failed to retrieve authentication token.")
                        })
                    }),
                },
            ))
        };

        if let Some(task_handle) = self.task_handle.take() {
            task_handle.cancel().await;
        }

        let client = connect_to_pubsub()?;

        let client_background = connect_to_pubsub()?;

        let (tx, rx) = async_std::channel::bounded(QSIZE.load(Ordering::Relaxed));
        let (ack_tx, ack_rx) = async_std::channel::bounded(QSIZE.load(Ordering::Relaxed));

        let join_handle = spawn_task(
            ctx.clone(),
            consumer_task(
                client_background,
                ctx.clone(),
                self.client_id.clone(),
                tx,
                self.config.subscription_id.clone(),
                Duration::from_nanos(self.config.ack_deadline),
                ack_rx,
            ),
        );

        self.receiver = Some(rx);
        self.ack_sender = Some(ack_tx);
        self.client = Some(client);
        self.task_handle = Some(join_handle);

        Ok(true)
    }

    async fn pull_data(&mut self, pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        let receiver = self.receiver.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "PubSub",
            "The receiver is not connected",
        ))?;
        let (ack_id, pubsub_message) = match receiver.recv().await? {
            Ok(response) => response,
            Err(error) => return Err(error),
        };

        *pull_id = ack_id;

        Ok(SourceReply::Data {
            origin_uri: EventOriginUri::default(),
            data: pubsub_message.data,
            meta: Some(pubsub_metadata(
                pubsub_message.message_id,
                pubsub_message.ordering_key,
                pubsub_message.publish_time.map(|x| {
                    Duration::from_nanos(
                        u64::try_from(x.seconds).unwrap_or(0) * 1_000_000_000u64
                            + u64::try_from(x.nanos).unwrap_or(0),
                    )
                }),
                pubsub_message.attributes,
            )),
            stream: Some(DEFAULT_STREAM_ID),
            port: None,
            codec_overwrite: None,
        })
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn asynchronous(&self) -> bool {
        true
    }

    async fn ack(&mut self, _stream_id: u64, pull_id: u64, _ctx: &SourceContext) -> Result<()> {
        let sender = self
            .ack_sender
            .as_mut()
            .ok_or(ErrorKind::ClientNotAvailable(
                "PubSub",
                "The client is not connected",
            ))?;
        sender.send(pull_id).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Connector for GSub {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = GSubSource::new(
            self.config.clone(),
            self.url.clone(),
            self.client_id.clone(),
        );
        builder.spawn(source, source_context).map(Some)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}
