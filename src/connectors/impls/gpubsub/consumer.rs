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

use crate::connectors::google::{AuthInterceptor, TokenProvider};
use crate::connectors::prelude::*;
use crate::{
    channel::{bounded, Receiver, Sender},
    connectors::google::TokenSrc,
};
use beef::generic::Cow;
use futures::StreamExt;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use googapis::google::pubsub::v1::{
    GetSubscriptionRequest, PubsubMessage, ReceivedMessage, StreamingPullRequest,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Code, Status};
use tremor_common::blue_green_hashmap::BlueGreenHashMap;

// controlling retries upon gpubsub returning `Unavailable` from StreamingPull
// this in on purpose not exposed via config as this should remain an internal thing
const RETRY_WAIT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RETRIES: u64 = 5;

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "crate::connectors::impls::gpubsub::default_connect_timeout")]
    pub connect_timeout: u64,
    #[serde(default = "default_ack_deadline")]
    pub ack_deadline: u64,
    pub subscription_id: String,
    #[serde(default = "default_max_outstanding_messages")]
    pub max_outstanding_messages: i64,
    #[serde(default = "default_max_outstanding_bytes")]
    pub max_outstanding_bytes: i64,
    pub token: TokenSrc,
    #[serde(default = "crate::connectors::impls::gpubsub::default_endpoint")]
    pub url: Url<HttpsDefaults>,
}
impl tremor_config::Impl for Config {}

fn default_ack_deadline() -> u64 {
    10_000_000_000u64 // 10 seconds
}

/// qsize or 128
fn default_max_outstanding_messages() -> i64 {
    i64::try_from(qsize()).unwrap_or(128)
}
/// 10 MB
fn default_max_outstanding_bytes() -> i64 {
    1024 * 1024 * 10
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[cfg(all(test, feature = "gcp-integration"))]
type GSubWithTokenProvider = GSub<crate::connectors::google::tests::TestTokenProvider>;

#[cfg(not(all(test, feature = "gcp-integration")))]
type GSubWithTokenProvider = GSub<crate::connectors::google::GouthTokenProvider>;

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "gpubsub_consumer".into()
    }

    async fn build_cfg(
        &self,
        alias: &alias::Connector,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(raw)?;
        let url = Url::<HttpsDefaults>::parse(config.url.as_str())?;
        let client_id = format!(
            "tremor-{}-{alias}-{:?}",
            hostname(),
            crate::utils::task_id()
        );

        Ok(Box::new(GSubWithTokenProvider {
            config,
            url,
            client_id,
            _phantom: PhantomData,
        }))
    }
}

struct GSub<T> {
    config: Config,
    url: Url<HttpsDefaults>,
    client_id: String,
    _phantom: PhantomData<T>,
}

type PubSubClient<T> = SubscriberClient<InterceptedService<Channel, AuthInterceptor<T>>>;
type AsyncTaskMessage = Result<(u64, PubsubMessage)>;

struct GSubSource<T: TokenProvider> {
    config: Config,
    client: Option<PubSubClient<T>>,
    receiver: Option<Receiver<AsyncTaskMessage>>,
    ack_sender: Option<async_std::channel::Sender<u64>>,
    task_handle: Option<JoinHandle<()>>,
    url: Url<HttpsDefaults>,
    client_id: String,
}

impl<T: TokenProvider> GSubSource<T> {
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

async fn consumer_task<T: TokenProvider>(
    mut client: PubSubClient<T>,
    ctx: SourceContext,
    client_id: String,
    sender: Sender<AsyncTaskMessage>,
    config: Config,
    ack_receiver: async_std::channel::Receiver<u64>,
) -> Result<()> {
    let mut ack_counter = 0;
    let ack_deadline = Duration::from_nanos(config.ack_deadline);
    let max_outstanding_messages = config.max_outstanding_messages;
    let max_outstanding_bytes = config.max_outstanding_bytes;
    let subscription_id = config.subscription_id;
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
        max_outstanding_messages,
        max_outstanding_bytes,
    };

    let ack_ids_c = ack_ids.clone();
    let mut retry_wait_interval = RETRY_WAIT_INTERVAL;
    let mut attempt: u64 = 0;

    'retry: loop {
        // yeah, i know, my name is george cloney
        let initial_req = initial_request.clone();
        let ack_ids_cc = ack_ids_c.clone();
        let client_id_c = client_id.clone();
        // TODO: This could be leading to a bug, we clone the receiver but the behaviour of the
        // stream is that only one receiver will get the response, so if can't guarantee that before
        // the next iteration we don't use this any more then we might have the problem that acks
        // don't get properly delivered
        let ack_recvr = ack_receiver.clone();
        let request_stream = async_stream::stream! {
            yield initial_req;

            while let Ok(pull_id) = ack_recvr.recv().await {
                if let Some(ack_id) = ack_ids_cc.write().await.remove(&pull_id) {
                    yield StreamingPullRequest {
                        subscription: String::new(),
                        ack_ids: vec![ack_id],
                        modify_deadline_seconds: vec![],
                        modify_deadline_ack_ids: vec![],
                        stream_ack_deadline_seconds: 0,
                        client_id: client_id_c.clone(),
                        max_outstanding_messages,
                        max_outstanding_bytes
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
                    tokio::time::sleep(retry_wait_interval).await;
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
impl<T: TokenProvider + 'static> Source for GSubSource<T> {
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

        if let Some(task_handle) = self.task_handle.take() {
            task_handle.abort();
        }

        let mut client = SubscriberClient::with_interceptor(
            channel.clone(),
            AuthInterceptor {
                token_provider: T::from(self.config.token.clone()),
            },
        );
        // check that the subscription exists
        let res = client
            .get_subscription(GetSubscriptionRequest {
                subscription: self.config.subscription_id.clone(),
            })
            .await?
            .into_inner();
        info!(
            "{ctx} Using subscription '{}' for topic '{}'",
            &res.name, &res.topic
        );
        debug!("{ctx} Subscription details {res:?}");

        let client_background = client.clone();

        let (tx, rx) = bounded(qsize());
        // TODO: get rid of async std but this is tricky, lets look at this after the gcp update
        let (ack_tx, ack_rx) = async_std::channel::bounded(qsize());

        let join_handle = spawn_task(
            ctx.clone(),
            consumer_task(
                client_background,
                ctx.clone(),
                self.client_id.clone(),
                tx,
                self.config.clone(),
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
        let (ack_id, pubsub_message) = receiver.recv().await.ok_or("The channel is closed")??;
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
impl<T: TokenProvider + 'static> Connector for GSub<T> {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = GSubSource::<T>::new(
            self.config.clone(),
            self.url.clone(),
            self.client_id.clone(),
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}
