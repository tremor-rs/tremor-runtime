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

use crate::connectors::impls::gpubsub::consumer::Builder;
use crate::connectors::tests::ConnectorHarness;
use crate::errors::Result;
use googapis::google::pubsub::v1::publisher_client::PublisherClient;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use googapis::google::pubsub::v1::{
    GetSubscriptionRequest, PublishRequest, PubsubMessage, Subscription, Topic,
};
use serial_test::serial;
use std::collections::HashMap;
use testcontainers::clients::Cli;
use testcontainers::RunnableImage;
use tonic::transport::Channel;
use tremor_pipeline::CbAction;
use tremor_value::{literal, Value};
use value_trait::ValueAccess;

#[tokio::test(flavor = "multi_thread")]
#[serial(gpubsub)]
async fn no_connection() -> Result<()> {
    let _: std::result::Result<_, _> = env_logger::try_init();
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "token": {"file": file!().to_string()},
            "url": "https://localhost:9090",
            "ack_deadline": 30_000_000_000u64,
            "connect_timeout": 100_000_000,
            "subscription_id": "projects/xxx/subscriptions/test-subscription-a"
        }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &Builder::default(), &connector_yaml).await?;
    assert!(harness.start().await.is_err());
    Ok(())
}

async fn create_subscription(endpoint: String, topic: &str, subscription: &str) -> Result<()> {
    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut publisher = PublisherClient::new(channel.clone());
    publisher
        .create_topic(Topic {
            name: topic.to_string(),
            labels: HashMap::default(),
            message_storage_policy: None,
            kms_key_name: String::new(),
            schema_settings: None,
            satisfies_pzs: false,
            message_retention_duration: None,
        })
        .await?;

    let mut subscriber = SubscriberClient::new(channel);
    subscriber
        .create_subscription(Subscription {
            name: subscription.to_string(),
            topic: topic.to_string(),
            push_config: None,
            ack_deadline_seconds: 0,
            retain_acked_messages: false,
            message_retention_duration: None,
            labels: HashMap::default(),
            enable_message_ordering: false,
            expiration_policy: None,
            filter: String::new(),
            dead_letter_policy: None,
            retry_policy: None,
            detached: false,
            topic_message_retention_duration: None,
        })
        .await?;
    // assert the system knows about our subscription now
    subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: subscription.to_string(),
        })
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial(gpubsub)]
async fn simple_subscribe() -> Result<()> {
    let _: std::result::Result<_, _> = env_logger::try_init();
    let runner = Cli::docker();

    let (pubsub, pubsub_args) =
        testcontainers::images::google_cloud_sdk_emulators::CloudSdk::pubsub();
    let runnable_image = RunnableImage::from((pubsub, pubsub_args));
    let container = runner.run(runnable_image);
    let port = container
        .get_host_port_ipv4(testcontainers::images::google_cloud_sdk_emulators::PUBSUB_PORT);
    let endpoint = format!("http://localhost:{port}");
    let topic = "projects/test/topics/test";
    let subscription = "projects/test/subscriptions/test-subscription-a";
    let connector_yaml: Value = literal!({
        "metrics_interval_s": 1,
        "codec": "binary",
        "config":{
            "token": {"file": file!().to_string()},
            "url": endpoint.clone(),
            "ack_deadline": 30_000_000_000_u64,
            "connect_timeout": 30_000_000_000_u64,
            "subscription_id": subscription,
        }
    });
    create_subscription(endpoint.clone(), topic, subscription).await?;
    let mut harness =
        ConnectorHarness::new(function_name!(), &Builder::default(), &connector_yaml).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    // TODO: why has this to go away?!? harness.consume_initial_sink_contraflow().await?;
    let mut attributes = HashMap::new();
    attributes.insert("a".to_string(), "b".to_string());
    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut publisher = PublisherClient::new(channel.clone());
    publisher
        .publish(PublishRequest {
            topic: "projects/test/topics/test".to_string(),
            messages: vec![PubsubMessage {
                data: Vec::from("abc1".as_bytes()),
                attributes,
                message_id: String::new(),
                publish_time: None,
                ordering_key: "test".to_string(),
            }],
        })
        .await?;
    let event = harness.out()?.get_event().await?;
    harness.send_contraflow(CbAction::Ack, event.id)?;
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());

    let (value, meta) = event.data.parts();
    assert_eq!(
        Some(Vec::from("abc1".as_bytes())),
        value.as_bytes().map(Vec::from)
    );

    assert_eq!(
        Some(&Value::from("b")),
        meta.get("gpubsub_consumer").get("attributes").get("a")
    );

    Ok(())
}
