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
use crate::connectors::utils::EnvHelper;
use crate::errors::Result;
use googapis::google::pubsub::v1::publisher_client::PublisherClient;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use googapis::google::pubsub::v1::{PublishRequest, PubsubMessage, Subscription, Topic};
use serial_test::serial;
use std::collections::HashMap;
use testcontainers::clients::Cli;
use testcontainers::RunnableImage;
use tonic::transport::Channel;
use tremor_pipeline::CbAction;
use tremor_runtime::instance::State;
use tremor_value::{literal, Value};
use value_trait::ValueAccess;

#[async_std::test]
#[serial(gpubsub, timeout_ms = 600000)]
async fn no_connection() -> Result<()> {
    let _ = env_logger::try_init();
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "url": "https://localhost:9090",
            "ack_deadline": 30000000000u64,
            "connect_timeout": 100000000,
            "subscription_id": "projects/xxx/subscriptions/test-subscription-a"
        }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &Builder::default(), &connector_yaml).await?;
    assert!(harness.start().await.is_err());
    Ok(())
}

#[async_std::test]
#[serial(gpubsub, timeout_ms = 600000)]
async fn no_token() -> Result<()> {
    let _ = env_logger::try_init();
    let mut env = EnvHelper::new();
    env.remove_var("GOOGLE_APPLICATION_CREDENTIALS");
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "ack_deadline": 30000000000u64,
            "connect_timeout": 100000000,
            "subscription_id": "projects/xxx/subscriptions/test-subscription-a"
        }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &Builder::default(), &connector_yaml).await?;

    harness.consume_initial_sink_contraflow().await?;
    harness.wait_for_state(State::Failed).await?;

    Ok(())
}

#[async_std::test]
#[serial(gpubsub, timeout_ms = 600000)]
async fn simple_subscribe() -> Result<()> {
    let _ = env_logger::try_init();

    let runner = Cli::docker();

    let (pubsub, pubsub_args) =
        testcontainers::images::google_cloud_sdk_emulators::CloudSdk::pubsub();
    let runnable_image = RunnableImage::from((pubsub, pubsub_args));
    let container = runner.run(runnable_image);

    let port = container
        .get_host_port_ipv4(testcontainers::images::google_cloud_sdk_emulators::PUBSUB_PORT);
    let endpoint = format!("http://localhost:{}", port);
    let endpoint_clone = endpoint.clone();

    let connector_yaml: Value = literal!({
        "metrics_interval_s": 1,
        "codec": "binary",
        "config":{
            "url": endpoint,
            "ack_deadline": 30000000000u64,
            "connect_timeout": 30000000000u64,
            "subscription_id": "projects/test/subscriptions/test-subscription-a",
            "skip_authentication": true
        }
    });

    let channel = Channel::from_shared(endpoint_clone)?.connect().await?;
    let mut publisher = PublisherClient::new(channel.clone());
    publisher
        .create_topic(Topic {
            name: "projects/test/topics/test".to_string(),
            labels: Default::default(),
            message_storage_policy: None,
            kms_key_name: "".to_string(),
            schema_settings: None,
            satisfies_pzs: false,
            message_retention_duration: None,
        })
        .await?;

    let mut subscriber = SubscriberClient::new(channel);
    subscriber
        .create_subscription(Subscription {
            name: "projects/test/subscriptions/test-subscription-a".to_string(),
            topic: "projects/test/topics/test".to_string(),
            push_config: None,
            ack_deadline_seconds: 0,
            retain_acked_messages: false,
            message_retention_duration: None,
            labels: Default::default(),
            enable_message_ordering: false,
            expiration_policy: None,
            filter: "".to_string(),
            dead_letter_policy: None,
            retry_policy: None,
            detached: false,
            topic_message_retention_duration: None,
        })
        .await?;

    let harness =
        ConnectorHarness::new(function_name!(), &Builder::default(), &connector_yaml).await?;

    let out_pipe = harness
        .out()
        .expect("No pipelines connected to out port of s3-reader");
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    let mut attributes = HashMap::new();
    attributes.insert("a".to_string(), "b".to_string());

    publisher
        .publish(PublishRequest {
            topic: "projects/test/topics/test".to_string(),
            messages: vec![PubsubMessage {
                data: Vec::from("abc1".as_bytes()),
                attributes,
                message_id: "".to_string(),
                publish_time: None,
                ordering_key: "test".to_string(),
            }],
        })
        .await?;

    let event = out_pipe.get_event().await?;
    harness.send_contraflow(CbAction::Ack, event.id).await?;
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());

    let (value, meta) = event.data.parts();

    assert_eq!(
        Some(Vec::from("abc1".as_bytes())),
        value.as_bytes().map(|x| Vec::from(x))
    );

    assert_eq!(
        Some(&Value::from("b")),
        meta.as_object()
            .unwrap()
            .get("gpubsub_consumer")
            .unwrap()
            .as_object()
            .unwrap()
            .get("attributes")
            .unwrap()
            .as_object()
            .unwrap()
            .get("a")
    );

    return Ok(());
}
