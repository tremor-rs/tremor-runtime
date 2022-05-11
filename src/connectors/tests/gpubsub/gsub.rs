use crate::connectors::tests::ConnectorHarness;
use crate::errors::Result;
use googapis::google::pubsub::v1::publisher_client::PublisherClient;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use googapis::google::pubsub::v1::{PublishRequest, PubsubMessage, Subscription, Topic};
use serial_test::serial;
use testcontainers::clients::Cli;
use testcontainers::RunnableImage;
use tonic::transport::Channel;
use tremor_value::{literal, Value};

#[async_std::test]
#[serial(gpubsub)]
async fn no_connection() -> Result<()> {
    let _ = env_logger::try_init();
    let connector_yaml = literal!({
        "codec": "binary",
        "config":{
            "endpoint": "http://localhost:9090",
            "connect_timeout": 100000000,
            "subscription_id": "projects/wf-gcp-us-tremor-sbx/subscriptions/test-subscription-a"
        }
    });

    let harness = ConnectorHarness::new(function_name!(), "gsub", &connector_yaml).await?;
    assert!(harness.start().await.is_err());
    Ok(())
}

#[async_std::test]
#[serial(gpubsub)]
async fn simple_subscribe() -> Result<()> {
    let _ = env_logger::try_init();

    let runner = Cli::docker();

    let (pubsub, pubsub_args) =
        testcontainers::images::google_cloud_sdk_emulators::CloudSdk::pubsub();
    let runnable_image = RunnableImage::from((pubsub, pubsub_args));
    let container = runner.run(runnable_image);

    // let hostname = container.get_bridge_ip_address();
    let port =
        container.get_host_port(testcontainers::images::google_cloud_sdk_emulators::PUBSUB_PORT);
    let endpoint = format!("http://localhost:{}", port);
    let endpoint_clone = endpoint.clone();

    let connector_yaml: Value = literal!({
        "codec": "binary",
        "config":{
            "endpoint": endpoint,
            "connect_timeout": 100000000,
            "subscription_id": "projects/test/subscriptions/test-subscription-a"
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

    let harness = ConnectorHarness::new(function_name!(), "gsub", &connector_yaml).await?;

    let out_pipe = harness
        .out()
        .expect("No pipelines connected to out port of s3-reader");
    harness.start().await?;

    publisher
        .publish(PublishRequest {
            topic: "projects/test/topics/test".to_string(),
            messages: vec![PubsubMessage {
                data: Vec::from("abc1".as_bytes()),
                attributes: Default::default(),
                message_id: "".to_string(),
                publish_time: None,
                ordering_key: "".to_string(),
            }],
        })
        .await?;

    let event = out_pipe.get_event().await?;

    assert_eq!(
        Some(Vec::from("abc1".as_bytes())),
        event.data.parts().0.as_bytes().map(|x| Vec::from(x))
    );

    return Ok(());
}
