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

use super::super::ConnectorHarness;
use super::redpanda_container;
use crate::connectors::tests::free_port::find_free_tcp_port;
use crate::{connectors::impls::kafka, errors::Result, Event};
use futures::StreamExt;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Headers,
    ClientConfig, Message,
};
use serial_test::serial;
use std::time::Duration;
use testcontainers::clients::Cli as DockerCli;
use tokio::time::timeout;
use tremor_common::ports::IN;
use tremor_pipeline::EventId;
use tremor_value::literal;

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]
async fn connector_kafka_producer() -> Result<()> {
    let _ = env_logger::try_init();
    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let mut admin_config = ClientConfig::new();
    let broker = format!("127.0.0.1:{port}");
    let topic = "tremor_test";
    let num_partitions = 3;
    let num_replicas = 1;
    admin_config
        .set("client.id", "test-admin")
        .set("bootstrap.servers", &broker);
    let admin_client = AdminClient::from_config(&admin_config)?;
    let options = AdminOptions::default();
    let res = admin_client
        .create_topics(
            vec![&NewTopic::new(
                topic,
                num_partitions,
                TopicReplication::Fixed(num_replicas),
            )],
            &options,
        )
        .await?;
    for r in res {
        match r {
            Err((topic, err)) => {
                error!("Error creating topic {}: {}", &topic, err);
            }
            Ok(topic) => {
                info!("Created topic {}", topic);
            }
        }
    }

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000_u64,
                "max_retries": 10_u64
            }
        },
        "codec": {"name": "json", "config": {"mode": "sorted"}},
        "config": {
            "brokers": [
                broker.clone()
            ],
            "topic": topic,
            "key": "snot",
            "rdkafka_options": {
            //    "debug": "all"
            }
        }
    });
    let mut harness = ConnectorHarness::new(
        function_name!(),
        &kafka::producer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    let consumer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("group.id", "connector_kafka_producer")
        //.set("client.id", "my-client")
        //.set("socket.timeout.ms", "2000")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        //.set("auto.commit.interval.ms", "100")
        .set("enable.auto.offset.store", "false")
        //.set("debug", "all")
        .create::<StreamConsumer>()
        .expect("Consumer creation error");
    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");
    let mut message_stream = consumer.stream();

    let data = literal!({
        "snot": "badger"
    });
    let meta = literal!({});
    let e1 = Event {
        id: EventId::default(),
        data: (data.clone(), meta).into(),
        transactional: false,
        ..Event::default()
    };
    harness.send_to_sink(e1, IN).await?;
    match timeout(Duration::from_secs(30), message_stream.next()) // first message, we might need to wait a little longer for the consumer to boot up and settle things with redpanda
        .await?
    {
        Some(Ok(msg)) => {
            assert_eq!(msg.key(), Some("snot".as_bytes()));
            assert_eq!(msg.payload(), Some("{\"snot\":\"badger\"}".as_bytes()));
            consumer
                .commit_message(&msg, CommitMode::Sync)
                .expect("Commit failed");
        }
        Some(Err(e)) => {
            return Err(e.into());
        }
        None => {
            return Err("Topic Stream unexpectedly finished.".into());
        }
    };
    assert!(harness.get_pipe(IN)?.get_contraflow_events().is_empty());

    let data2 = literal!([1, 2, 3]);
    let meta2 = literal!({
        "kafka_producer": {
            "key": "badger",
            "headers": {
                "foo": "baz"
            },
            "timestamp": 123_000_000,
            "partition": 0
        }
    });
    let e2 = Event {
        id: EventId::default(),
        data: (data2, meta2).into(),
        transactional: true,
        ..Event::default()
    };
    harness.send_to_sink(e2, IN).await?;
    match timeout(Duration::from_secs(5), message_stream.next()).await? {
        Some(Ok(msg)) => {
            assert_eq!(Some("badger".as_bytes()), msg.key());
            assert_eq!(Some("[1,2,3]".as_bytes()), msg.payload());
            assert_eq!(0_i32, msg.partition());
            assert_eq!(Some(123), msg.timestamp().to_millis());
            let headers = msg.headers().expect("No headers found");
            assert_eq!(1, headers.count());
            assert_eq!(Some(("foo", "baz".as_bytes())), headers.get(0));
            consumer
                .commit_message(&msg, CommitMode::Sync)
                .expect("Commit failed");
        }
        Some(Err(e)) => {
            return Err(e.into());
        }
        None => {
            return Err("EOF on kafka topic".into());
        }
    }

    // batched event
    let batched_data = literal!([{
        "data": {
            "value": {
                "field1": 0.1,
                "field3": []
            },
            "meta": {
                "kafka_producer": {
                    "key": "nananananana: batchman!"
                }
            }
        }
    }, {
        "data": {
            "value": {
                "field2": "just a string"
            },
            "meta": {}
        }
    }]);
    let batched_meta = literal!({});
    let batched_event = Event {
        id: EventId::from_id(0, 0, 1),
        data: (batched_data, batched_meta).into(),
        transactional: true,
        is_batch: true,
        ..Event::default()
    };
    harness.send_to_sink(batched_event, IN).await?;
    let borrowed_batchman_msg = timeout(Duration::from_secs(2), message_stream.next())
        .await?
        .expect("timeout waiting for batchman message")
        .expect("error waiting for batchman message");
    consumer
        .commit_message(&borrowed_batchman_msg, CommitMode::Sync)
        .expect("commit failed");
    let mut batchman_msg = borrowed_batchman_msg.detach();
    drop(borrowed_batchman_msg);

    let borrowed_snot_msg = timeout(Duration::from_secs(2), message_stream.next())
        .await?
        .expect("timeout waiting for batchman message")
        .expect("error waiting for batchman message");
    consumer
        .commit_message(&borrowed_snot_msg, CommitMode::Sync)
        .expect("commit failed");
    let mut snot_msg = borrowed_snot_msg.detach();
    drop(borrowed_snot_msg);
    if batchman_msg.key().eq(&Some("snot".as_bytes())) {
        core::mem::swap(&mut snot_msg, &mut batchman_msg);
    }
    assert_eq!(
        Some("nananananana: batchman!".as_bytes()),
        batchman_msg.key()
    );
    assert_eq!(
        Some("{\"field1\":0.1,\"field3\":[]}".as_bytes()),
        batchman_msg.payload()
    );
    assert!(batchman_msg.headers().is_none());

    assert_eq!(Some("snot".as_bytes()), snot_msg.key());
    assert_eq!(
        Some("{\"field2\":\"just a string\"}".as_bytes()),
        snot_msg.payload()
    );
    assert!(snot_msg.headers().is_none());

    consumer.unsubscribe();
    drop(message_stream);
    drop(consumer);

    // shutdown
    let (out_events, err_events) = harness.stop().await?;
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);
    // cleanup
    drop(container);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]
async fn producer_unreachable() -> Result<()> {
    let _ = env_logger::try_init();
    let port = find_free_tcp_port().await?;
    let broker = format!("127.0.0.1:{port}");
    let topic = "unreachable";
    let connector_config = literal!({
        "codec": {"name": "json", "config": {"mode": "sorted"}},
        "config": {
            "brokers": [
                broker.clone()
            ],
            "topic": topic,
            "key": "snot",
            "rdkafka_options": {
                "debug": "all"
            }
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::producer::Builder::default(),
        &connector_config,
    )
    .await?;
    assert!(harness.start().await.is_err());
    Ok(())
}

/*
#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]
async fn producer_unresolvable() -> Result<()> {
    let _ = env_logger::try_init();
    let port = find_free_tcp_port().await?;
    let broker = format!("i_do_not_resolve:{port}");
    let topic = "unresolvable";
    let connector_config = literal!({
        "codec": {"name": "json", "config": {"mode": "sorted"}},
        "config": {
            "brokers": [
                broker.clone()
            ],
            "topic": topic,
            "key": "snot",
            "rdkafka_options": {
                "debug": "all"
            }
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::producer::Builder::default(),
        &connector_config,
    )
    .await?;
    assert!(harness.start().await.is_err());
    Ok(())
}
*/
