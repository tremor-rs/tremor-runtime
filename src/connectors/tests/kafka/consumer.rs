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
use crate::{
    connectors::{impls::kafka, tests::free_port},
    errors::Result,
};
use async_std::task;
use beef::Cow;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    message::OwnedHeaders,
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};
use serial_test::serial;
use std::time::Duration;
use testcontainers::clients::Cli as DockerCli;
use tremor_pipeline::CbAction;
use tremor_value::{literal, Value};
use value_trait::Builder;

#[async_std::test]
#[serial(kafka)]
async fn connector_kafka_consumer_transactional_retry() -> Result<()> {
    serial_test::set_max_wait(Duration::from_secs(600));

    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let mut admin_config = ClientConfig::new();
    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test";
    let group_id = "transactional_retry";
    admin_config
        .set("client.id", "test-admin")
        .set("bootstrap.servers", &broker);
    let admin_client = AdminClient::from_config(&admin_config)?;
    let options = AdminOptions::default();
    let res = admin_client
        .create_topics(
            vec![&NewTopic::new(topic, 3, TopicReplication::Fixed(1))],
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
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000_u64,
                "max_retries": 10_u64
            }
        },
        "codec": "json-sorted",
        "config": {
            "brokers": [
                broker.clone()
            ],
            "group_id": group_id,
            "topics": [
                topic
            ],
            "mode": {
                "transactional": {}
            }
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    let err = harness.err().expect("No pipe connected to port ERR");
    harness.start().await?;
    harness.wait_for_connected().await?;

    // TODO: it seems to work reliably which hints at a timeout inside redpanda
    // TODO: verify
    task::sleep(Duration::from_secs(5)).await;

    let record = BaseRecord::to(topic)
        .payload("{\"snot\":\"badger\"}\n")
        .key("foo")
        .partition(1)
        .timestamp(42)
        .headers(OwnedHeaders::new().add("header", "snot"));
    if producer.send(record).is_err() {
        return Err("Unable to send record to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    let e1 = out.get_event().await?;
    assert_eq!(
        literal!({
            "snot": "badger"
        }),
        e1.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("foo".as_bytes().to_vec())),
                "headers": {
                    "header": Value::Bytes(Cow::owned("snot".as_bytes().to_vec()))
                },
                "topic": topic,
                "offset": 0,
                "partition": 1,
                "timestamp": 42000000
            }
        }),
        e1.data.suffix().meta()
    );

    // ack event
    harness
        .send_contraflow(CbAction::Ack, e1.id.clone())
        .await?;

    // second event
    let record2 = BaseRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));
    let e2 = out.get_event().await?;
    assert_eq!(Value::null(), e2.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12000000
            }
        }),
        e2.data.suffix().meta()
    );
    // fail -> ensure it is replayed
    harness
        .send_contraflow(CbAction::Fail, e2.id.clone())
        .await?;

    // we get the same event again
    let e3 = out.get_event().await?;
    assert_eq!(Value::null(), e3.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12000000
            }
        }),
        e3.data.suffix().meta()
    );
    assert_eq!(e2.id.pull_id(), e3.id.pull_id());
    assert_eq!(e2.id.stream_id(), e3.id.stream_id());

    // ack the event
    harness
        .send_contraflow(CbAction::Ack, e3.id.clone())
        .await?;

    // send another event and check that the previous one isn't replayed
    let record3 = BaseRecord::to(topic)
        .key("trigger")
        .payload("false\n")
        .partition(2)
        .timestamp(123);
    if producer.send(record3).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    // next event
    let e4 = out.get_event().await?;
    assert_eq!(Value::from(false), e4.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("trigger".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 2,
                "timestamp": 123000000
            }

        }),
        e4.data.suffix().meta()
    );

    // test failing logic
    let record4 = BaseRecord::to(topic)
        .key("failure")
        .payload("}\n")
        .partition(2)
        .timestamp(1234);
    if producer.send(record4).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    let e5 = err.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "source": "test::connector_kafka_consumer_transactional_retry",
            "stream_id": 8589934592_u64,
            "pull_id": 1u64
        }),
        e5.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("failure".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "partition": 2,
                "offset": 1,
                "timestamp": 1234000000
            }
        }),
        e5.data.suffix().meta()
    );

    let (out_events, err_events) = harness.stop().await?;
    assert!(out_events.is_empty());
    assert!(err_events.is_empty());
    // cleanup
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka)]
async fn connector_kafka_consumer_transactional_no_retry() -> Result<()> {
    serial_test::set_max_wait(Duration::from_secs(600));

    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let mut admin_config = ClientConfig::new();
    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_no_retry";
    admin_config
        .set("client.id", "test-admin")
        .set("bootstrap.servers", &broker);
    let admin_client = AdminClient::from_config(&admin_config)?;
    let options = AdminOptions::default();
    let res = admin_client
        .create_topics(
            vec![&NewTopic::new(topic, 3, TopicReplication::Fixed(1))],
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
        "codec": "json-sorted",
        "config": {
            "brokers": [
                broker.clone()
            ],
            "group_id": "test1",
            "topics": [
                topic
            ],
            "mode": {
                "custom": {
                    "rdkafka_options": {
                        "enable.auto.commit": "false"
                    //    "debug": "all"
                    },
                    "retry_failed_events": false
                }
            }
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    let err = harness.err().expect("No pipe connected to port ERR");
    harness.start().await?;
    harness.wait_for_connected().await?;

    // TODO: it seems to work reliably which hints at a timeout inside redpanda
    // TODO: verify
    task::sleep(Duration::from_secs(5)).await;

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");
    let record = BaseRecord::to(topic)
        .payload("{\"snot\":\"badger\"}\n")
        .key("foo")
        .partition(1)
        .timestamp(42)
        .headers(OwnedHeaders::new().add("header", "snot"));
    if producer.send(record).is_err() {
        return Err("Unable to send record to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    let e1 = out.get_event().await?;
    assert_eq!(
        literal!({
            "snot": "badger"
        }),
        e1.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("foo".as_bytes().to_vec())),
                "headers": {
                    "header": Value::Bytes(Cow::owned("snot".as_bytes().to_vec()))
                },
                "topic": topic,
                "offset": 0,
                "partition": 1,
                "timestamp": 42000000
            }
        }),
        e1.data.suffix().meta()
    );

    // ack event
    harness
        .send_contraflow(CbAction::Ack, e1.id.clone())
        .await?;

    // produce second event
    let record2 = BaseRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    // get second event
    let e2 = out.get_event().await?;
    assert_eq!(Value::null(), e2.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12000000
            }
        }),
        e2.data.suffix().meta()
    );
    // fail -> ensure it is not replayed
    harness
        .send_contraflow(CbAction::Fail, e2.id.clone())
        .await?;

    // we don't get no event
    assert!(out
        .expect_no_event_for(Duration::from_millis(200))
        .await
        .is_ok());

    // send another event and check that the previous one isn't replayed
    let record3 = BaseRecord::to(topic)
        .key("trigger")
        .payload("false\n")
        .partition(2)
        .timestamp(123);
    if producer.send(record3).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    // we only get the next event, no previous one
    let e3 = out.get_event().await?;
    assert_eq!(Value::from(false), e3.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("trigger".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 2,
                "timestamp": 123000000
            }

        }),
        e3.data.suffix().meta()
    );

    // test failing logic
    let record4 = BaseRecord::to(topic)
        .key("failure")
        .payload("}\n")
        .partition(2)
        .timestamp(1234);
    if producer.send(record4).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    let e5 = err.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "source": "test::connector_kafka_consumer_transactional_no_retry",
            "stream_id": 8589934592_u64,
            "pull_id": 1u64
        }),
        e5.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("failure".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "partition": 2,
                "offset": 1,
                "timestamp": 1234000000
            }
        }),
        e5.data.suffix().meta()
    );

    let (out_events, err_events) = harness.stop().await?;
    assert!(out_events.is_empty());
    assert!(err_events.is_empty());
    // cleanup
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka)]
async fn connector_kafka_consumer_non_transactional() -> Result<()> {
    serial_test::set_max_wait(Duration::from_secs(600));

    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let mut admin_config = ClientConfig::new();
    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_no_retry";
    let group_id = "group123";
    admin_config
        .set("client.id", "test-admin")
        .set("bootstrap.servers", &broker);
    let admin_client = AdminClient::from_config(&admin_config)?;
    let options = AdminOptions::default();
    let res = admin_client
        .create_topics(
            vec![&NewTopic::new(topic, 3, TopicReplication::Fixed(1))],
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
        "codec": "json-sorted",
        "config": {
            "brokers": [
                broker.clone()
            ],
            "group_id": group_id,
            "topics": [
                topic
            ],
            "mode": "performance"
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    let err = harness.err().expect("No pipe connected to port ERR");
    harness.start().await?;
    harness.wait_for_connected().await?;

    // TODO: it seems to work reliably which hints at a timeout inside redpanda
    // TODO: verify
    task::sleep(Duration::from_secs(5)).await;

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");
    let record = BaseRecord::to(topic)
        .payload("{\"snot\":\"badger\"}\n")
        .key("foo")
        .partition(1)
        .timestamp(42)
        .headers(OwnedHeaders::new().add("header", "snot"));
    if producer.send(record).is_err() {
        return Err("Unable to send record to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    let e1 = out.get_event().await?;
    assert_eq!(
        literal!({
            "snot": "badger"
        }),
        e1.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("foo".as_bytes().to_vec())),
                "headers": {
                    "header": Value::Bytes(Cow::owned("snot".as_bytes().to_vec()))
                },
                "topic": topic,
                "offset": 0,
                "partition": 1,
                "timestamp": 42000000
            }
        }),
        e1.data.suffix().meta()
    );

    // ack event
    harness
        .send_contraflow(CbAction::Ack, e1.id.clone())
        .await?;

    // produce second event
    let record2 = BaseRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    // get second event
    let e2 = out.get_event().await?;
    assert_eq!(Value::null(), e2.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12000000
            }
        }),
        e2.data.suffix().meta()
    );

    // fail -> ensure it is not replayed
    harness
        .send_contraflow(CbAction::Fail, e2.id.clone())
        .await?;

    // we don't get no event
    assert!(out
        .expect_no_event_for(Duration::from_millis(200))
        .await
        .is_ok());

    // send another event and check that the previous one isn't replayed
    let record3 = BaseRecord::to(topic)
        .key("trigger")
        .payload("false\n")
        .partition(2)
        .timestamp(123);
    if producer.send(record3).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    // we only get the next event, no previous one
    let e3 = out.get_event().await?;
    assert_eq!(Value::from(false), e3.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("trigger".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 2,
                "timestamp": 123000000
            }

        }),
        e3.data.suffix().meta()
    );

    // test failing logic
    let record4 = BaseRecord::to(topic)
        .key("failure")
        .payload("}\n")
        .partition(2)
        .timestamp(1234);
    if producer.send(record4).is_err() {
        return Err("Could not send to kafka".into());
    }
    producer.flush(Duration::from_secs(1));

    let e5 = err.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "source": "test::connector_kafka_consumer_non_transactional",
            "stream_id": 8589934592_u64,
            "pull_id": 1u64
        }),
        e5.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("failure".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "partition": 2,
                "offset": 1,
                "timestamp": 1234000000
            }
        }),
        e5.data.suffix().meta()
    );

    let (out_events, err_events) = harness.stop().await?;
    assert!(out_events.is_empty());
    assert!(err_events.is_empty());
    // cleanup
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka)]
async fn connector_kafka_consumer_unreachable() -> Result<()> {
    serial_test::set_max_wait(Duration::from_secs(600));

    let kafka_port = free_port::find_free_tcp_port().await?;
    let _ = env_logger::try_init();
    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 100_u64,
                "max_retries": 5_u64
            }
        },
        "codec": "json-sorted",
        "config": {
            "brokers": [
                format!("127.0.0.1:{kafka_port}")
            ],
            "group_id": "all_brokers_down",
            "topics": [
                "snot"
            ],
            "mode": "performance"
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    assert!(harness.start().await.is_err());

    let (out_events, err_events) = harness.stop().await?;
    assert!(out_events.is_empty());
    assert!(err_events.is_empty());
    Ok(())
}

#[async_std::test]
#[serial(kafka)]
async fn connector_kafka_consumer_pause_resume() -> Result<()> {
    serial_test::set_max_wait(Duration::from_secs(600));

    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let mut admin_config = ClientConfig::new();

    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_pause_resume";
    let group_id = "group_pause_resume";

    admin_config
        .set("client.id", "test-admin")
        .set("bootstrap.servers", &broker);
    let admin_client = AdminClient::from_config(&admin_config)?;
    let options = AdminOptions::default();
    let res = admin_client
        .create_topics(
            vec![&NewTopic::new(topic, 3, TopicReplication::Fixed(1))],
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

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");
    let connector_config = literal!({
        "codec": "json-sorted",
        "config": {
            "brokers": [
                broker
            ],
            "group_id": group_id,
            "topics": [
                topic
            ],
            "mode": {
                "custom": {
                    "rdkafka_options": {
                        "debug": "all"
                    },
                    "retry_failed_events": false
                }
            }
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;

    task::sleep(Duration::from_secs(5)).await;

    let record = BaseRecord::to(topic)
        .key("badger")
        .payload("{\"snot\": true}")
        .partition(1)
        .timestamp(0);
    if producer.send(record).is_err() {
        return Err("Unable to send record to Kafka".into());
    }
    producer.flush(Duration::from_secs(1));
    debug!("BEFORE GET EVENT 1");
    let e1 = out.get_event().await?;
    assert_eq!(
        literal!({
            "snot": true
        }),
        e1.data.suffix().value()
    );
    debug!("AFTER GET EVENT 1");

    harness.pause().await?;

    let record2 = BaseRecord::to(topic)
        .key("waiting around to die")
        .payload("\"R.I.P.\"")
        .partition(0)
        .timestamp(1);
    if producer.send(record2).is_err() {
        return Err("Unable to send record to Kafka".into());
    }
    producer.flush(Duration::from_secs(1));
    // we didn't receive shit because we are paused
    assert!(out
        .expect_no_event_for(Duration::from_millis(200))
        .await
        .is_ok());

    harness.resume().await?;
    debug!("BEFORE GET EVENT 2");
    let e2 = out.get_event().await?;
    debug!("AFTER GET EVENT 2");
    assert_eq!(Value::from("R.I.P."), e2.data.suffix().value());

    let (out_events, err_events) = harness.stop().await?;
    assert!(out_events.is_empty());
    assert!(err_events.is_empty());

    // cleanup
    drop(container);
    Ok(())
}
