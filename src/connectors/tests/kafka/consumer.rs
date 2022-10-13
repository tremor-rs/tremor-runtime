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

use crate::{
    connectors::{
        impls::kafka,
        tests::{
            free_port,
            kafka::{redpanda_container, PRODUCE_TIMEOUT},
            ConnectorHarness,
        },
    },
    errors::Result,
};
use async_std::prelude::FutureExt;
use async_std::task;
use beef::Cow;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::KafkaResult,
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Offset,
};
use serial_test::serial;
use std::collections::HashMap;
use std::time::Duration;
use testcontainers::clients::Cli as DockerCli;
use tremor_pipeline::CbAction;
use tremor_value::{literal, Value};
use value_trait::Builder;

#[async_std::test]
#[serial(kafka, timeout_ms = 600000)]
async fn transactional_retry() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test";
    let group_id = "transactional_retry";

    create_topic(&broker, topic, 3, TopicReplication::Fixed(1)).await?;

    let producer: FutureProducer = ClientConfig::new()
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
            },
            "test_options": {
                "auto.offset.reset": "beginning"
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

    let record = FutureRecord::to(topic)
        .payload("{\"snot\":\"badger\"}\n")
        .key("foo")
        .partition(1)
        .timestamp(42)
        .headers(OwnedHeaders::new().add("header", "snot"));
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to kafka".into());
    }

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
    let record2 = FutureRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }
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
    let record3 = FutureRecord::to(topic)
        .key("trigger")
        .payload("false\n")
        .partition(2)
        .timestamp(123);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

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
    let record4 = FutureRecord::to(topic)
        .key("failure")
        .payload("}\n")
        .partition(2)
        .timestamp(1234);
    if producer.send(record4, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

    let e5 = err.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "source": "test::transactional_retry",
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
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);

    // check out the committed offsets with another consumer
    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(1))
    );
    assert_eq!(
        offsets.get(&(topic.to_string(), 1)),
        Some(&Offset::Offset(1))
    );
    assert_eq!(offsets.get(&(topic.to_string(), 2)), Some(&Offset::Invalid)); // nothing committed yet

    // cleanup
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka, timeout_ms = 600000)]
async fn custom_no_retry() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_no_retry";
    let group_id = "test1";

    create_topic(&broker, topic, 3, TopicReplication::Fixed(1)).await?;

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
                "custom": {
                    "rdkafka_options": {
                        "enable.auto.commit": "false",
                        "auto.offset.reset": "beginning",
                        //"debug": "all"
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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("debug", "all")
        .create()
        .expect("Producer creation error");
    let record = FutureRecord::to(topic)
        .payload("{\"snot\":\"badger\"}\n")
        .key("foo")
        .partition(1)
        .timestamp(42)
        .headers(OwnedHeaders::new().add("header", "snot"));
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to kafka".into());
    }

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
    let record2 = FutureRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

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
    let record3 = FutureRecord::to(topic)
        .key("trigger")
        .payload("false\n")
        .partition(2)
        .timestamp(123);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

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
    let record4 = FutureRecord::to(topic)
        .key("failure")
        .payload("}\n")
        .partition(2)
        .timestamp(1234);
    if producer.send(record4, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

    let e5 = err.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "source": "test::custom_no_retry",
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
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);

    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(offsets.get(&(topic.to_string(), 0)), Some(&Offset::Invalid));
    assert_eq!(
        offsets.get(&(topic.to_string(), 1)),
        Some(&Offset::Offset(1))
    );
    assert_eq!(offsets.get(&(topic.to_string(), 2)), Some(&Offset::Invalid));
    // cleanup
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka, timeout_ms = 600000)]
async fn performance() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_no_retry";
    let group_id = "group123";

    create_topic(&broker, topic, 3, TopicReplication::Fixed(1)).await?;

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
            "mode": "performance",
            "test_options": {
                "auto.offset.reset": "beginning"
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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");
    let record = FutureRecord::to(topic)
        .payload("{\"snot\":\"badger\"}\n")
        .key("foo")
        .partition(1)
        .timestamp(42)
        .headers(OwnedHeaders::new().add("header", "snot"));
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to kafka".into());
    }

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
    let record2 = FutureRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

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
    let record3 = FutureRecord::to(topic)
        .key("trigger")
        .payload("false\n")
        .partition(2)
        .timestamp(123);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

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
    let record4 = FutureRecord::to(topic)
        .key("failure")
        .payload("}\n")
        .partition(2)
        .timestamp(1234);
    if producer.send(record4, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

    let e5 = err.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError at character 0 ('}')",
            "source": "test::performance",
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
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);

    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(1))
    );
    assert_eq!(
        offsets.get(&(topic.to_string(), 1)),
        Some(&Offset::Offset(1))
    );
    assert_eq!(
        offsets.get(&(topic.to_string(), 2)),
        Some(&Offset::Offset(2))
    );

    // cleanup
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka, timeout_ms = 600000)]
async fn connector_kafka_consumer_unreachable() -> Result<()> {
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
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);
    Ok(())
}

#[async_std::test]
async fn invalid_rdkafka_options() -> Result<()> {
    let _ = env_logger::try_init();
    let kafka_port = free_port::find_free_tcp_port().await?;
    let broker = format!("127.0.0.1:{kafka_port}");
    let topic = "tremor_test_pause_resume";
    let group_id = "invalid_rdkafka_options";

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
                broker.clone()
            ],
            "group_id": group_id,
            "topics": [
                topic
            ],
            "mode": {
                "custom": {
                    "rdkafka_options": {
                        "enable.auto.commit": false,
                        "hotzen": "PLOTZ"
                    },
                    "retry_failed_events": false
                }
            }
        }
    });
    assert!(ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await
    .is_err());
    Ok(())
}

#[async_std::test]
#[serial(kafka, timeout_ms = 600000)]
async fn connector_kafka_consumer_pause_resume() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);

    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_pause_resume";
    let group_id = "group_pause_resume";

    create_topic(&broker, topic, 3, TopicReplication::Fixed(1)).await?;

    let producer: FutureProducer = ClientConfig::new()
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
            "mode": "performance",
            "test_options": {
                "auto.offset.reset": "beginning"
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

    let record = FutureRecord::to(topic)
        .key("badger")
        .payload("{\"snot\": true}")
        .partition(1)
        .timestamp(0);
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }
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

    let record2 = FutureRecord::to(topic)
        .key("waiting around to die")
        .payload("\"R.I.P.\"")
        .partition(0)
        .timestamp(1);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }
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
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);

    // cleanup
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka, timeout_ms = 600000)]
async fn transactional_store_offset_handling() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);

    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_store_offsets";
    let group_id = "group_transactional_store_offsets";

    create_topic(&broker, topic, 1, TopicReplication::Fixed(1)).await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");
    let commit_interval: u64 = Duration::from_millis(100).as_nanos().try_into()?;
    let connector_config = literal!({
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
                "transactional": {
                    "commit_interval": commit_interval
                }
            },
            "test_options": {
                "auto.offset.reset": "beginning"
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

    // send message 1
    let record1 = FutureRecord::to(topic)
        .key("1")
        .payload("1")
        .partition(0)
        .timestamp(1);
    if producer.send(record1, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }
    // send message 2
    let record2 = FutureRecord::to(topic)
        .key("2")
        .payload("2")
        .partition(0)
        .timestamp(2);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }

    // send message 3
    let record3 = FutureRecord::to(topic)
        .key("3")
        .payload("3")
        .partition(0)
        .timestamp(3);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }

    // receive events
    let event1 = out.get_event().await?;
    assert_eq!(&Value::from(1), event1.data.suffix().value());
    let event2 = out.get_event().await?;
    assert_eq!(&Value::from(2), event2.data.suffix().value());
    let event3 = out.get_event().await?;
    assert_eq!(&Value::from(3), event3.data.suffix().value());

    // ack message 3
    harness
        .send_contraflow(CbAction::Ack, event3.id.clone())
        .await?;
    // ack message 1
    harness
        .send_contraflow(CbAction::Ack, event1.id.clone())
        .await?;

    // stop harness
    let (out_events, err_events) = harness.stop().await?;
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);

    debug!("getting offsets...");
    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(3))
    );

    debug!("before start");
    // no offset reset
    let connector_config = literal!({
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
                "transactional": {
                    "commit_interval": commit_interval
                }
            }
        }
    });

    // start new harness
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;
    debug!("connected");
    
    // give the background librdkafka time to start fetching the partitions before we fail
    // otherwise it will fail silently in the background
    // consider this a cry for help!
    task::sleep(Duration::from_millis(200)).await;
    // fail message 2
    harness
        .send_contraflow(CbAction::Fail, event2.id.clone())
        .await?;

    debug!("failed event2");

    // ensure message 2 and 3 are received
    // for some strange reason rdkafka might consumer event2 twice
    let mut event_again = out.get_event().await?;
    let mut data = event_again.data.suffix().value();
    while data == &Value::from(2) {
        event_again = out.get_event().await?;
        data = event_again.data.suffix().value();
    }
    assert_eq!(&Value::from(3), data);

    // stop harness
    let _ = harness.stop().await?;

    // check that fail did reset the offset correctly
    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(1))
    );
    drop(offsets);

    // start new harness
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;

    // ensure message 2 and 3 are received - yet again
    // for some strange reason rdkafka might consumer event2 twice
    let mut event_again = out.get_event().await?;
    let mut data = event_again.data.suffix().value();
    while data == &Value::from(2) {
        event_again = out.get_event().await?;
        data = event_again.data.suffix().value();
    }
    assert_eq!(&Value::from(3), data);

    // ack message 3 only
    harness
        .send_contraflow(CbAction::Ack, event_again.id.clone())
        .await?;

    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(3))
    );

    // stop harness
    let _ = harness.stop().await?;

    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(kafka, timeout_ms = 600000)]
async fn transactional_commit_offset_handling() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = DockerCli::default();
    let container = redpanda_container(&docker).await?;

    let port = container.get_host_port_ipv4(9092);
    let broker = format!("127.0.0.1:{}", port);
    let topic = "tremor_test_commit_offset";
    let group_id = "group_commit_offset";

    create_topic(&broker, topic, 1, TopicReplication::Fixed(1)).await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("acks", "all")
        .create()
        .expect("Producer creation error");
    let connector_config = literal!({
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
                "transactional": {
                   "commit_interval": 0 // trigger direct commits
                }
            },
            "test_options": {
                "auto.offset.reset": "beginning"
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

    // send message 1
    let record1 = FutureRecord::to(topic)
        .key("1")
        .payload("1")
        .partition(0)
        .timestamp(1);
    if producer.send(record1, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }
    // send message 2
    let record2 = FutureRecord::to(topic)
        .key("2")
        .payload("2")
        .partition(0)
        .timestamp(2);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }

    // send message 3
    let record3 = FutureRecord::to(topic)
        .key("3")
        .payload("3")
        .partition(0)
        .timestamp(3);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Unable to send record to Kafka".into());
    }

    // receive events
    let event1 = out.get_event().await?;
    assert_eq!(&Value::from(1), event1.data.suffix().value());
    let event2 = out.get_event().await?;
    assert_eq!(&Value::from(2), event2.data.suffix().value());
    let event3 = out.get_event().await?;
    assert_eq!(&Value::from(3), event3.data.suffix().value());

    // ack message 3
    harness
        .send_contraflow(CbAction::Ack, event3.id.clone())
        .await?;
    // ack message 1
    harness
        .send_contraflow(CbAction::Ack, event1.id.clone())
        .await?;

    // stop harness
    let (out_events, err_events) = harness.stop().await?;
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);

    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(3))
    );

    // no offset reset, pick up where we left off
    let connector_config = literal!({
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
                "transactional": {
                   "commit_interval": 0 // trigger direct commits
                }
            }
        }
    });
    // start new harness
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;

    // ensure no message is received
    assert!(out
        .get_event()
        .timeout(Duration::from_secs(1))
        .await
        .is_err());

    // fail message 2
    harness
        .send_contraflow(CbAction::Fail, event2.id.clone())
        .await?;

    // ensure message 2 is the next - seek was effective
    let event2_again = out.get_event().await?;
    assert_eq!(&Value::from(2), event2_again.data.suffix().value());

    // stop harness
    let _ = harness.stop().await?;

    // check that fail did reset the offset correctly - commit was effective
    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(1))
    );

    // no offset reset, pick up where we left off
    // add debug logging
    let connector_config = literal!({
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
                "transactional": {
                   "commit_interval": 0 // trigger direct commits
                }
            },
            "test_options": {
                "auto.offset.reset": "latest"
            }
        }
    });

    // start new harness
    let harness = ConnectorHarness::new(
        function_name!(),
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;

    // ensure message 2 and 3 are received
    // for some strange reason rdkafka might consumer event2 twice
    let mut event_again = out.get_event().await?;
    let mut data = event_again.data.suffix().value();
    while data == &Value::from(2) {
        event_again = out.get_event().await?;
        data = event_again.data.suffix().value();
    }
    assert_eq!(&Value::from(3), data);

    // ack message 3 only
    harness
        .send_contraflow(CbAction::Ack, event_again.id.clone())
        .await?;

    // expected order of things: event5, event6, ack6, fail5, event5, event6
    let record5 = FutureRecord::to(topic)
        .key("5")
        .payload("5")
        .partition(0)
        .timestamp(5678);
    if producer.send(record5, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }
    let record6 = FutureRecord::to(topic)
        .key("6")
        .payload("6")
        .partition(0)
        .timestamp(5679);
    if producer.send(record6, PRODUCE_TIMEOUT).await.is_err() {
        return Err("Could not send to kafka".into());
    }

    // discard old repeated events (We don't know why they come again)
    // rdkafka consumer seems to fetch them again when its internal state got updated
    // or something
    let mut event5 = dbg!(out.get_event().await?);
    while event5.data.suffix().value() != &Value::from(5) {
        event5 = dbg!(out.get_event().await?);
    }

    assert_eq!(&Value::from(5), event5.data.suffix().value());
    let event6 = dbg!(out.get_event().await?);
    assert_eq!(&Value::from(6), event6.data.suffix().value());

    harness
        .send_contraflow(CbAction::Ack, event6.id.clone())
        .await?;

    harness
        .send_contraflow(CbAction::Fail, event5.id.clone())
        .await?;

    let event5_again = out.get_event().await?;
    assert_eq!(&Value::from(5), event5_again.data.suffix().value());
    let event6_again = out.get_event().await?;
    assert_eq!(&Value::from(6), event6_again.data.suffix().value());

    // stop harness
    let _ = harness.stop().await?;

    // offset should be back to where it was before
    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(3))
    );

    drop(container);
    Ok(())
}

async fn create_topic(
    broker: impl Into<String>,
    topic: &str,
    partitions: i32,
    repl: TopicReplication<'_>,
) -> Result<()> {
    let mut admin_config = ClientConfig::new();
    admin_config
        .set("client.id", "test-admin")
        .set("bootstrap.servers", broker.into());
    let admin_client = AdminClient::from_config(&admin_config)?;
    let options = AdminOptions::default();
    let res = admin_client
        .create_topics(vec![&NewTopic::new(topic, partitions, repl)], &options)
        .await?;
    for r in res {
        match r {
            Err((topic, err)) => {
                error!("Error creating topic {}: {}", &topic, err);
                return Err(err.to_string().into());
            }
            Ok(topic) => {
                info!("Created topic {}", topic);
            }
        }
    }
    Ok(())
}

async fn get_offsets(
    broker: &str,
    group_id: &str,
    topic: &str,
) -> KafkaResult<HashMap<(String, i32), Offset>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("group.id", group_id)
        .create()
        .expect("Error creating consumer");
    consumer.subscribe(&[topic])?;
    let mut assignment = consumer.assignment()?;
    while assignment.count() == 0 {
        task::sleep(Duration::from_millis(100)).await;
        consumer.poll(Duration::ZERO);
        assignment = consumer.assignment()?;
    }
    drop(assignment);

    let offsets = consumer.committed(Duration::from_secs(5))?.to_topic_map();
    drop(consumer);
    Ok(offsets)
}
