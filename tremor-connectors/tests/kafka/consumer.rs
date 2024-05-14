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

use anyhow::bail;
use beef::Cow;
use log::{debug, error, info};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::KafkaResult,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Offset,
};
use serial_test::serial;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use tremor_connectors::{harness::Harness, impls::kafka};
use tremor_connectors_test_helpers::free_port;
use tremor_system::controlplane::CbAction;
use tremor_value::{literal, Value};
use value_trait::prelude::*;

use crate::{redpanda_container, PRODUCE_TIMEOUT};

#[tokio::test(flavor = "multi_thread")]
// #[serial(kafka)]
async fn transactional_retry() -> anyhow::Result<()> {
    let container = redpanda_container().await?;

    let port = container.get_host_port_ipv4(9092).await;
    let broker = format!("127.0.0.1:{port}");
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
        "codec": {"name": "json", "config": {"mode": "sorted"}},
        "config": {
            "brokers": [
                broker.clone()
            ],
            "group_id": group_id,
            "topics": [
                topic
            ],
            "mode": {
                // transctional
                "transactional": {}
            },
            "test_options": {
                "auto.offset.reset": "beginning"
            }
        }
    });
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;

    harness.start().await?;
    harness.wait_for_connected().await?;

    let record = FutureRecord::to(topic)
        .payload("{\"snot\":\"badger\"}\n")
        .key("foo")
        .partition(1)
        .timestamp(42)
        .headers(OwnedHeaders::new().insert(Header {
            key: "header",
            value: Some("snot"),
        }));
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Unable to send record to kafka");
    }

    let e1 = harness.out()?.get_event().await?;
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
                "timestamp": 42_000_000
            }
        }),
        e1.data.suffix().meta()
    );

    // ack event
    harness.send_contraflow(CbAction::Ack, e1.id.clone())?;

    // second event
    let record2 = FutureRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }
    let e2 = harness.out()?.get_event().await?;
    assert_eq!(Value::null(), e2.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12_000_000
            }
        }),
        e2.data.suffix().meta()
    );
    // fail -> ensure it is replayed
    harness.send_contraflow(CbAction::Fail, e2.id.clone())?;

    // we get the same event again
    let e3 = harness.out()?.get_event().await?;
    assert_eq!(Value::null(), e3.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12_000_000
            }
        }),
        e3.data.suffix().meta()
    );
    assert_eq!(e2.id.pull_id(), e3.id.pull_id());
    assert_eq!(e2.id.stream_id(), e3.id.stream_id());

    // ack the event
    harness.send_contraflow(CbAction::Ack, e3.id.clone())?;

    // send another event and check that the previous one isn't replayed
    let record3 = FutureRecord::to(topic)
        .key("trigger")
        .payload("false\n")
        .partition(2)
        .timestamp(123);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // next event
    let e4 = harness.out()?.get_event().await?;
    assert_eq!(Value::from(false), e4.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("trigger".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 2,
                "timestamp": 123_000_000
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
        bail!("Could not send to kafka");
    }

    let e5 = harness.err()?.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError(TapeError) at character 0 ('}')",
            "source": "harness::test",
            "stream_id": 8_589_934_592_u64,
            "pull_id": 1u64
        }),
        e5.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError(TapeError) at character 0 ('}')",
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("failure".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "partition": 2,
                "offset": 1,
                "timestamp": 1_234_000_000
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

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]

async fn custom_no_retry() -> anyhow::Result<()> {
    let container = redpanda_container().await?;

    let port = container.get_host_port_ipv4(9092).await;
    let broker = format!("127.0.0.1:{port}");
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
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
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
        .headers(OwnedHeaders::new().insert(Header {
            key: "header",
            value: Some("snot"),
        }));
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Unable to send record to kafka");
    }

    let e1 = harness.out()?.get_event().await?;
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
                "timestamp": 42_000_000
            }
        }),
        e1.data.suffix().meta()
    );

    // ack event
    harness.send_contraflow(CbAction::Ack, e1.id.clone())?;

    // produce second event
    let record2 = FutureRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // get second event
    let e2 = harness.out()?.get_event().await?;
    assert_eq!(Value::null(), e2.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12_000_000
            }
        }),
        e2.data.suffix().meta()
    );
    // fail -> ensure it is not replayed
    harness.send_contraflow(CbAction::Fail, e2.id.clone())?;

    // we don't get no event
    assert!(harness
        .out()?
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
        bail!("Could not send to kafka");
    }

    // we only get the next event, no previous one
    let e3 = harness.out()?.get_event().await?;
    assert_eq!(Value::from(false), e3.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("trigger".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 2,
                "timestamp": 123_000_000
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
        bail!("Could not send to kafka");
    }

    let e5 = harness.err()?.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError(TapeError) at character 0 ('}')",
            "source": "harness::test",
            "stream_id": 8_589_934_592_u64,
            "pull_id": 1u64
        }),
        e5.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError(TapeError) at character 0 ('}')",
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("failure".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "partition": 2,
                "offset": 1,
                "timestamp": 1_234_000_000
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

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]

async fn performance() -> anyhow::Result<()> {
    let container = redpanda_container().await?;
    let port = container.get_host_port_ipv4(9092).await;

    let broker = format!("127.0.0.1:{port}");
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
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
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
        .headers(OwnedHeaders::new().insert(Header {
            key: "header",
            value: Some("snot"),
        }));
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    let e1 = harness.out()?.get_event().await?;
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
                "timestamp": 42_000_000
            }
        }),
        e1.data.suffix().meta()
    );

    // ack event
    harness.send_contraflow(CbAction::Ack, e1.id.clone())?;

    // produce second event
    let record2 = FutureRecord::to(topic)
        .key("snot")
        .payload("null\n")
        .partition(0)
        .timestamp(12);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // get second event
    let e2 = harness.out()?.get_event().await?;
    assert_eq!(Value::null(), e2.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("snot".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 0,
                "timestamp": 12_000_000
            }
        }),
        e2.data.suffix().meta()
    );

    // fail -> ensure it is not replayed
    harness.send_contraflow(CbAction::Fail, e2.id.clone())?;

    // we don't get no event
    assert!(harness
        .out()?
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
        bail!("Could not send to kafka");
    }

    // we only get the next event, no previous one
    let e3 = harness.out()?.get_event().await?;
    assert_eq!(Value::from(false), e3.data.suffix().value());
    assert_eq!(
        &literal!({
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("trigger".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "offset": 0,
                "partition": 2,
                "timestamp": 123_000_000
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
        bail!("Could not send to kafka");
    }

    let e5 = harness.err()?.get_event().await?;
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError(TapeError) at character 0 ('}')",
            "source": "harness::test",
            "stream_id": 8_589_934_592_u64,
            "pull_id": 1u64
        }),
        e5.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "error": "SIMD JSON error: InternalError(TapeError) at character 0 ('}')",
            "kafka_consumer": {
                "key": Value::Bytes(Cow::owned("failure".as_bytes().to_vec())),
                "headers": null,
                "topic": topic,
                "partition": 2,
                "offset": 1,
                "timestamp": 1_234_000_000
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

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]
async fn connector_kafka_consumer_unreachable() -> anyhow::Result<()> {
    let kafka_port = free_port::find_free_tcp_port().await?;
    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 100_u64,
                "max_retries": 5_u64
            }
        },
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    let harness = Harness::new(
        "test",
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

#[tokio::test(flavor = "multi_thread")]
async fn invalid_rdkafka_options() -> anyhow::Result<()> {
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
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    assert!(Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await
    .is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]
async fn connector_kafka_consumer_pause_resume() -> anyhow::Result<()> {
    let container = redpanda_container().await?;

    let port = container.get_host_port_ipv4(9092).await;

    let broker = format!("127.0.0.1:{port}");
    let topic = "tremor_test_pause_resume";
    let group_id = "group_pause_resume";

    create_topic(&broker, topic, 3, TopicReplication::Fixed(1)).await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");
    let connector_config = literal!({
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    let record = FutureRecord::to(topic)
        .key("badger")
        .payload("{\"snot\": true}")
        .partition(1)
        .timestamp(0);
    if producer.send(record, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }
    debug!("BEFORE GET EVENT 1");
    let e1 = harness.out()?.get_event().await?;
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
        bail!("Could not send to kafka");
    }
    // we didn't receive shit because we are paused
    assert!(harness
        .out()?
        .expect_no_event_for(Duration::from_millis(200))
        .await
        .is_ok());

    harness.resume().await?;
    log::debug!("BEFORE GET EVENT 2");
    let e2 = harness.out()?.get_event().await?;
    debug!("AFTER GET EVENT 2");
    assert_eq!(Value::from("R.I.P."), e2.data.suffix().value());

    let (out_events, err_events) = harness.stop().await?;
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);

    // cleanup
    drop(container);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]
async fn transactional_store_offset_handling() -> anyhow::Result<()> {
    let container = redpanda_container().await?;

    let port = container.get_host_port_ipv4(9092).await;

    let broker = format!("127.0.0.1:{port}");
    let topic = "tremor_test_store_offsets";
    let group_id = "group_transactional_store_offsets";

    create_topic(&broker, topic, 1, TopicReplication::Fixed(1)).await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .create()
        .expect("Producer creation error");
    let commit_interval: u64 = Duration::from_millis(100).as_nanos().try_into()?;
    let connector_config = literal!({
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    // send message 1
    let record1 = FutureRecord::to(topic)
        .key("1")
        .payload("1")
        .partition(0)
        .timestamp(1);
    if producer.send(record1, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }
    // send message 2
    let record2 = FutureRecord::to(topic)
        .key("2")
        .payload("2")
        .partition(0)
        .timestamp(2);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // send message 3
    let record3 = FutureRecord::to(topic)
        .key("3")
        .payload("3")
        .partition(0)
        .timestamp(3);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // receive events
    let event1 = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(1), event1.data.suffix().value());
    let event2 = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(2), event2.data.suffix().value());
    let event3 = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(3), event3.data.suffix().value());

    // ack message 3
    harness.send_contraflow(CbAction::Ack, event3.id.clone())?;
    // ack message 1
    harness.send_contraflow(CbAction::Ack, event1.id.clone())?;

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
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
            }        }
    });

    // start new harness
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    debug!("connected");

    // give the background librdkafka time to start fetching the partitions before we fail
    // otherwise it will fail silently in the background
    // consider this a cry for help!
    tokio::time::sleep(Duration::from_millis(200)).await;
    // fail message 2
    harness.send_contraflow(CbAction::Fail, event2.id.clone())?;

    debug!("failed event2");

    // ensure message 2 and 3 are received
    // for some strange reason rdkafka might consumer event2 twice
    let mut event_again = harness.out()?.get_event().await?;
    let mut data = event_again.data.suffix().value();
    while data == &Value::from(2) {
        event_again = harness.out()?.get_event().await?;
        data = event_again.data.suffix().value();
    }
    assert_eq!(&Value::from(3), data);

    // stop harness
    harness.stop().await?;

    // check that fail did reset the offset correctly
    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(1))
    );
    drop(offsets);

    // start new harness
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    // ensure message 2 and 3 are received - yet again
    // for some strange reason rdkafka might consumer event2 twice
    let mut event_again = harness.out()?.get_event().await?;
    let mut data = event_again.data.suffix().value();
    while data == &Value::from(2) {
        event_again = harness.out()?.get_event().await?;
        data = event_again.data.suffix().value();
    }
    assert_eq!(&Value::from(3), data);

    // ack message 3 only
    harness.send_contraflow(CbAction::Ack, event_again.id.clone())?;

    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(3))
    );

    // stop harness
    harness.stop().await?;

    drop(container);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial(kafka)]
async fn transactional_commit_offset_handling() -> anyhow::Result<()> {
    let container = redpanda_container().await?;

    let port = container.get_host_port_ipv4(9092).await;
    let broker = format!("127.0.0.1:{port}");
    let topic = "tremor_test_commit_offset";
    let group_id = "group_commit_offset";

    create_topic(&broker, topic, 1, TopicReplication::Fixed(1)).await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("acks", "all")
        .create()
        .expect("Producer creation error");
    let connector_config = literal!({
        "codec": {"name": "json", "config": {"mode": "sorted"}},
        "config": {
            "brokers": [
                broker.clone()
            ],
            "group_id": group_id,
            "topics": [
                topic
            ],
            "mode": {
                // transctransactionaltional
                "transactional": {}
            },
            "test_options": {
                "auto.offset.reset": "beginning"
            }
        }
    });
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    // send message 1
    let record1 = FutureRecord::to(topic)
        .key("1")
        .payload("1")
        .partition(0)
        .timestamp(1);
    if producer.send(record1, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }
    // send message 2
    let record2 = FutureRecord::to(topic)
        .key("2")
        .payload("2")
        .partition(0)
        .timestamp(2);
    if producer.send(record2, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // send message 3
    let record3 = FutureRecord::to(topic)
        .key("3")
        .payload("3")
        .partition(0)
        .timestamp(3);
    if producer.send(record3, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // receive events
    let event1 = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(1), event1.data.suffix().value());
    let event2 = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(2), event2.data.suffix().value());
    let event3 = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(3), event3.data.suffix().value());

    // ack message 3
    harness.send_contraflow(CbAction::Ack, event3.id.clone())?;
    // ack message 1
    harness.send_contraflow(CbAction::Ack, event1.id.clone())?;

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
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    // start new harness
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    // ensure no message is received
    assert!(timeout(Duration::from_secs(1), harness.out()?.get_event())
        .await
        .is_err());

    // fail message 2
    harness.send_contraflow(CbAction::Fail, event2.id.clone())?;

    // ensure message 2 is the next - seek was effective
    let event2_again = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(2), event2_again.data.suffix().value());

    // stop harness
    harness.stop().await?;

    // check that fail did reset the offset correctly - commit was effective
    let offsets = get_offsets(broker.as_str(), group_id, topic).await?;
    assert_eq!(
        offsets.get(&(topic.to_string(), 0)),
        Some(&Offset::Offset(1))
    );

    // no offset reset, pick up where we left off
    // add debug logging
    let connector_config = literal!({
        "codec": {"name": "json", "config": {"mode": "sorted"}},
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
    let mut harness = Harness::new(
        "test",
        &kafka::consumer::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    // ensure message 2 and 3 are received
    // for some strange reason rdkafka might consumer event2 twice
    let mut event_again = harness.out()?.get_event().await?;
    let mut data = event_again.data.suffix().value();
    while data == &Value::from(2) {
        event_again = harness.out()?.get_event().await?;
        data = event_again.data.suffix().value();
    }
    assert_eq!(&Value::from(3), data);

    // ack message 3 only
    harness.send_contraflow(CbAction::Ack, event_again.id.clone())?;

    // expected order of things: event5, event6, ack6, fail5, event5, event6
    let record5 = FutureRecord::to(topic)
        .key("5")
        .payload("5")
        .partition(0)
        .timestamp(5678);
    if producer.send(record5, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }
    let record6 = FutureRecord::to(topic)
        .key("6")
        .payload("6")
        .partition(0)
        .timestamp(5679);
    if producer.send(record6, PRODUCE_TIMEOUT).await.is_err() {
        bail!("Could not send to kafka");
    }

    // discard old repeated events (We don't know why they come again)
    // rdkafka consumer seems to fetch them again when its internal state got updated
    // or something
    let mut event5 = harness.out()?.get_event().await?;
    while event5.data.suffix().value() != &Value::from(5) {
        event5 = harness.out()?.get_event().await?;
    }

    assert_eq!(&Value::from(5), event5.data.suffix().value());
    let event6 = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(6), event6.data.suffix().value());

    harness.send_contraflow(CbAction::Ack, event6.id.clone())?;

    harness.send_contraflow(CbAction::Fail, event5.id.clone())?;

    let event5_again = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(5), event5_again.data.suffix().value());
    let event6_again = harness.out()?.get_event().await?;
    assert_eq!(&Value::from(6), event6_again.data.suffix().value());

    // stop harness
    harness.stop().await?;

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
) -> anyhow::Result<()> {
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
                return Err(err.into());
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
        tokio::time::sleep(Duration::from_millis(100)).await;
        consumer.poll(Duration::ZERO);
        assignment = consumer.assignment()?;
    }
    drop(assignment);

    let offsets = consumer.committed(Duration::from_secs(5))?.to_topic_map();
    drop(consumer);
    Ok(offsets)
}
