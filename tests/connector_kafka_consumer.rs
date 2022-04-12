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

mod connectors;

#[cfg(feature = "integration")]
#[macro_use]
extern crate log;

#[cfg(feature = "integration")]
mod test {

    use async_std::task;
    use serial_test::serial;
    use std::time::Duration;

    use super::connectors::ConnectorHarness;
    use beef::Cow;
    use futures::StreamExt;
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        config::FromClientConfig,
        message::OwnedHeaders,
        producer::{BaseProducer, BaseRecord, Producer},
        ClientConfig,
    };
    use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
    use signal_hook_async_std::Signals;
    use testcontainers::{
        clients::Cli as DockerCli,
        images::generic::{GenericImage, Stream as WaitForStream, WaitFor},
        Docker, Image, RunArgs,
    };
    use tremor_pipeline::CbAction;
    use tremor_runtime::errors::Result;
    use tremor_value::{literal, Value};
    use value_trait::Builder;

    const IMAGE: &'static str = "docker.vectorized.io/vectorized/redpanda";
    const VERSION: &'static str = "latest"; //FIXME: pin version

    #[async_std::test]
    #[serial(kafka)]
    async fn connector_kafka_consumer_transactional_retry() -> Result<()> {
        let _ = env_logger::try_init();
        let docker = DockerCli::default();
        let args = vec![
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp",
            "1",
            "--memory",
            "1G",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            "--kafka-addr=0.0.0.0:9092",
            "--advertise-kafka-addr=127.0.0.1:9092",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect();
        let image = GenericImage::new(format!("{}:{}", IMAGE, VERSION))
            .with_args(args)
            .with_wait_for(WaitFor::LogMessage {
                message: "Successfully started Redpanda!".to_string(),
                stream: WaitForStream::StdErr,
            });
        let container = docker.run_with_args(
            image,
            RunArgs::default().with_mapped_port((9092_u16, 9092_u16)),
        );

        let container_id = container.id().to_string();
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let signal_handle = signals.handle();
        let signal_handler_task = async_std::task::spawn(async move {
            let signal_docker = DockerCli::default();
            while let Some(signal) = signals.next().await {
                signal_docker.stop(container_id.as_str());
                signal_docker.rm(container_id.as_str());
                let _ = signal_hook::low_level::emulate_default_handler(signal);
            }
        });

        let port = container.get_host_port(9092).unwrap_or(9092);
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
                "custom": {
                    "interval_ms": 1000_u64,
                    "max_retries": 10_u64
                }
            },
            "codec": "json-sorted",
            "preprocessors": [
                "lines"
            ],
            "config": {
                "brokers": [
                    broker.clone()
                ],
                "group_id": group_id,
                "topics": [
                    topic
                ],
                "retry_failed_events": true,
                "rdkafka_options": {
                    "enable.auto.commit": "false"
                //    "debug": "all"
                }
            }
        });
        let harness = ConnectorHarness::new("kafka_consumer", connector_config).await?;
        let out = harness.out().expect("No pipe connected to port OUT");
        let err = harness.err().expect("No pipe connected to port ERR");
        harness.start().await?;
        harness.wait_for_connected(Duration::from_secs(10)).await?;

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
                "source": "test",
                "stream_id": 8589934592_u64,
                "pull_id": 1
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
        signal_handle.close();
        signal_handler_task.cancel().await;
        drop(container);
        Ok(())
    }

    #[async_std::test]
    #[serial(kafka)]
    async fn connector_kafka_consumer_transactional_no_retry() -> Result<()> {
        let _ = env_logger::try_init();
        let docker = DockerCli::default();
        let args = vec![
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp",
            "1",
            "--memory",
            "1G",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            "--kafka-addr=0.0.0.0:9092",
            "--advertise-kafka-addr=127.0.0.1:9092",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect();
        let image = GenericImage::new(format!("{}:{}", IMAGE, VERSION))
            .with_args(args)
            .with_wait_for(WaitFor::LogMessage {
                message: "Successfully started Redpanda!".to_string(),
                stream: WaitForStream::StdErr,
            });
        let container = docker.run_with_args(
            image,
            RunArgs::default().with_mapped_port((9092_u16, 9092_u16)),
        );

        let container_id = container.id().to_string();
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let signal_handle = signals.handle();
        let signal_handler_task = async_std::task::spawn(async move {
            let signal_docker = DockerCli::default();
            while let Some(_signal) = signals.next().await {
                signal_docker.stop(container_id.as_str());
                signal_docker.rm(container_id.as_str());
            }
        });

        let port = container.get_host_port(9092).unwrap_or(9092);
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
                "custom": {
                    "interval_ms": 1000_u64,
                    "max_retries": 10_u64
                }
            },
            "codec": "json-sorted",
            "preprocessors": [
                "lines"
            ],
            "config": {
                "brokers": [
                    broker.clone()
                ],
                "group_id": "test1",
                "topics": [
                    topic
                ],
                "rdkafka_options": {
                    "enable.auto.commit": "false"
                //    "debug": "all"
                }
            }
        });
        let harness = ConnectorHarness::new("kafka_consumer", connector_config).await?;
        let out = harness.out().expect("No pipe connected to port OUT");
        let err = harness.err().expect("No pipe connected to port ERR");
        harness.start().await?;
        harness.wait_for_connected(Duration::from_secs(10)).await?;

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
        assert!(out.get_event().await.is_err());

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
                "source": "test",
                "stream_id": 8589934592_u64,
                "pull_id": 1
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
        signal_handle.close();
        signal_handler_task.cancel().await;
        drop(container);
        Ok(())
    }

    #[async_std::test]
    #[serial(kafka)]
    async fn connector_kafka_consumer_non_transactional() -> Result<()> {
        let _ = env_logger::try_init();
        let docker = DockerCli::default();
        let args = vec![
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp",
            "1",
            "--memory",
            "1G",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            "--kafka-addr=0.0.0.0:9092",
            "--advertise-kafka-addr=127.0.0.1:9092",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect();
        let image = GenericImage::new(format!("{}:{}", IMAGE, VERSION))
            .with_args(args)
            .with_wait_for(WaitFor::LogMessage {
                message: "Successfully started Redpanda!".to_string(),
                stream: WaitForStream::StdErr,
            });
        let container = docker.run_with_args(
            image,
            RunArgs::default().with_mapped_port((9092_u16, 9092_u16)),
        );

        let container_id = container.id().to_string();
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let signal_handle = signals.handle();
        let signal_handler_task = async_std::task::spawn(async move {
            let signal_docker = DockerCli::default();
            while let Some(_signal) = signals.next().await {
                signal_docker.stop(container_id.as_str());
                signal_docker.rm(container_id.as_str());
            }
        });

        let port = container.get_host_port(9092).unwrap_or(9092);
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
                "custom": {
                    "interval_ms": 1000_u64,
                    "max_retries": 10_u64
                }
            },
            "codec": "json-sorted",
            "preprocessors": [
                "lines"
            ],
            "config": {
                "brokers": [
                    broker.clone()
                ],
                "group_id": group_id,
                "topics": [
                    topic
                ],
                "rdkafka_options": {
                //    "debug": "all"
                }
            }
        });
        let harness = ConnectorHarness::new("kafka_consumer", connector_config).await?;
        let out = harness.out().expect("No pipe connected to port OUT");
        let err = harness.err().expect("No pipe connected to port ERR");
        harness.start().await?;
        harness.wait_for_connected(Duration::from_secs(10)).await?;

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
        assert!(out.get_event().await.is_err());

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
                "source": "test",
                "stream_id": 8589934592_u64,
                "pull_id": 1
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
        signal_handle.close();
        signal_handler_task.cancel().await;
        drop(container);
        Ok(())
    }

    #[async_std::test]
    #[serial(kafka)]
    async fn connector_kafka_consumer_unreachable() -> Result<()> {
        let _ = env_logger::try_init();
        let connector_config = literal!({
            "reconnect": {
                "custom": {
                    "interval_ms": 100_u64,
                    "max_retries": 5_u64
                }
            },
            "codec": "json-sorted",
            "preprocessors": [
                "lines"
            ],
            "config": {
                "brokers": [
                    "127.0.0.1:9092"
                ],
                "group_id": "all_brokers_down",
                "topics": [
                    "snot"
                ],
                "rdkafka_options": {
                //    "debug": "all"
                }
            }
        });
        let harness = ConnectorHarness::new("kafka_consumer", connector_config).await?;
        assert!(harness.start().await.is_err());

        let (out_events, err_events) = harness.stop().await?;
        assert!(out_events.is_empty());
        assert!(err_events.is_empty());
        Ok(())
    }

    #[async_std::test]
    #[serial(kafka)]
    async fn connector_kafka_consumer_unresolvable() -> Result<()> {
        let _ = env_logger::try_init();
        let connector_config = literal!({
            "codec": "json-sorted",
            "preprocessors": [
                "lines"
            ],
            "config": {
                "brokers": [
                    "snot:9092"
                ],
                "group_id": "cannot_resolve",
                "topics": [
                    "snot"
                ],
                "rdkafka_options": {
                //    "debug": "all"
                }
            }
        });
        let harness = ConnectorHarness::new("kafka_consumer", connector_config).await?;
        assert!(harness.start().await.is_err());

        let (out_events, err_events) = harness.stop().await?;
        assert!(out_events.is_empty());
        assert!(err_events.is_empty());
        Ok(())
    }

    #[async_std::test]
    #[serial(kafka)]
    async fn connector_kafka_consumer_pause_resume() -> Result<()> {
        let _ = env_logger::try_init();

        let docker = DockerCli::default();
        let args = vec![
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp",
            "1",
            "--memory",
            "1G",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            "--kafka-addr=0.0.0.0:9092",
            "--advertise-kafka-addr=127.0.0.1:9092",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect();
        let image = GenericImage::new(format!("{}:{}", IMAGE, VERSION))
            .with_args(args)
            .with_wait_for(WaitFor::LogMessage {
                message: "Successfully started Redpanda!".to_string(),
                stream: WaitForStream::StdErr,
            });
        let container = docker.run_with_args(
            image,
            RunArgs::default().with_mapped_port((9092_u16, 9092_u16)),
        );

        let container_id = container.id().to_string();
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let signal_handle = signals.handle();
        let signal_handler_task = async_std::task::spawn(async move {
            let signal_docker = DockerCli::default();
            while let Some(_signal) = signals.next().await {
                signal_docker.stop(container_id.as_str());
                signal_docker.rm(container_id.as_str());
            }
        });

        let port = container.get_host_port(9092).unwrap_or(9092);
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
                "rdkafka_options": {
                //    "debug": "all"
                }
            }
        });
        let harness = ConnectorHarness::new("kafka_consumer", connector_config).await?;
        let out = harness.out().expect("No pipe connected to port OUT");
        harness.start().await?;
        harness.wait_for_connected(Duration::from_secs(10)).await?;

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
        let e1 = out.get_event().await?;
        assert_eq!(
            literal!({
                "snot": true
            }),
            e1.data.suffix().value()
        );

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
        assert!(out.get_event().await.is_err());

        harness.resume().await?;
        let e2 = out.get_event().await?;
        assert_eq!(Value::from("R.I.P."), e2.data.suffix().value());

        let (out_events, err_events) = harness.stop().await?;
        assert!(out_events.is_empty());
        assert!(err_events.is_empty());

        // cleanup
        signal_handle.close();
        signal_handler_task.cancel().await;
        drop(container);
        Ok(())
    }
}
