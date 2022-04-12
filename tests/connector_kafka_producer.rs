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
    use tremor_common::url::ports::IN;

    use super::connectors::ConnectorHarness;
    use futures::StreamExt;
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        config::FromClientConfig,
        consumer::{BaseConsumer, Consumer},
        message::Headers,
        ClientConfig, Message,
    };
    use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
    use signal_hook_async_std::Signals;
    use testcontainers::{
        clients::Cli as DockerCli,
        images::generic::{GenericImage, Stream as WaitForStream, WaitFor},
        Docker, Image, RunArgs,
    };
    use tremor_pipeline::{CbAction, EventId};
    use tremor_runtime::{errors::Result, Event};
    use tremor_value::literal;

    const IMAGE: &'static str = "docker.vectorized.io/vectorized/redpanda";
    const VERSION: &'static str = "latest"; //FIXME: pin version

    #[async_std::test]
    #[serial(kafka)]
    async fn connector_kafka_producer() -> Result<()> {
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
            "postprocessors": [
                "lines"
            ],
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
        let harness = ConnectorHarness::new("kafka_producer", connector_config).await?;
        let in_pipe = harness.get_pipe(IN).expect("No pipe connected to port IN");
        harness.start().await?;

        // CB Open is sent upon being connected
        let cf_event = in_pipe.get_contraflow().await?;
        assert_eq!(CbAction::Open, cf_event.cb);

        let consumer: BaseConsumer<_> = ClientConfig::new()
            .set("bootstrap.servers", &broker)
            .set("group.id", "group")
            .set("client.id", "client")
            .set("socket.timeout.ms", "2000")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "100")
            .set("enable.auto.offset.store", "true")
            .create()
            .expect("Consumer creation error");
        consumer.subscribe(&[topic]).unwrap();

        // TODO: it seems to work reliably which hints at a timeout inside redpanda
        // TODO: verify
        task::sleep(Duration::from_secs(5)).await;

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
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                assert_eq!(msg.key(), Some("snot".as_bytes()));
                assert_eq!(msg.payload(), Some("{\"snot\":\"badger\"}\n".as_bytes()));
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
            None => {
                assert!(false, "No message received from kafka.");
            }
        };
        assert!(in_pipe.get_contraflow_events()?.is_empty());

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
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                assert_eq!(Some("badger".as_bytes()), msg.key());
                assert_eq!(Some("[1,2,3]\n".as_bytes()), msg.payload());
                assert_eq!(0_i32, msg.partition());
                assert_eq!(Some(123), msg.timestamp().to_millis());
                let headers = msg.headers().unwrap();
                assert_eq!(1, headers.count());
                assert_eq!(Some(("foo", "baz".as_bytes())), headers.get(0));
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
            None => {
                assert!(false, "No message received from kafka.");
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
        let mut batchman_msg = consumer.poll(Duration::from_secs(2)).unwrap().unwrap();
        let mut snot_msg = consumer.poll(Duration::from_secs(2)).unwrap().unwrap();
        if batchman_msg.key().eq(&Some("snot".as_bytes())) {
            core::mem::swap(&mut snot_msg, &mut batchman_msg);
        }
        assert_eq!(
            Some("nananananana: batchman!".as_bytes()),
            batchman_msg.key()
        );
        assert_eq!(
            Some("{\"field1\":0.1,\"field3\":[]}\n".as_bytes()),
            batchman_msg.payload()
        );
        assert!(batchman_msg.headers().is_none());
        assert_eq!(Some("snot".as_bytes()), snot_msg.key());
        assert_eq!(
            Some("{\"field2\":\"just a string\"}\n".as_bytes()),
            snot_msg.payload()
        );
        assert!(snot_msg.headers().is_none());

        // shutdown
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
