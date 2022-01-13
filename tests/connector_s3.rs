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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or imelied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod connectors;

#[cfg(feature = "integration")]
extern crate log;
#[cfg(feature = "integration")]
mod test {
    // use async_std::stream::StreamExt;
    use futures::StreamExt;
    use rand::{distributions::Alphanumeric, Rng};
    use std::env;
    use std::io::Read;
    use std::time::{Duration, Instant};
    // use bytes::buf::buf_impl::Buf;
    use bytes::Buf;

    use connectors::{ConnectorHarness, TestPipeline};
    use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
    use signal_hook_async_std::Signals;
    use testcontainers::clients;
    use testcontainers::images::generic::GenericImage;
    use testcontainers::{Docker, RunArgs};

    use tremor_common::url::ports::IN;
    use tremor_pipeline::{CbAction, Event, EventId};
    use tremor_runtime::errors::{Error, Result};
    use tremor_value::{literal, value};
    use value_trait::{Builder, Mutable, ValueAccess};

    use aws_sdk_s3 as s3;
    use s3::client::Client as S3Client;
    use s3::{Credentials, Endpoint, Region};

    #[async_std::test]
    async fn connector_s3() -> Result<()> {
        let _ = env_logger::try_init();

        // Run the mock s3 locally
        let docker = clients::Cli::default();
        let image = GenericImage::new("adobe/s3mock").with_env_var("initialBuckets", "tremor");

        let container = docker.run_with_args(
            image,
            RunArgs::default()
                .with_mapped_port((9090_u16, 9090_u16))
                .with_mapped_port((9191_u16, 9191_u16)),
        );
        // signal handling - stop and rm the container, even if we quit the test in the middle of everything
        let container_id = container.id().to_string();
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        let _signal_handle = signals.handle();
        let _signal_handler_task = async_std::task::spawn(async move {
            let signal_docker = clients::Cli::default();
            while let Some(_signal) = signals.next().await {
                signal_docker.stop(container_id.as_str());
                signal_docker.rm(container_id.as_str());
            }
        });

        let s3_client: S3Client = get_client();

        let wait_for = Duration::from_secs(30);
        let start = Instant::now();

        while let Err(e) = s3_client
            .head_bucket()
            .bucket("tremor".to_string())
            .send()
            .await
        {
            if start.elapsed() > wait_for {
                return Err(Error::from(e).chain_err(|| "Waiting for mock-s3 container timed out"));
            }

            async_std::task::sleep(Duration::from_secs(1)).await;
        }

        // set the needed environment variables. keys are not required for mock-s3
        env::set_var("AWS_ACCESS_KEY_ID", "KEY_NOT_REQD");
        env::set_var("AWS_SECRET_ACCESS_KEY", "KEY_NOT_REQD");
        env::set_var("AWS_REGION", "ap-south-1");

        // connector setup
        let connector_yaml = literal!(
                {
        "codec": "binary",
        "config":{
            "aws_access_token": "AWS_ACCESS_KEY_ID",
            "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
            "bucket": "tremor",
            "endpoint": "http://localhost:9090",
        }
        });

        let harness = ConnectorHarness::new("s3", connector_yaml).await?;
        let in_pipe = harness
            .get_pipe(IN)
            .expect("No pipe connectored to port IN");
        harness.start().await?;
        harness.wait_for_connected(Duration::from_secs(7)).await?;

        let cf_event = in_pipe.get_contraflow().await?;
        assert_eq!(CbAction::Open, cf_event.cb);

        let (unbatched_event, unbatched_value) = get_unbatched_event();
        verify_contraflow(&harness, &unbatched_event, in_pipe).await?;

        let (batched_event, batched_value_0, batched_value_1, batched_value_2) =
            get_batched_event();
        verify_contraflow(&harness, &batched_event, in_pipe).await?;

        let (large_unbatched_event, large_unbatched_bytes) = large_unbatched_event();
        verify_contraflow(&harness, &large_unbatched_event, in_pipe).await?;

        let (large_batched_event, large_batched_value) = large_batched_event();
        verify_contraflow(&harness, &large_batched_event, in_pipe).await?;

        harness.stop().await?;
        // fetch the commited events from mock s3

        // verify a small unbatched event.
        let unbatched_value_recv = get_object_value(&s3_client, "tremor", "unbatched_key").await;
        assert_eq!(unbatched_value, unbatched_value_recv);

        // verify small and different batched events.
        let batched_value_0_recv = get_object_value(&s3_client, "tremor", "batched_key0").await;
        assert_eq!(batched_value_0, batched_value_0_recv);

        let batched_value_1_recv = get_object_value(&s3_client, "tremor", "batched_key1").await;
        assert_eq!(batched_value_1, batched_value_1_recv);

        let batched_value_2_recv = get_object_value(&s3_client, "tremor", "batched_key2").await;
        assert_eq!(batched_value_2, batched_value_2_recv);

        // verify a large unbatched_event. Checked directly against the bytes.
        let large_unbatched_bytes_recv =
            get_object(&s3_client, "tremor", "large_unbatched_event").await;
        assert_eq!(large_unbatched_bytes, large_unbatched_bytes_recv);

        // verify a large batched event having multiples keys for the same field.
        let large_batched_value_recv =
            get_object(&s3_client, "tremor", "large_batched_event").await;
        assert_eq!(large_batched_value, large_batched_value_recv);

        drop(container);
        Ok(())
    }

    async fn verify_contraflow(
        harness: &ConnectorHarness,
        event: &Event,
        in_pipe: &TestPipeline,
    ) -> Result<()> {
        harness.send_to_sink(event.clone(), IN).await?;
        let cf_event = in_pipe.get_contraflow().await?;
        assert_eq!(CbAction::Ack, cf_event.cb);
        Ok(())
    }

    async fn get_object(client: &S3Client, bucket: &str, key: &str) -> Vec<u8> {
        let resp = client
            .get_object()
            .bucket(bucket.clone())
            .key(key.clone())
            .send()
            .await
            .unwrap();

        let mut v = Vec::new();
        let read_bytes = resp
            .body
            .collect()
            .await
            .unwrap()
            .reader()
            .read_to_end(&mut v)
            .unwrap();
        v.truncate(read_bytes);
        v
    }

    async fn get_object_value(client: &S3Client, bucket: &str, key: &str) -> value::Value<'static> {
        let mut v = get_object(client, bucket, key).await;
        let obj = value::parse_to_value(&mut v).unwrap();
        return obj.into_static();
    }

    fn get_unbatched_event() -> (Event, value::Value<'static>) {
        let data = literal!({
            "numField": 12.5,
            "strField": "string",
            "listField": [true, false],
            "nestedField" : {
                "nested": true
            },
        });
        let meta = literal!({
            "s3": {
                    "key": "unbatched_key"
                }
        });

        (
            Event {
                id: EventId::from_id(0, 0, 1),
                data: (data.clone(), meta).into(),
                transactional: false,
                ..Event::default()
            },
            data,
        )
    }

    // handle seperately because 3 seperate events.
    fn get_batched_event() -> (
        Event,
        value::Value<'static>,
        value::Value<'static>,
        value::Value<'static>,
    ) {
        let batched_data = literal!([{
                "data": {
                    "value": {
                        "field1": 0.1,
                        "field2": "another_string",
                        "field3": [],
                    },
                    "meta": {
                        "s3": {
                            "key": "batched_key0"
                        }
                    }
                }
            },
            {
                "data": {
                    "value": {
                        "field3": 12,
                        "field4": {
                            "nested": false,
                            "actually": "no"
                        }
                    },
                    "meta": {
                        "s3": {
                            "key": "batched_key1"
                        }
                    }
                }
               },
            {
                "data": {
                    "value": {
                        "some_more_fields" :1,
                        "vec_field": ["elem1", "elem2", "elem3"],
                    },
                    "meta": {
                        "s3": {
                            "key": "batched_key2"
                        }
                    }
                }
            }
        ]);

        let batched_meta = literal!({});
        let batched_id = EventId::new(0, 0, 2, 2);
        (
            Event {
                id: batched_id,
                is_batch: true,
                transactional: false,
                data: (batched_data.clone(), batched_meta).into(),
                ..Event::default()
            },
            batched_data[0]
                .get("data")
                .unwrap()
                .get("value")
                .unwrap()
                .clone(),
            batched_data[1]
                .get("data")
                .unwrap()
                .get("value")
                .unwrap()
                .clone(),
            batched_data[2]
                .get("data")
                .unwrap()
                .get("value")
                .unwrap()
                .clone(),
        )
    }

    fn large_unbatched_event() -> (Event, Vec<u8>) {
        let ten_mbs = 10 * 1024 * 1024;
        let large_text = random_alphanum_string(ten_mbs).into_bytes();
        let large_data = value::Value::Bytes(large_text.clone().into());

        let large_meta = literal!({
            "s3": {
                "key": "large_unbatched_event"
            }
        });

        (
            Event {
                id: EventId::from_id(0, 0, 3),
                data: (large_data.clone(), large_meta).into(),
                transactional: true,
                ..Event::default()
            },
            large_text,
        )
    }

    fn large_batched_event() -> (Event, Vec<u8>) {
        let mut batched_data = value::Value::array_with_capacity(1000);
        let mut batched_value = Vec::new();

        let batched_meta = literal!({});

        for idx in 0..1000 {
            let field = format!("field{}", idx);
            let field_val = random_alphanum_string(10000);

            let lit = literal! ({field.clone():field_val.clone()});
            batched_value.append(&mut simd_json::to_vec(&lit).unwrap());

            let event = literal!({
                "data": {
                    "value": lit,
                    "meta" : {
                        "s3" : {
                            "key": "large_batched_event",
                        }
                    }
                }
            });

            batched_data.push(event).unwrap();
        }

        (
            Event {
                id: EventId::from_id(0, 0, 3),
                data: (batched_data, batched_meta).into(),
                transactional: true,
                is_batch: true,
                ..Event::default()
            },
            batched_value,
        )
    }

    fn random_alphanum_string(str_size: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(str_size)
            .map(char::from)
            .collect()
    }

    // fn random_num_vector(vec_size: usize) -> Vec<usize> {
    //     rand::thread_rnd()
    //         .sample_iter(&Standard)
    //         .take(vec_size)
    //         .collect()
    // }

    fn get_client() -> S3Client {
        let s3_config = s3::config::Config::builder()
            .credentials_provider(Credentials::new(
                "KEY_NOT_REQD",
                "KEY_NOT_REQD",
                None,
                None,
                "Environment",
            ))
            .region(Region::new("ap-south-1"))
            .endpoint_resolver(Endpoint::immutable(
                "http://localhost:9090".parse().unwrap(),
            ))
            .build();

        S3Client::from_conf(s3_config)
    }
}
