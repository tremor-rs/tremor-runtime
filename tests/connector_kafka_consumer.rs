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

    use futures::StreamExt;
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        config::FromClientConfig,
        ClientConfig,
    };
    use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
    use signal_hook_async_std::Signals;
    use testcontainers::{
        clients::Cli as DockerCli,
        images::generic::{GenericImage, Stream as WaitForStream, WaitFor},
        Docker, Image, RunArgs,
    };
    use tremor_runtime::errors::Result;

    const IMAGE: &'static str = "docker.vectorized.io/vectorized/redpanda";
    const VERSION: &'static str = "latest";

    #[async_std::test]
    async fn connector_kafka_consumer() -> Result<()> {
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
            RunArgs::default()
                .with_mapped_port((9092_u16, 9092_u16))
                .with_mapped_port((9644_u16, 9644_u16)),
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
        admin_config
            .set("client.id", "test-admin")
            .set("bootstrap.servers", &format!("localhost:{}", port));
        let admin_client = AdminClient::from_config(&admin_config)?;
        let options = AdminOptions::default();
        let res = admin_client
            .create_topics(
                vec![&NewTopic::new("topic", 3, TopicReplication::Fixed(1))],
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

        // TODO: start connector
        // cleanup
        signal_handle.close();
        signal_handler_task.cancel().await;
        drop(container);
        Ok(())
    }
}
