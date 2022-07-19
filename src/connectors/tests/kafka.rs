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
mod consumer;
mod producer;

use crate::connectors::tests::free_port::find_free_tcp_port;
use crate::errors::Result;
use testcontainers::{
    clients::Cli as DockerCli, core::WaitFor, images::generic::GenericImage, Container,
    RunnableImage,
};

const IMAGE: &str = "vectorized/redpanda";
const VERSION: &str = "v21.11.15";

async fn redpanda_container<'d>(docker: &'d DockerCli) -> Result<Container<'d, GenericImage>> {
    let kafka_port = find_free_tcp_port().await?;
    let args = vec![
        "redpanda",
        "start",
        "--overprovisioned",
        "--smp",
        "1",
        "--memory",
        "512M",
        "--reserve-memory=0M",
        "--node-id=0",
        "--check=false",
        "--kafka-addr=0.0.0.0:9092",
        &format!("--advertise-kafka-addr=127.0.0.1:{kafka_port}"),
    ]
    .into_iter()
    .map(ToString::to_string)
    .collect();
    let image = GenericImage::new(IMAGE, VERSION).with_wait_for(WaitFor::StdErrMessage {
        message: "Successfully started Redpanda!".to_string(),
    });
    let image = RunnableImage::from((image, args))
        // .with_mapped_port((free_port::find_free_tcp_port().await?, 9664_u16))
        // .with_mapped_port((free_port::find_free_tcp_port().await?, 8081_u16))
        // .with_mapped_port((free_port::find_free_tcp_port().await?, 8082_u16))
        .with_mapped_port((kafka_port, 9092_u16));
    dbg!(kafka_port);
    Ok(docker.run(image))
}
