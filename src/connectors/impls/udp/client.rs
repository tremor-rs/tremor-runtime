// Copyright 2020-2021, The Tremor Team
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
// use crate::connectors::prelude::*;

//! UDP Client

use crate::connectors::prelude::*;
use async_std::net::UdpSocket;

#[derive(Deserialize, Debug, Clone)]
struct Host {
    host: String,
    port: u16,
}
impl Default for Host {
    fn default() -> Self {
        Self {
            host: String::from("0.0.0.0"),
            port: 0,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Host to connect to
    host: String,
    /// port to connect to
    port: u16,
    #[serde(default = "Host::default")]
    bind: Host,
}

impl ConfigImpl for Config {}

struct UdpClient {
    config: Config,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "udp_client".into()
    }
    async fn from_config(
        &self,
        _id: &str,
        raw_config: &ConnectorConfig,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = &raw_config.config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(UdpClient { config }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("udp-client")).into())
        }
    }
}

// TODO: We do not handle destination changes via metadata
// there is a reason for this, and that is where it gets complicated,
// metadata is assigned to a single event, but with postprocessors
// so if event A includes metadata to set a host / port to send to, then which
// data does that correlate to? The next, all previosu, this is hard to answer
// questions and we need to come up with a good answer that isn't throwing out
// unexpected behaviour - so far we've none

#[async_trait::async_trait()]
impl Connector for UdpClient {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = UdpClientSink {
            config: self.config.clone(),
            socket: None,
        };
        builder.spawn(sink, ctx).map(Some)
    }
}

struct UdpClientSink {
    config: Config,
    socket: Option<UdpSocket>,
}

impl UdpClientSink {
    async fn send_event(socket: &UdpSocket, data: Vec<Vec<u8>>) -> Result<()> {
        for chunk in data {
            socket.send(chunk.as_slice()).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait()]
impl Sink for UdpClientSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let socket =
            UdpSocket::bind((self.config.bind.host.as_str(), self.config.bind.port)).await?;
        socket
            .connect((self.config.host.as_str(), self.config.port))
            .await?;
        self.socket = Some(socket);
        Ok(true)
    }
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| Error::from(ErrorKind::NoSocket))?;
        for value in event.value_iter() {
            let data = serializer.serialize(value, event.ingest_ns)?;
            if let Err(e) = Self::send_event(socket, data).await {
                error!("{} UDP Error: {}. Initiating Reconnect...", &ctx, &e);
                // TODO: upon which errors to actually trigger a reconnect?
                self.socket = None;
                ctx.notifier().connection_lost().await?;
                return Err(e);
            }
        }
        Ok(SinkReply::NONE)
    }

    fn auto_ack(&self) -> bool {
        true
    }
}
