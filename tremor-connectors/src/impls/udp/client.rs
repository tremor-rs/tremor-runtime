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
// use crate::prelude::*;

//! UDP Client
use super::{udp_socket, UdpSocketOptions};
use crate::prelude::*;
use tokio::net::UdpSocket;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Host to connect to
    url: Url<super::UdpDefaults>,
    /// Optional ip/port to bind to
    bind: Option<Url<super::UdpDefaults>>,
    #[serde(default)]
    socket_options: UdpSocketOptions,
}

impl tremor_config::Impl for Config {}

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
    async fn build_cfg(
        &self,
        _id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config: Config = Config::new(config)?;
        if config.url.port().is_none() {
            return Err("Missing port for UDP client".into());
        }

        Ok(Box::new(UdpClient { config }))
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
        Ok(Some(builder.spawn(sink, ctx)))
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
    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        let connect_addrs = tokio::net::lookup_host((
            self.config.url.host_or_local(),
            self.config.url.port_or_dflt(),
        ))
        .await?
        .collect::<Vec<_>>();
        let bind = if let Some(bind) = self.config.bind.clone() {
            bind
        } else {
            // chose default bind if unspecified by checking the first resolved connect addr
            let is_ipv4 = connect_addrs
                .first()
                .ok_or_else(|| format!("unable to resolve {}", self.config.url))?
                .is_ipv4();
            if is_ipv4 {
                Url::parse(super::UDP_IPV4_UNSPECIFIED)?
            } else {
                Url::parse(super::UDP_IPV6_UNSPECIFIED)?
            }
        };
        debug!("{ctx} Binding to {}...", &bind);
        let socket = udp_socket(&bind, &self.config.socket_options).await?;
        debug!("{ctx} Bound to {}", socket.local_addr()?);
        debug!("{ctx} Connecting to {}...", &self.config.url);
        socket.connect(connect_addrs.as_slice()).await?;
        debug!("{ctx} Connected to {}", socket.peer_addr()?);
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
        for (value, meta) in event.value_meta_iter() {
            let data = serializer.serialize(value, meta, event.ingest_ns).await?;
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
