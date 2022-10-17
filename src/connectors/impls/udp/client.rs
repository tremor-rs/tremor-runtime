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

use crate::connectors::impls::udp::UdpDefaults;
use crate::connectors::{
    prelude::*,
    utils::socket::{udp_socket, UdpSocketOptions},
};
use std::{collections::HashMap, net::ToSocketAddrs};
use tokio::net::UdpSocket;
use tremor_value::structurize;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    default_handle: Option<String>,
    /// Optional ip/port to bind to
    bind: Option<Url<super::UdpDefaults>>,
    #[serde(default)]
    socket_options: UdpSocketOptions,
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
    async fn build_cfg(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config: Config = Config::new(config)?;

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
            sockets: HashMap::new(),
        };
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn validate(&self, config: &ConnectorConfig) -> Result<()> {
        for command in &config.initial_commands {
            match structurize::<Command>(command.clone())? {
                Command::SocketClient(_) => {}
                _ => {
                    return Err(ErrorKind::NotImplemented(
                        "Only SocketClient commands are supported for the UDP client connector."
                            .to_string(),
                    )
                    .into())
                }
            }
        }
        Ok(())
    }
}

#[derive(FileIo, SocketServer, QueueSubscriber, DatabaseWriter)]
struct UdpClientSink {
    config: Config,
    sockets: HashMap<String, UdpSocket>,
}

#[async_trait::async_trait]
impl SocketClient for UdpClientSink {
    async fn connect_socket(
        &mut self,
        address: &str,
        handle: &str,
        ctx: &SinkContext,
    ) -> Result<()> {
        let url = Url::<UdpDefaults>::parse(address)?;
        if url.port().is_none() {
            return Err("Missing port for UDP client".into());
        }
        let connect_addrs = (url.host_or_local(), url.port_or_dflt())
            .to_socket_addrs()?
            .collect::<Vec<_>>();
        let bind = if let Some(bind) = self.config.bind.clone() {
            bind
        } else {
            // chose default bind if unspecified by checking the first resolved connect addr
            let is_ipv4 = connect_addrs
                .first()
                .ok_or_else(|| format!("unable to resolve {url}"))?
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
        debug!("{ctx} Connecting to {}...", &url);
        socket.connect(connect_addrs.as_slice()).await?;
        debug!("{ctx} Connected to {}", socket.peer_addr()?);

        self.sockets.insert(handle.to_string(), socket);

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        self.sockets.remove(handle);

        Ok(())
    }
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
        for (value, meta) in event.value_meta_iter() {
            let handle = ctx
                .extract_meta(meta)
                .get("handle")
                .as_str()
                .map(ToString::to_string)
                .or_else(|| self.config.default_handle.clone())
                .ok_or("Missing handle")?;

            let socket = self
                .sockets
                .get(&handle)
                .ok_or_else(|| Error::from(ErrorKind::NoSocket))?;
            let data = serializer.serialize(value, event.ingest_ns)?;
            if let Err(e) = Self::send_event(socket, data).await {
                error!("{} UDP Error: {}. Initiating Reconnect...", &ctx, &e);
                // TODO: upon which errors to actually trigger a reconnect?
                self.sockets.remove(&handle);
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
