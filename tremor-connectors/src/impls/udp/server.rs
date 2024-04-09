// Copyright 2020-2022, The Tremor Team
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

//! The UDP server will close the udp spcket on stop
use super::{udp_socket, UdpSocketOptions};
use crate::{source::prelude::*, utils::socket, Context};
use tokio::net::UdpSocket;
use tremor_common::{alias, url::Url};
use tremor_system::event::DEFAULT_STREAM_ID;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The port to listen on.
    pub(crate) url: Url<super::UdpDefaults>,
    /// UDP: receive buffer size
    #[serde(default = "crate::prelude::default_buf_size")]
    buf_size: usize,

    /// whether to set the SO_REUSEPORT option
    /// to allow multiple servers binding to the same address
    /// so the incoming load can be shared scross multiple connectors
    #[serde(default)]
    socket_options: UdpSocketOptions,
}

impl tremor_config::Impl for Config {}

struct UdpServer {
    config: Config,
}

/// UDP server connector
#[derive(Debug, Default)]
pub struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "udp_server".into()
    }
    async fn build_cfg(
        &self,
        _: &alias::Connector,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> anyhow::Result<Box<dyn Connector>> {
        let config = Config::new(raw)?;
        Ok(Box::new(UdpServer { config }))
    }
}

#[async_trait::async_trait()]
impl Connector for UdpServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> anyhow::Result<Option<SourceAddr>> {
        let source = UdpServerSource::new(self.config.clone());
        Ok(Some(builder.spawn(source, ctx)))
    }
}

struct UdpServerSource {
    config: Config,
    origin_uri: EventOriginUri,
    listener: Option<UdpSocket>,
    buffer: Vec<u8>,
}

impl UdpServerSource {
    fn new(config: Config) -> Self {
        let buffer = vec![0; config.buf_size];
        let origin_uri = EventOriginUri {
            scheme: "udp-server".to_string(),
            host: config.url.host_or_local().to_string(),
            port: Some(config.url.port_or_dflt()),
            path: vec![],
        };
        Self {
            config,
            origin_uri,
            listener: None,
            buffer,
        }
    }
}

#[async_trait::async_trait]
impl Source for UdpServerSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let listener = udp_socket(&self.config.url, &self.config.socket_options).await?;
        self.listener = Some(listener);
        Ok(true)
    }

    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        let socket = self.listener.as_ref().ok_or(socket::Error::NoSocket)?;
        match socket.recv(&mut self.buffer).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    Ok(SourceReply::EndStream {
                        origin_uri: self.origin_uri.clone(),
                        meta: None,
                        stream: DEFAULT_STREAM_ID,
                    })
                } else {
                    Ok(SourceReply::Data {
                        origin_uri: self.origin_uri.clone(),
                        stream: Some(DEFAULT_STREAM_ID),
                        meta: None,
                        // ALLOW: we know bytes_read is smaller than or equal buf_size
                        data: self.buffer[0..bytes_read].to_vec(),
                        port: None,
                        codec_overwrite: None,
                    })
                }
            }
            Err(e) => {
                error!(
                    "{} Error receiving from socket: {}. Initiating reconnect...",
                    ctx, &e
                );
                self.listener = None;
                ctx.notifier().connection_lost().await?;
                return Err(e.into());
            }
        }
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
