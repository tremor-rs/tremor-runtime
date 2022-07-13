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

///! The UDP server will close the udp spcket on stop
use crate::connectors::prelude::*;
use async_std::net::UdpSocket;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The port to listen on.
    pub(crate) url: Url<super::UdpDefaults>,
    // UDP: receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

struct UdpServer {
    config: Config,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "udp_server".into()
    }
    async fn build_cfg(
        &self,
        _: &ConnectorAlias,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
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
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = UdpServerSource::new(self.config.clone());
        builder.spawn(source, source_context).map(Some)
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
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        let listener = UdpSocket::bind((
            self.config.url.host_or_local(),
            self.config.url.port_or_dflt(),
        ))
        .await?;
        self.listener = Some(listener);
        Ok(true)
    }

    async fn pull_data(&mut self, _pull_id: &mut u64, ctx: &SourceContext) -> Result<SourceReply> {
        let socket = self
            .listener
            .as_ref()
            .ok_or_else(|| Error::from(ErrorKind::NoSocket))?;
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
