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

use super::source::StreamReader;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    pub host: String,
    // TCP: receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

struct UdpServer {
    config: Config,
    origin_uri: EventOriginUri,
    src_runtime: Option<ChannelSourceRuntime>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}
impl ConnectorBuilder for Builder {
    fn from_config(
        &self,
        _id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(raw) = raw_config {
            let config = Config::new(raw)?;
            let origin_uri = EventOriginUri {
                scheme: "udp-server".to_string(),
                host: config.host.clone(),
                port: Some(config.port),
                path: vec![],
            };
            Ok(Box::new(UdpServer {
                config,
                origin_uri,
                src_runtime: None,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("udp-server")).into())
        }
    }
}

struct UdpReader {
    socket: UdpSocket,
    buffer: Vec<u8>,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait]
impl StreamReader for UdpReader {
    async fn read(&mut self, stream: u64) -> Result<SourceReply> {
        let bytes_read = self.socket.recv(&mut self.buffer).await?;
        if bytes_read == 0 {
            Ok(SourceReply::EndStream(stream))
        } else {
            Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                stream,
                meta: None,
                // ALLOW: we know bytes_read is smaller than or equal buf_size
                data: self.buffer[0..bytes_read].to_vec(),
            })
        }
    }

    fn on_done(&self, _stream: u64) -> StreamDone {
        StreamDone::ConnectorClosed
    }
}

#[async_trait::async_trait()]
impl Connector for UdpServer {
    async fn connect(&mut self, ctx: &ConnectorContext) -> Result<bool> {
        let reader = UdpReader {
            socket: UdpSocket::bind((self.config.host.as_str(), self.config.port)).await?,
            origin_uri: self.origin_uri.clone(),
            buffer: vec![0_u8; self.config.buf_size],
        };
        self.src_runtime
            .as_ref()
            .ok_or("source channel not initialized")?
            .register_stream_reader(DEFAULT_STREAM_ID, ctx, reader);

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: super::source::SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = ChannelSource::new(source_context.clone(), builder.qsize());
        self.src_runtime = Some(source.sender());
        let addr = builder.spawn(source, source_context)?;
        Ok(Some(addr))
    }

    async fn on_resume(&mut self, _ctx: &ConnectorContext) {}

    async fn on_stop(&mut self, _ctx: &ConnectorContext) {}
}
