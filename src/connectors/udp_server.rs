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
use async_std::{
    channel::bounded,
    net::UdpSocket,
    task::{self, JoinHandle},
};

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    pub host: String,
}

impl ConfigImpl for Config {}

struct UdpServer {
    config: Config,
    task: Option<JoinHandle<()>>,
    origin_uri: EventOriginUri,
    source_channel: SourceReplySender,
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
                task: None,
                // This is a dummy, we need 1 here since 0 is not allowed, it'll get replaced in create_source
                source_channel: bounded(1).0,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("udp-server")).into())
        }
    }
}

// impl UdpServer {
//     fn register_reader<F, R>(stream: u64, u: F)
//     where
//         // F: FnOnce(u64) -> Pin<Box<dyn core::future::Future<Output = SourceReply> + Send>>,
//         R: core::future::Future<Output = SourceReply>,
//         F: FnOnce(u64) -> R,
//     {
//     }
// }

#[async_trait::async_trait()]
impl Connector for UdpServer {
    async fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        notifier: super::reconnect::ConnectionLostNotifier,
        q: &QuiescenceBeacon,
    ) -> Result<bool> {
        if self.task.is_none() {
            let socket = UdpSocket::bind((self.config.host.as_str(), self.config.port)).await?;
            let origin_uri = self.origin_uri.clone();
            let q = q.clone();
            // let mut buffer = [0_u8; 1024];
            // Self::register_reader(DEFAULT_STREAM_ID, |stream| async move {
            //     let bytes_read = socket.recv(&mut buffer).await.unwrap_or_default();
            //     if bytes_read == 0 {
            //         SourceReply::EndStream(stream)
            //     } else {
            //         SourceReply::Data {
            //             origin_uri: origin_uri.clone(),
            //             stream,
            //             meta: None,
            //             // ALLOW: we know bytes_read is smaller than or equal buf_size
            //             data: buffer[0..bytes_read].to_vec(),
            //         }
            //     }
            // });

            let source_channel = self.source_channel.clone();
            // .ok_or("source channel not initialized")?;

            self.task = Some(task::spawn(async move {
                let mut buffer = [0_u8; 1024];
                while let (true, Ok(bytes_read)) =
                    (q.continue_reading(), socket.recv(&mut buffer).await)
                {
                    if bytes_read == 0 {
                        break;
                    }
                    let sc_data = SourceReply::Data {
                        origin_uri: origin_uri.clone(),
                        stream: DEFAULT_STREAM_ID,
                        meta: None,
                        // ALLOW: we know bytes_read is smaller than or equal buf_size
                        data: buffer[0..bytes_read].to_vec(),
                    };
                    if source_channel.send(sc_data).await.is_err() {
                        break;
                    };
                }
                if let Err(e) = notifier.notify().await {
                    error!("Failed to notify about UDP server stop: {}", e);
                };
            }));
        }
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
        let source = ChannelSource::new(builder.qsize());
        self.source_channel = source.sender();
        let addr = builder.spawn(source, source_context)?;
        Ok(Some(addr))
    }

    async fn on_resume(&mut self, _ctx: &ConnectorContext) {}

    async fn on_stop(&mut self, _ctx: &ConnectorContext) {}
}
