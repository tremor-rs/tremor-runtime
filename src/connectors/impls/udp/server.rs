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

use std::sync::Arc;

///! The UDP server will close the udp spcket on stop
use crate::connectors::{
    prelude::*,
    utils::socket::{udp_socket, UdpSocketOptions},
};
use crate::{connectors::impls::udp::UdpDefaults, errors::already_created_error};
use crate::{
    connectors::traits::{
        Command, DatabaseWriter, FileIo, QueueSubscriber, SocketClient, SocketServer,
    },
    errors::empty_error,
};
use dashmap::DashMap;
use tokio::task::JoinHandle;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// UDP: receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,

    /// whether to set the SO_REUSEPORT option
    /// to allow multiple servers binding to the same address
    /// so the incoming load can be shared across multiple connectors
    #[serde(default)]
    socket_options: UdpSocketOptions,
}

impl ConfigImpl for Config {}

struct UdpServer {
    config: Config,
    listeners: Arc<DashMap<String, JoinHandle<()>>>,
    rx: Option<Receiver<SourceReply>>,
    tx: Sender<SourceReply>,
}

impl UdpServer {
    fn new(config: Config) -> Self {
        let (tx, rx) = crate::channel::bounded(qsize());
        Self {
            config,
            listeners: Arc::new(DashMap::new()),
            rx: Some(rx),
            tx,
        }
    }
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
        _: &Alias,
        _: &ConnectorConfig,
        raw: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(raw)?;
        Ok(Box::new(UdpServer::new(config)))
    }
}

const INPUT_PORTS: [Port<'static>; 2] = [CONTROL, IN];
const INPUT_PORTS_REF: &[Port<'static>] = &INPUT_PORTS;

#[async_trait::async_trait()]
impl Connector for UdpServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = UdpServerSource::new(
            self.listeners.clone(),
            self.rx.take().ok_or_else(already_created_error)?,
        );
        Ok(Some(builder.spawn(source, ctx)))
    }

    fn input_ports(&self) -> &[Port<'static>] {
        INPUT_PORTS_REF
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = UdpServerSink {
            listeners: self.listeners.clone(),
            config: self.config.clone(),
            tx: self.tx.clone(),
        };

        Ok(Some(builder.spawn(sink, sink_context)))
    }

    fn validate(&self, config: &ConnectorConfig) -> Result<()> {
        for command in &config.initial_commands {
            match structurize::<Command>(command.clone())? {
                Command::SocketServer(_) => {}
                _ => {
                    return Err(ErrorKind::NotImplemented(
                        "Only SocketServer commands are supported for UDP connectors.".to_string(),
                    )
                    .into())
                }
            }
        }
        Ok(())
    }
}

#[derive(FileIo, SocketClient, Debug, QueueSubscriber, DatabaseWriter)]
struct UdpServerSink {
    listeners: Arc<DashMap<String, JoinHandle<()>>>,
    config: Config,
    tx: Sender<SourceReply>,
}

#[async_trait::async_trait]
impl Sink for UdpServerSink {
    async fn on_event(
        &mut self,
        _input: &str,
        _event: Event,
        _ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        Err(ErrorKind::NotImplemented("UDP server sink does not accept inputs".into()).into())
    }

    fn auto_ack(&self) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl SocketServer for UdpServerSink {
    async fn listen(&mut self, address: &str, handle: &str, ctx: &SinkContext) -> Result<()> {
        let tx = self.tx.clone();
        let config = self.config.clone();
        let url = Url::<UdpDefaults>::parse(address)?;
        let ctx = ctx.clone();

        let task = spawn_task(ctx.clone(), async move {
            let mut buffer = vec![0; config.buf_size];
            let socket = udp_socket(&url, &config.socket_options).await?;
            let origin_uri = EventOriginUri {
                scheme: "udp-server".to_string(),
                host: url.host_or_local().to_string(),
                port: Some(url.port_or_dflt()),
                path: vec![],
            };

            loop {
                match socket.recv(&mut buffer).await {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            tx.send(SourceReply::EndStream {
                                origin_uri: origin_uri.clone(),
                                meta: None,
                                stream: DEFAULT_STREAM_ID,
                            })
                            .await?;

                            return Ok(());
                        }
                        tx.send(SourceReply::Data {
                            origin_uri: origin_uri.clone(),
                            stream: Some(DEFAULT_STREAM_ID),
                            meta: None,
                            // ALLOW: we know bytes_read is smaller than or equal buf_size
                            data: buffer[0..bytes_read].to_vec(),
                            port: None,
                            codec_overwrite: None,
                        })
                        .await?;
                    }
                    Err(e) => {
                        error!(
                            "{} Error receiving from socket: {}. Initiating reconnect...",
                            ctx, &e
                        );
                        ctx.notifier().connection_lost().await?;
                        return Err(e.into());
                    }
                }
            }
        });

        self.listeners.insert(handle.to_string(), task);

        Ok(())
    }

    async fn close(&mut self, handle: &str, _ctx: &SinkContext) -> Result<()> {
        if let Some((_, listener)) = self.listeners.remove(handle) {
            listener.abort();
        }

        Ok(())
    }
}

struct UdpServerSource {
    listeners: Arc<DashMap<String, JoinHandle<()>>>,
    rx: Receiver<SourceReply>,
}

impl UdpServerSource {
    fn new(listeners: Arc<DashMap<String, JoinHandle<()>>>, rx: Receiver<SourceReply>) -> Self {
        Self { listeners, rx }
    }
}

#[async_trait::async_trait]
impl Source for UdpServerSource {
    async fn connect(&mut self, _ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        for mut e in self.listeners.iter_mut() {
            e.value_mut().abort();
        }
        self.listeners.clear();
        Ok(true)
    }

    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        self.rx.recv().await.ok_or_else(empty_error)
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        false
    }
}
