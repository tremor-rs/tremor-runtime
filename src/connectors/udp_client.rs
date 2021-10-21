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

use crate::connectors::prelude::*;
use async_std::net::UdpSocket;
use tremor_value::prelude::*;

use super::sink::ChannelSinkRuntime;

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
pub struct Config {
    /// Host to use as source
    host: String,
    port: u16,
    #[serde(default = "Host::default")]
    bind: Host,
}

impl ConfigImpl for Config {}

struct UdpClient {
    config: Config,
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        _id: &TremorUrl,
        raw_config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = raw_config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(UdpClient {
                config,
                sink_runtime: None,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("udp-client")).into())
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ConnectionMeta {
    host: String,
    port: u16,
}

fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMeta> {
    meta.get_u16("port")
        .zip(meta.get_str("host"))
        .map(|(port, host)| -> ConnectionMeta {
            ConnectionMeta {
                host: host.to_string(),
                port,
            }
        })
}

struct UdpWriter {
    socket: UdpSocket,
}

// FIXME: We do not handle destination changes via metadata
// there is a reason for this, and that is where it gets complicated,
// metadata is assigned to a single event, but with postprocessors
// so if event A includes metadata to set a host / port to send to, then which
// data does that correlate to? The next, all previosu, this is hard to answer
// questions and we need to come up with a good answer that isn't throwing out
// unexpected behaviour - so far we've none

#[async_trait::async_trait]
impl StreamWriter for UdpWriter {
    async fn write(&mut self, data: Vec<Vec<u8>>, _meta: Option<SinkMeta>) -> Result<()> {
        for data in data {
            self.socket.send(&data).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait()]
impl Connector for UdpClient {
    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        let runtime = self
            .sink_runtime
            .as_mut()
            .ok_or("sink runtime not started")?;
        let socket =
            UdpSocket::bind((self.config.bind.host.as_str(), self.config.bind.port)).await?;
        socket
            .connect((self.config.host.as_str(), self.config.port))
            .await?;
        runtime.register_stream_writer(DEFAULT_STREAM_ID, None, ctx, UdpWriter { socket });

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink =
            ChannelSink::new_no_meta(builder.qsize(), resolve_connection_meta, builder.reply_tx());
        // FIXME: rename sender
        self.sink_runtime = Some(sink.runtime());
        let addr = builder.spawn(sink, ctx)?;
        Ok(Some(addr))
    }
}
