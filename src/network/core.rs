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

use crate::network::prelude::*;
use crate::{codec::Codec, source::Source};
use crate::{errors::Result, url::TremorURL};
use crate::{
    postprocessor::{make_postprocessors, postprocess, Postprocessors},
    source::SourceState,
};
use async_channel::{bounded, Receiver, Sender, TryRecvError};
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use futures::{SinkExt, StreamExt};
use halfbrown::HashMap;
use simd_json::Builder;
use simd_json::Mutable;
use std::collections::BTreeMap;
use tremor_pipeline::Event;
use tremor_pipeline::EventOriginUri;
use tremor_script::Value;
use tungstenite::protocol::Message;

use crate::source::SourceReply;

// WsNetMessage use crate::source::ws::WsSourceReply;

#[derive(Deserialize, Debug, Clone)]
pub struct WsNetConfig {
    pub port: u16,
    pub host: String,
}

// impl ConfigImpl for WsNetConfig {}

pub struct WsNet {
    pub config: WsNetConfig,
}

enum WsNetMessage {
    //    Connect()
    StartProtocol(usize, Option<Sender<SerializedResponse>>),
    EndProtocol(usize),
    Data(SourceReply),
}

pub struct SerializedResponse {
    //    event_id: Ids,
    ingest_ns: u64,
    data: Vec<u8>,
    is_binary: bool,
}

pub struct Net {
    config: WsNetConfig,
    listener: Option<Receiver<WsNetMessage>>,
    messages: BTreeMap<u64, usize>,
    streams: BTreeMap<usize, Sender<SerializedResponse>>,
    post_processors: Vec<String>,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Net")
    }
}

impl Net {
    fn from_config(config: &WsNetConfig) -> Result<Self> {
        let config = config.clone();

        Ok(Self {
            config,
            listener: None,
            messages: BTreeMap::new(),
            streams: BTreeMap::new(),
            post_processors: Vec::new(),
        })
    }
}

async fn handle_connection(
    source_url: TremorURL,
    tx: Sender<WsNetMessage>,
    raw_stream: TcpStream,
    origin_uri: EventOriginUri,
    processors: Vec<String>,
    stream: usize,
) -> Result<()> {
    const link: bool = true;
    let net_stream = async_tungstenite::accept_async(raw_stream).await?;
    let (mut net_read, mut net_write) = net_stream.split();
    // TODO maybe send ws_write from tx and get rid of this task + extra channel?
    let stream_sender = if link {
        let (stream_tx, stream_rx): (Sender<SerializedResponse>, Receiver<SerializedResponse>) =
            bounded(crate::QSIZE);
        // response handling task
        task::spawn::<_, Result<()>>(async move {
            // create post-processors for this stream
            match make_postprocessors(processors.as_slice()) {
                Ok(mut post_processors) => {
                    // wait for response messages to arrive (via reply_event)
                    while let Ok(response) = stream_rx.recv().await {
                        let event_id = "fake".to_string();
                        let msgs = match make_messages(response, &mut post_processors) {
                            // post-process
                            Ok(messages) => messages,
                            Err(e) => {
                                error!(
                                    "[Source::{}] Error post-processing response event: {}",
                                    &source_url,
                                    e.to_string()
                                );
                                let err = create_error_response(
                                    format!("Error post-processing messages: {}", e),
                                    event_id,
                                    &source_url,
                                );
                                let mut msgs = Vec::with_capacity(1);
                                if let Ok(data) = simd_json::to_vec(&err) {
                                    msgs.push(Message::Binary(data));
                                } else {
                                    error!(
                                        "[Source::{}] Error serializing error response to json.",
                                        &source_url
                                    );
                                }
                                msgs
                            }
                        };
                        for msg in msgs {
                            net_write.send(msg).await?;
                        }
                    }
                }
                Err(e) => error!(
                    "[Net::Core] Invalid Post Processors, not starting response receiver task: {}",
                    e
                ), // shouldn't happen, got validated before in init and is not changes after
            }
            Ok(())
        });
        Some(stream_tx)
    } else {
        None
    };

    tx.send(WsNetMessage::StartProtocol(stream, stream_sender))
        .await?;

    while let Some(msg) = net_read.next().await {
        let mut meta = Value::object_with_capacity(1);
        match msg {
            Ok(Message::Text(t)) => {
                meta.insert("binary", false)?;
                tx.send(WsNetMessage::Data(SourceReply::Data {
                    origin_uri: origin_uri.clone(),
                    data: t.into_bytes(),
                    meta: Some(meta),
                    codec_override: None,
                    stream,
                }))
                .await?;
            }
            Ok(Message::Binary(data)) => {
                meta.insert("binary", true)?;
                tx.send(WsNetMessage::Data(SourceReply::Data {
                    origin_uri: origin_uri.clone(),
                    data,
                    meta: Some(meta),
                    codec_override: None,
                    stream,
                }))
                .await?;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => (),
            Ok(Message::Close(_)) => {
                tx.send(WsNetMessage::EndProtocol(stream)).await?;
                break;
            }
            Err(e) => error!("Net error returned while waiting for client data: {}", e),
        }
    }
    Ok(())
}

fn make_messages(
    response: SerializedResponse,
    processors: &mut Postprocessors,
) -> Result<Vec<Message>> {
    let send_as_binary = response.is_binary;
    let processed = postprocess(processors.as_mut_slice(), response.ingest_ns, response.data)?;
    Ok(processed
        .into_iter()
        .map(|data| {
            if send_as_binary {
                Message::Binary(data)
            } else {
                let str = String::from_utf8_lossy(&data).to_string();
                Message::Text(str)
            }
        })
        .collect())
}

fn create_error_response(err: String, event_id: String, source_id: &TremorURL) -> Value<'static> {
    let mut err_data = tremor_script::Object::with_capacity(3);
    err_data.insert_nocheck("error".into(), Value::from(err));
    err_data.insert_nocheck("event_id".into(), Value::from(event_id));
    err_data.insert_nocheck("source_id".into(), Value::from(source_id.to_string()));
    Value::from(err_data)
}

#[async_trait::async_trait()]
impl Source for Net {
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {}

    async fn reply_event(
        &mut self,
        event: Event,
        codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
    ) -> Result<()> {
        Ok(())
    }

    async fn init(&mut self) -> Result<SourceState> {
        let listen_port = self.config.port;
        let listener = TcpListener::bind((self.config.host.as_str(), listen_port)).await?;
        let (tx, rx) = bounded(crate::QSIZE);
        let uid = 0; // self.uid;
        let source_url = TremorURL::from_onramp_id("net")?;

        let link = true;

        make_postprocessors(self.post_processors.as_slice())?; // just for verification before starting the onramp
        let processors = self.post_processors.clone();
        task::spawn(async move {
            let mut stream_id = 0;
            while let Ok((stream, socket)) = listener.accept().await {
                let uri = EventOriginUri {
                    uid,
                    scheme: "tremor-net".to_string(),
                    host: socket.ip().to_string(),
                    port: Some(socket.port()),
                    path: vec![listen_port.to_string()],
                };

                stream_id += 1;
                task::spawn(handle_connection(
                    source_url.clone(),
                    tx.clone(),
                    stream,
                    uri,
                    processors.clone(),
                    stream_id,
                    //                    link,
                ));
            }
        });

        self.listener = Some(rx);

        Ok(SourceState::Connected)
    }

    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }
}
