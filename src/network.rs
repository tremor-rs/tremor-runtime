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

use crate::{
    codec, errors::Result, metrics::RampReporter, onramp::OnrampConfig, pipeline,
    source::Processors, source::SourceReply, source::SourceState, system::Conductor,
    url::ports::ERR, url::ports::OUT, url::TremorURL,
};
use crate::{onramp, sink::Sink, source::Source};
use crate::{
    sink::tnt::{Config as TntSinkConfig, Tnt as TntSink},
    source::tnt::{Config as TntSourceConfig, SerializedResponse, TntImpl as TntSource},
    version,
};
use async_channel::{bounded, unbounded, Sender};
use async_std::task::{self, JoinHandle};
use control::{ControlProtocol, ControlState};
use simd_json::{json, StaticNode};
use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr};
use tremor_common::time::nanotime;
use tremor_pipeline::{CBAction, Event, EventId, EventOriginUri, OpMeta};
use tremor_script::{LineValue, Value, ValueAndMeta};

#[macro_use]
pub(crate) mod prelude;
pub(crate) mod nana;

// Supported network protocol variants
mod api;
mod control;
mod echo;
mod microring;
mod pubsub;

use prelude::*;

/// Address for a network
#[derive(Debug, Clone)]
pub struct Addr {
    pub(crate) addr: async_channel::Sender<Msg>,
    pub(crate) ctrl: async_channel::Sender<ControlMsg>,
    pub(crate) id: StreamId,
    pub(crate) url: TremorURL,
}

impl Addr {
    /// creates a new address
    pub(crate) fn new(
        addr: async_channel::Sender<Msg>,
        ctrl_addr: async_channel::Sender<ControlMsg>,
        id: StreamId,
        url: TremorURL,
    ) -> Self {
        Self {
            addr,
            ctrl: ctrl_addr,
            id,
            url,
        }
    }

    #[cfg(not(tarpaulin_include))]
    pub fn len(&self) -> usize {
        self.addr.len()
    }

    #[cfg(not(tarpaulin_include))]
    pub fn id(&self) -> &StreamId {
        &self.id
    }

    pub(crate) async fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.addr.send(msg).await?)
    }

    pub(crate) async fn send_control(&self, msg: ControlMsg) -> Result<()> {
        Ok(self.ctrl.send(msg).await?)
    }
}

#[derive(Debug)]
pub enum Msg {
    Event { event: Event },
}

#[derive(Debug, Clone)]
pub enum ControlMsg {
    ConnectOnramp {
        source: TremorURL,
        target: TremorURL,
        addr: Addr,
    },
    DisconnectOnramp {
        addr: Addr,
        reply: bool,
    },
    // ConnectPipeline {
    //     source: TremorURL,
    //     target: TremorURL,
    //     addr: Addr,
    // },
    // DisconnectPipeline {
    //     addr: Addr,
    //     reply: bool,
    // },
    ConnectOfframp {
        source: TremorURL,
        target: TremorURL,
        addr: Addr,
    },
    DisconnectOfframp {
        addr: Addr,
        reply: bool,
    },
}

/// Representation of the network abstraction
#[derive(Clone)]
pub struct Network {}

pub(crate) struct Manager {
    #[allow(unused_must_use)]
    pub(crate) control: ControlProtocol,
    qsize: usize,
    source: TntSource,
    sinks: Vec<TntSink>,
}

pub(crate) enum ManagerMsg {
    Stop,
}

pub(crate) type NetworkSender = async_channel::Sender<ManagerMsg>;

impl std::fmt::Display for ManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ManagerMsg::Stop => write!(f, "Network Manager Stop Command"),
        }
    }
}

pub(crate) struct NetworkManager {
    control: ControlProtocol,
    network_id: TremorURL,
    pub source: TntSource,
    metrics_reporter: RampReporter,
    pipelines_out: Vec<(TremorURL, pipeline::Addr)>,
    pipelines_err: Vec<(TremorURL, pipeline::Addr)>,
    id: u64,
    /// Unique Id for the source
    uid: u64,
    sessions: HashMap<StreamId, NetworkSession>,
}
unsafe impl Send for NetworkManager {}
unsafe impl Sync for NetworkManager {}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NetworkCont {
    ConnectProtocol(String, String, ControlState),
    DisconnectProtocol(String),
    SourceReply(Event),
    Close(Event),
    None,
}

impl NetworkManager {
    async fn handle_value(
        &mut self,
        ingest_ns: u64,
        data: LineValue,
        origin_uri: EventOriginUri,
    ) -> Result<()> {
        self.transmit_event(data, ingest_ns, origin_uri, OUT).await;
        Ok(())
    }

    async fn handle_raw_text<'event>(&mut self, sid: StreamId, event: Event) -> Result<()> {
        if let Some(session) = self.sessions.get_mut(&sid) {
            let origin = self.source.streams.get(&sid).unwrap();
            //dbg!("handling raw text via the control protocol");
            match session.on_event(origin, &event).await? {
                NetworkCont::ConnectProtocol(protocol, alias, _next_state) => {
                    let origin = self.source.streams.get(&sid).unwrap();
                    //dbg!("sending back connect-ack");
                    origin
                        .send(SerializedResponse {
                            event_id: EventId::new(0, sid as u64, 0), // FIXME TODO
                            ingest_ns: event.ingest_ns,
                            binary: false,
                            data: simd_json::to_string(&json!({ "tremor": {
                                "connect-ack": {
                                    "protocol": protocol,
                                    "alias": alias,
                                }
                            }}))?
                            .as_bytes()
                            .to_vec(),
                            should_close: false,
                        })
                        .await?;
                }
                NetworkCont::DisconnectProtocol(alias) => {
                    let origin = self.source.streams.get(&sid).unwrap();
                    origin
                        .send(SerializedResponse {
                            event_id: EventId::new(0, sid as u64, 0), // FIXME TODO
                            ingest_ns: event.ingest_ns,
                            binary: false,
                            data: simd_json::to_string(&json!({ "tremor": {
                                "disconnect-ack": {
                                    "alias": alias
                                }
                            }}))?
                            .as_bytes()
                            .to_vec(),
                            should_close: false,
                        })
                        .await?;
                    session.fsm.transition(ControlState::Disconnecting)?;
                }
                NetworkCont::SourceReply(event) => {
                    //dbg!("sending back source reply");
                    let origin = self.source.streams.get(&sid).unwrap();
                    origin
                        .send(SerializedResponse {
                            event_id: EventId::new(0, sid as u64, 0), // FIXME TODO
                            ingest_ns: event.ingest_ns,
                            binary: false,
                            data: simd_json::to_vec(event.data.parts().0)?,
                            should_close: false,
                        })
                        .await?;
                }
                NetworkCont::Close(event) => {
                    let origin = self.source.streams.get(&sid).unwrap();
                    origin
                        .send(SerializedResponse {
                            event_id: EventId::new(0, sid as u64, 0), // FIXME TODO
                            ingest_ns: event.ingest_ns,
                            binary: false,
                            data: simd_json::to_vec(event.data.parts().0)?,
                            should_close: true,
                        })
                        .await?;
                }
                NetworkCont::None => (),
            };
        } else {
            // FIXME log an error
        }

        Ok(())
    }

    async fn handle_raw_binary(&mut self, _ingest_ns: u64, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn transmit_event(
        &mut self,
        data: LineValue,
        ingest_ns: u64,
        origin_uri: EventOriginUri,
        port: beef::Cow<'static, str>,
    ) -> bool {
        let event = Event {
            id: EventId::new(self.uid, self.id, 0), // FIXME TODO
            data,
            ingest_ns,
            // TODO make origin_uri non-optional here too?
            origin_uri: Some(origin_uri),
            ..Event::default()
        };
        let mut error = false;
        self.id += 1;
        let pipelines = if OUT == port {
            &mut self.pipelines_out
        } else if ERR == port {
            &mut self.pipelines_err
        } else {
            return false;
        };
        if let Some((last, pipelines)) = pipelines.split_last_mut() {
            if let Some(t) = self.metrics_reporter.periodic_flush(ingest_ns) {
                self.metrics_reporter.send(self.source.metrics(t))
            }

            // TODO refactor metrics_reporter to do this by port now
            if ERR == port {
                self.metrics_reporter.increment_err();
            } else {
                self.metrics_reporter.increment_out();
            }

            for (input, addr) in pipelines {
                if let Some(input) = input.instance_port() {
                    if let Err(e) = addr
                        .send(pipeline::Msg::Event {
                            input: input.to_string().into(),
                            event: event.clone(),
                        })
                        .await
                    {
                        error!(
                            "[Source::{}] [Onramp] failed to send to pipeline: {}",
                            self.network_id, e
                        );
                        error = true;
                    }
                }
            }
            if let Some(input) = last.0.instance_port() {
                if let Err(e) = last
                    .1
                    .send(pipeline::Msg::Event {
                        input: input.to_string().into(),
                        event,
                    })
                    .await
                {
                    error!(
                        "[Source::{}] [Onramp] failed to send to pipeline: {}",
                        self.network_id, e
                    );
                    error = true;
                }
            }
        }
        error
    }

    pub async fn new(
        control: ControlProtocol,
        mut source: TntSource,
        mut sinks: Vec<TntSink>,
        config: OnrampConfig<'_>,
    ) -> Result<(Self, Sender<onramp::Msg>)> {
        // We use a unbounded channel for counterflow, while an unbounded channel seems dangerous
        // there is soundness to this.
        // The unbounded channel ensures that on counterflow we never have to block, or in other
        // words that sinks or pipelines sending data backwards always can progress past
        // the sending.
        // This prevents a livelock where the pipeline is waiting for a full channel to send data to
        // the source and the source is waiting for a full channel to send data to the pipeline.
        // We prevent unbounded growth by two mechanisms:
        // 1) counterflow is ALWAYS and ONLY created in response to a message
        // 2) we always process counterflow prior to forward flow
        //
        // As long as we have counterflow messages to process, and channel size is growing we do
        // not process any forward flow. Without forward flow we stave the counterflow ensuring that
        // the counterflow channel is always bounded by the forward flow in a 1:N relationship where
        // N is the maximum number of counterflow events a single event can trigger.
        // N is normally < 1.
        let (tx, _rx) = unbounded();
        let codec = codec::lookup(&config.codec)?;
        let mut resolved_codec_map = codec::builtin_codec_map();
        // override the builtin map
        for (k, v) in config.codec_map {
            resolved_codec_map.insert(k, codec::lookup(&v)?);
        }
        source.init().await?;

        // TODO do better here
        let mut sink_uid: u64 = 0;
        for sink in &mut sinks {
            let (reply_tx, _reply_rx) = bounded(crate::QSIZE);

            let sink_url = TremorURL::from_network_id(&sink_uid.to_string())?;
            sink.init(
                sink_uid,
                &sink_url,
                &*codec,
                &resolved_codec_map,
                Processors {
                    pre: &[],
                    post: &[],
                },
                true,
                //reply_channel: Sender<sink::Reply>,
                reply_tx.clone(),
            )
            .await?;
            sink_uid += 1;
        }

        Ok((
            Self {
                control,
                network_id: source.id().clone(),
                source,
                metrics_reporter: config.metrics_reporter,
                id: 0,
                pipelines_out: Vec::new(),
                pipelines_err: Vec::new(),
                uid: config.onramp_uid,
                sessions: HashMap::new(),
            },
            tx,
        ))
    }

    pub async fn run(mut self) -> Result<()> {
        //dbg!("running core network run loop");
        loop {
            match self.source.pull_event(self.id).await {
                Ok(SourceReply::StartStream(sid)) => {
                    //dbg!("network start stream");
                    self.sessions
                        .insert(sid, NetworkSession::new(sid, self.control.clone())?);
                    // how we talk back to the other end
                    let origin = self.source.streams.get(&sid).unwrap();
                    origin
                        .send(SerializedResponse {
                            event_id: EventId::new(0, sid as u64, 0), // FIXME TODO
                            ingest_ns: nanotime(),
                            binary: false,
                            data: simd_json::to_string(&json!({ "tremor": {
                                "version": version::VERSION,
                            }}))?
                            .as_bytes()
                            .to_vec(),
                            should_close: false,
                        })
                        .await?;
                }
                Ok(SourceReply::EndStream(id)) => {
                    self.sessions.remove(&id);
                }
                // TODO use this for node-to-node?
                Ok(SourceReply::Structured { origin_uri, data }) => {
                    let ingest_ns = nanotime();
                    self.handle_value(ingest_ns, data, origin_uri).await?;
                }
                Ok(SourceReply::Data {
                    origin_uri,
                    mut data,
                    meta,
                    stream,
                    ..
                }) => {
                    //dbg!("network got data");
                    let ingest_ns = nanotime();
                    match tremor_value::parse_to_value(&mut data) {
                        Ok(x) => {
                            let x = x.into_static();
                            let event = Event {
                                id: EventId::new(0, stream as u64, 0),
                                data: LineValue::new(vec![], |_| {
                                    ValueAndMeta::from_parts(x, json!({}).into())
                                }),
                                cb: CBAction::None,         // FIXME
                                op_meta: OpMeta::default(), // FIXME
                                transactional: false,       // FIXME
                                is_batch: false,
                                kind: None,
                                ingest_ns,
                                origin_uri: Some(origin_uri),
                            };
                            if let Some(Value::Object(m)) = &meta {
                                if Some(&Value::Static(StaticNode::Bool(false))) == m.get("binary")
                                {
                                    // here is where incoming text is firt handled
                                    if let Err(e) = self.handle_raw_text(stream, event).await {
                                        error!("Unexpected error during client connect {}", e)
                                    }
                                } else {
                                    // FIXME binary unsupported TODO close with error
                                    if let Err(e) = self.handle_raw_binary(ingest_ns, &[]).await {
                                        error!("Unexpected error during client connect {}", e);
                                    }
                                }
                            };
                        }
                        Err(e) => {
                            debug!("error parsing source data {}", e);
                        }
                    }
                }
                Ok(SourceReply::StateChange(SourceState::Disconnected)) => return Ok(()),
                Ok(SourceReply::StateChange(SourceState::Connected)) => (),
                Ok(SourceReply::Empty(sleep_ms)) => {
                    task::sleep(Duration::from_millis(sleep_ms)).await
                }
                Err(e) => {
                    warn!("[Source::{}] Error: {}", self.network_id, e);
                    self.metrics_reporter.increment_err();
                }
            }

            for s in &mut self.sessions {
                let stream_id = *s.0;
                let origin = self.source.streams.get(&stream_id).unwrap();
                let data = s.1.on_data().await?;
                if let Some(data) = data {
                    for d in data {
                        origin
                            .send(SerializedResponse {
                                event_id: EventId::new(0, stream_id as u64, 0), // FIXME TODO
                                ingest_ns: d.ingest_ns,
                                binary: false,
                                data: simd_json::to_vec(d.data.parts().0)?,
                                should_close: false,
                            })
                            .await?;
                    }
                }
            }
        }
    }
}

impl Manager {
    pub fn new(
        conductor: &Conductor,
        network_addr: SocketAddr,
        network_peers: Option<Vec<String>>,
        qsize: usize,
    ) -> Self {
        let onramp_id = TremorURL::from_network_id("self").unwrap();
        //dbg!("creating control protocol/network");

        let sinks = if let Some(peers) = network_peers {
            peers
                .iter()
                .map(|peer| {
                    TntSink::from_config2(TntSinkConfig {
                        url: peer.clone(),
                        binary: false,
                    })
                    .unwrap()
                })
                .collect()
        } else {
            vec![]
        };

        Self {
            control: ControlProtocol::new(conductor),
            qsize,
            source: TntSource::from_config(
                0u64,
                onramp_id,
                &[],
                &TntSourceConfig {
                    port: network_addr.port(),
                    host: network_addr.ip().to_string(),
                },
                true, // is always linked
            )
            .unwrap(),
            sinks,
        }
    }

    pub fn start(self) -> (JoinHandle<Result<()>>, NetworkSender) {
        let (tx, _rx) = bounded(self.qsize);
        let mut codec_map: HashMap<String, String> = HashMap::new();
        codec_map.insert("application/json".into(), "json".into());

        //dbg!("initializing control protocol/network");
        let h = task::spawn::<_, Result<()>>(async move {
            let (manager, _tx2) = NetworkManager::new(
                self.control,
                self.source,
                self.sinks,
                OnrampConfig {
                    onramp_uid: 0u64,
                    codec: "json",
                    processors: Processors {
                        pre: &[],
                        post: &[],
                    },
                    is_linked: true,
                    err_required: true,
                    codec_map: halfbrown::HashMap::<String, String>::new(),
                    metrics_reporter: RampReporter::new(
                        TremorURL::parse("/network/self").unwrap(),
                        Some(1),
                    ),
                },
            )
            .await
            .unwrap();
            info!("Network manager started");

            let _idle = manager.run().await.unwrap();
            info!("Network manager stopped.");
            Ok(())
        });

        (h, tx)
    }
}
