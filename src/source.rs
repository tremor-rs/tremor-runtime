// Copyright 2020, The Tremor Team
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
use crate::codec::{self, Codec};
use crate::metrics::RampReporter;
use crate::onramp;
use crate::pipeline;
use crate::preprocessor::{self, Preprocessors};
use crate::system::METRICS_PIPELINE;
use crate::url::TremorURL;
use crate::utils::nanotime;
use crate::Result;
use async_channel::{self, unbounded, Receiver, Sender};
use async_std::task;
use halfbrown::HashMap;
use std::time::Duration;
use tremor_pipeline::{CBAction, Event, EventOriginUri, Ids};
use tremor_script::LineValue;

pub(crate) mod blaster;
pub(crate) mod crononome;
pub(crate) mod file;
pub(crate) mod kafka;
pub(crate) mod metronome;
pub(crate) mod postgres;
pub(crate) mod prelude;
pub(crate) mod rest;
pub(crate) mod tcp;
pub(crate) mod udp;
pub(crate) mod ws;

pub fn make_preprocessors(preprocessors: &[String]) -> Result<Preprocessors> {
    preprocessors
        .iter()
        .map(|n| preprocessor::lookup(&n))
        .collect()
}

pub(crate) enum SourceState {
    Connected,
    Disconnected,
}

pub(crate) enum SourceReply {
    /// A normal data event with a `Vec<u8>` for data
    Data {
        origin_uri: EventOriginUri,
        data: Vec<u8>,
        stream: usize,
    },
    /// Allow for passthrough of already structured events
    Structured {
        origin_uri: EventOriginUri,
        data: LineValue,
    },
    /// Allow for passthrough of already structured (request-style) events where
    /// response is expected (for linked onramps)
    // add similar DataRequest variant when needed
    StructuredRequest {
        origin_uri: EventOriginUri,
        data: LineValue,
        response_tx: Sender<Event>,
    },
    /// A stream is opened
    StartStream(usize),
    /// A stream is closed
    EndStream(usize),
    /// We change the connection state of the source
    StateChange(SourceState),
    /// There is no event currently ready and we're asked to wait an ammount of ms
    Empty(u64),
}

#[async_trait::async_trait]
#[allow(unused_variables)]
pub(crate) trait Source {
    /// Pulls an event from the source if one exists
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply>;
    /// Pulls metrics from the source
    fn metrics(&mut self, t: u64) -> Vec<Event> {
        vec![]
    }

    /// Initializes the onramp (ideally this should be idempotent)
    async fn init(&mut self) -> Result<SourceState>;
    /// Graceful shutdown
    async fn terminate(&mut self) {}

    /// Trigger the circuit breaker on the source
    fn trigger_breaker(&mut self) {}
    /// Restore the circuit breaker on the source
    fn restore_breaker(&mut self) {}

    /// Acknowledge an event
    fn ack(&mut self, id: u64) {}
    /// Fail an event
    fn fail(&mut self, id: u64) {}

    /// Gives a human readable ID for the source
    fn id(&self) -> &TremorURL;
    /// Is this source transactional or can acks/fails be ignored
    fn is_transactional(&self) -> bool {
        false
    }
}

pub(crate) struct SourceManager<T>
where
    T: Source,
{
    source_id: TremorURL,
    source: T,
    rx: Receiver<onramp::Msg>,
    tx: Sender<onramp::Msg>,
    pp_template: Vec<String>,
    preprocessors: Vec<Option<Preprocessors>>,
    codec: Box<dyn Codec>,
    metrics_reporter: RampReporter,
    triggered: bool,
    pipelines: Vec<(TremorURL, pipeline::Addr)>,
    id: u64,
    is_transactional: bool,
    /// Unique Id for the source
    uid: u64,
    // TODO better way to manage this?
    response_txes: HashMap<u64, Sender<Event>>,
}

impl<T> SourceManager<T>
where
    T: Source + Send + 'static + std::fmt::Debug,
{
    fn handle_pp(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        data: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut data = vec![data];
        let mut data1 = Vec::new();
        if let Some(pps) = self.preprocessors.get_mut(stream).and_then(Option::as_mut) {
            for pp in pps {
                data1.clear();
                for (i, d) in data.iter().enumerate() {
                    match pp.process(ingest_ns, d) {
                        Ok(mut r) => data1.append(&mut r),
                        Err(e) => {
                            error!("Preprocessor[{}] error {}", i, e);
                            return Err(e);
                        }
                    }
                }
                std::mem::swap(&mut data, &mut data1);
            }
        }
        Ok(data)
    }

    async fn send_event(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        origin_uri: &tremor_pipeline::EventOriginUri,
        data: Vec<u8>,
    ) {
        let original_id = self.id;
        let mut error = false;
        if let Ok(data) = self.handle_pp(stream, ingest_ns, data) {
            for d in data {
                match self.codec.decode(d, *ingest_ns) {
                    Ok(Some(data)) => {
                        error |= self
                            .transmit_event(data, *ingest_ns, origin_uri.clone())
                            .await;
                    }
                    Ok(None) => (),
                    Err(e) => {
                        self.metrics_reporter.increment_error();
                        error!("[Codec] {}", e);
                    }
                }
            }
        } else {
            // record preprocessor failures too
            self.metrics_reporter.increment_error();
        };
        // We ONLY fail on transmit errors as preprocessor errors might be
        // problematic
        if error {
            self.source.fail(original_id);
        }
    }

    async fn handle_pipelines(&mut self) -> Result<bool> {
        loop {
            let msg = if self.pipelines.is_empty() || self.triggered || !self.rx.is_empty() {
                self.rx.recv().await?
            } else {
                return Ok(false);
            };

            match msg {
                onramp::Msg::Connect(ps) => {
                    for p in ps {
                        if p.0 == *METRICS_PIPELINE {
                            self.metrics_reporter.set_metrics_pipeline(p);
                        } else {
                            let msg = pipeline::MgmtMsg::ConnectOnramp {
                                id: self.source_id.clone(),
                                addr: self.tx.clone(),
                                reply: self.is_transactional,
                            };
                            p.1.send_mgmt(msg).await?;
                            self.pipelines.push(p);
                        }
                    }
                }
                onramp::Msg::Disconnect { id, tx } => {
                    if let Some((_, p)) = self.pipelines.iter().find(|(pid, _)| pid == &id) {
                        p.send_mgmt(pipeline::MgmtMsg::DisconnectInput(id.clone()))
                            .await?;
                    }

                    self.pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if self.pipelines.is_empty() {
                        tx.send(true).await?;
                        self.source.terminate().await;
                        return Ok(true);
                    } else {
                        tx.send(false).await?;
                    }
                }
                onramp::Msg::Cb(CBAction::Fail, ids) => {
                    if let Some(id) = ids.get(self.uid) {
                        self.source.fail(id);
                    }
                }
                // Circuit breaker explicit acknowledgement of an event
                onramp::Msg::Cb(CBAction::Ack, ids) => {
                    if let Some(id) = ids.get(self.uid) {
                        self.source.ack(id);
                    }
                }
                // Circuit breaker source failure - triggers close
                onramp::Msg::Cb(CBAction::Close, _ids) => {
                    self.source.trigger_breaker();
                    self.triggered = true;
                }
                //Circuit breaker source recovers - triggers open
                onramp::Msg::Cb(CBAction::Open, _ids) => {
                    self.source.restore_breaker();
                    self.triggered = false;
                }
                onramp::Msg::Cb(CBAction::None, _ids) => {}

                onramp::Msg::Response(event) => {
                    // TODO send errors here if eid/tx are not found
                    if let Some(eid) = event.id.get(self.uid) {
                        if let Some(tx) = self.response_txes.remove(&eid) {
                            tx.send(event).await?;
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn transmit_event(
        &mut self,
        data: LineValue,
        ingest_ns: u64,
        origin_uri: EventOriginUri,
    ) -> bool {
        let event = Event {
            id: Ids::new(self.uid, self.id),
            data,
            ingest_ns,
            // TODO make origin_uri non-optional here too?
            origin_uri: Some(origin_uri),
            ..Event::default()
        };
        let mut error = false;
        self.id += 1;
        if let Some((last, pipelines)) = self.pipelines.split_last_mut() {
            if let Some(t) = self.metrics_reporter.periodic_flush(ingest_ns) {
                for e in self.source.metrics(t) {
                    self.metrics_reporter.send(e)
                }
            }
            self.metrics_reporter.increment_out();

            for (input, addr) in pipelines {
                if let Some(input) = input.instance_port() {
                    if let Err(e) = addr
                        .send(pipeline::Msg::Event {
                            input: input.to_string().into(),
                            event: event.clone(),
                        })
                        .await
                    {
                        error!("[Onramp] failed to send to pipeline: {}", e);
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
                    error!("[Onramp] failed to send to pipeline: {}", e);
                    error = true;
                }
            }
        }
        error
    }

    async fn new(
        uid: u64,
        mut source: T,
        preprocessors: &[String],
        codec: &str,
        metrics_reporter: RampReporter,
    ) -> Result<(Self, Sender<onramp::Msg>)> {
        // We use a unbounded channel for counterflow, while an unbounded channel seems dangerous
        // there is soundness to this.
        // The unbounded channel ensures that on counterflow we never have to block, or in other
        // words that sinks or pipelines sending data backwards always can progress passt
        // the sending.
        // This prevents a livelock where the pipeline is waiting for a full channel to send data to
        // the source and the source is waiting for a full channel to send data to the pipeline.
        // We prevent unbounded groth by two mechanisms:
        // 1) counterflow is ALWAYS and ONLY created in response to a message
        // 2) we always process counterflow prior to forward flow
        //
        // As long as we have counterflow messages to process, and channel size is growing we do
        // not process any forward flow. Without forwardflow we stave the counterflow ensuring that
        // the counterflow channel is always bounded by the forward flow in a 1:N relationship where
        // N is the maximum number of counterflow events a single event can trigger.
        // N is normally < 1.
        let (tx, rx) = unbounded();
        let codec = codec::lookup(&codec)?;
        let pp_template = preprocessors.to_vec();
        let preprocessors = vec![Some(make_preprocessors(&pp_template)?)];
        source.init().await?;
        let is_transactional = source.is_transactional();
        Ok((
            Self {
                source_id: source.id().clone(),
                pp_template,
                source,
                rx,
                tx: tx.clone(),
                preprocessors,
                codec,
                metrics_reporter,
                triggered: false,
                id: 0,
                pipelines: Vec::new(),
                uid,
                is_transactional,
                response_txes: HashMap::new(),
            },
            tx,
        ))
    }

    async fn start(
        uid: u64,
        source: T,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let name = source.id().short_id("src");
        let (manager, tx) =
            SourceManager::new(uid, source, preprocessors, codec, metrics_reporter).await?;
        task::Builder::new().name(name).spawn(manager.run())?;
        Ok(tx)
    }

    async fn run(mut self) -> Result<()> {
        loop {
            if self.handle_pipelines().await? {
                return Ok(());
            }

            let pipelines_empty = self.pipelines.is_empty();

            if !self.triggered && !pipelines_empty {
                match self.source.pull_event(self.id).await {
                    Ok(SourceReply::StartStream(id)) => {
                        while self.preprocessors.len() <= id {
                            self.preprocessors.push(None)
                        }

                        self.preprocessors
                            .push(Some(make_preprocessors(&self.pp_template)?));
                    }
                    Ok(SourceReply::EndStream(id)) => {
                        if let Some(v) = self.preprocessors.get_mut(id) {
                            *v = None
                        }

                        while let Some(None) = self.preprocessors.last() {
                            self.preprocessors.pop();
                        }
                    }
                    Ok(SourceReply::Structured { origin_uri, data }) => {
                        let ingest_ns = nanotime();

                        self.transmit_event(data, ingest_ns, origin_uri).await;
                    }
                    Ok(SourceReply::StructuredRequest {
                        origin_uri,
                        data,
                        response_tx,
                    }) => {
                        let ingest_ns = nanotime();

                        self.response_txes.insert(self.id, response_tx);
                        self.transmit_event(data, ingest_ns, origin_uri).await;
                    }
                    Ok(SourceReply::Data {
                        mut origin_uri,
                        data,
                        stream,
                    }) => {
                        origin_uri.maybe_set_uid(self.uid);
                        let mut ingest_ns = nanotime();
                        self.send_event(stream, &mut ingest_ns, &origin_uri, data)
                            .await;
                    }
                    Ok(SourceReply::StateChange(SourceState::Disconnected)) => return Ok(()),
                    Ok(SourceReply::StateChange(SourceState::Connected)) => (),
                    Ok(SourceReply::Empty(sleep_ms)) => {
                        task::sleep(Duration::from_millis(sleep_ms)).await
                    }
                    Err(e) => {
                        warn!("Source Error: {}", e);
                        self.metrics_reporter.increment_error();
                    }
                }
            }
        }
    }
}
