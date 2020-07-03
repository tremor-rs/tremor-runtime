// Copyright 2018-2020, Wayfair GmbH
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
use crate::onramp::{self, prelude::make_preprocessors, prelude::PipeHandlerResult};
use crate::pipeline;
use crate::preprocessor::Preprocessors;
use crate::system::METRICS_PIPELINE;
use crate::url::TremorURL;
use crate::utils::nanotime;
use crate::Result;
use async_std::sync::{self, channel, Receiver};
use async_std::task;
use std::time::Duration;
use tremor_pipeline::{CBAction, Event, EventOriginUri, Ids};
use tremor_script::LineValue;

pub(crate) mod blaster;
pub(crate) mod crononome;
pub(crate) mod file;
pub(crate) mod kafka;
pub(crate) mod metronome;
pub(crate) mod tcp;

pub(crate) enum SourceState {
    Connected,
    Disconnected,
}
pub(crate) enum SourceReply {
    Data {
        origin_uri: EventOriginUri,
        data: Vec<u8>,
        stream: usize,
    },
    Structured {
        origin_uri: EventOriginUri,
        data: LineValue,
    },
    StartStream(usize),
    EndStream(usize),
    StateChange(SourceState),
    Empty(u64),
}
#[async_trait::async_trait]
pub(crate) trait Source {
    async fn read(&mut self, id: u64) -> Result<SourceReply>;
    async fn init(&mut self) -> Result<SourceState>;
    fn id(&self) -> &TremorURL;
    fn trigger_breaker(&mut self) {}
    fn restore_breaker(&mut self) {}
    fn ack(&mut self, id: u64) {
        let _ = id;
    }
    fn fail(&mut self, id: u64) {
        let _ = id;
    }
}

pub(crate) struct SourceManager<T>
where
    T: Source,
{
    source_id: TremorURL,
    source: T,
    rx: Receiver<onramp::Msg>,
    tx: sync::Sender<onramp::Msg>,
    pp_template: Vec<String>,
    preprocessors: Vec<Option<Preprocessors>>,
    codec: Box<dyn Codec>,
    metrics_reporter: RampReporter,
    triggered: bool,
    pipelines: Vec<(TremorURL, pipeline::Addr)>,
    id: u64,
    /// Unique Id for the source
    uid: u64,
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
    // We are borrowing a dyn box as we don't want to pass ownership.
    fn send_event(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        origin_uri: &tremor_pipeline::EventOriginUri,
        data: Vec<u8>,
    ) {
        let original_id = self.id;
        let mut error = false;
        // FIXME record 1st id for pp data
        if let Ok(data) = self.handle_pp(stream, ingest_ns, data) {
            for d in data {
                match self.codec.decode(d, *ingest_ns) {
                    Ok(Some(data)) => {
                        error |= self.transmit_event(data, *ingest_ns, origin_uri.clone());
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
    fn handle_pipelines_msg(&mut self, msg: onramp::Msg) -> Result<PipeHandlerResult> {
        if self.pipelines.is_empty() {
            match msg {
                onramp::Msg::Connect(ps) => {
                    for p in &ps {
                        if p.0 == *METRICS_PIPELINE {
                            self.metrics_reporter.set_metrics_pipeline(p.clone());
                        } else {
                            p.1.send(pipeline::Msg::ConnectOnramp(
                                self.source_id.clone(),
                                self.tx.clone(),
                            ))?;
                            self.pipelines.push(p.clone());
                        }
                    }
                    Ok(PipeHandlerResult::Retry)
                }
                onramp::Msg::Disconnect { tx, .. } => {
                    tx.send(true)?;
                    Ok(PipeHandlerResult::Terminate)
                }
                onramp::Msg::Cb(cb, ids) => Ok(PipeHandlerResult::Cb(cb, ids)),
            }
        } else {
            match msg {
                onramp::Msg::Connect(mut ps) => {
                    for p in &ps {
                        p.1.send(pipeline::Msg::ConnectOnramp(
                            self.source_id.clone(),
                            self.tx.clone(),
                        ))?;
                    }
                    self.pipelines.append(&mut ps);
                    Ok(PipeHandlerResult::Normal)
                }
                onramp::Msg::Disconnect { id, tx } => {
                    for (pid, p) in &self.pipelines {
                        if pid == &id {
                            p.send(pipeline::Msg::DisconnectInput(id.clone()))?;
                        }
                    }
                    self.pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if self.pipelines.is_empty() {
                        tx.send(true)?;
                        Ok(PipeHandlerResult::Terminate)
                    } else {
                        tx.send(false)?;
                        Ok(PipeHandlerResult::Normal)
                    }
                }
                onramp::Msg::Cb(cb, ids) => Ok(PipeHandlerResult::Cb(cb, ids)),
            }
        }
    }

    async fn handle_pipelines2(&mut self) -> Result<PipeHandlerResult> {
        if self.pipelines.is_empty() || self.triggered {
            let msg = self.rx.recv().await?;
            self.handle_pipelines_msg(msg)
        } else if self.rx.is_empty() {
            Ok(PipeHandlerResult::Normal)
        } else {
            let msg = self.rx.recv().await?;
            self.handle_pipelines_msg(msg)
        }
    }

    pub(crate) fn transmit_event(
        &mut self,
        data: LineValue,
        ingest_ns: u64,
        origin_uri: EventOriginUri,
    ) -> bool {
        // We only try to send here since we can't guarantee
        // that nothing else has send (and overfilled) the pipelines
        // inbox.
        // We try to avoid this situation by checking but given
        // we can't coordinate w/ other onramps we got to
        // ensure that we are ready to discard messages and prioritize
        // progress.
        //
        // Notably in a Guaranteed delivery scenario those discarded
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
        if let Some(((input, addr), pipelines)) = self.pipelines.split_last() {
            self.metrics_reporter.periodic_flush(ingest_ns);
            self.metrics_reporter.increment_out();

            for (input, addr) in pipelines {
                if let Some(input) = input.instance_port() {
                    if let Err(e) = addr.try_send(pipeline::Msg::Event {
                        input: input.to_string().into(),
                        event: event.clone(),
                    }) {
                        error!("[Onramp] failed to send to pipeline: {}", e);
                        error = true;
                    }
                }
            }
            if let Some(input) = input.instance_port() {
                if let Err(e) = addr.try_send(pipeline::Msg::Event {
                    input: input.to_string().into(),
                    event,
                }) {
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
    ) -> Result<(Self, sync::Sender<onramp::Msg>)> {
        let (tx, rx) = channel(1);
        let codec = codec::lookup(&codec)?;
        let pp_template = preprocessors.to_vec();
        let preprocessors = vec![Some(make_preprocessors(&pp_template)?)];
        source.init().await?;
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

    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            match self.handle_pipelines2().await? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => (),
                PipeHandlerResult::Cb(CBAction::Fail, ids) => {
                    if let Some(id) = ids.get(self.uid) {
                        self.source.fail(id);
                    }
                }
                PipeHandlerResult::Cb(CBAction::Ack, ids) => {
                    if let Some(id) = ids.get(self.uid) {
                        self.source.ack(id);
                    }
                }
                PipeHandlerResult::Cb(CBAction::Trigger, _ids) => {
                    // FIXME eprintln!("triggered for: {:?}", self.source);
                    self.source.trigger_breaker();
                    self.triggered = true
                }
                PipeHandlerResult::Cb(CBAction::Restore, _ids) => {
                    // FIXME eprintln!("restored for: {:?}", self.source);
                    self.source.restore_breaker();
                    self.triggered = false
                }
            }

            if !self.triggered
                && !self.pipelines.is_empty()
                && self.pipelines.iter().all(|(_, p)| p.ready())
            {
                match self.source.read(self.id).await? {
                    SourceReply::StartStream(id) => {
                        while self.preprocessors.len() <= id {
                            self.preprocessors.push(None)
                        }

                        self.preprocessors
                            .push(Some(make_preprocessors(&self.pp_template)?));
                    }
                    SourceReply::EndStream(id) => {
                        if let Some(v) = self.preprocessors.get_mut(id) {
                            *v = None
                        }

                        while let Some(None) = self.preprocessors.last() {
                            self.preprocessors.pop();
                        }
                    }
                    SourceReply::Structured { origin_uri, data } => {
                        let ingest_ns = nanotime();

                        self.transmit_event(data, ingest_ns, origin_uri);
                    }
                    SourceReply::Data {
                        mut origin_uri,
                        data,
                        stream,
                    } => {
                        origin_uri.maybe_set_uid(self.uid);
                        let mut ingest_ns = nanotime();
                        self.send_event(stream, &mut ingest_ns, &origin_uri, data);
                    }
                    SourceReply::StateChange(SourceState::Disconnected) => return Ok(()),
                    SourceReply::StateChange(SourceState::Connected) => (),
                    SourceReply::Empty(sleep_ms) => {
                        task::sleep(Duration::from_millis(sleep_ms)).await
                    }
                }
            }
        }
    }
}
