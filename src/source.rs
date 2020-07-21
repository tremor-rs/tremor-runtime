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
use crate::onramp::{self, prelude::make_preprocessors};
use crate::pipeline;
use crate::preprocessor::Preprocessors;
use crate::system::METRICS_PIPELINE;
use crate::url::TremorURL;
use crate::utils::nanotime;
use crate::Result;
use async_channel::{self, unbounded, Receiver, Sender};
// use async_std::stream::Stream;
use async_std::task;
// use core::task::{Context, Poll};
// use futures_timer::Delay;
// use pin_project_lite::pin_project;
// use std::future::Future;
// use std::pin::Pin;
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
#[allow(unused_variables)]
pub(crate) trait Source {
    async fn read(&mut self, id: u64) -> Result<SourceReply>;
    async fn init(&mut self) -> Result<SourceState>;
    fn id(&self) -> &TremorURL;
    fn trigger_breaker(&mut self) {}
    fn restore_breaker(&mut self) {}
    fn ack(&mut self, id: u64) {}
    fn fail(&mut self, id: u64) {}
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
}

// pin_project! {
//     struct DelayedRead<'r>
//     {
//         ready: bool,
//         delayed: bool,
//         #[pin]
//         delay: Delay,
//         #[pin]
//         ramp: Pin<Box<dyn Future<Output = Result<SourceReply>> + Send + 'r>>,
//     }
// }
// impl<'r> Future for DelayedRead<'r> {
//     type Output = MaybeRead;
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let this = self.project();
//         if !*this.ready {
//             cx.waker().wake_by_ref();
//             return Poll::Pending;
//         }
//         if *this.delayed {
//             match this.delay.poll(cx) {
//                 Poll::Ready(_) => {
//                     *this.delayed = false;
//                     this.ramp.poll(cx).map(MaybeRead::Read)
//                 }
//                 Poll::Pending => Poll::Pending,
//             }
//         } else {
//             this.ramp.poll(cx).map(MaybeRead::Read)
//         }
//     }
// }

// pin_project! {
//     struct SourceRead<'r>
//     {
//         #[pin]
//         channel: Receiver<CachePadded<onramp::Msg>>,

//         #[pin]
//         ramp: DelayedRead<'r>,
//     }
// }

// enum MaybeRead {
//     CounterFlow(onramp::Msg),
//     Read(Result<SourceReply>),
//     // Read(()),
// }

// impl<'r> Future for SourceRead<'r> {
//     type Output = MaybeRead;
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let this = self.project();

//         match this.channel.poll_next(cx) {
//             Poll::Ready(Some(item)) => Poll::Ready(MaybeRead::CounterFlow(item.into_inner())),
//             Poll::Ready(None) | Poll::Pending => this.ramp.poll(cx),
//         }
//     }
// }

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
    async fn send_event(
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
            let msg = if self.pipelines.is_empty() || self.triggered {
                self.rx.recv().await?
            } else if let Ok(msg) = self.rx.try_recv() {
                msg
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
                    for (pid, p) in &self.pipelines {
                        if pid == &id {
                            p.send_mgmt(pipeline::MgmtMsg::DisconnectInput(id.clone()))
                                .await?;
                        }
                    }

                    self.pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if self.pipelines.is_empty() {
                        tx.send(true).await?;
                        return Ok(self.pipelines.is_empty());
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
                // Circuit breaker soure failure -triggers close
                onramp::Msg::Cb(CBAction::Close, _ids) => {
                    // FIXME eprintln!("triggered for: {:?}", self.source);
                    self.source.trigger_breaker();
                    self.triggered = true;
                }
                //Circuit breaker source recovers - triggers open
                onramp::Msg::Cb(CBAction::Open, _ids) => {
                    // FIXME eprintln!("restored for: {:?}", self.source);
                    self.source.restore_breaker();
                    self.triggered = false;
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
        if let Some((last, pipelines)) = self.pipelines.split_last_mut() {
            self.metrics_reporter.periodic_flush(ingest_ns);
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
        // let SourceManager { mut source, .. } = self;
        // let mut thing = SourceRead {
        //     channel: self.rx.clone(),
        //     ramp: DelayedRead {
        //         delayed: true,
        //         delay: Delay::new(Duration::from_millis(1)),
        //         ready: false,
        //         ramp: source.read(self.id),
        //     },
        // };
        loop {
            // match (&thing).await {
            //     MaybeRead::CounterFlow(cf) => {
            //         if self.handle_pipelines().await? {
            //             return Ok(());
            //         }
            //     }
            //     MaybeRead::Read(_) => panic!(),
            // }

            // In non transactional sources we get very few replies so we don't need to check
            // as often
            if self.handle_pipelines().await? {
                return Ok(());
            }

            let pipelines_empty = self.pipelines.is_empty();

            if !self.triggered && !pipelines_empty {
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

                        self.transmit_event(data, ingest_ns, origin_uri).await;
                    }
                    SourceReply::Data {
                        mut origin_uri,
                        data,
                        stream,
                    } => {
                        origin_uri.maybe_set_uid(self.uid);
                        let mut ingest_ns = nanotime();
                        self.send_event(stream, &mut ingest_ns, &origin_uri, data)
                            .await;
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
