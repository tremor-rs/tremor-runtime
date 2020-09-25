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
use crate::errors::*;
use crate::metrics::RampReporter;
use crate::onramp;
use crate::pipeline;
use crate::preprocessor::{make_preprocessors, preprocess, Preprocessors};
use crate::ramp::{ERROR, OUT};
use crate::system::METRICS_PIPELINE;
use crate::url::TremorURL;
use crate::Result;
use async_channel::{self, unbounded, Receiver, Sender};
use async_std::task;
use halfbrown::HashMap;
use simd_json::Builder;
use std::borrow::Cow;
use std::time::Duration;
use tremor_common::time::nanotime;
use tremor_pipeline::{CBAction, Event, EventOriginUri, Ids};
use tremor_script::{LineValue, Value, ValueAndMeta};

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

struct StaticValue(Value<'static>);
// This is ugly but we need to handle comments, thanks rental!
pub(crate) enum RentalSnot {
    Error(Error),
    Skip,
}

impl From<std::str::Utf8Error> for RentalSnot {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::Error(e.into())
    }
}

pub(crate) enum SourceState {
    Connected,
    Disconnected,
}

// TODO rename without reply keyword to avoid confusion with linked transport "reply"
pub(crate) enum SourceReply {
    /// A normal data event with a `Vec<u8>` for data
    Data {
        origin_uri: EventOriginUri,
        data: Vec<u8>,
        meta: Option<Value<'static>>,
        /// allow source to override codec when pulling event
        /// the given string must be configured in the `config-map` as part of the source config
        codec_override: Option<String>,
        stream: usize,
    },
    /// Allow for passthrough of already structured events
    Structured {
        origin_uri: EventOriginUri,
        data: LineValue,
    },
    /// A stream is opened
    StartStream(usize, Option<Sender<Event>>),
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
    /// determine the codec to be used
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply>;

    /// Send event back from source (for linked onramps)
    async fn reply_event(
        &mut self,
        event: Event,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
    ) -> Result<()> {
        Ok(())
    }

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
    codec_map: HashMap<String, Box<dyn Codec>>,
    metrics_reporter: RampReporter,
    triggered: bool,
    // TODO maybe just have pipelines_out and pipelines_error as Vec
    // instead of port -> pipelines mapping here
    pipelines_out: Vec<(TremorURL, pipeline::Addr)>,
    pipelines_err: Vec<(TremorURL, pipeline::Addr)>,
    id: u64,
    is_transactional: bool,
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
        if let Some(Some(preprocessors)) = self.preprocessors.get_mut(stream) {
            preprocess(
                preprocessors.as_mut_slice(),
                ingest_ns,
                data,
                &self.source_id,
            )
        } else {
            Ok(vec![])
        }
    }

    async fn make_event_data(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        codec_override: Option<String>,
        data: Vec<u8>,
        meta: Option<StaticValue>, // See: https://github.com/rust-lang/rust/issues/63033
    ) -> Vec<Result<LineValue>> {
        let mut results = vec![];
        match self.handle_pp(stream, ingest_ns, data) {
            Ok(data) => {
                let meta_value = meta.map_or_else(Value::object, |m| m.0);
                for d in data {
                    let line_value = LineValue::try_new(vec![d], |mutd| {
                        // this is safe, because we get the vec we created in the previous argument and we now it has 1 element
                        // so it will never panic.
                        // take this, rustc!
                        let mut_data = mutd[0].as_mut_slice();
                        let decoded = if let Some(doh) = &codec_override {
                            if let Some(c) = self.codec_map.get_mut(doh) {
                                c.decode(mut_data, *ingest_ns)
                            } else {
                                self.codec.decode(mut_data, *ingest_ns)
                            }
                        } else {
                            self.codec.decode(mut_data, *ingest_ns)
                        };
                        match decoded {
                            Ok(None) => Err(RentalSnot::Skip),
                            Err(e) => Err(RentalSnot::Error(e)),
                            Ok(Some(decoded)) => {
                                Ok(ValueAndMeta::from_parts(decoded, meta_value.clone()))
                            }
                        }
                    })
                    .map_err(|e| e.0);

                    match line_value {
                        Ok(decoded) => results.push(Ok(decoded)),
                        Err(RentalSnot::Skip) => (),
                        Err(RentalSnot::Error(e)) => {
                            results.push(Err(format!("[Codec] {}", e).into()));
                        }
                    }
                }
            }
            Err(e) => {
                // record preprocessor failures too
                results.push(Err(e));
            }
        }
        results
    }

    async fn handle_pipelines(&mut self) -> Result<bool> {
        loop {
            let msg = if self.pipelines_out.is_empty() || self.triggered || !self.rx.is_empty() {
                self.rx.recv().await?
            } else {
                return Ok(false);
            };

            match msg {
                onramp::Msg::Connect(port, ps) => {
                    for p in ps {
                        if p.0 == *METRICS_PIPELINE {
                            self.metrics_reporter.set_metrics_pipeline(p);
                        } else {
                            let pipelines = if port == OUT {
                                &mut self.pipelines_out
                            } else if port == ERROR {
                                &mut self.pipelines_err
                            } else {
                                return Err(format!(
                                    "Invalid Onramp Port: {}. Cannot connect.",
                                    port
                                )
                                .into());
                            };
                            let msg = pipeline::MgmtMsg::ConnectOnramp {
                                id: self.source_id.clone(),
                                addr: self.tx.clone(),
                                reply: self.is_transactional,
                            };
                            p.1.send_mgmt(msg).await?;
                            pipelines.push(p);
                        }
                    }
                }
                onramp::Msg::Disconnect { id, tx } => {
                    for (_, p) in self
                        .pipelines_out
                        .iter()
                        .chain(self.pipelines_err.iter())
                        .filter(|(pid, _)| pid == &id)
                    {
                        p.send_mgmt(pipeline::MgmtMsg::DisconnectInput(id.clone()))
                            .await?;
                    }

                    let mut empty_pipelines = true;
                    self.pipelines_out.retain(|(pipeline, _)| pipeline != &id);
                    empty_pipelines &= self.pipelines_out.is_empty();
                    self.pipelines_err.retain(|(pipeline, _)| pipeline != &id);
                    empty_pipelines &= self.pipelines_err.is_empty();

                    if empty_pipelines {
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
                    if let Err(e) = self
                        .source
                        .reply_event(event, self.codec.as_ref(), &self.codec_map)
                        .await
                    {
                        error!(
                            "[Source::{}] [Onramp] failed to reply event from source: {}",
                            self.source_id, e
                        );
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
        port: Cow<'static, str>,
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
        let pipelines = if port == OUT {
            &mut self.pipelines_out
        } else if port == ERROR {
            &mut self.pipelines_err
        } else {
            return false;
        };
        if let Some((last, pipelines)) = pipelines.split_last_mut() {
            if let Some(t) = self.metrics_reporter.periodic_flush(ingest_ns) {
                for e in self.source.metrics(t) {
                    self.metrics_reporter.send(e)
                }
            }

            // TODO refactor metrics_reporter to do this by port now
            if port == ERROR {
                self.metrics_reporter.increment_error();
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
                            self.source_id, e
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
                        self.source_id, e
                    );
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
        _postprocessors: &[String],
        codec: &str,
        codec_map: HashMap<String, String>,
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
        let mut resolved_codec_map = codec::builtin_codec_map();
        // override the builtin map
        for (k, v) in codec_map {
            resolved_codec_map.insert(k, codec::lookup(&v)?);
        }
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
                //postprocessors,
                codec,
                codec_map: resolved_codec_map,
                metrics_reporter,
                triggered: false,
                id: 0,
                pipelines_out: Vec::new(),
                pipelines_err: Vec::new(),
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
        codec_map: HashMap<String, String>,
        preprocessors: &[String],
        postprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let name = source.id().short_id("src");
        let (manager, tx) = SourceManager::new(
            uid,
            source,
            preprocessors,
            postprocessors,
            codec,
            codec_map,
            metrics_reporter,
        )
        .await?;
        task::Builder::new().name(name).spawn(manager.run())?;
        Ok(tx)
    }

    async fn run(mut self) -> Result<()> {
        loop {
            if self.handle_pipelines().await? {
                return Ok(());
            }

            let pipelines_out_empty = self.pipelines_out.is_empty();

            // TODO: add a flag to the onramp to wait for the error pipelines to be populated as well
            //       lets call it `wait_for_error_pipelines` (horrible name)
            if !self.triggered && !pipelines_out_empty {
                match self.source.pull_event(self.id).await {
                    Ok(SourceReply::StartStream(id, _)) => {
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

                        self.transmit_event(data, ingest_ns, origin_uri, OUT).await;
                    }
                    // TODO: remove
                    Ok(SourceReply::Data {
                        mut origin_uri,
                        data,
                        meta,
                        codec_override,
                        stream,
                    }) => {
                        origin_uri.maybe_set_uid(self.uid);
                        let mut ingest_ns = nanotime();
                        let mut error = false;
                        let original_id = self.id;
                        let results = self
                            .make_event_data(
                                stream,
                                &mut ingest_ns,
                                codec_override,
                                data,
                                meta.map(StaticValue),
                            )
                            .await;
                        for result in results {
                            let (port, data) = match result {
                                Ok(d) => (OUT, d),
                                Err(e) => {
                                    // TODO do not log these now that we have error events here?
                                    error!("[Source::{}] Error: {}", self.source_id, e);
                                    // TODO pass meta alongside which can be useful for
                                    // errors too [will probably need to return (port, data)
                                    // as part of results itself]
                                    let mut error_data =
                                        simd_json::borrowed::Object::with_capacity(3);
                                    error_data.insert_nocheck("error".into(), e.to_string().into());
                                    error_data
                                        .insert_nocheck("event_id".into(), original_id.into());
                                    error_data.insert_nocheck(
                                        "source_id".into(),
                                        self.source_id.to_string().into(),
                                    );
                                    (ERROR, Value::from(error_data).into())
                                }
                            };
                            error |= self
                                .transmit_event(data, ingest_ns, origin_uri.clone(), port)
                                .await;
                        }
                        // We ONLY fail on transmit errors as preprocessor errors might be
                        // problematic
                        if error {
                            self.source.fail(original_id);
                        }
                    }
                    Ok(SourceReply::StateChange(SourceState::Disconnected)) => return Ok(()),
                    Ok(SourceReply::StateChange(SourceState::Connected)) => (),
                    Ok(SourceReply::Empty(sleep_ms)) => {
                        task::sleep(Duration::from_millis(sleep_ms)).await
                    }
                    Err(e) => {
                        warn!("[Source::{}] Error: {}", self.source_id, e);
                        self.metrics_reporter.increment_error();
                    }
                }
            }
        }
    }
}
