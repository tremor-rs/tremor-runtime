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

pub(crate) use crate::codec::{self, Codec};
pub(crate) use crate::errors::*;
pub(crate) use crate::metrics::RampReporter;
pub(crate) use crate::onramp::{self, Onramp};
pub(crate) use crate::pipeline;
pub(crate) use crate::preprocessor::{self, Preprocessors};
pub(crate) use crate::source::{Source, SourceManager, SourceReply, SourceState};
pub(crate) use crate::system::METRICS_PIPELINE;
pub(crate) use crate::url::TremorURL;
pub(crate) use crate::utils::{hostname, nanotime, ConfigImpl};
pub(crate) use async_channel::{bounded, Receiver};
pub(crate) use async_std::task;
pub(crate) use tremor_pipeline::{CBAction, Event, EventOriginUri, Ids};
use tremor_script::LineValue;

// TODO pub here too?
use std::mem;

pub fn make_preprocessors(preprocessors: &[String]) -> Result<Preprocessors> {
    preprocessors
        .iter()
        .map(|n| preprocessor::lookup(&n))
        .collect()
}

pub fn handle_pp(
    preprocessors: &mut Preprocessors,
    ingest_ns: &mut u64,
    data: Vec<u8>,
) -> Result<Vec<Vec<u8>>> {
    let mut data = vec![data];
    let mut data1 = Vec::new();
    for pp in preprocessors {
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
        mem::swap(&mut data, &mut data1);
    }
    Ok(data)
}

pub(crate) async fn transmit_event(
    pipelines: &mut [(TremorURL, pipeline::Addr)],
    metrics_reporter: &mut RampReporter,
    data: LineValue,
    ingest_ns: u64,
    origin_uri: EventOriginUri,
    id: u64,
) -> Option<u64> {
    // We only try to send here since we can't guarantee
    // that nothing else has send (and overfilled) the pipelines
    // inbox.
    // We try to avoid this situation by checking but given
    // we can't coordinate w/ other onramps we got to
    // ensure that we are ready to discard messages and prioritize
    // progress.
    //
    // Notably in a Guaranteed delivery scenario those discarded
    let mut result = None;
    let event = Event {
        /// FIXME: this should be replaced by the source trait
        id: Ids::new(origin_uri.uid, id),
        data,
        ingest_ns,
        // TODO make origin_uri non-optional here too?
        origin_uri: Some(origin_uri),
        ..Event::default()
    };
    if let Some((last, pipelines)) = pipelines.split_last_mut() {
        metrics_reporter.periodic_flush(ingest_ns);
        metrics_reporter.increment_out();

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
                    result = Some(id);
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
                result = Some(id);
            }
        }
    }
    result
}

// We are borrowing a dyn box as we don't want to pass ownership.
#[allow(clippy::borrowed_box, clippy::too_many_arguments)]
pub(crate) fn send_event(
    pipelines: &mut [(TremorURL, pipeline::Addr)],
    preprocessors: &mut Preprocessors,
    codec: &mut Box<dyn Codec>,
    metrics_reporter: &mut RampReporter,
    ingest_ns: &mut u64,
    origin_uri: &tremor_pipeline::EventOriginUri,
    id: u64,
    data: Vec<u8>,
) {
    task::block_on(async {
        if let Ok(data) = handle_pp(preprocessors, ingest_ns, data) {
            for d in data {
                match codec.decode(d, *ingest_ns) {
                    Ok(Some(data)) => {
                        transmit_event(
                            pipelines,
                            metrics_reporter,
                            data,
                            *ingest_ns,
                            origin_uri.clone(),
                            id,
                        )
                        .await;
                    }
                    Ok(None) => (),
                    Err(e) => {
                        metrics_reporter.increment_error();
                        error!("[Codec] {}", e);
                    }
                }
            }
        } else {
            // record preprocessor failures too
            metrics_reporter.increment_error();
        };
    })
}

pub(crate) enum PipeHandlerResult {
    Terminate,
    Idle,
    Normal,
    Cb(CBAction, Ids),
}

// Handles pipeline connections for an onramp
pub(crate) async fn handle_pipelines(
    triggered: bool,
    rx: &Receiver<onramp::Msg>,
    pipelines: &mut Vec<(TremorURL, pipeline::Addr)>,
    metrics_reporter: &mut RampReporter,
) -> Result<PipeHandlerResult> {
    if pipelines.is_empty() || triggered {
        let msg = rx.recv().await?;
        handle_pipelines_msg(msg, pipelines, metrics_reporter).await
    } else if rx.is_empty() {
        Ok(PipeHandlerResult::Normal)
    } else {
        let msg = rx.recv().await?;
        handle_pipelines_msg(msg, pipelines, metrics_reporter).await
    }
}
// Handles pipeline connections for an onramp

pub(crate) async fn handle_pipelines_msg(
    msg: onramp::Msg,
    pipelines: &mut Vec<(TremorURL, pipeline::Addr)>,
    metrics_reporter: &mut RampReporter,
) -> Result<PipeHandlerResult> {
    if pipelines.is_empty() {
        match msg {
            onramp::Msg::Connect(ps) => {
                for p in &ps {
                    if p.0 == *METRICS_PIPELINE {
                        metrics_reporter.set_metrics_pipeline(p.clone());
                    } else {
                        pipelines.push(p.clone());
                    }
                }
                Ok(PipeHandlerResult::Idle)
            }
            onramp::Msg::Disconnect { tx, .. } => {
                tx.send(true).await?;
                Ok(PipeHandlerResult::Terminate)
            }
            onramp::Msg::Cb(cb, ids) => Ok(PipeHandlerResult::Cb(cb, ids)),
        }
    } else {
        match msg {
            onramp::Msg::Connect(mut ps) => {
                pipelines.append(&mut ps);
                Ok(PipeHandlerResult::Normal)
            }
            onramp::Msg::Disconnect { id, tx } => {
                pipelines.retain(|(pipeline, _)| pipeline != &id);
                if pipelines.is_empty() {
                    tx.send(true).await?;
                    Ok(PipeHandlerResult::Terminate)
                } else {
                    tx.send(false).await?;
                    Ok(PipeHandlerResult::Normal)
                }
            }
            onramp::Msg::Cb(cb, ids) => Ok(PipeHandlerResult::Cb(cb, ids)),
        }
    }
}
