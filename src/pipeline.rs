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
use crate::channel::{bounded, unbounded, Receiver, Sender, UnboundedReceiver};
use crate::errors::Result;
use futures::StreamExt;
use std::{collections::HashMap, fmt, time::Duration};
use tokio::task::{self, JoinHandle};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tremor_common::{
    alias, ids::OperatorIdGen, ports::Port, primerge::PriorityMerge, time::nanotime,
};
use tremor_pipeline::{errors::ErrorKind as PipelineErrorKind, ExecutableGraph};
use tremor_script::{ast::DeployEndpoint, highlighter::Dumb};
use tremor_system::{
    contraflow,
    controlplane::{self, CbAction},
    dataplane::{self, InputTarget, OutputTarget},
    event::Event,
    instance::State,
    pipeline, qsize,
};

const TICK_MS: u64 = 100;
type Inputs = halfbrown::HashMap<DeployEndpoint, (bool, InputTarget)>;
type Dests = halfbrown::HashMap<Port<'static>, Vec<(DeployEndpoint, OutputTarget)>>;
type EventSet = Vec<(Port<'static>, Event)>;

pub(crate) fn spawn(
    pipeline_alias: alias::Pipeline,
    config: &tremor_pipeline::query::Query,
    operator_id_gen: &mut OperatorIdGen,
) -> Result<pipeline::Addr> {
    let qsize = qsize();
    let mut pipeline = config.to_executable_graph(operator_id_gen)?;
    pipeline.optimize();

    let (tx, rx) = bounded::<Box<dataplane::Msg>>(qsize);
    // We use a unbounded channel for counterflow, while an unbounded channel seems dangerous
    // there is soundness to this.
    // The unbounded channel ensures that on counterflow we never have to block, or in other
    // words that sinks or pipelines sending data backwards always can progress passt
    // the sending.
    // This prevents a livelock where the sink is waiting for a full channel to send data to
    // the pipeline and the pipeline is waiting for a full channel to send data to the sink.
    // We prevent unbounded groth by two mechanisms:
    // 1) counterflow is ALWAYS and ONLY created in response to a message
    // 2) we always process counterflow prior to forward flow
    //
    // As long as we have counterflow messages to process, and channel size is growing we do
    // not process any forward flow. Without forwardflow we stave the counterflow ensuring that
    // the counterflow channel is always bounded by the forward flow in a 1:N relationship where
    // N is the maximum number of counterflow events a single event can trigger.
    // N is normally < 1.
    let (cf_tx, cf_rx) = unbounded::<contraflow::Msg>();
    let (mgmt_tx, mgmt_rx) = bounded::<controlplane::Msg>(qsize);

    let tick_handler = task::spawn(tick(tx.clone()));

    let addr = pipeline::Addr::new(tx, cf_tx, mgmt_tx, pipeline_alias.clone());

    task::spawn(pipeline_task(
        pipeline_alias,
        pipeline,
        rx,
        cf_rx,
        mgmt_rx,
        tick_handler,
    ));
    Ok(addr)
}

/// wrapper for all possible messages handled by the pipeline task
#[derive(Debug)]
pub(crate) enum AnyMsg {
    Flow(dataplane::Msg),
    Contraflow(contraflow::Msg),
    Mgmt(controlplane::Msg),
}

#[inline]
async fn send_events(eventset: &mut EventSet, dests: &mut Dests) -> Result<()> {
    for (output, event) in eventset.drain(..) {
        if let Some(destinations) = dests.get_mut(&output) {
            if let Some((last, rest)) = destinations.split_last_mut() {
                for (id, dest) in rest {
                    let port = id.port().to_string().into();
                    dest.send_event(port, event.clone()).await?;
                }
                let last_port = last.0.port().to_string().into();
                last.1.send_event(last_port, event).await?;
            }
        }
    }
    Ok(())
}

#[inline]
async fn send_signal(own_id: &alias::Pipeline, signal: Event, dests: &mut Dests) -> Result<()> {
    let mut destinations = dests.values_mut().flatten();
    let first = destinations.next();
    for (id, dest) in destinations {
        // if we are connected to ourselves we should not forward signals
        if matches!(dest, OutputTarget::Sink(_)) || id.alias() != own_id.pipeline_alias() {
            dest.send_signal(signal.clone()).await?;
        }
    }
    if let Some((id, dest)) = first {
        // if we are connected to ourselves we should not forward signals
        if matches!(dest, OutputTarget::Sink(_)) || id.alias() != own_id.pipeline_alias() {
            dest.send_signal(signal).await?;
        }
    }
    Ok(())
}

#[inline]
fn handle_insight(
    skip_to: Option<usize>,
    insight: Event,
    pipeline: &mut ExecutableGraph,
    inputs: &Inputs,
) {
    let insight = pipeline.contraflow(skip_to, insight);
    if insight.cb != CbAction::None {
        let mut input_iter = inputs.iter();
        let first = input_iter.next();
        let always_deliver = insight.cb.always_deliver();
        for (url, (input_is_transactional, input)) in input_iter {
            if always_deliver || *input_is_transactional {
                if let Err(e) = input.send_insight(insight.clone()) {
                    error!(
                        "[Pipeline::{}] failed to send insight to input: {} {}",
                        pipeline.id, e, url
                    );
                }
            }
        }
        if let Some((url, (input_is_transactional, input))) = first {
            if always_deliver || *input_is_transactional {
                if let Err(e) = input.send_insight(insight) {
                    error!(
                        "[Pipeline::{}] failed to send insight to input: {} {}",
                        pipeline.id, e, url
                    );
                }
            }
        }
    }
}

#[inline]
fn handle_insights(pipeline: &mut ExecutableGraph, onramps: &Inputs) {
    if !pipeline.insights.is_empty() {
        let mut insights = Vec::with_capacity(pipeline.insights.len());
        std::mem::swap(&mut insights, &mut pipeline.insights);
        for (skip_to, insight) in insights.drain(..) {
            handle_insight(Some(skip_to), insight, pipeline, onramps);
        }
    }
}

pub(crate) async fn tick(tick_tx: Sender<Box<dataplane::Msg>>) {
    let mut e = Event::signal_tick();
    while tick_tx
        .send(Box::new(dataplane::Msg::Signal(e.clone())))
        .await
        .is_ok()
    {
        tokio::time::sleep(Duration::from_millis(TICK_MS)).await;
        e.ingest_ns = nanotime();
    }
}

fn handle_cf_msg(msg: contraflow::Msg, pipeline: &mut ExecutableGraph, inputs: &Inputs) {
    match msg {
        contraflow::Msg::Insight(insight) => handle_insight(None, insight, pipeline, inputs),
    }
}

fn maybe_send(r: Result<()>) {
    if let Err(e) = r {
        error!("Failed to send : {}", e);
    }
}

/// Context for pipeline instances
///
/// currently only used for printing
struct PipelineContext {
    alias: alias::Pipeline,
}

impl std::fmt::Display for PipelineContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[Pipeline::{}]", &self.alias)
    }
}

impl From<&alias::Pipeline> for PipelineContext {
    fn from(alias: &alias::Pipeline) -> Self {
        Self {
            alias: alias.clone(),
        }
    }
}

impl From<alias::Pipeline> for PipelineContext {
    fn from(alias: alias::Pipeline) -> Self {
        Self { alias }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn pipeline_task(
    id: alias::Pipeline,
    mut pipeline: ExecutableGraph,
    rx: Receiver<Box<dataplane::Msg>>,
    cf_rx: UnboundedReceiver<contraflow::Msg>,
    mgmt_rx: Receiver<controlplane::Msg>,
    tick_handler: JoinHandle<()>,
) -> Result<()> {
    pipeline.id = id.to_string();

    let ctx = PipelineContext::from(&id);

    let mut dests: Dests = halfbrown::HashMap::new();
    let mut inputs: Inputs = halfbrown::HashMap::new();
    let mut eventset = Vec::new();

    let mut state: State = State::Initializing;

    info!("{ctx} Starting Pipeline.");

    let ff = ReceiverStream::new(rx).map(|e| AnyMsg::Flow(*e));
    let cf = UnboundedReceiverStream::new(cf_rx).map(AnyMsg::Contraflow);
    let mf = ReceiverStream::new(mgmt_rx).map(AnyMsg::Mgmt);

    // prioritize management flow over contra flow over forward event flow
    let mut s = PriorityMerge::new(mf, PriorityMerge::new(cf, ff));
    while let Some(msg) = s.next().await {
        match msg {
            AnyMsg::Contraflow(msg) => handle_cf_msg(msg, &mut pipeline, &inputs),
            AnyMsg::Flow(dataplane::Msg::Event { input, event }) => {
                match pipeline.enqueue(input.clone(), event, &mut eventset) {
                    Ok(()) => {
                        handle_insights(&mut pipeline, &inputs);
                        maybe_send(send_events(&mut eventset, &mut dests).await);
                    }
                    Err(e) => {
                        let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                            let script_error = tremor_script::errors::Error(script_kind, e.1);

                            Dumb::error_to_string(&script_error)
                                .unwrap_or_else(|e| format!(" {script_error}: {e}"))
                        } else {
                            format!(" {e}")
                        };
                        error!("{ctx} Error handling event:{err_str}");
                    }
                }
            }
            AnyMsg::Flow(dataplane::Msg::Signal(signal)) => {
                if let Err(e) = pipeline.enqueue_signal(signal.clone(), &mut eventset) {
                    let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                        let script_error = tremor_script::errors::Error(script_kind, e.1);
                        Dumb::error_to_string(&script_error)?
                    } else {
                        format!(" {e:?}")
                    };
                    error!("{ctx} Error handling signal: {err_str}");
                } else {
                    maybe_send(send_signal(&id, signal, &mut dests).await);
                    handle_insights(&mut pipeline, &inputs);
                    maybe_send(send_events(&mut eventset, &mut dests).await);
                }
            }
            AnyMsg::Mgmt(controlplane::Msg::ConnectInput {
                endpoint,
                port,
                tx,
                target,
                is_transactional,
            }) => {
                info!("{ctx} Connecting '{endpoint}' to port '{port}'");
                if !pipeline.input_exists(&port) {
                    error!("{ctx} Error connecting input pipeline '{port}' as it does not exist",);
                    if tx
                        .send(Err(anyhow::anyhow!("input port doesn't exist")))
                        .await
                        .is_err()
                    {
                        error!("{ctx} Error sending status report.");
                    }
                    continue;
                }
                inputs.insert(endpoint, (is_transactional, target));
                if tx.send(Ok(())).await.is_err() {
                    error!("{ctx} Status report sent.");
                }
            }

            AnyMsg::Mgmt(controlplane::Msg::ConnectOutput {
                port,
                endpoint,
                target,
                tx,
            }) => {
                info!("{ctx} Connecting port '{port}' to {endpoint}",);
                // add error statement for port out in pipeline, currently no error messages
                if !pipeline.output_exists(&port) {
                    error!("{ctx} Error connecting output pipeline {port}");
                    if tx
                        .send(Err(anyhow::anyhow!("output port doesn't exist")))
                        .await
                        .is_err()
                    {
                        error!("{ctx} Error sending status report.");
                    }
                    continue;
                }
                // notify other pipeline about a new input

                if tx.send(Ok(())).await.is_err() {
                    error!("{ctx} Status report sent.");
                }

                if let Some(output_dests) = dests.get_mut(&port) {
                    output_dests.push((endpoint, target));
                } else {
                    dests.insert(port, vec![(endpoint, target)]);
                }
            }
            AnyMsg::Mgmt(controlplane::Msg::Start) if state == State::Initializing => {
                // No-op
                state = State::Running;
            }
            AnyMsg::Mgmt(controlplane::Msg::Start) => {
                info!("{ctx} Ignoring Start Msg. Current state: {state}",);
            }
            AnyMsg::Mgmt(controlplane::Msg::Pause) if state == State::Running => {
                // No-op
                state = State::Paused;
            }
            AnyMsg::Mgmt(controlplane::Msg::Pause) => {
                info!("{ctx} Ignoring Pause Msg. Current state: {state}",);
            }
            AnyMsg::Mgmt(controlplane::Msg::Resume) if state == State::Paused => {
                // No-op
                state = State::Running;
            }
            AnyMsg::Mgmt(controlplane::Msg::Resume) => {
                info!("{ctx} Ignoring Resume Msg. Current state: {state}",);
            }
            AnyMsg::Mgmt(controlplane::Msg::Stop) => {
                info!("{ctx} Stopping...");
                break;
            }
            AnyMsg::Mgmt(controlplane::Msg::Inspect(tx)) => {
                use tremor_system::pipeline::report;
                let inputs: Vec<report::Input> = inputs
                    .iter()
                    .map(|(k, v)| report::Input::new(k, &v.1))
                    .collect::<Vec<_>>();
                let outputs: HashMap<String, Vec<report::Output>> = dests
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            v.iter().map(report::Output::from).collect::<Vec<_>>(),
                        )
                    })
                    .collect();

                let report = report::Status::new(state, inputs, outputs);
                if tx.send(report).await.is_err() {
                    error!("{ctx} Error sending status report.");
                }
            }
        }
    }
    // stop ticks
    tick_handler.abort();

    info!("{ctx} Stopped.");
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::errors::empty_error;
    use std::time::Instant;
    use tremor_common::{
        ids::Id as _,
        ids::SourceId,
        ports::{IN, OUT},
    };
    use tremor_script::{aggr_registry, lexer::Location, NodeMeta, FN_REGISTRY};
    use tremor_system::{
        connector::{sink, source},
        dataplane::{self, SignalKind},
        event::EventId,
        instance::State,
        pipeline::{report, OpMeta},
    };
    use tremor_value::Value;

    #[tokio::test(flavor = "multi_thread")]
    async fn report() -> Result<()> {
        let _: std::result::Result<_, _> = env_logger::try_init();
        let mut operator_id_gen = OperatorIdGen::new();
        let trickle = r#"select event from in into out;"#;
        let aggr_reg = aggr_registry();
        let query =
            tremor_pipeline::query::Query::parse(trickle, &*FN_REGISTRY.read()?, &aggr_reg)?;
        let addr = spawn(
            alias::Pipeline::new("report", "test-pipe1"),
            &query,
            &mut operator_id_gen,
        )?;
        let addr2 = spawn(
            alias::Pipeline::new("report", "test-pipe2"),
            &query,
            &mut operator_id_gen,
        )?;
        let addr3 = spawn(
            alias::Pipeline::new("report", "test-pipe3"),
            &query,
            &mut operator_id_gen,
        )?;
        println!("{addr:?}"); // coverage
        let yolo_mid = NodeMeta::new(Location::yolo(), Location::yolo());
        let (tx, mut rx) = bounded(1);
        // interconnect 3 pipelines
        addr.send_mgmt(controlplane::Msg::ConnectOutput {
            port: Port::Out,
            endpoint: DeployEndpoint::new("snot2", IN, &yolo_mid),
            tx,
            target: OutputTarget::Pipeline(Box::new(addr2.clone())),
        })
        .await?;
        rx.recv().await.ok_or_else(empty_error)??;
        let (tx, mut rx) = bounded(1);
        addr2
            .send_mgmt(controlplane::Msg::ConnectInput {
                port: Port::In,
                endpoint: DeployEndpoint::new("snot", OUT, &yolo_mid),
                tx,
                target: InputTarget::Pipeline(Box::new(addr.clone())),
                is_transactional: true,
            })
            .await?;
        rx.recv().await.ok_or_else(empty_error)??;
        let (tx, mut rx) = bounded(1);
        let msg = controlplane::Msg::ConnectOutput {
            tx,
            port: Port::Out,
            endpoint: DeployEndpoint::new("snot3", IN, &yolo_mid),
            target: OutputTarget::Pipeline(Box::new(addr3.clone())),
        };
        addr2.send_mgmt(msg).await?;
        rx.recv().await.ok_or_else(empty_error)??;
        let (tx, mut rx) = bounded(1);

        let msg = controlplane::Msg::ConnectInput {
            port: Port::In,
            endpoint: DeployEndpoint::new("snot2", OUT, &yolo_mid),
            tx,
            target: InputTarget::Pipeline(Box::new(addr2.clone())),
            is_transactional: false,
        };
        addr3.send_mgmt(msg).await?;
        rx.recv().await.ok_or_else(empty_error)??;
        // get a status report from every single one
        let (tx, mut rx) = bounded(1);
        addr.send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let mut report1 = rx.recv().await.ok_or_else(empty_error)?;
        assert!(report1.inputs().is_empty());
        let mut output1 = report1
            .outputs_mut()
            .remove("out")
            .expect("nothing at port `out`");
        assert!(report1.outputs().is_empty());
        assert_eq!(1, output1.len());
        let output1 = output1.pop().ok_or("no data")?;
        assert_eq!(output1, report::Output::pipeline("snot2", IN));
        addr2
            .send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let mut report2 = rx.recv().await.ok_or_else(empty_error)?;
        let input2 = report2.inputs().last().expect("no input at port in");
        assert_eq!(input2, &report::Input::pipeline("snot", OUT));
        let mut output2 = report2
            .outputs_mut()
            .remove("out")
            .expect("no outputs on out port");
        assert!(output2.is_empty());
        assert_eq!(1, output2.len());
        let output2 = output2.pop().ok_or("no data")?;
        assert_eq!(output2, report::Output::pipeline("snot3", IN));

        addr3
            .send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let report3 = rx.recv().await.ok_or_else(empty_error)?;
        assert!(report3.outputs().is_empty());
        let input3 = report3.inputs().last().expect("no inputs");
        assert_eq!(input3, &report::Input::pipeline("snot2", OUT));

        addr.stop().await?;
        addr2.stop().await?;
        addr3.stop().await?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)] // this is a test
    #[tokio::test(flavor = "multi_thread")]
    async fn pipeline_spawn() -> Result<()> {
        let _: std::result::Result<_, _> = env_logger::try_init();
        let mut operator_id_gen = OperatorIdGen::new();
        let trickle = r#"select event from in into out;"#;
        let aggr_reg = aggr_registry();
        let pipeline_id = alias::Pipeline::new("flow", "test-pipe");
        let query =
            tremor_pipeline::query::Query::parse(trickle, &*FN_REGISTRY.read()?, &aggr_reg)?;
        let addr = spawn(pipeline_id, &query, &mut operator_id_gen)?;

        let (tx, mut rx) = bounded(1);
        addr.send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let report = rx.recv().await.ok_or_else(empty_error)?;
        assert_eq!(State::Initializing, report.state());
        assert!(report.inputs().is_empty());
        assert!(report.outputs().is_empty());

        addr.start().await?;
        addr.send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let report = rx.recv().await.ok_or_else(empty_error)?;
        assert_eq!(State::Running, report.state());
        assert!(report.inputs().is_empty());
        assert!(report.outputs().is_empty());

        // connect a fake source as input
        let (source_tx, mut source_rx) = unbounded();
        let source_addr = source::Addr::new(source_tx);
        let target = InputTarget::Source(source_addr);
        let mid = NodeMeta::new(Location::yolo(), Location::yolo());
        let (tx, mut rx) = bounded(1);
        addr.send_mgmt(controlplane::Msg::ConnectInput {
            port: IN,
            endpoint: DeployEndpoint::new(&"source_01", OUT, &mid),
            tx,
            target,
            is_transactional: true,
        })
        .await?;
        rx.recv().await.ok_or_else(empty_error)??;

        let (tx, mut rx) = bounded(1);
        addr.send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let report = rx.recv().await.ok_or_else(empty_error)?;
        assert_eq!(1, report.inputs().len());
        assert_eq!(report::Input::source("source_01", OUT), report.inputs()[0]);

        // connect a fake sink as output
        let (sink_tx, mut sink_rx) = bounded(1);
        let sink_addr = sink::Addr::new(sink_tx);
        let target = OutputTarget::Sink(sink_addr);
        let (tx, mut rx) = bounded(1);
        addr.send_mgmt(controlplane::Msg::ConnectOutput {
            endpoint: DeployEndpoint::new(&"sink_01", IN, &mid),
            port: OUT,
            tx,
            target,
        })
        .await?;
        rx.recv().await.ok_or_else(empty_error)??;

        let (tx, mut rx) = bounded(1);
        addr.send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let report = rx.recv().await.ok_or_else(empty_error)?;
        assert_eq!(1, report.outputs().len());
        assert_eq!(
            Some(&vec![report::Output::sink("sink_01", IN)]),
            report.outputs().get(&OUT.to_string())
        );

        // send an event
        let event = Event {
            data: (Value::from(42_u64), Value::from(true)).into(),
            ..Event::default()
        };
        addr.send(Box::new(dataplane::Msg::Event { event, input: IN }))
            .await?;

        let mut sink_msg = sink_rx.recv().await.ok_or_else(empty_error)?;
        while let sink::Msg::Signal { .. } = sink_msg {
            sink_msg = sink_rx.recv().await.ok_or_else(empty_error)?;
        }
        match sink_msg {
            sink::Msg::Event { event, port: _ } => {
                assert_eq!(Value::from(42_usize), event.data.suffix().value());
                assert_eq!(Value::from(true), event.data.suffix().meta());
            }
            other => panic!("Expected Event, got: {other:?}"),
        }

        // send a signal
        addr.send(Box::new(dataplane::Msg::Signal(Event::signal_drain(
            SourceId::new(42),
        ))))
        .await?;

        let start = Instant::now();
        let mut sink_msg = sink_rx.recv().await.ok_or_else(empty_error)?;

        while !matches!(sink_msg, sink::Msg::Signal {
                signal:
                    Event {
                        kind: Some(SignalKind::Drain(id)),
                        ..
                    },
            } if id.id() == 42)
        {
            assert!(
                start.elapsed() < Duration::from_secs(4),
                "Timed out waiting for drain signal on the sink"
            );

            sink_msg = sink_rx.recv().await.ok_or_else(empty_error)?;
        }

        let event_id = EventId::from_id(1, 1, 1);
        addr.send_insight(Event::cb_ack(0, event_id.clone(), OpMeta::default()))?;
        let source_msg = source_rx.recv().await.ok_or_else(empty_error)?;
        if let source::Msg::Cb(cb_action, cb_id) = source_msg {
            assert_eq!(event_id, cb_id);
            assert_eq!(CbAction::Ack, cb_action);
        } else {
            panic!("Expected source::Msg::Cb, got: {source_msg:?}");
        }

        // test pause and resume
        addr.pause().await?;
        addr.send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let report = rx.recv().await.ok_or_else(empty_error)?;
        assert_eq!(State::Paused, report.state());
        assert_eq!(1, report.inputs().len());
        assert_eq!(1, report.outputs().len());

        addr.resume().await?;
        addr.send_mgmt(controlplane::Msg::Inspect(tx.clone()))
            .await?;
        let report = rx.recv().await.ok_or_else(empty_error)?;
        assert_eq!(State::Running, report.state());
        assert_eq!(1, report.inputs().len());
        assert_eq!(1, report.outputs().len());

        // finally stop
        addr.stop().await?;
        // verify we cannot send to the pipeline after it is stopped
        let start = Instant::now();
        while addr
            .send(Box::new(dataplane::Msg::Signal(Event::signal_tick())))
            .await
            .is_ok()
        {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "Pipeline didnt stop!"
            );

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
}
