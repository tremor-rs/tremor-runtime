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

use std::borrow::Cow;

use crate::{common_cow, errors::Result, op::prelude::IN, ExecPortIndexMap, NodeMetrics};
use crate::{op::EventAndInsights, Event, NodeKind, Operator};
use halfbrown::HashMap;
use simd_json::BorrowedValue;
use tremor_script::{LineValue, ValueAndMeta};

#[derive(Clone, Debug, Default)]
pub(crate) struct State {
    // for operator node-level state (ordered in the same way as nodes in the executable graph)
    ops: Vec<BorrowedValue<'static>>,
}

impl State {
    /// Creates a new Sate
    pub fn new(ops: Vec<BorrowedValue<'static>>) -> Self {
        Self { ops }
    }
}

/// An executable operator
#[derive(Debug)]
pub struct OperatorNode {
    /// ID of the operator
    pub id: Cow<'static, str>,
    /// Typoe of the operator
    pub kind: NodeKind,
    /// operator namepsace
    pub op_type: String,
    /// The executable operator
    pub op: Box<dyn Operator>,
    /// Tremor unique identifyer
    pub uid: u64,
}

impl Operator for OperatorNode {
    fn on_event(
        &mut self,
        _uid: u64,
        port: &str,
        state: &mut BorrowedValue<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        self.op.on_event(self.uid, port, state, event)
    }

    fn handles_signal(&self) -> bool {
        self.op.handles_signal()
    }
    fn on_signal(&mut self, _uid: u64, signal: &mut Event) -> Result<EventAndInsights> {
        self.op.on_signal(self.uid, signal)
    }

    fn handles_contraflow(&self) -> bool {
        self.op.handles_contraflow()
    }
    fn on_contraflow(&mut self, _uid: u64, contraevent: &mut Event) {
        self.op.on_contraflow(self.uid, contraevent)
    }

    fn metrics(
        &self,
        tags: HashMap<Cow<'static, str>, BorrowedValue<'static>>,
        timestamp: u64,
    ) -> Result<Vec<BorrowedValue<'static>>> {
        self.op.metrics(tags, timestamp)
    }

    fn skippable(&self) -> bool {
        self.op.skippable()
    }
}

/// An executable graph, this is the executable
/// form of a pipeline
#[derive(Debug)]
pub struct ExecutableGraph {
    /// ID of the graph
    pub id: String,
    pub(crate) graph: Vec<OperatorNode>,
    pub(crate) state: State,
    pub(crate) inputs: HashMap<Cow<'static, str>, usize>,
    pub(crate) stack: Vec<(usize, Cow<'static, str>, Event)>,
    pub(crate) signalflow: Vec<usize>,
    pub(crate) contraflow: Vec<usize>,
    pub(crate) port_indexes: ExecPortIndexMap,
    pub(crate) metrics: Vec<NodeMetrics>,
    pub(crate) metrics_idx: usize,
    pub(crate) last_metrics: u64,
    pub(crate) metric_interval: Option<u64>,
    /// snot
    pub insights: Vec<(usize, Event)>,
    /// the dot representation of the graph
    pub dot: String,
}

/// The return of a graph execution
pub type Returns = Vec<(Cow<'static, str>, Event)>;
impl ExecutableGraph {
    /// Tries to optimise a pipeline
    pub fn optimize(&mut self) -> Option<()> {
        let mut i = 0;
        while self.optimize_()? {
            i += 1;
            if i > 1000 {
                error!("Failed to optimise loop after 1000 iterations");
                return None;
            }
        }
        Some(())
    }
    fn optimize_(&mut self) -> Option<bool> {
        let mut did_chage = false;
        // remove skippable nodes from contraflow and signalflow
        self.contraflow = (*self.contraflow)
            .iter()
            .filter(|id| {
                !self
                    .graph
                    .get(**id)
                    .map(|n| n.skippable())
                    .unwrap_or_default()
            })
            .cloned()
            .collect();
        self.signalflow = (*self.signalflow)
            .iter()
            .filter(|id| {
                !self
                    .graph
                    .get(**id)
                    .map(|n| n.skippable())
                    .unwrap_or_default()
            })
            .cloned()
            .collect();

        let mut input_ids: Vec<usize> = Vec::new();

        // first we check the inputs, if an input points to a skippable
        // node and does not connect to more then one other node with the same
        // input name it can be removed.
        for (input_name, target) in &mut self.inputs.iter_mut() {
            let target_node = self.graph.get(*target)?;
            input_ids.push(*target);
            // the target of the input is skippable
            if target_node.skippable() {
                let mut next_nodes = self
                    .port_indexes
                    .iter()
                    .filter(|((from_id, _), _)| from_id == target);

                if let Some((_, dsts)) = next_nodes.next() {
                    // we only connect from one output
                    if next_nodes.next().is_none() && dsts.len() == 1 {
                        let (next_id, next_input) = dsts.get(0)?;
                        if next_input == input_name {
                            *target = *next_id;
                        }
                    }
                }
            }
        }
        // The id's of all nodes that are skippable
        let skippables: Vec<_> = self
            .graph
            .iter()
            .enumerate()
            .filter_map(|(id, e)| {
                if e.skippable() && e.kind == NodeKind::Operator {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        for skippable_id in &skippables {
            // We iterate over all the skippable ID's

            // We collect all the outputs that the skippable node
            // sends data to.
            // So of a passthrough sends data to node 4 and 5
            // 4 and 5 becomes the output
            let outputs: Vec<_> = self
                .port_indexes
                .iter()
                .filter_map(|((from_id, _), outputs)| {
                    if from_id == skippable_id {
                        Some(outputs)
                    } else {
                        None
                    }
                })
                .flatten()
                .cloned()
                .collect();

            // Find all nodes that connect to the skippable
            // so if node 1 and 7 connect to the passthrough
            // then this is going to be 1 and 7
            let inputs: Vec<_> = self
                .port_indexes
                .iter()
                .filter_map(|(from, connections)| {
                    if connections
                        .iter()
                        .any(|(target_id, _)| target_id == skippable_id)
                    {
                        Some(from)
                    } else {
                        None
                    }
                })
                .cloned()
                .collect();

            // We iterate over all nodes that connect to the skippable
            // we're handling.

            for i in inputs {
                // Take the nodes connections for the indexes
                let srcs: Vec<_> = self.port_indexes.remove(&i)?;
                let mut srcs1 = Vec::new();

                // We then iterate over all the destinations that input
                // node connects to
                for (src_id, src_port) in srcs {
                    if src_id == *skippable_id {
                        did_chage = true;
                        // If it is the skippable node replace this entry
                        // with all the outputs the skippable had
                        for o in &outputs {
                            srcs1.push(o.clone())
                        }
                    } else {
                        // Otherwise keep the connection untoucehd.
                        srcs1.push((src_id, src_port))
                    }
                }
                // Add the node back in
                self.port_indexes.insert(i, srcs1);
            }
        }
        Some(did_chage)
    }
    /// This is a performance critial function!
    pub fn enqueue(
        &mut self,
        stream_name: &str,
        event: Event,
        returns: &mut Returns,
    ) -> Result<()> {
        // Resolve the input stream or entrypoint for this enqueue operation
        if let Some(ival) = self.metric_interval {
            if event.ingest_ns - self.last_metrics > ival {
                let mut tags = HashMap::new();
                tags.insert("pipeline".into(), common_cow(&self.id).into());
                self.enqueue_metrics("events", tags, event.ingest_ns);
                self.last_metrics = event.ingest_ns;
            }
        }
        self.stack.push((self.inputs[stream_name], IN, event));
        self.run(returns)
    }

    #[inline]
    fn run(&mut self, returns: &mut Returns) -> Result<()> {
        while self.next(returns)? {}
        returns.reverse();
        Ok(())
    }

    #[inline]
    fn next(&mut self, returns: &mut Returns) -> Result<bool> {
        if let Some((idx, port, event)) = self.stack.pop() {
            // If we have emitted a signal event we got to handle it as a signal flow
            // the signal flow will
            if event.kind.is_some() {
                self.signalflow(event)?;
                return Ok(!self.stack.is_empty());
            }

            // count ingres
            let node = unsafe { self.graph.get_unchecked_mut(idx) };
            if node.kind == NodeKind::Output {
                returns.push((node.id.clone(), event));
            } else {
                let EventAndInsights { events, insights } =
                    node.on_event(0, &port, &mut self.state.ops[idx], event)?;

                for (out_port, _) in &events {
                    let metrics = unsafe { self.metrics.get_unchecked_mut(idx) };
                    if let Some(count) = metrics.outputs.get_mut(out_port) {
                        *count += 1;
                    } else {
                        metrics.outputs.insert(out_port.clone(), 1);
                    }
                }
                for insight in insights {
                    self.insights.push((idx, insight))
                }
                self.enqueue_events(idx, events);
            };
            Ok(!self.stack.is_empty())
        } else {
            error!("next was called on an empty graph stack, this should never happen");
            Ok(false)
        }
    }

    fn enqueue_metrics(
        &mut self,
        metric_name: &str,
        mut tags: HashMap<Cow<'static, str>, BorrowedValue<'static>>,
        timestamp: u64,
    ) {
        for (i, m) in self.metrics.iter().enumerate() {
            tags.insert("node".into(), unsafe {
                self.graph.get_unchecked(i).id.clone().into()
            });
            if let Ok(metrics) =
                unsafe { self.graph.get_unchecked(i) }.metrics(tags.clone(), timestamp)
            {
                for value in metrics {
                    self.stack.push((
                        self.metrics_idx,
                        IN,
                        Event {
                            data: LineValue::new(vec![], |_| ValueAndMeta::from(value)),
                            ingest_ns: timestamp,
                            // TODO update this to point to tremor instance producing the metrics?
                            origin_uri: None,
                            ..Event::default()
                        },
                    ));
                }
            }
            if let Ok(metrics) = m.to_value(&metric_name, &mut tags, timestamp) {
                for value in metrics {
                    self.stack.push((
                        self.metrics_idx,
                        IN,
                        Event {
                            data: LineValue::new(vec![], |_| ValueAndMeta::from(value)),
                            ingest_ns: timestamp,
                            // TODO update this to point to tremor instance producing the metrics?
                            origin_uri: None,
                            ..Event::default()
                        },
                    ));
                }
            }
        }
    }
    #[inline]
    fn enqueue_events(&mut self, idx: usize, events: Vec<(Cow<'static, str>, Event)>) {
        for (out_port, event) in events {
            if let Some(outgoing) = self.port_indexes.get(&(idx, out_port)) {
                if let Some((last, rest)) = outgoing.split_last() {
                    for (idx, in_port) in rest {
                        let metrics = unsafe { self.metrics.get_unchecked_mut(*idx) };

                        if let Some(count) = metrics.inputs.get_mut(in_port) {
                            *count += 1;
                        } else {
                            metrics.inputs.insert(in_port.clone(), 1);
                        }
                        self.stack.push((*idx, in_port.clone(), event.clone()));
                    }
                    let (idx, in_port) = last;
                    let metrics = unsafe { self.metrics.get_unchecked_mut(*idx) };
                    if let Some(count) = metrics.inputs.get_mut(in_port) {
                        *count += 1;
                    } else {
                        metrics.inputs.insert(in_port.clone(), 1);
                    }
                    self.stack.push((*idx, in_port.clone(), event))
                }
            }
        }
    }
    /// Enque a contraflow insight
    pub fn contraflow(&mut self, mut skip_to: Option<usize>, mut insight: Event) -> Event {
        for idx in &self.contraflow {
            if skip_to.is_none() {
                let op = unsafe { self.graph.get_unchecked_mut(*idx) }; // We know this exists
                op.on_contraflow(op.uid, &mut insight);
            } else if skip_to == Some(*idx) {
                skip_to = None
            }
        }
        insight
    }
    /// Enque a signal
    pub fn enqueue_signal(&mut self, signal: Event, returns: &mut Returns) -> Result<()> {
        if self.signalflow(signal)? {
            self.run(returns)?;
        }
        Ok(())
    }

    fn signalflow(&mut self, mut signal: Event) -> Result<bool> {
        let mut has_events = false;
        for idx in 0..self.signalflow.len() {
            let i = self.signalflow[idx];
            let EventAndInsights { events, insights } = {
                let op = unsafe { self.graph.get_unchecked_mut(i) }; // We know this exists
                op.on_signal(op.uid, &mut signal)?
            };
            for cf in insights {
                self.insights.push((i, cf));
            }
            has_events = has_events || !events.is_empty();
            self.enqueue_events(i, events);
            // We shouldn't call run in signal flow it should just enqueue
            // self.run(returns)?
        }
        Ok(has_events)
    }
}
