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

//! Tremor event processing pipeline

#![forbid(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate, clippy::missing_errors_doc)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rental;

use crate::errors::{Error, ErrorKind, Result};
use crate::op::prelude::*;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use op::trickle::select::WindowImpl;
use petgraph::algo::is_cyclic_directed;
use petgraph::dot::{Config, Dot};
use petgraph::graph::{self, NodeIndex};
use petgraph::visit::EdgeRef;
use serde::Serialize;
use simd_json::prelude::*;
use simd_json::{json, BorrowedValue, OwnedValue};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::iter::{self, Iterator};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

use tremor_script::prelude::*;
use tremor_script::query::StmtRentalWrapper;

/// Pipeline Configuration
pub mod config;
/// Pipeline Errors
pub mod errors;
#[macro_use]
mod macros;
pub(crate) mod op;
/// Tools to turn tremor query into pipelines
pub mod query;
use op::EventAndInsights;

pub use op::{ConfigImpl, InitializableOperator, Operator};
pub use tremor_script::prelude::EventOriginUri;
pub(crate) type PortIndexMap =
    HashMap<(NodeIndex, Cow<'static, str>), Vec<(NodeIndex, Cow<'static, str>)>>;
pub(crate) type ExecPortIndexMap =
    HashMap<(usize, Cow<'static, str>), Vec<(usize, Cow<'static, str>)>>;
/// A lookup function to used to look up operators
pub type NodeLookupFn = fn(
    config: &NodeConfig,
    uid: u64,
    defn: Option<StmtRentalWrapper>,
    node: Option<StmtRentalWrapper>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode>;
pub(crate) type NodeMap = HashMap<Cow<'static, str>, NodeIndex>;

lazy_static! {
    /// Function registory for the pipeline to look up functions
    // We wrap the registry in a mutex so that we can add functions from the outside
    // if required.
    pub static ref FN_REGISTRY: Mutex<Registry> = {
        let registry: Registry = tremor_script::registry();
        Mutex::new(registry)
    };
}

pub(crate) fn common_cow(s: &str) -> Cow<'static, str> {
    macro_rules! cows {
        ($target:expr, $($cow:expr),*) => {
            match $target {
                $($cow => $cow.into()),*,
                _ => Cow::Owned($target.to_string()),
            }
        };
    }
    cows!(s, "in", "out", "err", "main")
}

/// A non yet compiled pipeline
#[derive(Clone, Debug)]
pub struct Pipeline {
    /// ID of the Pipeline
    pub id: config::ID,
    pub(crate) inputs: HashMap<Cow<'static, str>, NodeIndex>,
    pub(crate) port_indexes: PortIndexMap,
    pub(crate) outputs: Vec<NodeIndex>,
    /// Configuration of the pipeline
    pub config: config::Pipeline,
    pub(crate) nodes: NodeMap,
    pub(crate) graph: ConfigGraph,
}

/// Type of nodes
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum NodeKind {
    /// An input, this is the one end of the graph
    Input,
    /// An output, this is the other end of the graph
    Output,
    /// An operator
    Operator,
}

/// A circuit breaker action
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, simd_json_derive::Serialize)]
pub enum CBAction {
    /// The circuit breaker is triggerd and should break
    Trigger,
    /// The circuit breaker is restored and should work again
    Restore,
    /// Acknowledge delivery of messages up to a given ID.
    /// All messages prior to and including  this will be considered delivered.
    Ack,
    /// Fail backwards to a given ID
    /// All messages after and including this will be considered non delivered
    Fail,
}
/// IDs for covering multiple event sources. We use a vector to represent
/// this as this:
/// * Simplifies cloning (represented  inconsecutive memory) :sob:
/// * Allows easier serialisation/deserialisation
/// * We don't ever expect a huge number of sources at the same time
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, simd_json_derive::Serialize)]
pub struct Ids(Vec<(u64, u64)>);

impl fmt::Display for Ids {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut i = self.0.iter();
        if let Some((oid, eid)) = i.next() {
            write!(f, "{}: {}", oid, eid)?
        }
        for (oid, eid) in i {
            write!(f, ", {}: {}", oid, eid)?
        }
        Ok(())
    }
}

impl Ids {
    /// Fetches the registered eid for a given source
    pub fn get(&self, uid: u64) -> Option<u64> {
        self.0
            .iter()
            .find_map(|(u, v)| if *u == uid { Some(*v) } else { None })
    }
    /// Adds a Id to the id map
    pub fn add(&mut self, uri: Option<tremor_script::EventOriginUri>, e_id: u64) {
        if let Some(uri) = uri {
            self.add_id(uri.uid, e_id)
        }
    }

    fn add_id(&mut self, u_id: u64, e_id: u64) {
        for (cur_uid, cur_eid) in &mut self.0 {
            if u_id == *cur_uid {
                if *cur_eid < e_id {
                    *cur_eid = e_id;
                }
                return;
            }
        }
        self.0.push((u_id, e_id))
    }
    /// Creates a new Id map
    pub fn new(u_id: u64, e_id: u64) -> Self {
        let mut m = Vec::with_capacity(4);
        m.push((u_id, e_id));
        Self(m)
    }
    /// Merges two id sets, ensures we only ever track the largest id for each
    /// source
    pub fn merge(&mut self, other: Self) {
        for (uid, eid) in other.0 {
            self.add_id(uid, eid)
        }
    }
}

#[cfg(test)]
impl From<u64> for Ids {
    fn from(eid: u64) -> Self {
        Self::new(0, eid)
    }
}

/// A tremor event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, simd_json_derive::Serialize)]
pub struct Event {
    /// The event ID
    pub id: Ids,
    /// The event Data
    pub data: tremor_script::LineValue,
    /// Nanoseconds at when the event was ingested
    pub ingest_ns: u64,
    /// URI to identify the origin of th event
    pub origin_uri: Option<tremor_script::EventOriginUri>,
    /// The kind of the event
    pub kind: Option<SignalKind>,
    /// If this event is batched (containing multiple events itself)
    pub is_batch: bool,
    /// Circuit breaker action
    pub cb: Option<CBAction>,
    /// Metadata for operators
    pub op_meta: BTreeMap<u64, OwnedValue>,
}

impl Event {
    /// Creates a new ack insight from the event, consums the `op_meta` and
    /// `origin_uri` of the event may return None if no insight is needed
    pub fn insight_ack(&mut self) -> Option<Event> {
        let mut e = Event::cb_ack(self.ingest_ns, self.id.clone());
        std::mem::swap(&mut e.op_meta, &mut self.op_meta);
        std::mem::swap(&mut e.origin_uri, &mut self.origin_uri);
        Some(e)
    }

    /// Creates a new fail insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    pub fn insight_fail(&mut self) -> Option<Event> {
        let mut e = Event::cb_fail(self.ingest_ns, self.id.clone());
        std::mem::swap(&mut e.op_meta, &mut self.op_meta);
        std::mem::swap(&mut e.origin_uri, &mut self.origin_uri);
        Some(e)
    }

    /// Creates a restore insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    pub fn insight_restore(&mut self) -> Option<Event> {
        let mut e = Event::cb_restore(self.ingest_ns);
        std::mem::swap(&mut e.op_meta, &mut self.op_meta);
        std::mem::swap(&mut e.origin_uri, &mut self.origin_uri);
        Some(e)
    }

    /// Creates a trigger insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    pub fn insight_trigger(&mut self) -> Option<Event> {
        let mut e = Event::cb_trigger(self.ingest_ns);
        std::mem::swap(&mut e.op_meta, &mut self.op_meta);
        std::mem::swap(&mut e.origin_uri, &mut self.origin_uri);
        Some(e)
    }

    /// allows to iterate over the values and metadatas
    /// in an event, if it is batched this can be multiple
    /// otherwise it's a singular event
    pub fn value_meta_iter(&self) -> ValueMetaIter {
        ValueMetaIter {
            event: self,
            idx: 0,
        }
    }
    /// Creates a new event to restore a CB
    pub fn cb_restore(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: Some(CBAction::Restore),
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    pub fn cb_trigger(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: Some(CBAction::Trigger),
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    pub fn cb_ack(ingest_ns: u64, id: Ids) -> Self {
        Event {
            ingest_ns,
            id,
            cb: Some(CBAction::Ack),
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    pub fn cb_fail(ingest_ns: u64, id: Ids) -> Self {
        Event {
            ingest_ns,
            id,
            cb: Some(CBAction::Fail),
            ..Event::default()
        }
    }
}

/// Iterator over the event value and metadata
/// if the event is a batch this will allow iterating
/// over all the batched events
pub struct ValueMetaIter<'value> {
    event: &'value Event,
    idx: usize,
}

impl<'value> Iterator for ValueMetaIter<'value> {
    type Item = (&'value Value<'value>, &'value Value<'value>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .as_array()
                .and_then(|arr| arr.get(self.idx))
                .and_then(|e| Some((e.get("data")?.get("value")?, e.get("data")?.get("meta")?)));
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = self.event.data.suffix();
            self.idx += 1;
            Some((&v.value(), &v.meta()))
        } else {
            None
        }
    }
}

impl Event {
    /// Iterate over the values in an event
    /// this will result in multiple entries
    /// if the event was batched otherwise
    /// have only a single element
    pub fn value_iter(&self) -> ValueIter {
        ValueIter {
            event: self,
            idx: 0,
        }
    }
}

/// Iterator over the values of an event
pub struct ValueIter<'value> {
    event: &'value Event,
    idx: usize,
}

impl<'value> Iterator for ValueIter<'value> {
    type Item = &'value BorrowedValue<'value>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .as_array()
                .and_then(|arr| arr.get(self.idx))
                .and_then(|e| e.get("data")?.get("value"));
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = &self.event.data.suffix().value();
            self.idx += 1;
            Some(v)
        } else {
            None
        }
    }
}

/// The kind of signal this is
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, simd_json_derive::Serialize)]
pub enum SignalKind {
    // Lifecycle
    /// Init singnal    
    Init,
    /// Shutdown Signal
    Shutdown,
    // Pause, TODO debug trace
    // Resume, TODO debug trace
    // Step, TODO ( into, over, to next breakpoint )
    /// Control
    Control,
    /// Periodic Tick
    Tick,
}

/// Configuration for a node
#[derive(Debug, Clone, PartialOrd, Eq)]
pub struct NodeConfig {
    pub(crate) id: Cow<'static, str>,
    pub(crate) kind: NodeKind,
    pub(crate) op_type: String,
    pub(crate) config: config::ConfigMap,
    pub(crate) defn: Option<Arc<StmtRentalWrapper>>,
    pub(crate) node: Option<Arc<StmtRentalWrapper>>,
}

impl NodeConfig {
    /// Creates a `NodeConfig` from a config struct
    pub fn from_config<C, I>(id: I, config: C) -> Result<Self>
    where
        C: Serialize,
        Cow<'static, str>: From<I>,
    {
        let config = serde_yaml::to_vec(&config)?;

        Ok(NodeConfig {
            id: id.into(),
            kind: NodeKind::Operator,
            op_type: "".into(),
            config: serde_yaml::from_slice(&config)?,
            defn: None,
            node: None,
        })
    }
}

// We ignore stmt on equality and hasing as they're only
// carried through for implementation purposes not part
// if the identiy of a node

impl PartialEq for NodeConfig {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.kind == other.kind
            && self.op_type == other.op_type
            && self.config == other.config
    }
}

impl std::hash::Hash for NodeConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.kind.hash(state);
        self.op_type.hash(state);
        self.config.hash(state);
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
    uid: u64,
}

impl Operator for OperatorNode {
    fn on_event(
        &mut self,
        _uid: u64,
        port: &str,
        state: &mut Value<'static>,
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
        tags: HashMap<Cow<'static, str>, Value<'static>>,
        timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        self.op.metrics(tags, timestamp)
    }

    fn skippable(&self) -> bool {
        self.op.skippable()
    }
}

fn factory(node: &NodeConfig) -> Result<Box<dyn InitializableOperator>> {
    use op::debug::EventHistoryFactory;
    use op::generic::{BatchFactory, CounterFactory};
    use op::grouper::BucketGrouperFactory;
    use op::identity::PassthroughFactory;
    use op::qos::{BackpressureFactory, PercentileFactory, RoundRobinFactory, WalFactory};
    use op::runtime::TremorFactory;
    let name_parts: Vec<&str> = node.op_type.split("::").collect();
    let factory = match name_parts.as_slice() {
        ["passthrough"] => PassthroughFactory::new_boxed(),
        ["debug", "history"] => EventHistoryFactory::new_boxed(),
        ["runtime", "tremor"] => TremorFactory::new_boxed(),
        ["grouper", "bucket"] => BucketGrouperFactory::new_boxed(),
        ["generic", "batch"] => BatchFactory::new_boxed(),
        ["generic", "backpressure"] => {
            error!("The generic::backpressure operator is depricated, please use qos::backpressure instread.");
            BackpressureFactory::new_boxed()
        }
        ["generic", "counter"] => CounterFactory::new_boxed(),
        ["qos", "backpressure"] => BackpressureFactory::new_boxed(),
        ["qos", "roundrobin"] => RoundRobinFactory::new_boxed(),
        ["qos", "wal"] => WalFactory::new_boxed(),
        ["qos", "percentile"] => PercentileFactory::new_boxed(),
        [namespace, name] => {
            return Err(ErrorKind::UnknownOp((*namespace).to_string(), (*name).to_string()).into());
        }
        _ => return Err(ErrorKind::UnknownNamespace(node.op_type.clone()).into()),
    };
    Ok(factory)
}

fn operator(node: &NodeConfig) -> Result<Box<dyn Operator + 'static>> {
    factory(&node)?.from_node(node)
}

// TODO We need an actual operator registry ...
// because we really don't care here.
// We allow needless pass by value since the function type
// and it's other implementations use the values and require
// them passed
/// A default 'registry' function for built in operators
#[allow(clippy::implicit_hasher, clippy::needless_pass_by_value)]
pub fn buildin_ops(
    node: &NodeConfig,
    uid: u64,
    _defn: Option<StmtRentalWrapper>,
    _nobody_knows: Option<StmtRentalWrapper>,
    _windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode> {
    // Resolve from registry

    Ok(OperatorNode {
        uid,
        id: node.id.clone(),
        kind: node.kind,
        op_type: node.op_type.clone(),
        op: operator(node)?,
    })
}
impl NodeConfig {
    pub(crate) fn to_op(
        &self,
        uid: u64,
        resolver: NodeLookupFn,
        defn: Option<StmtRentalWrapper>,
        node: Option<StmtRentalWrapper>,
        window: Option<HashMap<String, WindowImpl>>,
    ) -> Result<OperatorNode> {
        resolver(&self, uid, defn, node, window)
    }
}

type Weightless = ();
pub(crate) type ConfigGraph = graph::DiGraph<NodeConfig, Weightless>;

/// Builds an pipelinefrom a configuration
pub fn build_pipeline(config: config::Pipeline) -> Result<Pipeline> {
    let mut graph = ConfigGraph::new();
    let mut nodes: HashMap<Cow<'static, str>, _> = HashMap::new(); // <String, NodeIndex>
    let mut inputs: HashMap<Cow<'static, str>, _> = HashMap::new();
    let mut outputs = Vec::new();
    let mut port_indexes: PortIndexMap = HashMap::new();
    for stream in &config.interface.inputs {
        let stream = common_cow(&stream);
        let id = graph.add_node(NodeConfig {
            id: stream.clone(),
            kind: NodeKind::Input,
            op_type: "passthrough".to_string(),
            config: None, // passthrough has no config
            defn: None,
            node: None,
        });
        nodes.insert(stream.clone(), id);
        inputs.insert(stream, id);
    }

    for node in &config.nodes {
        let node_id = common_cow(&node.id);
        let id = graph.add_node(NodeConfig {
            id: node_id.clone(),
            kind: NodeKind::Operator,
            op_type: node.node_type.clone(),
            config: node.config.clone(),
            defn: None,
            node: None,
        });
        nodes.insert(node_id, id);
    }

    for stream in &config.interface.outputs {
        let stream = common_cow(&stream);
        let id = graph.add_node(NodeConfig {
            id: stream.clone(),
            kind: NodeKind::Output,
            op_type: "passthrough".into(),
            config: None, // passthrough has no config
            defn: None,
            node: None,
        });
        nodes.insert(stream, id);
        outputs.push(id);
    }

    // Add metrics output port
    let id = graph.add_node(NodeConfig {
        id: "metrics".into(),
        kind: NodeKind::Output,
        op_type: "passthrough".to_string(),
        config: None, // passthrough has no config
        defn: None,
        node: None,
    });
    nodes.insert("metrics".into(), id);
    outputs.push(id);

    for (from, tos) in &config.links {
        for to in tos {
            let from_idx = nodes[&from.id];
            let to_idx = nodes[&to.id];

            let from_tpl = (from_idx, from.port.clone());
            let to_tpl = (to_idx, to.port.clone());
            match port_indexes.get_mut(&from_tpl) {
                None => {
                    port_indexes.insert(from_tpl, vec![to_tpl]);
                }
                Some(ports) => {
                    ports.push(to_tpl);
                }
            }
            graph.add_edge(from_idx, to_idx, ());
        }
    }

    if is_cyclic_directed(&graph) {
        Err(ErrorKind::CyclicGraphError(format!(
            "{:?}",
            Dot::with_config(&graph, &[Config::EdgeNoLabel])
        ))
        .into())
    } else {
        Ok(Pipeline {
            id: config.id.clone(),
            graph,
            nodes,
            inputs,
            outputs,
            config,
            port_indexes,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct NodeMetrics {
    inputs: HashMap<Cow<'static, str>, u64>,
    outputs: HashMap<Cow<'static, str>, u64>,
}

impl NodeMetrics {
    fn to_value(
        &self,
        metric_name: &str,
        tags: &mut HashMap<Cow<'static, str>, Value<'static>>,
        timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        let mut res = Vec::with_capacity(self.inputs.len() + self.outputs.len());
        for (k, v) in &self.inputs {
            tags.insert("direction".into(), "input".into());
            tags.insert("port".into(), Value::from(k.clone()));
            //TODO: This is ugly
            res.push(
                json!({
                    "measurement": metric_name,
                    "tags": tags,
                    "fields": {
                        "count": v
                    },
                    "timestamp": timestamp
                })
                .into(),
            )
        }
        for (k, v) in &self.outputs {
            tags.insert("direction".into(), "output".into());
            tags.insert("port".into(), Value::from(k.clone()));
            //TODO: This is ugly
            res.push(
                json!({
                    "measurement": metric_name,
                    "tags": tags,
                    "fields": {
                        "count": v
                    },
                    "timestamp": timestamp
                })
                .into(),
            )
        }
        Ok(res)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct State {
    // for operator node-level state (ordered in the same way as nodes in the executable graph)
    ops: Vec<Value<'static>>,
}

/// An executable graph, this is the executable
/// form of a pipeline
#[derive(Debug)]
pub struct ExecutableGraph {
    /// ID of the graph
    pub id: String,
    graph: Vec<OperatorNode>,
    state: State,
    inputs: HashMap<Cow<'static, str>, usize>,
    stack: Vec<(usize, Cow<'static, str>, Event)>,
    signalflow: Vec<usize>,
    contraflow: Vec<usize>,
    port_indexes: ExecPortIndexMap,
    metrics: Vec<NodeMetrics>,
    metrics_idx: usize,
    last_metrics: u64,
    metric_interval: Option<u64>,
    /// snot
    pub insights: Vec<(usize, Event)>,
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
                    if let Some(count) = unsafe { self.metrics.get_unchecked_mut(idx) }
                        .outputs
                        .get_mut(out_port)
                    {
                        *count += 1;
                    } else {
                        unsafe { self.metrics.get_unchecked_mut(idx) }
                            .outputs
                            .insert(out_port.clone(), 1);
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
        mut tags: HashMap<Cow<'static, str>, Value<'static>>,
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
                let len = outgoing.len();
                for (idx, in_port) in outgoing.iter().take(len - 1) {
                    if let Some(count) = unsafe { self.metrics.get_unchecked_mut(*idx) }
                        .inputs
                        .get_mut(in_port)
                    {
                        *count += 1;
                    } else {
                        unsafe { self.metrics.get_unchecked_mut(*idx) }
                            .inputs
                            .insert(in_port.clone(), 1);
                    }
                    self.stack.push((*idx, in_port.clone(), event.clone()));
                }
                let (idx, in_port) = unsafe { outgoing.get_unchecked(len - 1) };
                if let Some(count) = unsafe { self.metrics.get_unchecked_mut(*idx) }
                    .inputs
                    .get_mut(in_port)
                {
                    *count += 1;
                } else {
                    unsafe { self.metrics.get_unchecked_mut(*idx) }
                        .inputs
                        .insert(in_port.clone(), 1);
                }
                self.stack.push((*idx, in_port.clone(), event))
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

impl Pipeline {
    /// Print the pipeline as a .dot file
    pub fn to_dot(&self) -> String {
        let mut res = "digraph{\n".to_string();
        let mut edges = String::new();
        for nx in self.graph.node_indices() {
            let n = &self.graph[nx];
            res.push_str(format!(r#"  {:?}[label="{}"]"#, nx.index(), n.id).as_str());
            res.push('\n');
            for e in self.graph.edges(nx) {
                edges.push_str(format!(r#"  {}->{}"#, nx.index(), e.target().index()).as_str());
                edges.push('\n');
            }
        }
        res.push_str(edges.as_str());
        res.push('}');
        res
    }

    /// Turns a pipeline into its executable form
    pub fn to_executable_graph(
        &self,
        uid: &mut u64,
        resolver: NodeLookupFn,
    ) -> Result<ExecutableGraph> {
        let mut i2pos = HashMap::new();
        let mut graph = Vec::new();
        // Nodes that handle contraflow
        let mut contraflow = Vec::new();
        // Nodes that handle signals
        let mut signalflow = Vec::new();
        for (i, nx) in self.graph.node_indices().enumerate() {
            *uid += 1;
            i2pos.insert(nx, i);
            let op = self.graph[nx].to_op(*uid, resolver, None, None, None)?;
            if op.handles_contraflow() {
                contraflow.push(i);
            }
            if op.handles_signal() {
                signalflow.push(i);
            }
            graph.push(op);
        }
        // since contraflow is the reverse we need to reverse it.
        contraflow.reverse();
        //pub type PortIndexMap = HashMap<(NodeIndex, String), Vec<(NodeIndex, String)>>;

        let mut port_indexes = HashMap::new();
        for ((i1, s1), connections) in &self.port_indexes {
            let connections = connections
                .iter()
                .map(|(i, s)| (i2pos[&i], s.clone()))
                .collect();
            port_indexes.insert((i2pos[&i1], s1.clone()), connections);
        }

        let mut inputs = HashMap::new();
        for (k, idx) in &self.inputs {
            inputs.insert(k.clone(), i2pos[&idx]);
        }

        let metric_interval = self.config.metrics_interval_s.map(|s| s * 1_000_000_000);
        Ok(ExecutableGraph {
            metrics: iter::repeat(NodeMetrics::default())
                .take(graph.len())
                .collect(),
            stack: Vec::with_capacity(graph.len()),
            id: self.id.clone(),
            metrics_idx: i2pos[&self
                .nodes
                .get("metrics")
                .ok_or_else(|| Error::from("metrics node missing"))?],
            last_metrics: 0,
            state: State {
                ops: iter::repeat(Value::null()).take(graph.len()).collect(),
            },
            graph,
            inputs,
            port_indexes,
            contraflow,
            signalflow,
            metric_interval,
            insights: Vec::new(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use serde_yaml;
    use simd_json::json;
    use std::fs::File;
    use std::io::BufReader;

    fn slurp(file: &str) -> config::Pipeline {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).expect("failed to read config")
    }

    #[test]
    fn distsys_exec() {
        // FIXME check/fix and move to integration tests - this is out of place as a unit test
        let c = slurp("tests/configs/distsys.yaml");
        let p: Pipeline = build_pipeline(c).expect("failed to build pipeline");
        let mut uid = 0;
        let mut e = p
            .to_executable_graph(&mut uid, buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            ingest_ns: 1,
            data: Value::from(json!({"snot": "badger"})).into(),
            ..Event::default()
        };
        let mut results = Vec::new();
        e.enqueue("in", event1, &mut results)
            .expect("failed to enqueue event");
        dbg!(&results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "out");
        assert_eq!(results[0].1.data.suffix().meta()["class"], "default");
        dbg!(&e.metrics);
        // We ignore the first, and the last three nodes because:
        // * The first one is the input, handled separately
        assert_eq!(e.metrics[0].inputs.get("in"), None);
        assert_eq!(e.metrics[0].outputs.get("out"), Some(&1));
        // * The last-2 is the output, handled separately
        assert_eq!(e.metrics[e.metrics.len() - 3].inputs.get("in"), Some(&1));
        assert_eq!(e.metrics[e.metrics.len() - 3].outputs.get("out"), Some(&1));
        // * the last-1 is the error output
        // assert_eq!(e.metrics[e.metrics.len() - 2].inputs.get("in"), None);
        // assert_eq!(e.metrics[e.metrics.len() - 2].outputs.get("out"), None);
        // * last is the metrics output
        // assert_eq!(e.metrics[e.metrics.len() - 1].inputs.get("in"), None);
        // assert_eq!(e.metrics[e.metrics.len() - 1].outputs.get("out"), None);

        // Now for the normal case
        for m in &e.metrics[1..e.metrics.len() - 3] {
            assert_eq!(m.inputs["in"], 1);
            assert_eq!(m.outputs["out"], 1);
        }
    }

    #[test]
    fn simple_graph_exec() {
        let c = slurp("tests/configs/simple_graph.yaml");
        let p: Pipeline = build_pipeline(c).expect("failed to build pipeline");
        let mut uid = 0;
        let mut e = p
            .to_executable_graph(&mut uid, buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            id: Ids::new(0, 1),
            ingest_ns: 1,
            data: Value::null().into(),
            ..Event::default()
        };
        let mut results = Vec::new();
        e.enqueue("in", event1, &mut results)
            .expect("failed to enqueue");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "out");
        assert_eq!(results[0].1.id, Ids::new(0, 1));
    }

    #[test]
    fn complex_graph_exec() {
        let c = slurp("tests/configs/two_input_complex_graph.yaml");
        let p: Pipeline = build_pipeline(c).expect("failed to build pipeline");
        println!("{}", p.to_dot());
        let mut uid = 0;

        let mut e = p
            .to_executable_graph(&mut uid, buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            id: Ids::new(0, 1),
            ingest_ns: 1,
            ..Event::default()
        };
        let event2 = Event {
            id: Ids::new(0, 2),
            ingest_ns: 2,
            ..Event::default()
        };
        let mut results = Vec::new();
        e.enqueue("in1", event1, &mut results)
            .expect("failed to enqueue event");
        assert_eq!(results.len(), 6);
        for r in &results {
            assert_eq!(r.0, "out");
            assert_eq!(r.1.id, Ids::new(0, 1));
        }
        assert_eq!(
            results[0].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: branch(0: 1)",
                "evt: m1(0: 1)",
                "evt: combine(0: 1)"
            ]))
        );
        assert_eq!(
            results[1].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: branch(0: 1)",
                "evt: m1(0: 1)",
                "evt: m2(0: 1)",
                "evt: combine(0: 1)"
            ]))
        );
        assert_eq!(
            results[2].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: branch(0: 1)",
                "evt: m1(0: 1)",
                "evt: m2(0: 1)",
                "evt: m3(0: 1)",
                "evt: combine(0: 1)"
            ]))
        );
        assert_eq!(
            results[3].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: branch(0: 1)",
                "evt: m2(0: 1)",
                "evt: combine(0: 1)"
            ]))
        );
        assert_eq!(
            results[4].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: branch(0: 1)",
                "evt: m2(0: 1)",
                "evt: m3(0: 1)",
                "evt: combine(0: 1)"
            ]))
        );
        assert_eq!(
            results[5].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: branch(0: 1)",
                "evt: m3(0: 1)",
                "evt: combine(0: 1)"
            ]))
        );

        let mut results = Vec::new();
        e.enqueue("in2", event2, &mut results)
            .expect("failed to enqueue event");
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(r.0, "out");
            assert_eq!(r.1.id, Ids::new(0, 2));
        }
        assert_eq!(
            results[0].1.data.suffix().meta()["debug::history"],
            Value::from(json!(["evt: m1(0: 2)", "evt: combine(0: 2)"]))
        );
        assert_eq!(
            results[1].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: m1(0: 2)",
                "evt: m2(0: 2)",
                "evt: combine(0: 2)"
            ]))
        );
        assert_eq!(
            results[2].1.data.suffix().meta()["debug::history"],
            Value::from(json!([
                "evt: m1(0: 2)",
                "evt: m2(0: 2)",
                "evt: m3(0: 2)",
                "evt: combine(0: 2)"
            ]))
        );
    }

    #[test]
    fn load_simple() {
        let c = slurp("tests/configs/pipe.simple.yaml");
        let p: Pipeline = build_pipeline(c).expect("failed to build pipeline");
        let l = p.config.links.iter().next().expect("no links");
        assert_eq!(
            (&IN, vec![OUT]),
            (&l.0.id, l.1.iter().map(|u| u.id.clone()).collect())
        );
    }
}
