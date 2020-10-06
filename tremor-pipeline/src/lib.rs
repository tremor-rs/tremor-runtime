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

//! Tremor event processing pipeline

#![forbid(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::forget_copy)] // FIXME needed for simd json derive
#![allow(clippy::missing_errors_doc)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rental;

use crate::errors::{ErrorKind, Result};
use crate::op::prelude::*;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use op::trickle::select::WindowImpl;
use petgraph::graph::{self, NodeIndex};
use serde::Serialize;
use simd_json::prelude::*;
use simd_json::{json, BorrowedValue, OwnedValue};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::iter::Iterator;
use std::str::FromStr;
use std::{
    fmt,
    mem::swap,
    sync::{Arc, Mutex},
};
use tremor_script::prelude::*;
use tremor_script::query::StmtRentalWrapper;

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

/// A configuration map
pub type ConfigMap = Option<serde_yaml::Value>;

/// A lookup function to used to look up operators
pub type NodeLookupFn = fn(
    config: &NodeConfig,
    uid: u64,
    defn: Option<StmtRentalWrapper>,
    node: Option<StmtRentalWrapper>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode>;

/// Stringified numeric key
/// from <https://github.com/serde-rs/json-benchmark/blob/master/src/prim_str.rs>
#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct PrimStr<T>(T)
where
    T: Copy + Ord + Display + FromStr;

impl<T> simd_json_derive::SerializeAsKey for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "{}", self.0)
    }
}

impl<T> simd_json_derive::Serialize for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "{}", self.0)
    }
}

impl<'input, T> simd_json_derive::Deserialize<'input> for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    #[inline]
    fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: std::marker::Sized + 'input,
    {
        if let Some(simd_json::Node::String(s)) = tape.next() {
            Ok(PrimStr(FromStr::from_str(s).map_err(|_e| {
                simd_json::Error::generic(simd_json::ErrorType::Serde("not a number".into()))
            })?))
        } else {
            Err(simd_json::Error::generic(
                simd_json::ErrorType::ExpectedNull,
            ))
        }
    }
}

/// Operator metadata
#[derive(
    Clone, Debug, Default, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct OpMeta(BTreeMap<PrimStr<u64>, OwnedValue>);

impl OpMeta {
    /// inserts a value
    pub fn insert(&mut self, key: u64, value: OwnedValue) -> Option<OwnedValue> {
        self.0.insert(PrimStr(key), value)
    }
    /// reads a value
    pub fn get(&mut self, key: u64) -> Option<&OwnedValue> {
        self.0.get(&PrimStr(key))
    }
    /// checks existance of a key
    #[must_use]
    pub fn contains_key(&self, key: u64) -> bool {
        self.0.contains_key(&PrimStr(key))
    }

    /// Merges two op meta maps, overwriting values with `other` on duplicates
    pub fn merge(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
    }
}

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
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub enum CBAction {
    /// Nothing of note
    None,
    /// The circuit breaker is triggerd and should break
    Close,
    /// The circuit breaker is restored and should work again
    Open,
    /// Acknowledge delivery of messages up to a given ID.
    /// All messages prior to and including  this will be considered delivered.
    Ack,
    /// Fail backwards to a given ID
    /// All messages after and including this will be considered non delivered
    Fail,
}
impl Default for CBAction {
    fn default() -> Self {
        Self::None
    }
}
impl From<bool> for CBAction {
    fn from(success: bool) -> Self {
        if success {
            CBAction::Ack
        } else {
            CBAction::Fail
        }
    }
}

impl CBAction {
    /// This is a Circuit Breaker related message
    #[must_use]
    pub fn is_cb(self) -> bool {
        self == CBAction::Close || self == CBAction::Open
    }
    /// This is a Guaranteed Delivery related message
    #[must_use]
    pub fn is_gd(self) -> bool {
        self == CBAction::Ack || self == CBAction::Fail
    }
}
/// IDs for covering multiple event sources. We use a vector to represent
/// this as this:
/// * Simplifies cloning (represented  inconsecutive memory) :sob:
/// * Allows easier serialisation/deserialisation
/// * We don't ever expect a huge number of sources at the same time
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
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
    #[must_use]
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
    #[must_use]
    pub fn new(u_id: u64, e_id: u64) -> Self {
        let mut m = Vec::with_capacity(4);
        m.push((u_id, e_id));
        Self(m)
    }
    /// Merges two id sets, ensures we only ever track the largest id for each
    /// source
    pub fn merge(&mut self, other: &Self) {
        for (uid, eid) in &other.0 {
            self.add_id(*uid, *eid)
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
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
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
    pub cb: CBAction,
    /// Metadata for operators
    pub op_meta: OpMeta,
    /// this needs transactional data
    pub transactional: bool,
}

impl Event {
    /// turns the event in an insight given it's success
    #[must_use]
    pub fn insight(self, success: bool) -> Event {
        Event {
            cb: success.into(),
            ingest_ns: self.ingest_ns,
            id: self.id,
            op_meta: self.op_meta,
            origin_uri: self.origin_uri,
            ..Event::default()
        }
    }

    /// Creates either a restore or trigger event
    #[must_use]
    pub fn restore_or_break(restore: bool, ingest_ns: u64) -> Self {
        if restore {
            Event::cb_restore(ingest_ns)
        } else {
            Event::cb_trigger(ingest_ns)
        }
    }

    /// Creates either a ack or fail event
    #[must_use]
    pub fn ack_or_fail(ack: bool, ingest_ns: u64, ids: Ids) -> Self {
        if ack {
            Event::cb_ack(ingest_ns, ids)
        } else {
            Event::cb_fail(ingest_ns, ids)
        }
    }
    /// Creates a new ack insight from the event, consums the `op_meta` and
    /// `origin_uri` of the event may return None if no insight is needed
    pub fn insight_ack(&mut self) -> Event {
        let mut e = Event::cb_ack(self.ingest_ns, self.id.clone());
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// Creates a new fail insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    pub fn insight_fail(&mut self) -> Option<Event> {
        let mut e = Event::cb_fail(self.ingest_ns, self.id.clone());
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        Some(e)
    }

    /// Creates a restore insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    pub fn insight_restore(&mut self) -> Option<Event> {
        let mut e = Event::cb_restore(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        Some(e)
    }

    /// Creates a trigger insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    pub fn insight_trigger(&mut self) -> Option<Event> {
        let mut e = Event::cb_trigger(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        Some(e)
    }

    /// allows to iterate over the values and metadatas
    /// in an event, if it is batched this can be multiple
    /// otherwise it's a singular event
    #[must_use]
    pub fn value_meta_iter(&self) -> ValueMetaIter {
        ValueMetaIter {
            event: self,
            idx: 0,
        }
    }
    /// Creates a new event to restore a CB
    #[must_use]
    pub fn cb_restore(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CBAction::Open,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_trigger(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CBAction::Close,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_ack(ingest_ns: u64, id: Ids) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CBAction::Ack,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_fail(ingest_ns: u64, id: Ids) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CBAction::Fail,
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
    #[must_use]
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
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
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
    pub(crate) config: ConfigMap,
    pub(crate) defn: Option<Arc<StmtRentalWrapper>>,
    pub(crate) node: Option<Arc<StmtRentalWrapper>>,
}

impl Display for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            NodeKind::Input => write!(f, "--> {}", self.id),
            NodeKind::Output => write!(f, "{} -->", self.id),
            NodeKind::Operator => write!(f, "{}", self.id),
        }
    }
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
    #[cfg(feature = "bert")]
    use op::bert::{SequenceClassificationFactory, SummerizationFactory};
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
        #[cfg(feature = "bert")]
        ["bert", "sequence_classification"] => SequenceClassificationFactory::new_boxed(),
        #[cfg(feature = "bert")]
        ["bert", "summarization"] => SummerizationFactory::new_boxed(),
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

pub(crate) type ConfigGraph = graph::DiGraph<NodeConfig, u8>;

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
