// Copyright 2018-2019, Wayfair GmbH
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

#![forbid(warnings)]
#![recursion_limit = "1024"]
#![cfg_attr(
    feature = "cargo-clippy",
    deny(
        clippy::all,
        clippy::result_unwrap_used,
        clippy::option_unwrap_used,
        clippy::unnecessary_unwrap,
        clippy::pedantic
    )
)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rental;

use crate::errors::*;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use op::trickle::select::WindowImpl;
use petgraph::algo::is_cyclic_directed;
use petgraph::dot::{Config, Dot};
use petgraph::graph;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use serde::Serialize;
use simd_json::json;
use simd_json::value::ValueTrait;
use simd_json::BorrowedValue;
use std::borrow::Cow;
use std::iter;
use std::iter::Iterator;
use std::sync::Arc;
use std::sync::Mutex;
use tremor_script::prelude::*;
use tremor_script::query::*;
use url::Url;

pub mod config;
pub mod errors;
#[macro_use]
mod macros;
pub mod op;
pub mod query;

pub use op::{ConfigImpl, InitializableOperator, Operator};
pub type PortIndexMap =
    HashMap<(NodeIndex, Cow<'static, str>), Vec<(NodeIndex, Cow<'static, str>)>>;
pub type ExecPortIndexMap = HashMap<(usize, Cow<'static, str>), Vec<(usize, Cow<'static, str>)>>;
pub type NodeLookupFn = fn(
    config: &NodeConfig,
    defn: Option<StmtRentalWrapper>,
    node: Option<StmtRentalWrapper>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode>;
pub type NodeMap = HashMap<Cow<'static, str>, NodeIndex>;

lazy_static! {
    // We wrap the registry in a mutex so that we can add functions from the outside
    // if required.
    pub static ref FN_REGISTRY: Mutex<Registry> = {
        let registry: Registry = tremor_script::registry();
        Mutex::new(registry)
    };
}

pub fn common_cow(s: String) -> Cow<'static, str> {
    macro_rules! cows {
        ($target:expr, $($cow:expr),*) => {
            match $target.as_str() {
                $($cow => $cow.into()),*,
                _ => Cow::Owned($target),
            }
        };
    }
    cows!(s, "in", "out", "err", "main")
}

#[derive(Clone, Debug)]
pub struct Pipeline {
    pub id: config::ID,
    pub inputs: HashMap<Cow<'static, str>, NodeIndex>,
    pub port_indexes: PortIndexMap,
    pub outputs: Vec<NodeIndex>,
    pub config: config::Pipeline,
    pub nodes: NodeMap,
    pub graph: ConfigGraph,
}

pub type PipelineVec = Vec<Pipeline>;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum NodeKind {
    Input,
    Output,
    Operator,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: u64,
    pub data: tremor_script::LineValue,
    pub ingest_ns: u64,
    pub origin_uri: Option<Url>,
    pub kind: Option<SignalKind>,
    pub is_batch: bool,
}

impl Default for Event {
    fn default() -> Self {
        Self {
            id: 0,
            data: Value::Null.into(),
            ingest_ns: 0,
            origin_uri: None,
            kind: None,
            is_batch: false,
        }
    }
}

impl Event {
    pub fn value_meta_iter(&self) -> ValueMetaIter {
        ValueMetaIter {
            event: self,
            idx: 0,
        }
    }
}

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
                .value
                .as_array()
                .and_then(|arr| arr.get(self.idx))
                .and_then(|e| Some((e.get("data")?.get("value")?, e.get("data")?.get("meta")?)));
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = self.event.data.suffix();
            self.idx += 1;
            Some((&v.value, &v.meta))
        } else {
            None
        }
    }
}

impl Event {
    pub fn value_iter(&self) -> ValueIter {
        ValueIter {
            event: self,
            idx: 0,
        }
    }
}

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
                .value
                .as_array()
                .and_then(|arr| arr.get(self.idx))
                .and_then(|e| e.get("data")?.get("value"));
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = &self.event.data.suffix().value;
            self.idx += 1;
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum SignalKind {
    // Lifecycle
    Init,
    Shutdown,
    // Pause, TODO debug trace
    // Resume, TODO debug trace
    // Step, TODO ( into, over, to next breakpoint )
    Control,
}

#[derive(Debug, Clone, PartialOrd, Eq)]
pub struct NodeConfig {
    pub id: Cow<'static, str>,
    pub kind: NodeKind,
    pub op_type: String,
    pub config: config::ConfigMap,
    pub defn: Option<Arc<StmtRentalWrapper>>,
    pub node: Option<Arc<StmtRentalWrapper>>,
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

#[derive(Debug)]
pub struct OperatorNode {
    pub id: Cow<'static, str>,
    pub kind: NodeKind,
    pub op_type: String,
    pub op: Box<dyn Operator>,
}

impl Operator for OperatorNode {
    fn on_event(&mut self, port: &str, event: Event) -> Result<Vec<(Cow<'static, str>, Event)>> {
        self.op.on_event(port, event)
    }

    fn handles_signal(&self) -> bool {
        self.op.handles_signal()
    }
    fn on_signal(&mut self, signal: &mut Event) -> Result<Vec<(Cow<'static, str>, Event)>> {
        self.op.on_signal(signal)?;
        Ok(vec![])
    }

    fn handles_contraflow(&self) -> bool {
        self.op.handles_contraflow()
    }
    fn on_contraflow(&mut self, contraevent: &mut Event) {
        self.op.on_contraflow(contraevent)
    }
    fn metrics(
        &self,
        tags: HashMap<Cow<'static, str>, Value<'static>>,
        timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        self.op.metrics(tags, timestamp)
    }
}

// TODO We need an actual operator registry ...
// because we really don't care here.
// We allow needless pass by value since the function type
// and it's other implementations use the values and require
// them passed
#[allow(clippy::implicit_hasher, clippy::needless_pass_by_value)]
pub fn buildin_ops(
    node: &NodeConfig,
    _defn: Option<StmtRentalWrapper>,
    _nobody_knows: Option<StmtRentalWrapper>,
    _windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode> {
    // Resolve from registry

    use op::debug::EventHistoryFactory;
    use op::generic::{BackpressureFactory, BatchFactory};
    use op::grouper::BucketGrouperFactory;
    use op::identity::PassthroughFactory;
    use op::runtime::TremorFactory;
    let name_parts: Vec<&str> = node.op_type.split("::").collect();
    let factory = match name_parts.as_slice() {
        ["passthrough"] => PassthroughFactory::new_boxed(),
        ["debug", "history"] => EventHistoryFactory::new_boxed(),
        ["runtime", "tremor"] => TremorFactory::new_boxed(),
        ["grouper", "bucket"] => BucketGrouperFactory::new_boxed(),
        ["generic", "batch"] => BatchFactory::new_boxed(),
        ["generic", "backpressure"] => BackpressureFactory::new_boxed(),
        [namespace, name] => {
            return Err(ErrorKind::UnknownOp(namespace.to_string(), name.to_string()).into());
        }
        _ => return Err(ErrorKind::UnknownNamespace(node.op_type.clone()).into()),
    };
    Ok(OperatorNode {
        id: node.id.clone(),
        kind: node.kind,
        op_type: node.op_type.clone(),
        op: factory.from_node(node)?,
    })
}

impl NodeConfig {
    pub fn to_op(
        &self,
        resolver: NodeLookupFn,
        defn: Option<StmtRentalWrapper>,
        node: Option<StmtRentalWrapper>,
        window: Option<HashMap<String, WindowImpl>>,
    ) -> Result<OperatorNode> {
        resolver(&self, defn, node, window)
    }
}

#[derive(Debug)]
pub enum Edge {
    Link(config::OutputPort, config::InputPort),
    InputStreamLink(config::PipelineInputPort, config::InputPort),
    OutputStreamLink(config::OutputPort, config::PipelineOutputPort),
    PassthroughStream(String, String),
}

type Weightless = ();
pub type ConfigGraph = graph::DiGraph<NodeConfig, Weightless>;
pub type OperatorGraph = graph::DiGraph<OperatorNode, Weightless>;

pub fn build_pipeline(config: config::Pipeline) -> Result<Pipeline> {
    let mut graph = ConfigGraph::new();
    let mut nodes: HashMap<Cow<'static, str>, _> = HashMap::new(); // <String, NodeIndex>
    let mut inputs: HashMap<Cow<'static, str>, _> = HashMap::new();
    let mut outputs = Vec::new();
    let mut port_indexes: PortIndexMap = HashMap::new();
    for stream in &config.interface.inputs {
        let stream = common_cow(stream.clone());
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
        let node_id = common_cow(node.id.clone());
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
        let stream = common_cow(stream.clone());
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
pub struct NodeMetrics {
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
            tags.insert("port".into(), Value::String(k.clone()));
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
            tags.insert("port".into(), Value::String(k.clone()));
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

#[derive(Debug)]
pub struct ExecutableGraph {
    pub id: String,
    pub graph: Vec<OperatorNode>,
    pub inputs: HashMap<Cow<'static, str>, usize>,
    pub stack: Vec<(usize, Cow<'static, str>, Event)>,
    pub signalflow: Vec<usize>,
    pub contraflow: Vec<usize>,
    pub port_indexes: ExecPortIndexMap,
    pub metrics: Vec<NodeMetrics>,
    pub metrics_idx: usize,
    pub last_metrics: u64,
    pub metric_interval: Option<u64>,
}

pub type Returns = Vec<(Cow<'static, str>, Event)>;
impl ExecutableGraph {
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
                tags.insert("pipeline".into(), common_cow(self.id.clone()).into());
                self.enqueue_metrics("events", tags, event.ingest_ns);
                self.last_metrics = event.ingest_ns;
            }
        }
        self.stack
            .push((self.inputs[stream_name], "in".into(), event));
        self.run(returns)
    }

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
                // TODO: Do we want to fail on here or do something different?
                // got to discuss this.
                let res = node.on_event(&port, event)?;
                for (out_port, _) in &res {
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
                self.enqueue_events(idx, res);
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
                        "in".into(),
                        Event {
                            id: 0,
                            data: LineValue::new(vec![], |_| ValueAndMeta {
                                value,
                                meta: Value::from(Object::default()),
                            }),
                            ingest_ns: timestamp,
                            // TODO update this to point to tremor instance producing the metrics?
                            origin_uri: None,
                            kind: None,
                            is_batch: false,
                        },
                    ));
                }
            }
            if let Ok(metrics) = m.to_value(&metric_name, &mut tags, timestamp) {
                for value in metrics {
                    self.stack.push((
                        self.metrics_idx,
                        "in".into(),
                        Event {
                            id: 0,
                            data: LineValue::new(vec![], |_| ValueAndMeta {
                                value,
                                meta: Value::from(Object::default()),
                            }),
                            ingest_ns: timestamp,
                            // TODO update this to point to tremor instance producing the metrics?
                            origin_uri: None,
                            kind: None,
                            is_batch: false,
                        },
                    ));
                }
            }
        }
    }

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

    pub fn contraflow(&mut self, mut insight: Event) -> Event {
        for idx in &self.contraflow {
            let op = unsafe { self.graph.get_unchecked_mut(*idx) }; // We know this exists
            op.on_contraflow(&mut insight);
        }
        insight
    }

    pub fn enqueue_signal(&mut self, signal: Event, returns: &mut Returns) -> Result<()> {
        self.signalflow(signal)?;
        self.run(returns)?;
        Ok(())
    }

    pub fn signalflow(&mut self, mut signal: Event) -> Result<()> {
        for idx in 0..self.signalflow.len() {
            let i = self.signalflow[idx];
            let res = {
                let op = unsafe { self.graph.get_unchecked_mut(i) }; // We know this exists
                op.on_signal(&mut signal)?
            };
            self.enqueue_events(i, res);
            // We shouldn't call run in signal flow it should just enqueue
            // self.run(returns)?
        }
        Ok(())
    }
}

impl Pipeline {
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

    pub fn to_executable_graph(&self, resolver: NodeLookupFn) -> Result<ExecutableGraph> {
        let mut i2pos = HashMap::new();
        let mut graph = Vec::new();
        // Nodes that handle contraflow
        let mut contraflow = Vec::new();
        // Nodes that handle signals
        let mut signalflow = Vec::new();
        for (i, nx) in self.graph.node_indices().enumerate() {
            i2pos.insert(nx, i);
            let op = self.graph[nx].to_op(resolver, None, None, None)?;
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
            metrics_idx: i2pos[&self.nodes["metrics"]],
            last_metrics: 0,
            graph,
            inputs,
            port_indexes,
            contraflow,
            signalflow,
            metric_interval,
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
        let mut e = p
            .to_executable_graph(buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            origin_uri: None,
            data: Value::from(json!({"snot": "badger"})).into(),
            kind: None,
        };
        let mut results = Vec::new();
        e.enqueue("in", event1, &mut results)
            .expect("failed to enqueue event");
        dbg!(&results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "out");
        assert_eq!(results[0].1.data.suffix().meta["class"], "default");
        dbg!(&e.metrics);
        // We ignore the first, and the last three nodes because:
        // * The first one is the input, handled seperately
        assert_eq!(e.metrics[0].inputs.get("in"), None);
        assert_eq!(e.metrics[0].outputs.get("out"), Some(&1));
        // * The last-2 is the output, handled seperately
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
        let mut e = p
            .to_executable_graph(buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            origin_uri: None,
            data: Value::Null.into(),
            kind: None,
        };
        let mut results = Vec::new();
        e.enqueue("in", event1, &mut results)
            .expect("failed to enqueue");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "out");
        assert_eq!(results[0].1.id, 1);
    }

    #[test]
    fn complex_graph_exec() {
        let c = slurp("tests/configs/two_input_complex_graph.yaml");
        let p: Pipeline = build_pipeline(c).expect("failed to build pipeline");
        println!("{}", p.to_dot());
        let mut e = p
            .to_executable_graph(buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            origin_uri: None,
            data: Value::Null.into(),
            kind: None,
        };
        let event2 = Event {
            is_batch: false,
            id: 2,
            ingest_ns: 2,
            origin_uri: None,
            data: Value::Null.into(),
            kind: None,
        };
        let mut results = Vec::new();
        e.enqueue("in1", event1, &mut results)
            .expect("failed to enqueue event");
        assert_eq!(results.len(), 6);
        for r in &results {
            assert_eq!(r.0, "out");
            assert_eq!(r.1.id, 1);
        }
        assert_eq!(
            results[0].1.data.suffix().meta["debug::history"],
            Value::from(json!(["evt: branch(1)", "evt: m1(1)", "evt: combine(1)"]))
        );
        assert_eq!(
            results[1].1.data.suffix().meta["debug::history"],
            Value::from(json!([
                "evt: branch(1)",
                "evt: m1(1)",
                "evt: m2(1)",
                "evt: combine(1)"
            ]))
        );
        assert_eq!(
            results[2].1.data.suffix().meta["debug::history"],
            Value::from(json!([
                "evt: branch(1)",
                "evt: m1(1)",
                "evt: m2(1)",
                "evt: m3(1)",
                "evt: combine(1)"
            ]))
        );
        assert_eq!(
            results[3].1.data.suffix().meta["debug::history"],
            Value::from(json!(["evt: branch(1)", "evt: m2(1)", "evt: combine(1)"]))
        );
        assert_eq!(
            results[4].1.data.suffix().meta["debug::history"],
            Value::from(json!([
                "evt: branch(1)",
                "evt: m2(1)",
                "evt: m3(1)",
                "evt: combine(1)"
            ]))
        );
        assert_eq!(
            results[5].1.data.suffix().meta["debug::history"],
            Value::from(json!(["evt: branch(1)", "evt: m3(1)", "evt: combine(1)"]))
        );

        let mut results = Vec::new();
        e.enqueue("in2", event2, &mut results)
            .expect("failed to enqueue event");
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(r.0, "out");
            assert_eq!(r.1.id, 2);
        }
        assert_eq!(
            results[0].1.data.suffix().meta["debug::history"],
            Value::from(json!(["evt: m1(2)", "evt: combine(2)"]))
        );
        assert_eq!(
            results[1].1.data.suffix().meta["debug::history"],
            Value::from(json!(["evt: m1(2)", "evt: m2(2)", "evt: combine(2)"]))
        );
        assert_eq!(
            results[2].1.data.suffix().meta["debug::history"],
            Value::from(json!([
                "evt: m1(2)",
                "evt: m2(2)",
                "evt: m3(2)",
                "evt: combine(2)"
            ]))
        );
    }

    #[test]
    fn load_simple() {
        let c = slurp("tests/configs/pipe.simple.yaml");
        let p: Pipeline = build_pipeline(c).expect("failed to build pipeline");
        let l = p.config.links.iter().next().expect("no links");
        assert_eq!(
            (&"in".into(), vec!["out".into()]),
            (&l.0.id, l.1.iter().map(|u| u.id.clone()).collect())
        );
    }
}
