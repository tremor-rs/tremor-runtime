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
    deny(clippy::all, clippy::result_unwrap_used, clippy::unnecessary_unwrap)
)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

use crate::errors::*;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use petgraph::algo::is_cyclic_directed;
use petgraph::dot::{Config, Dot};
use petgraph::graph;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use simd_json::json;
use simd_json::{BorrowedValue, OwnedValue};
use std::iter;
use std::iter::Iterator;
use std::sync::Mutex;
use tremor_script::{LineValue, Registry, TremorFnWrapper};

pub mod config;
pub mod errors;
#[macro_use]
mod macros;
mod op;

pub use op::runtime::tremor::TremorContext;
pub use op::{InitializableOperator, Operator};
pub type MetaValue = simd_json::value::owned::Value;
pub type MetaMap = simd_json::value::owned::Map;
pub type PortIndexMap = HashMap<(NodeIndex, String), Vec<(NodeIndex, String)>>;
pub type ExecPortIndexMap = HashMap<(usize, String), Vec<(usize, String)>>;
pub type NodeLookupFn = fn(node: &NodeConfig) -> Result<OperatorNode>;
pub type NodeMap = HashMap<String, NodeIndex>;

lazy_static! {
    // We wrap the registry in a mutex so that we can add functions from the outside
    // if required.
    pub static ref FN_REGISTRY: Mutex<Registry<TremorContext>> = {
        use tremor_script::registry::FResult;
        let mut registry: Registry<TremorContext> = tremor_script::registry();
        #[allow(unused_variables)]
        fn ingest_ns<'event>(ctx: &TremorContext, _args: &[&BorrowedValue<'event>]) -> FResult<BorrowedValue<'event>> {
            Ok(BorrowedValue::I64(ctx.ingest_ns as i64))
        }
        registry
            .insert(TremorFnWrapper {
                module: "system".to_owned(),
                name: "ingest_ns".to_string(),
                fun: ingest_ns,
                argc: 0
            });
        Mutex::new(registry)
    };
}

#[derive(Clone, Debug)]
pub struct Pipeline {
    pub id: config::ID,
    inputs: HashMap<String, NodeIndex>,
    port_indexes: PortIndexMap,
    outputs: Vec<NodeIndex>,
    pub config: config::Pipeline,
    nodes: NodeMap,
    graph: ConfigGraph,
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
    pub meta: MetaMap,
    pub value: tremor_script::LineValue,
    pub ingest_ns: u64,
    pub kind: Option<SignalKind>,
    pub is_batch: bool,
}

pub struct EventIter {
    current: Option<Box<EventIter>>,
    data: Vec<Event>,
}

impl Iterator for EventIter {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut iter) = &mut self.current {
            if let Some(v) = iter.next() {
                return Some(v);
            } else {
                self.current = None;
            }
        }
        let s = self.data.pop()?;
        if s.is_batch {
            self.current = Some(Box::new(s.into_iter()));
            return self.next();
        }
        Some(s)
    }
}

impl IntoIterator for Event {
    type Item = Event;
    type IntoIter = EventIter;
    fn into_iter(self) -> Self::IntoIter {
        if self.is_batch {
            self.value.rent(|j| {
                use tremor_script::Value;
                if let Value::Array(data) = j {
                    let data = data
                        .iter()
                        .filter_map(|e| {
                            // TODO: This is ugly
                            if let Ok(r) = simd_json::serde::from_borrowed_value(e.clone()) {
                                Some(r)
                            } else {
                                None
                            }
                        })
                        .rev()
                        .collect();
                    EventIter {
                        data,
                        current: None,
                    }
                } else {
                    EventIter {
                        data: vec![],
                        current: None,
                    }
                }
            })
        } else {
            EventIter {
                data: vec![self],
                current: None,
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SignalKind {
    // Lifecycle
    Init,
    Shutdown,
    // Pause, TODO debug trace
    // Resume, TODO debug trace
    // Step, TODO ( into, over, to next breakpoint )
    Control,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash)]
pub struct NodeConfig {
    id: String,
    kind: NodeKind,
    _type: String,
    config: config::ConfigMap,
}

#[derive(Debug)]
pub struct OperatorNode {
    id: String,
    kind: NodeKind,
    _type: String,
    op: Box<dyn Operator>,
}

impl Operator for OperatorNode {
    fn on_event(&mut self, port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        self.op.on_event(port, event)
    }

    fn handles_signal(&self) -> bool {
        self.op.handles_signal()
    }
    fn on_signal(&mut self, signal: &mut Event) -> Result<Vec<(String, Event)>> {
        self.op.on_signal(signal)?;
        Ok(vec![])
    }

    fn handles_contraflow(&self) -> bool {
        self.op.handles_contraflow()
    }
    fn on_contraflow(&mut self, contraevent: &mut Event) {
        self.op.on_contraflow(contraevent)
    }
    fn metrics(&self, tags: HashMap<String, String>, timestamp: u64) -> Result<Vec<OwnedValue>> {
        self.op.metrics(tags, timestamp)
    }
}

// TODO We need an actual operator registry ...
pub fn buildin_ops(node: &NodeConfig) -> Result<OperatorNode> {
    // Resolve from registry
    use op::debug::EventHistoryFactory;
    use op::generic::{BackpressureFactory, BatchFactory};
    use op::grouper::BucketGrouperFactory;
    use op::identity::PassthroughFactory;
    use op::runtime::TremorFactory;
    let name_parts: Vec<&str> = node._type.split("::").collect();
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
        _ => return Err(ErrorKind::UnknownNamespace(node._type.clone()).into()),
    };
    Ok(OperatorNode {
        id: node.id.clone(),
        kind: node.kind,
        _type: node._type.clone(),
        op: factory.from_node(node)?,
    })
}

impl NodeConfig {
    fn to_op(&self, resolver: NodeLookupFn) -> Result<OperatorNode> {
        resolver(&self)
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
    let mut nodes = HashMap::new(); // <String, NodeIndex>
    let mut inputs = HashMap::new();
    let mut outputs = Vec::new();
    let mut port_indexes: PortIndexMap = HashMap::new();
    for stream in &config.interface.inputs {
        let id = graph.add_node(NodeConfig {
            id: stream.clone(),
            kind: NodeKind::Input,
            _type: "passthrough".to_string(),
            config: None, // passthrough has no config
        });
        nodes.insert(stream.clone(), id);
        inputs.insert(stream.clone(), id);
    }

    for node in &config.nodes {
        let node_id = node.id.clone();
        let id = graph.add_node(NodeConfig {
            id: node_id.clone(),
            kind: NodeKind::Operator,
            _type: node.node_type.clone(),
            config: node.config.clone(),
        });
        nodes.insert(node_id, id);
    }

    for stream in &config.interface.outputs {
        let id = graph.add_node(NodeConfig {
            id: stream.clone(),
            kind: NodeKind::Output,
            _type: "passthrough".to_string(),
            config: None, // passthrough has no config
        });
        nodes.insert(stream.clone(), id);
        outputs.push(id);
    }

    // Add metrics output port
    let id = graph.add_node(NodeConfig {
        id: "metrics".to_string(),
        kind: NodeKind::Output,
        _type: "passthrough".to_string(),
        config: None, // passthrough has no config
    });
    nodes.insert("metrics".to_string(), id);
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
struct NodeMetrics {
    inputs: HashMap<String, u64>,
    outputs: HashMap<String, u64>,
}

impl NodeMetrics {
    fn to_value(
        &self,
        metric_name: &str,
        tags: &mut HashMap<String, String>,
        timestamp: u64,
    ) -> Result<Vec<OwnedValue>> {
        let mut res = Vec::with_capacity(self.inputs.len() + self.outputs.len());
        for (k, v) in &self.inputs {
            tags.insert("direction".to_owned(), "input".to_owned());
            tags.insert("port".to_owned(), k.to_owned());
            //TODO: This is ugly
            res.push(json!({
                "measurement": metric_name,
                "tags": tags,
                "fields": {
                    "count": v
                },
                "timestamp": timestamp
            }))
        }
        for (k, v) in &self.outputs {
            tags.insert("direction".to_owned(), "output".to_owned());
            tags.insert("port".to_owned(), k.to_owned());
            //TODO: This is ugly
            res.push(json!({
                "measurement": metric_name,
                "tags": tags,
                "fields": {
                    "count": v
                },
                "timestamp": timestamp
            }))
        }
        Ok(res)
    }
}

#[derive(Debug)]
pub struct ExecutableGraph {
    pub id: String,
    pub graph: Vec<OperatorNode>,
    pub inputs: HashMap<String, usize>,
    stack: Vec<(usize, String, Event)>,
    signalflow: Vec<usize>,
    contraflow: Vec<usize>,
    port_indexes: ExecPortIndexMap,
    metrics: Vec<NodeMetrics>,
    metrics_idx: usize,
    last_metrics: u64,
    metric_interval: Option<u64>,
}

type Returns = Vec<(String, Event)>;
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
                tags.insert("pipeline".to_string(), self.id.clone());
                self.enqueue_metrics("events".to_string(), tags, event.ingest_ns);
                self.last_metrics = event.ingest_ns;
            }
        }
        self.stack
            .push((self.inputs[stream_name], "in".to_string(), event));
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
            unreachable!()
        }
    }

    fn enqueue_metrics(
        &mut self,
        metric_name: String,
        mut tags: HashMap<String, String>,
        timestamp: u64,
    ) {
        for (i, m) in self.metrics.iter().enumerate() {
            tags.insert("node".to_owned(), unsafe {
                self.graph.get_unchecked(i).id.clone()
            });
            if let Ok(metrics) =
                unsafe { self.graph.get_unchecked(i) }.metrics(tags.clone(), timestamp)
            {
                for v in metrics {
                    self.stack.push((
                        self.metrics_idx,
                        "in".to_owned(),
                        Event {
                            id: 0,
                            meta: MetaMap::default(),
                            value: LineValue::new(Box::new(vec![]), |_| v.into()),
                            ingest_ns: timestamp,
                            kind: None,
                            is_batch: false,
                        },
                    ));
                }
            }
            if let Ok(metrics) = m.to_value(&metric_name, &mut tags, timestamp) {
                for v in metrics {
                    self.stack.push((
                        self.metrics_idx,
                        "in".to_owned(),
                        Event {
                            id: 0,
                            meta: MetaMap::default(),
                            value: LineValue::new(Box::new(vec![]), |_| v.into()),
                            ingest_ns: timestamp,
                            kind: None,
                            is_batch: false,
                        },
                    ));
                }
            }
        }
    }

    fn enqueue_events(&mut self, idx: usize, events: Vec<(String, Event)>) {
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

    pub fn signalflow(&mut self, mut signal: Event, returns: &mut Returns) -> Result<()> {
        for idx in 0..self.signalflow.len() {
            let i = self.signalflow[idx];
            let res = {
                let op = unsafe { self.graph.get_unchecked_mut(i) }; // We know this exists
                op.on_signal(&mut signal)?
            };
            self.enqueue_events(i, res);
            self.run(returns)?
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
            let op = self.graph[nx].to_op(resolver)?;
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
        let p = build_pipeline(c).expect("failed to build pipeline");
        let mut e = p
            .to_executable_graph(buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: HashMap::new(),
            value: json!({"snot": "badger"}).into(),
            kind: None,
        };
        let mut results = Vec::new();
        e.enqueue("in", event1, &mut results)
            .expect("failed to enqueue event");
        dbg!(&results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "out");
        //if let simd_json::borrowed::Value::Object(value) = results[0].1.value.suffix() {
        //    assert_eq!(value["class"], "default");
        //}
        assert_eq!(results[0].1.meta["class"], "default");
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
        let p = build_pipeline(c).expect("failed to build pipeline");
        let mut e = p
            .to_executable_graph(buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: HashMap::new(),
            value: OwnedValue::Null.into(),
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
        let p = build_pipeline(c).expect("failed to build pipeline");
        println!("{}", p.to_dot());
        let mut e = p
            .to_executable_graph(buildin_ops)
            .expect("failed to build executable graph");
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: HashMap::new(),
            value: OwnedValue::Null.into(),
            kind: None,
        };
        let event2 = Event {
            is_batch: false,
            id: 2,
            ingest_ns: 2,
            meta: HashMap::new(),
            value: OwnedValue::Null.into(),
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
            results[0].1.meta["debug::history"],
            json!(["evt: branch(1)", "evt: m1(1)", "evt: combine(1)"])
        );
        assert_eq!(
            results[1].1.meta["debug::history"],
            json!([
                "evt: branch(1)",
                "evt: m1(1)",
                "evt: m2(1)",
                "evt: combine(1)"
            ])
        );
        assert_eq!(
            results[2].1.meta["debug::history"],
            json!([
                "evt: branch(1)",
                "evt: m1(1)",
                "evt: m2(1)",
                "evt: m3(1)",
                "evt: combine(1)"
            ])
        );
        assert_eq!(
            results[3].1.meta["debug::history"],
            json!(["evt: branch(1)", "evt: m2(1)", "evt: combine(1)"])
        );
        assert_eq!(
            results[4].1.meta["debug::history"],
            json!([
                "evt: branch(1)",
                "evt: m2(1)",
                "evt: m3(1)",
                "evt: combine(1)"
            ])
        );
        assert_eq!(
            results[5].1.meta["debug::history"],
            json!(["evt: branch(1)", "evt: m3(1)", "evt: combine(1)"])
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
            results[0].1.meta["debug::history"],
            json!(["evt: m1(2)", "evt: combine(2)"])
        );
        assert_eq!(
            results[1].1.meta["debug::history"],
            json!(["evt: m1(2)", "evt: m2(2)", "evt: combine(2)"])
        );
        assert_eq!(
            results[2].1.meta["debug::history"],
            json!(["evt: m1(2)", "evt: m2(2)", "evt: m3(2)", "evt: combine(2)"])
        );
    }

    #[test]
    fn load_simple() {
        let c = slurp("tests/configs/pipe.simple.yaml");
        let p = build_pipeline(c).expect("failed to build pipeline");
        let l = p.config.links.iter().next().expect("no links");
        assert_eq!(
            (&"in".to_string(), vec!["out".to_string()]),
            (&l.0.id, l.1.iter().map(|u| u.id.clone()).collect())
        );
    }

}
