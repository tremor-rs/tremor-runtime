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
#![cfg_attr(feature = "cargo-clippy", deny(clippy::all))]

#[macro_use]
extern crate serde_derive;

use crate::errors::*;
use hashbrown::HashMap;
use lazy_static::lazy_static;
use petgraph::algo::is_cyclic_directed;
use petgraph::dot::{Config, Dot};
use petgraph::graph;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use serde_json;
use std::fmt;
use std::iter::Iterator;
use std::sync::Mutex;
use tremor_script::{tremor_fn, Registry};

pub mod config;
pub mod errors;
#[macro_use]
mod macros;
mod op;
mod utils;

pub use op::runtime::tremor::Context as TremorContext;
pub use op::{InitializableOperator, Operator};
pub type MetaValue = serde_json::Value;
pub type MetaMap = HashMap<String, MetaValue>;
pub type PortIndexMap = HashMap<(NodeIndex, String), Vec<(NodeIndex, String)>>;
pub type ExecPortIndexMap = HashMap<(usize, String), Vec<(usize, String)>>;
pub type NodeLookupFn = fn(node: &NodeConfig) -> Result<OperatorNode>;
pub type NodeMap = HashMap<String, NodeIndex>;

lazy_static! {
    // We wrap the registry in a mutex so that we can add functions from the outside
    // if required.
    pub static ref FN_REGISTRY: Mutex<Registry<op::runtime::tremor::Context>> = {
        use tremor_script::errors::*;
        let mut registry = tremor_script::registry();
        #[allow(unused_variables)]
        registry.insert(tremor_fn!(system::ingest_ns(ctx) {
            //Ok(serde_json::json!(ctx.ingest_ns.into()))
            Ok(serde_json::Value::Null)
        }));
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ValueType {
    Raw,  // Raw ( binary? )
    JSON, // UTF-8 encoded JSON
    Any,  // Opaque type
    Same, // Same as the input FIXME refactor? type-checking
    None, // Things end here
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: u64,
    pub meta: MetaMap,
    pub value: EventValue,
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
            if let EventValue::JSON(serde_json::Value::Array(data)) = self.value {
                let data = data
                    .into_iter()
                    .filter_map(|e| {
                        if let Ok(r) = serde_json::from_value(e) {
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
        } else {
            EventIter {
                data: vec![self],
                current: None,
            }
        }
    }
}
impl Event {
    // TODO
    pub fn is_type(&self, t: ValueType) -> bool {
        self.value.t() == t
    }
    pub fn map_raw_value<F>(mut self, location: &str, map_fn: F) -> Result<Self>
    where
        F: Fn(Vec<u8>) -> Result<EventValue>,
    {
        if let EventValue::Raw(r) = self.value {
            let v = map_fn(r)?;
            self.value = v;
            Ok(self)
        } else {
            type_error!(location.to_string(), self.value.t(), ValueType::Raw)
        }
    }
    pub fn map_json_value<F>(mut self, location: &str, map_fn: F) -> Result<Self>
    where
        F: Fn(serde_json::Value) -> Result<EventValue>,
    {
        if let EventValue::JSON(j) = self.value {
            let v = map_fn(j)?;
            self.value = v;
            Ok(self)
        } else {
            type_error!(location.to_string(), self.value.t(), ValueType::JSON)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventValue {
    Raw(Vec<u8>),
    JSON(serde_json::Value),
    None,
}

impl EventValue {
    pub fn t(&self) -> ValueType {
        match self {
            EventValue::Raw(_) => ValueType::Raw,
            EventValue::None => ValueType::None,
            EventValue::JSON(_) => ValueType::JSON,
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
    op: Box<Operator>,
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

#[derive(Debug)]
pub struct ExecutableGraph {
    pub graph: Vec<OperatorNode>,
    pub inputs: HashMap<String, usize>,
    stack: Vec<(usize, String, Event)>,
    signalflow: Vec<usize>,
    contraflow: Vec<usize>,
    port_indexes: ExecPortIndexMap,
}

impl ExecutableGraph {
    /// This is a performance critial function!
    pub fn enqueue(&mut self, stream_name: &str, event: Event) -> Result<Vec<(String, Event)>> {
        // Resolve the input stream or entrypoint for this enqueue operation
        self.stack
            .push((self.inputs[stream_name], "in".to_string(), event));
        self.run()
    }

    fn run(&mut self) -> Result<Vec<(String, Event)>> {
        let mut returns: Vec<(String, Event)> = Vec::with_capacity(self.graph.len());
        loop {
            if let Some((idx, port, event)) = self.stack.pop() {
                let res = {
                    let node = &mut self.graph[idx];
                    if node.kind == NodeKind::Output {
                        returns.push((node.id.clone(), event));
                        vec![]
                    } else {
                        // TODO: Do we want to fail on here or do something different?
                        // got to discuss this.
                        node.on_event(&port, event)?
                    }
                };
                self.enqueue_events(idx, res);
                if self.stack.is_empty() {
                    returns.reverse();
                    return Ok(returns);
                }
            } else {
                unreachable!()
            }
        }
    }

    fn enqueue_events(&mut self, idx: usize, events: Vec<(String, Event)>) {
        for (out_port, event) in events {
            if let Some(outgoing) = self.port_indexes.get(&(idx, out_port)) {
                let len = outgoing.len();
                for (idx, in_port) in outgoing.iter().take(len - 1) {
                    self.stack.push((*idx, in_port.clone(), event.clone()))
                }
                let (idx, in_port) = &outgoing[len - 1];
                self.stack.push((*idx, in_port.clone(), event))
            }
        }
    }

    pub fn contraflow(&mut self, mut insight: Event) -> Event {
        for idx in &self.contraflow {
            let op = &mut self.graph[*idx]; // We know this exists
            op.on_contraflow(&mut insight);
        }
        insight
    }

    pub fn signalflow(&mut self, mut signal: Event) -> Result<Vec<(String, Event)>> {
        let mut returns: Vec<(String, Event)> = Vec::with_capacity(self.graph.len());
        for idx in 0..self.contraflow.len() {
            let i = self.contraflow[idx];
            let res = {
                let op = &mut self.graph[i]; // We know this exists
                op.on_signal(&mut signal)?
            };
            self.enqueue_events(i, res);
            returns.append(&mut self.run()?)
        }
        Ok(returns)
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

        Ok(ExecutableGraph {
            stack: Vec::with_capacity(graph.len()),
            graph,
            inputs,
            port_indexes,
            contraflow,
            signalflow,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use serde_json::json;
    use serde_yaml;
    use std::fs::File;
    use std::io::BufReader;

    fn slurp(file: &str) -> config::Pipeline {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).unwrap()
    }

    #[test]
    fn distsys_exec() {
        let c = slurp("tests/configs/distsys.yaml");
        let p = build_pipeline(c).unwrap();
        let mut e = p.to_executable_graph(buildin_ops).unwrap();
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: HashMap::new(),
            value: EventValue::JSON(json!({"snot": "badger"})),
            kind: None,
        };
        let results = e.enqueue("in", event1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "out");
        assert_eq!(results[0].1.meta["class"], "default");
    }

    #[test]
    fn simple_graph_exec() {
        let c = slurp("tests/configs/simple_graph.yaml");
        let p = build_pipeline(c).unwrap();
        let mut e = p.to_executable_graph(buildin_ops).unwrap();
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: HashMap::new(),
            value: EventValue::Raw(r#"{"snot": "badger"}"#.as_bytes().to_vec()),
            kind: None,
        };
        let results = e.enqueue("in", event1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "out");
        assert_eq!(results[0].1.id, 1);
    }

    #[test]
    fn complex_graph_exec() {
        let c = slurp("tests/configs/two_input_complex_graph.yaml");
        let p = build_pipeline(c).unwrap();
        println!("{}", p.to_dot());
        let mut e = p.to_executable_graph(buildin_ops).unwrap();
        let event1 = Event {
            is_batch: false,
            id: 1,
            ingest_ns: 1,
            meta: HashMap::new(),
            value: EventValue::Raw(r#"{"snot": "badger"}"#.as_bytes().to_vec()),
            kind: None,
        };
        let event2 = Event {
            is_batch: false,
            id: 2,
            ingest_ns: 2,
            meta: HashMap::new(),
            value: EventValue::Raw(r#"{"snot": "badger"}"#.as_bytes().to_vec()),
            kind: None,
        };
        let results = e.enqueue("in1", event1).unwrap();
        assert_eq!(results.len(), 6);
        for i in 0..=5 {
            assert_eq!(results[i].0, "out");
            assert_eq!(results[i].1.id, 1);
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

        let results = e.enqueue("in2", event2).unwrap();
        assert_eq!(results.len(), 3);
        for i in 0..=2 {
            assert_eq!(results[i].0, "out");
            assert_eq!(results[i].1.id, 2);
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
        let config = slurp("tests/configs/pipe.simple.yaml");
        println!("{:?}", &config);
        let p = build_pipeline(config).unwrap();
        let l = p.config.links.iter().next().unwrap();
        assert_eq!(
            (&"in".to_string(), vec!["out".to_string()]),
            (&l.0.id, l.1.iter().map(|u| u.id.clone()).collect())
        );
    }

}
