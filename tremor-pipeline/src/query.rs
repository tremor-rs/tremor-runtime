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

use crate::config::{self, InputPort, OutputPort};
use crate::errors::*;
use crate::op;
use crate::op::trickle::select::WindowImpl;
use crate::OperatorNode;
use crate::{ConfigGraph, NodeConfig, NodeKind, PortIndexMap};
use halfbrown::HashMap;
use indexmap::IndexMap;
use petgraph::algo::is_cyclic_directed;
use petgraph::dot::{Config, Dot};
use simd_json::ValueTrait;
use std::sync::Arc;
use tremor_script::ast::Stmt;
use tremor_script::ast::{WindowDecl, WindowKind};
use tremor_script::errors::query_stream_not_defined;
use tremor_script::highlighter::Dumb as DumbHighlighter;
use tremor_script::{AggrRegistry, Registry, Value};

fn resolve_input_port(port: String) -> Result<InputPort> {
    let v: Vec<&str> = port.split('/').collect();
    match v.as_slice() {
        [id, port] => Ok(InputPort {
            id: id.to_string(),
            port: port.to_string(),
            had_port: true,
        }),
        [id] => Ok(InputPort {
            id: id.to_string(),
            port: "in".to_string(),
            had_port: false,
        }),
        _ => Err(ErrorKind::PipelineError(
            "Bad port syntax, needs to be <id>/<port> or <id> (where port becomes 'out')"
                .to_string(),
        )
        .into()),
    }
}

fn resolve_output_port(port: String) -> Result<OutputPort> {
    let v: Vec<&str> = port.split('/').collect();
    match v.as_slice() {
        [id, port] => Ok(OutputPort {
            id: id.to_string(),
            port: port.to_string(),
            had_port: true,
        }),
        [id] => Ok(OutputPort {
            id: id.to_string(),
            port: "out".to_string(),
            had_port: false,
        }),
        _ => Err(ErrorKind::PipelineError(
            "Bad port syntax, needs to be <id>/<port> or <id> (where port becomes 'out')"
                .to_string(),
        )
        .into()),
    }
}

fn window_decl_to_impl<'script>(d: &WindowDecl<'script>) -> Result<WindowImpl> {
    use op::trickle::select::*;
    match &d.kind {
        WindowKind::Sliding => Err("Sliding windows are not yet implemented".into()),
        WindowKind::Tumbling => {
            let interval = d
                .params
                .as_ref()
                .and_then(|p| p.get("interval"))
                .and_then(Value::as_u64)
                .ok_or_else(|| error_missing_config("interval"))?;
            Ok(TumblingWindowOnEventTime {
                size: interval,
                next_window: None,
            }
            .into())
        }
    }
}
#[derive(Clone, Debug)]
pub struct Query(pub tremor_script::query::Query);
impl From<tremor_script::query::Query> for Query {
    fn from(q: tremor_script::query::Query) -> Self {
        Query(q)
    }
}
impl Query {
    pub fn parse(script: &str, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self> {
        Ok(Query(tremor_script::query::Query::parse(
            script, reg, aggr_reg,
        )?))
    }
    pub fn to_pipe(&self) -> Result<crate::ExecutableGraph> {
        use crate::op::Operator;
        use crate::ExecutableGraph;
        use crate::NodeMetrics;
        use std::iter;

        let script = self.0.query.suffix();

        let mut pipe_graph = ConfigGraph::new();
        let mut pipe_ops = HashMap::new();
        let mut nodes = HashMap::new();
        let mut links: IndexMap<_, Vec<InputPort>> = IndexMap::new();
        let mut inputs = HashMap::new();
        let mut outputs: Vec<petgraph::graph::NodeIndex> = Vec::new();

        // FIXME compute public streams - do not hardcode
        let _in = "in".to_string();
        let id = pipe_graph.add_node(NodeConfig {
            id: _in.clone(),
            kind: NodeKind::Input,
            _type: "passthrough".to_string(),
            config: None,
            defn: None,
            node: None,
        });
        nodes.insert(_in.clone(), id);
        let op = pipe_graph[id].to_op(supported_operators, None, None, None)?;
        pipe_ops.insert(id, op);
        inputs.insert(_in, id);

        // FIXME compute public streams - do not hardcode
        let out = "out".to_string();
        let id = pipe_graph.add_node(NodeConfig {
            id: out.clone(),
            kind: NodeKind::Output,
            _type: "passthrough".to_string(),
            config: None,
            defn: None,
            node: None,
        });
        nodes.insert(out.clone(), id);
        let op = pipe_graph[id].to_op(supported_operators, None, None, None)?;
        pipe_ops.insert(id, op);
        outputs.push(id);

        let mut port_indexes: PortIndexMap = HashMap::new();

        let mut windows = HashMap::new();
        let mut operators = HashMap::new();
        let mut scripts = HashMap::new();

        for stmt in &script.stmts {
            let stmt_rental =
                tremor_script::query::rentals::Stmt::new(Arc::new(self.0.clone()), |_| unsafe {
                    std::mem::transmute(stmt.clone())
                });

            let that = tremor_script::query::StmtRentalWrapper {
                stmt: std::sync::Arc::new(stmt_rental),
            };

            match stmt {
                Stmt::Select(ref select) => {
                    let s = &select.stmt;
                    let from = s.from.id.clone().to_string();
                    if !nodes.contains_key(&from.clone()) {
                        let mut h = DumbHighlighter::default();
                        let arse = query_stream_not_defined(&s, &s.from, from)?;
                        tremor_script::query::Query::format_error_from_script(
                            &self.0.source,
                            &mut h,
                            &arse,
                        )?;
                        return Err("Missing node".into());
                    }
                    let into = s.into.id.clone().to_string();

                    let from = resolve_output_port(from)?;
                    let select_in = resolve_input_port(from.id.clone() + "_select")?;
                    let select_out = resolve_output_port(from.id.clone() + "_select")?;
                    let into = resolve_input_port(into)?;
                    if links.contains_key(&from) {
                        match links.get_mut(&from) {
                            Some(x) => x.push(select_in.clone()),
                            None => return Err("should never get here - link should be ok".into()),
                        }
                        match links.get_mut(&select_out) {
                            Some(x) => x.push(into),
                            None => return Err("should never get here - link should be ok".into()),
                        }
                    } else {
                        links.insert(from, vec![select_in.clone()]);
                        links.insert(select_out, vec![into]);
                    }

                    let node = NodeConfig {
                        id: select_in.id.to_string(),
                        kind: NodeKind::Operator,
                        _type: "trickle::select".to_string(),
                        config: None,
                        defn: None,
                        node: None,
                    };
                    let id = pipe_graph.add_node(node.clone());
                    let op =
                        node.to_op(supported_operators, None, Some(that), Some(windows.clone()))?;
                    pipe_ops.insert(id, op);
                    nodes.insert(select_in.id.clone(), id);
                    outputs.push(id);
                }
                Stmt::Stream(s) => {
                    let name = s.id.clone().to_string();
                    let src = resolve_output_port(name.clone())?;
                    if !nodes.contains_key(&src.id) {
                        let node = NodeConfig {
                            id: src.id.to_string(),
                            kind: NodeKind::Operator,
                            _type: "passthrough".to_string(),
                            config: None,
                            defn: None,
                            node: None,
                        };
                        let id = pipe_graph.add_node(node.clone());
                        nodes.insert(name.clone(), id);
                        let op = node.to_op(
                            supported_operators,
                            None,
                            Some(that),
                            Some(windows.clone()),
                        )?;
                        pipe_ops.insert(id, op);
                        outputs.push(id);
                    };
                }
                Stmt::WindowDecl(w) => {
                    let name = w.id.clone().to_string();
                    windows.insert(name, window_decl_to_impl(w)?);
                }
                Stmt::OperatorDecl(o) => {
                    let name = o.id.clone().to_string();
                    operators.insert(name, Stmt::OperatorDecl(o.clone()));
                }
                Stmt::Operator(o) => {
                    let name = o.id.clone().to_string();
                    let target = o.target.clone().to_string();

                    let _op_in = resolve_input_port(name.clone())?;
                    let _op_out = resolve_output_port(name.clone())?;

                    let node = NodeConfig {
                        id: name.to_string(),
                        kind: NodeKind::Operator,
                        _type: "trickle::operator".to_string(),
                        config: None,
                        defn: None,
                        node: None,
                    };
                    let id = pipe_graph.add_node(node.clone());
                    let stmt_rental = tremor_script::query::rentals::Stmt::new(
                        Arc::new(self.0.clone()),
                        |_| unsafe {
                            let stmt: tremor_script::ast::Stmt =
                                (*operators.get(&target).expect("not found")).clone();
                            std::mem::transmute(stmt)
                        },
                    );

                    let that = tremor_script::query::StmtRentalWrapper {
                        stmt: std::sync::Arc::new(stmt_rental),
                    };
                    let op = node.to_op(supported_operators, None, Some(that), None)?;
                    pipe_ops.insert(id, op);
                    nodes.insert(o.id.clone(), id);
                    outputs.push(id);
                }
                Stmt::ScriptDecl(s) => {
                    let name = s.id.clone().to_string();
                    scripts.insert(name, Stmt::ScriptDecl(s.clone()));
                }
                Stmt::Script(o) => {
                    let name = o.id.clone().to_string();
                    let target = o.target.clone().to_string();

                    let _op_in = resolve_input_port(name.clone())?;
                    let _op_out = resolve_output_port(name.clone())?;

                    let stmt_rental = tremor_script::query::rentals::Stmt::new(
                        Arc::new(self.0.clone()),
                        |_| unsafe {
                            let stmt: tremor_script::ast::Stmt =
                                (*scripts.get(&target).expect("not found")).clone();
                            std::mem::transmute(stmt)
                        },
                    );

                    let that_defn = tremor_script::query::StmtRentalWrapper {
                        stmt: std::sync::Arc::new(stmt_rental),
                    };

                    let node = NodeConfig {
                        id: name.to_string(),
                        kind: NodeKind::Operator,
                        _type: "trickle::script".to_string(),
                        config: None,
                        defn: Some(std::sync::Arc::new(that_defn.clone())),
                        node: Some(std::sync::Arc::new(that.clone())),
                    };
                    let id = pipe_graph.add_node(node.clone());

                    let op = node.to_op(
                        supported_operators,
                        Some(that_defn),
                        Some(that),
                        None,
                    )?;
                    pipe_ops.insert(id, op);
                    nodes.insert(o.id.clone(), id);
                    outputs.push(id);
                }
            };
        }

        // Add metrics output port
        let id = pipe_graph.add_node(NodeConfig {
            id: "_metrics".to_string(),
            kind: NodeKind::Output,
            _type: "passthrough".to_string(),
            config: None,
            defn: None,
            node: None,
        });
        nodes.insert("metrics".to_string(), id);
        let op = pipe_graph[id].to_op(supported_operators, None, None, None)?;
        pipe_ops.insert(id, op);
        outputs.push(id);

        // Link graph edges
        for (from, tos) in &links {
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
                pipe_graph.add_edge(from_idx, to_idx, ());
            }
        }

        // iff cycles, fail and bail
        if is_cyclic_directed(&pipe_graph) {
            Err(ErrorKind::CyclicGraphError(format!(
                "{:?}",
                Dot::with_config(&pipe_graph, &[Config::EdgeNoLabel])
            ))
            .into())
        } else {
            let mut i2pos = HashMap::new();
            let mut graph = Vec::new();
            // Nodes that handle contraflow
            let mut contraflow = Vec::new();
            // Nodes that handle signals
            let mut signalflow = Vec::new();
            let mut i = 0;
            for nx in pipe_graph.node_indices() {
                match pipe_ops.remove(&nx) {
                    Some(op) => {
                        i2pos.insert(nx, i);
                        if op.handles_contraflow() {
                            contraflow.push(i);
                        }
                        if op.handles_signal() {
                            signalflow.push(i);
                        }
                        graph.push(op);
                        i += 1;
                    }
                    _ => {
                        return Err(format!("Invalid pipeline can't find node {:?}", &nx).into());
                    }
                }
            }

            // since contraflow is the reverse we need to reverse it.
            contraflow.reverse();

            //pub type PortIndexMap = HashMap<(NodeIndex, String), Vec<(NodeIndex, String)>>;

            let mut port_indexes2 = HashMap::new();
            for ((i1, s1), connections) in &port_indexes {
                let connections = connections
                    .iter()
                    .map(|(i, s)| (i2pos[&i], s.clone()))
                    .collect();
                port_indexes2.insert((i2pos[&i1], s1.clone()), connections);
            }

            let mut inputs2: HashMap<String, usize> = HashMap::new();
            for (k, idx) in &inputs {
                inputs2.insert(k.clone(), i2pos[idx]);
            }

            let metric_interval = Some(1_000_000_000); // FIXME either make configurable or define sensible default
            let exec = ExecutableGraph {
                metrics: iter::repeat(NodeMetrics::default())
                    .take(graph.len())
                    .collect(),
                stack: Vec::with_capacity(graph.len()),
                id: "generated".to_string(), // FIXME make configurable
                metrics_idx: i2pos[&nodes["metrics"]],
                last_metrics: 0,
                graph,
                inputs: inputs2,
                port_indexes: port_indexes2,
                contraflow,
                signalflow,
                metric_interval,
            };

            Ok(exec)
        }
    }

    pub fn to_config(&self) -> Result<config::Pipeline> {
        let script = self.0.query.suffix();
        let stmts = script.stmts.iter();

        let mut config = config::Pipeline {
            id: "generated".to_string(), // FIXME derive from some other ctx
            description: "Generated from <generated.trickle>".to_string(),
            links: IndexMap::new(), // FIXME compute below
            interface: config::Interfaces {
                inputs: vec!["in".to_string()],
                outputs: vec!["out".to_string()],
            }, // FIXME compute below
            nodes: Vec::new(),
            metrics_interval_s: Some(10),
        };
        let mut graph = ConfigGraph::new();
        let mut nodes = HashMap::new();
        let mut inputs = HashMap::new();
        for stream in config.interface.inputs.clone() {
            let id = graph.add_node(NodeConfig {
                id: stream.to_string(),
                kind: NodeKind::Input,
                _type: "passthrough".to_string(),
                config: None,
                defn: None,
                node: None,
            });
            nodes.insert(stream.clone(), id);
            inputs.insert(stream.clone(), id);
        }
        let mut outputs: Vec<petgraph::graph::NodeIndex> = Vec::new();
        for stream in config.interface.outputs.clone() {
            let id = graph.add_node(NodeConfig {
                id: stream.to_string(),
                kind: NodeKind::Output,
                _type: "passthrough".to_string(),
                config: None,
                defn: None,
                node: None,
            });
            nodes.insert(stream.clone(), id);
            outputs.push(id);
        }
        let mut port_indexes: PortIndexMap = HashMap::new();

        for stmt in stmts {
            match stmt {
                Stmt::Select(select) => {
                    let s = &select.stmt;
                    let from = s.from.id.clone().to_string();
                    if !nodes.contains_key(&from.clone()) {
                        let mut h = DumbHighlighter::default();
                        let arse = query_stream_not_defined(&s, &s.from, from)?;
                        tremor_script::query::Query::format_error_from_script(
                            &self.0.source,
                            &mut h,
                            &arse,
                        )?;
                        return Err("Invalid graph node".into());
                    }
                    let into = s.into.id.clone().to_string();

                    let from = resolve_output_port(from)?;
                    let select_in = resolve_input_port(from.id.clone() + "_select")?;
                    let select_out = resolve_output_port(from.id.clone() + "_select")?;
                    let into = resolve_input_port(into)?;
                    if config.links.contains_key(&from) {
                        match config.links.get_mut(&from) {
                            Some(x) => x.push(select_in.clone()),
                            None => return Err("should never get here - link should be ok".into()),
                        }
                        match config.links.get_mut(&select_out) {
                            Some(x) => x.push(into),
                            None => return Err("should never get here - link should be ok".into()),
                        }
                    } else {
                        config.links.insert(from, vec![select_in.clone()]);
                        config.links.insert(select_out, vec![into]);
                    }

                    let id = graph.add_node(NodeConfig {
                        id: select_in.id.to_string(),
                        kind: NodeKind::Operator,
                        _type: "trickle::select".to_string(),
                        config: None,
                        defn: None,
                        node: None,
                    });
                    nodes.insert(select_in.id.clone(), id);
                    outputs.push(id);
                    // };
                }
                Stmt::Stream(s) => {
                    let name = s.id.clone().to_string();
                    let src = resolve_output_port(name.clone())?;
                    if !nodes.contains_key(&src.id) {
                        let id = graph.add_node(NodeConfig {
                            id: src.id.to_string(),
                            kind: NodeKind::Operator,
                            _type: "passthrough".to_string(),
                            config: None,
                            defn: None,
                            node: None,
                        });
                        nodes.insert(name.clone(), id);
                        outputs.push(id);
                    };
                }
                Stmt::WindowDecl(s) => {
                    let name = s.id.clone().to_string();
                    let src = resolve_output_port(name.clone())?;
                    if !nodes.contains_key(&src.id) {
                        let id = graph.add_node(NodeConfig {
                            id: src.id.to_string(),
                            kind: NodeKind::Operator,
                            _type: "fake.window".to_string(),
                            config: None,
                            defn: None,
                            node: None,
                        });
                        nodes.insert(name.clone(), id);
                        outputs.push(id);
                    };
                }
                Stmt::OperatorDecl(s) => {
                    let name = s.id.clone().to_string();
                    let src = resolve_output_port(name.clone())?;
                    if !nodes.contains_key(&src.id) {
                        let id = graph.add_node(NodeConfig {
                            id: src.id.to_string(),
                            kind: NodeKind::Operator,
                            _type: "fake.operator".to_string(),
                            config: None,
                            defn: None,
                            node: None,
                        });
                        nodes.insert(name.clone(), id);
                        outputs.push(id);
                    };
                }
                not_yet_implemented => {
                    return Err(format!("not yet implemented: {:?}", not_yet_implemented).into());
                }
            };
        }

        // Add metrics output port
        let id = graph.add_node(NodeConfig {
            id: "_metrics".to_string(),
            kind: NodeKind::Output,
            _type: "passthrough".to_string(),
            config: None,
            defn: None,
            node: None,
        });
        nodes.insert("metrics".to_string(), id);
        outputs.push(id);

        // Link graph edges
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

        // iff cycles, fail and bail
        if is_cyclic_directed(&graph) {
            Err(ErrorKind::CyclicGraphError(format!(
                "{:?}",
                Dot::with_config(&graph, &[Config::EdgeNoLabel])
            ))
            .into())
        } else {
            config.nodes = graph
                .node_indices()
                .map(|i| {
                    let nc = &graph[i];
                    crate::config::Node {
                        id: nc.id.clone(),
                        node_type: nc._type.clone(),
                        description: "generated".to_string(),
                        config: nc.config.clone(),
                    }
                })
                .collect();

            Ok(config)
        }
    }
}

#[allow(clippy::implicit_hasher)]
pub fn supported_operators(
    config: &NodeConfig,
    defn: Option<tremor_script::query::StmtRentalWrapper>,
    node: Option<tremor_script::query::StmtRentalWrapper>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode> {
    //    use op::grouper::BucketGrouperFactory;
    use op::identity::PassthroughFactory;
    //    use op::runtime::TremorFactory;
    use op::trickle::operator::TrickleOperator;
    use op::trickle::script::TrickleScript;
    use op::trickle::select::{SelectDims, TrickleSelect};

    let name_parts: Vec<&str> = config._type.split("::").collect();

    let op: Box<dyn op::Operator> = match name_parts.as_slice() {
        ["trickle", "select"] => {
            let node = if let Some(node) = node {
                node
            } else {
                return Err(ErrorKind::MissingOpConfig(
                    "trickle operators require a statement".into(),
                )
                .into());
            };
            let groups = SelectDims::from_query(node.stmt.clone());
            let windows = if let Some(windows) = windows {
                windows
            } else {
                return Err(ErrorKind::MissingOpConfig(
                    "select operators require a window mapping".into(),
                )
                .into());
            };
            let windows: Result<Vec<(String, WindowImpl)>> =
                if let tremor_script::ast::Stmt::Select(s) = node.stmt.suffix() {
                    s.stmt
                        .windows
                        .iter()
                        .map(|w| {
                            Ok(windows
                                .get(&w.id)
                                .map(|imp| (w.id.to_string(), imp.clone()))
                                .ok_or_else(|| {
                                    ErrorKind::BadOpConfig(format!("Unknown window: {}", &w.id))
                                })?)
                        })
                        .collect()
                } else {
                    Err("Declared as select but isn't a select".into())
                };
            Box::new(TrickleSelect::with_stmt(
                config.id.clone(),
                groups,
                windows?,
                node,
            )?)
        }
        ["trickle", "operator"] => {
            let node = if let Some(node) = node {
                node
            } else {
                return Err(ErrorKind::MissingOpConfig(
                    "trickle operators require a statement".into(),
                )
                .into());
            };
            Box::new(TrickleOperator::with_stmt(config.id.clone(), node)?)
        }
        ["trickle", "script"] => {
            let node = if let Some(node) = node {
                node
            } else {
                return Err(ErrorKind::MissingOpConfig(
                    "trickle operators require a statement".into(),
                )
                .into());
            };
            Box::new(TrickleScript::with_stmt(
                config.id.clone(),
                defn.expect(""),
                node,
            )?)
        }
        ["passthrough"] => {
            let op = PassthroughFactory::new_boxed();
            op.from_node(config)?
        }
        // ["debug", "history"] => EventHistoryFactory::new_boxed(),
        // ["runtime", "tremor"] => TremorFactory::new_boxed(),
        // ["grouper", "bucket"] => BucketGrouperFactory::new_boxed(),
        // ["generic", "batch"] => BatchFactory::new_boxed(),
        // ["generic", "backpressure"] => BackpressureFactory::new_boxed(),
        [namespace, name] => {
            return Err(ErrorKind::UnknownOp(namespace.to_string(), name.to_string()).into());
        }
        _ => return Err(ErrorKind::UnknownNamespace(config._type.clone()).into()),
    };
    Ok(OperatorNode {
        id: config.id.clone(),
        kind: config.kind,
        _type: config._type.clone(),
        op,
    })
}
