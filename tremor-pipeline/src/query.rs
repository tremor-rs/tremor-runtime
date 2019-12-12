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

use crate::config::{InputPort, OutputPort};
use crate::errors::*;
use crate::op;
use crate::op::trickle::select::WindowImpl;
use crate::OperatorNode;
use crate::{common_cow, ConfigGraph, NodeConfig, NodeKind, PortIndexMap};
use halfbrown::HashMap;
use indexmap::IndexMap;
use petgraph::algo::is_cyclic_directed;
use petgraph::dot::{Config, Dot};
use simd_json::Value as ValueTrait;
use std::borrow::Cow;
use std::mem;
use std::sync::Arc;
use tremor_script::ast::Stmt;
use tremor_script::ast::{Ident, WindowDecl, WindowKind};
use tremor_script::errors::query_stream_not_defined;
use tremor_script::highlighter::Dumb as DumbHighlighter;
use tremor_script::query::StmtRentalWrapper;
use tremor_script::{AggrRegistry, Registry, Value};

// Legacy ops for backwards compat with pipeline.yaml at runtime in trickle / extension
use op::debug::EventHistoryFactory;
use op::generic::{BackpressureFactory, BatchFactory};
use op::grouper::BucketGrouperFactory;
use op::runtime::TremorFactory;

fn resolve_input_port(port: &(Ident, Ident)) -> InputPort {
    InputPort {
        id: common_cow(port.0.id.to_string()),
        port: common_cow(port.1.id.to_string()),
        had_port: true,
    }
}

fn resolve_output_port(port: &(Ident, Ident)) -> OutputPort {
    OutputPort {
        id: common_cow(port.0.id.to_string()),
        port: common_cow(port.1.id.to_string()),
        had_port: true,
    }
}

fn window_decl_to_impl<'script>(
    d: &WindowDecl<'script>,
    stmt: &StmtRentalWrapper,
) -> Result<WindowImpl> {
    use op::trickle::select::*;
    match &d.kind {
        WindowKind::Sliding => Err("Sliding windows are not yet implemented".into()),
        WindowKind::Tumbling => {
            let script = if d.script.is_some() { Some(d) } else { None };
            let ttl = d.params.get("eviction_period").and_then(Value::as_u64);
            if let Some(interval) = d.params.get("interval").and_then(Value::as_u64) {
                Ok(TumblingWindowOnTime::from_stmt(interval, ttl, script, stmt).into())
            } else if let Some(size) = d.params.get("size").and_then(Value::as_u64) {
                Ok(TumblingWindowOnNumber::from_stmt(size, ttl, script, stmt).into())
            } else {
                Err(Error::from(
                    "Bad window configuration, either `size` or `interval` is required",
                ))
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct Query(pub tremor_script::query::Query);
impl From<tremor_script::query::Query> for Query {
    fn from(q: tremor_script::query::Query) -> Self {
        Self(q)
    }
}
impl Query {
    pub fn parse(script: &str, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self> {
        Ok(Self(tremor_script::query::Query::parse(
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
        let mut nodes: HashMap<Cow<'static, str>, _> = HashMap::new();
        let mut links: IndexMap<OutputPort, Vec<InputPort>> = IndexMap::new();
        let mut inputs = HashMap::new();
        let mut outputs: Vec<petgraph::graph::NodeIndex> = Vec::new();

        // FIXME compute public streams - do not hardcode
        let in_s: Cow<'static, str> = "in".into();
        let id = pipe_graph.add_node(NodeConfig {
            id: in_s.clone(),
            kind: NodeKind::Input,
            op_type: "passthrough".to_string(),
            config: None,
            defn: None,
            node: None,
        });
        nodes.insert(in_s.clone(), id);
        let op = pipe_graph[id].to_op(supported_operators, None, None, None)?;
        pipe_ops.insert(id, op);
        inputs.insert(in_s, id);

        // FIXME compute public streams - do not hardcode
        let err: Cow<'static, str> = "err".into();
        let id = pipe_graph.add_node(NodeConfig {
            id: err.clone(),
            kind: NodeKind::Output,
            op_type: "passthrough".to_string(),
            config: None,
            defn: None,
            node: None,
        });
        nodes.insert(err.clone(), id);
        let op = pipe_graph[id].to_op(supported_operators, None, None, None)?;
        pipe_ops.insert(id, op);
        outputs.push(id);

        // FIXME compute public streams - do not hardcode
        let out: Cow<'static, str> = "out".into();
        let id = pipe_graph.add_node(NodeConfig {
            id: out.clone(),
            kind: NodeKind::Output,
            op_type: "passthrough".to_string(),
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
        let mut select_num = 0;

        for stmt in &script.stmts {
            let stmt_rental =
                tremor_script::query::rentals::Stmt::new(Arc::new(self.0.clone()), |_| unsafe {
                    mem::transmute(stmt.clone())
                });

            let that = tremor_script::query::StmtRentalWrapper {
                stmt: std::sync::Arc::new(stmt_rental),
            };

            match stmt {
                Stmt::Select(ref select) => {
                    let s = &select.stmt;
                    if !nodes.contains_key(&s.from.0.id) {
                        let from = s.from.0.id.clone().to_string();
                        let mut h = DumbHighlighter::default();
                        let butt =
                            query_stream_not_defined(&s, &s.from.0, from, &script.node_meta)?;
                        tremor_script::query::Query::format_error_from_script(
                            &self.0.source,
                            &mut h,
                            &butt,
                        )?;
                        return Err("Missing node".into());
                    }

                    let select_in = InputPort {
                        id: format!("select_{}", select_num).into(),
                        port: "out".into(),
                        had_port: false,
                    };
                    let select_out = OutputPort {
                        id: format!("select_{}", select_num).into(),
                        port: "out".into(),
                        had_port: false,
                    };
                    select_num += 1;
                    let mut from = resolve_output_port(&s.from);
                    if from.id == "in" && from.port != "out" {
                        let name: Cow<'static, str> = from.port;

                        if !nodes.contains_key(&name) {
                            let id = pipe_graph.add_node(NodeConfig {
                                id: name.clone(),
                                kind: NodeKind::Input,
                                op_type: "passthrough".to_string(),
                                config: None,
                                defn: None,
                                node: None,
                            });
                            nodes.insert(name.clone(), id);
                            let op = pipe_graph[id].to_op(supported_operators, None, None, None)?;
                            pipe_ops.insert(id, op);
                            inputs.insert(name.clone(), id);
                        }
                        from.id = name.clone();
                        from.had_port = false;
                        from.port = "out".into();
                    }
                    let mut into = resolve_input_port(&s.into);
                    if into.id == "out" && into.port != "in" {
                        let name: Cow<'static, str> = into.port;
                        if !nodes.contains_key(&name) {
                            let id = pipe_graph.add_node(NodeConfig {
                                id: name.clone(),
                                kind: NodeKind::Output,
                                op_type: "passthrough".to_string(),
                                config: None,
                                defn: None,
                                node: None,
                            });
                            nodes.insert(name.clone(), id);
                            let op = pipe_graph[id].to_op(supported_operators, None, None, None)?;
                            pipe_ops.insert(id, op);
                            outputs.push(id);
                        }
                        into.id = name.clone();
                        into.had_port = false;
                        into.port = "in".into();
                    }

                    links.entry(from).or_default().push(select_in.clone());
                    links.entry(select_out).or_default().push(into);

                    let node = NodeConfig {
                        id: select_in.id.clone(),
                        kind: NodeKind::Operator,
                        op_type: "trickle::select".to_string(),
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
                    let name = common_cow(s.id.clone());
                    let id = name.clone();
                    if !nodes.contains_key(&id) {
                        let node = NodeConfig {
                            id,
                            kind: NodeKind::Operator,
                            op_type: "passthrough".to_string(),
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
                        inputs.insert(name.clone(), id);
                        pipe_ops.insert(id, op);
                        outputs.push(id);
                    };
                }
                Stmt::WindowDecl(w) => {
                    let name = w.id.clone().to_string();
                    windows.insert(name, window_decl_to_impl(w, &that)?);
                }
                Stmt::OperatorDecl(o) => {
                    let name = o.id.clone().to_string();
                    operators.insert(name, Stmt::OperatorDecl(o.clone()));
                }
                Stmt::Operator(o) => {
                    let name = o.id.clone().to_string();
                    let target = o.target.clone().to_string();

                    let node = NodeConfig {
                        id: common_cow(name),
                        kind: NodeKind::Operator,
                        op_type: "trickle::operator".to_string(),
                        config: None,
                        defn: None,
                        node: None,
                    };
                    let id = pipe_graph.add_node(node.clone());
                    let inner_stmt: tremor_script::ast::Stmt = operators
                        .get(&target)
                        .ok_or_else(|| Error::from("operator not found"))?
                        .clone();
                    let stmt_rental = tremor_script::query::rentals::Stmt::new(
                        Arc::new(self.0.clone()),
                        |_| unsafe { mem::transmute(inner_stmt) },
                    );

                    let that = tremor_script::query::StmtRentalWrapper {
                        stmt: std::sync::Arc::new(stmt_rental),
                    };
                    let op = node.to_op(supported_operators, None, Some(that), None)?;
                    pipe_ops.insert(id, op);
                    nodes.insert(common_cow(o.id.clone()), id);
                    outputs.push(id);
                }
                Stmt::ScriptDecl(s) => {
                    let name = s.id.clone().to_string();
                    scripts.insert(name, Stmt::ScriptDecl(s.clone()));
                }
                Stmt::Script(o) => {
                    let name = o.id.clone().to_string();
                    let target = o.target.clone().to_string();
                    let inner_stmt: tremor_script::ast::Stmt = scripts
                        .get(&target)
                        .ok_or_else(|| Error::from("script not found"))?
                        .clone();
                    let stmt_rental = tremor_script::query::rentals::Stmt::new(
                        Arc::new(self.0.clone()),
                        |_| unsafe { mem::transmute(inner_stmt) },
                    );

                    let that_defn = tremor_script::query::StmtRentalWrapper {
                        stmt: std::sync::Arc::new(stmt_rental),
                    };

                    let node = NodeConfig {
                        id: common_cow(name),
                        kind: NodeKind::Operator,
                        op_type: "trickle::script".to_string(),
                        config: None,
                        defn: Some(std::sync::Arc::new(that_defn.clone())),
                        node: Some(std::sync::Arc::new(that.clone())),
                    };

                    let id = pipe_graph.add_node(node.clone());

                    let op = node.to_op(supported_operators, Some(that_defn), Some(that), None)?;
                    pipe_ops.insert(id, op);
                    nodes.insert(common_cow(o.id.clone()), id);
                    outputs.push(id);
                }
            };
        }

        // Add metrics output port
        let id = pipe_graph.add_node(NodeConfig {
            id: "_metrics".into(),
            kind: NodeKind::Output,
            op_type: "passthrough".to_string(),
            config: None,
            defn: None,
            node: None,
        });
        nodes.insert("metrics".into(), id);
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

        /*
        println!(
            "{:?}",
            petgraph::dot::Dot::with_config(&pipe_graph, &[Config::EdgeNoLabel])
        );
        */
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
                if let Some(op) = pipe_ops.remove(&nx) {
                    i2pos.insert(nx, i);
                    if op.handles_contraflow() {
                        contraflow.push(i);
                    }
                    if op.handles_signal() {
                        signalflow.push(i);
                    }
                    graph.push(op);
                    i += 1;
                } else {
                    return Err(format!("Invalid pipeline can't find node {:?}", &nx).into());
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

            let mut inputs2: HashMap<Cow<'static, str>, usize> = HashMap::new();
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
}

#[allow(clippy::implicit_hasher)]
pub fn supported_operators(
    config: &NodeConfig,
    defn: Option<tremor_script::query::StmtRentalWrapper>,
    node: Option<tremor_script::query::StmtRentalWrapper>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode> {
    use op::identity::PassthroughFactory;
    //    use op::runtime::TremorFactory;
    use op::trickle::operator::TrickleOperator;
    use op::trickle::script::TrickleScript;
    use op::trickle::select::{SelectDims, TrickleSelect};

    let name_parts: Vec<&str> = config.op_type.split("::").collect();

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
                config.id.clone().to_string(),
                &groups,
                windows?,
                &node,
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
            Box::new(TrickleOperator::with_stmt(
                config.id.clone().to_string(),
                node,
            )?)
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
                config.id.clone().to_string(),
                defn.ok_or_else(|| Error::from("Script definition missing"))?,
                node,
            )?)
        }
        ["passthrough"] => {
            let op = PassthroughFactory::new_boxed();
            op.from_node(config)?
        }
        ["debug", "history"] => EventHistoryFactory::new_boxed().from_node(config)?,
        ["runtime", "tremor"] => TremorFactory::new_boxed().from_node(config)?,
        ["grouper", "bucket"] => BucketGrouperFactory::new_boxed().from_node(config)?,
        ["generic", "batch"] => BatchFactory::new_boxed().from_node(config)?,
        ["generic", "backpressure"] => BackpressureFactory::new_boxed().from_node(config)?,
        [namespace, name] => {
            return Err(ErrorKind::UnknownOp(namespace.to_string(), name.to_string()).into());
        }
        _ => return Err(ErrorKind::UnknownNamespace(config.op_type.clone()).into()),
    };
    Ok(OperatorNode {
        id: config.id.clone(),
        kind: config.kind,
        op_type: config.op_type.clone(),
        op,
    })
}
