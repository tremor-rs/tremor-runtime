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

use crate::{
    common_cow,
    errors::{Error, ErrorKind, Result},
    op::{
        self,
        identity::PassthroughFactory,
        prelude::{ERR, IN, OUT},
        trickle::{operator::TrickleOperator, select::Select, simple_select::SimpleSelect, window},
    },
    ConfigGraph, Connection, ExecutableGraph, NodeConfig, NodeKind, NodeMetrics, Operator,
    OperatorNode, PortIndexMap, State, METRICS_CHANNEL,
};
use beef::Cow;
use halfbrown::HashMap;
use indexmap::IndexMap;
use petgraph::{algo::is_cyclic_directed, graph::NodeIndex};
use std::collections::BTreeSet;
use std::iter;
use tremor_common::ids::OperatorIdGen;
use tremor_script::{
    ast::{
        self, Helper, Ident, OperatorDefinition, PipelineCreate, PipelineDefinition,
        ScriptDefinition, SelectType, Stmt, WindowDefinition, WindowKind,
    },
    errors::{
        err_generic, query_node_duplicate_name_err, query_node_reserved_name_err,
        query_stream_duplicate_name_err, query_stream_not_defined_err,
    },
    highlighter::{Dumb, Highlighter},
    prelude::*,
    AggrRegistry, NodeMeta, Registry, Value,
};

const BUILTIN_NODES: [(Cow<'static, str>, NodeKind); 3] = [
    (IN, NodeKind::Input),
    (OUT, NodeKind::Output(OUT)),
    (ERR, NodeKind::Output(ERR)),
];

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct InputPort {
    pub id: Cow<'static, str>,
    pub port: Cow<'static, str>,
    pub had_port: bool,
    pub mid: Box<NodeMeta>,
}
impl BaseExpr for InputPort {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct OutputPort {
    pub id: Cow<'static, str>,
    pub port: Cow<'static, str>,
    pub had_port: bool,
    pub mid: Box<NodeMeta>,
}
impl BaseExpr for OutputPort {
    fn meta(&self) -> &NodeMeta {
        &self.mid
    }
}

fn resolve_input_port(port: &(Ident, Ident)) -> InputPort {
    InputPort {
        id: common_cow(&port.0.id),
        port: common_cow(&port.1.id),
        had_port: true,
        mid: Box::new(port.0.meta().clone()),
    }
}

fn resolve_output_port(port: &(Ident, Ident)) -> OutputPort {
    OutputPort {
        id: common_cow(&port.0.id),
        port: common_cow(&port.1.id),
        had_port: true,
        mid: Box::new(port.0.meta().clone()),
    }
}

pub(crate) fn window_decl_to_impl(d: &WindowDefinition) -> Result<window::Impl> {
    use op::trickle::window::{TumblingOnNumber, TumblingOnTime};
    match &d.kind {
        WindowKind::Sliding => Err("Sliding windows are not yet implemented".into()),
        WindowKind::Tumbling => {
            let script = if d.script.is_some() { Some(d) } else { None };
            let with = d.params.render()?;
            let max_groups = with
                .get(WindowDefinition::MAX_GROUPS)
                .and_then(Value::as_usize)
                .unwrap_or(window::Impl::DEFAULT_MAX_GROUPS);

            match (
                with.get(WindowDefinition::INTERVAL).and_then(Value::as_u64),
                with.get(WindowDefinition::SIZE).and_then(Value::as_u64),
            ) {
                (Some(interval), None) => Ok(window::Impl::from(TumblingOnTime::from_stmt(
                    interval, max_groups, script,
                ))),
                (None, Some(size)) => Ok(window::Impl::from(TumblingOnNumber::from_stmt(
                    size, max_groups, script,
                ))),
                (Some(_), Some(_)) => Err(Error::from(
                    "Bad window configuration, only one of `size` or `interval` is allowed.",
                )),
                (None, None) => Err(Error::from(
                    "Bad window configuration, either `size` or `interval` is required.",
                )),
            }
        }
    }
}
/// A Tremor Query
#[derive(Clone, Debug)]
pub struct Query(pub tremor_script::query::Query);

impl Query {
    /// Fetches the ID of the query if it was provided
    pub fn id(&self) -> Option<&str> {
        self.0.query.config.get("id").and_then(ValueAccess::as_str)
    }

    /// Parse a query
    ///
    /// # Errors
    /// if the trickle script can not be parsed
    pub fn parse<S: ToString>(script: S, reg: &Registry, aggr_reg: &AggrRegistry) -> Result<Self> {
        Ok(Self(tremor_script::query::Query::parse(
            script, reg, aggr_reg,
        )?))
    }

    /// Turn a query into a executable pipeline graph
    ///
    /// # Errors
    /// if the graph can not be turned into a pipeline
    #[allow(clippy::too_many_lines)]
    pub fn to_pipe(&self, idgen: &mut OperatorIdGen) -> Result<ExecutableGraph> {
        let stmts = self.0.query.stmts.clone();
        to_executable_graph(&self.0, stmts, idgen)
    }
}

struct InlcudedGraph {
    prefix: String,
    graph: ExecutableGraph,
    from_map: HashMap<String, NodeIndex>,
    into_map: HashMap<String, NodeIndex>,
}

/// Turn a query into a executable pipeline graph
///
/// # Errors
/// if the graph can not be turned into a pipeline
#[allow(clippy::too_many_lines)]
fn to_executable_graph(
    query: &tremor_script::Query,
    stmts: Vec<tremor_script::ast::Stmt<'static>>,
    idgen: &mut OperatorIdGen,
) -> Result<ExecutableGraph> {
    let query_borrowed = query.query.clone();
    let aggr_reg = tremor_script::aggr_registry();
    let reg = tremor_script::FN_REGISTRY.read()?;
    let mut helper = Helper::new(&reg, &aggr_reg);
    helper.scope = query_borrowed.scope.clone();
    let mut pipe_graph = ConfigGraph::new();
    let mut pipe_ops = HashMap::new();
    let mut nodes: HashMap<Cow<'static, str>, _> = HashMap::new();
    let mut links: IndexMap<OutputPort, Vec<InputPort>> = IndexMap::new();
    let mut inputs = HashMap::new();
    let mut outputs: Vec<NodeIndex> = Vec::new();
    let metric_interval = query_borrowed
        .config
        .get("metrics_interval_s")
        .and_then(Value::as_u64)
        .map(|i| i * 1_000_000_000);

    let pipeline_id = query_borrowed
        .config
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("<generated>");

    for (name, node_kind) in &BUILTIN_NODES {
        let id = pipe_graph.add_node(NodeConfig {
            id: name.to_string(),
            kind: node_kind.clone(),
            op_type: "passthrough".to_string(),
            ..NodeConfig::default()
        });
        nodes.insert(name.clone(), id);
        let op = pipe_graph
            .raw_nodes()
            .get(id.index())
            .ok_or_else(|| Error::from("Error finding freshly added node."))
            .and_then(|node| {
                node.weight.to_op(
                    idgen.next_id(),
                    supported_operators,
                    None,
                    None,
                    &mut helper,
                )
            })?;
        pipe_ops.insert(id, op);
        match node_kind {
            NodeKind::Input => {
                inputs.insert(name.clone(), id);
            }
            NodeKind::Output(_) => outputs.push(id),
            _ => {
                return Err(format!(
                    "Builtin node {} has unsupported node kind: {:?}",
                    name, node_kind
                )
                .into())
            }
        }
    }

    let mut port_indexes: PortIndexMap = HashMap::new();
    let mut select_num = 0;

    let has_builtin_node_name = make_builtin_node_name_checker();

    let mut included_graphs: HashMap<String, InlcudedGraph> = HashMap::new();
    for (i, stmt) in stmts.into_iter().enumerate() {
        match &stmt {
            // Ignore the definitions
            Stmt::WindowDefinition(_)
            | Stmt::ScriptDefinition(_)
            | Stmt::OperatorDefinition(_)
            | Stmt::PipelineDefinition(_) => {}

            Stmt::SelectStmt(ref select) => {
                // Rewrite pipeline/port to their internal streams
                let mut select_rewrite = select.clone();
                // Note we connecto from stmt.from to graph.into
                // and stmt.into to graph.from
                // FIXME: error handling
                let (node, port) = &mut select_rewrite.stmt.from;
                if let Some(g) = included_graphs.get(node.as_str()) {
                    let name = into_name(&g.prefix, port.as_str());
                    node.id = name.into();
                    port.id = "out".into();
                }
                let (node, port) = &mut select_rewrite.stmt.into;
                if let Some(g) = included_graphs.get(node.as_str()) {
                    let name = from_name(&g.prefix, port.as_str());
                    node.id = name.into();
                    port.id = "in".into();
                }

                let select = select_rewrite;

                let s: &ast::Select<'_> = &select.stmt;

                if !nodes.contains_key(&s.from.0.id) {
                    return Err(
                        query_stream_not_defined_err(s, &s.from.0, s.from.0.to_string()).into(),
                    );
                }
                let e = select.stmt.extent();
                let mut h = Dumb::new();
                let label = h
                    .highlight_range(e)
                    .ok()
                    .map(|_| h.to_string().trim_end().to_string());

                let select_in = InputPort {
                    id: format!("select_{}", select_num).into(),
                    port: IN,
                    had_port: false,
                    mid: Box::new(s.meta().clone()),
                };
                let select_out = OutputPort {
                    id: format!("select_{}", select_num,).into(),
                    port: OUT,
                    had_port: false,
                    mid: Box::new(s.meta().clone()),
                };
                select_num += 1;
                let mut from = resolve_output_port(&s.from);
                if from.id == "in" && from.port != "out" {
                    let name: Cow<'static, str> = format!("in/{}", from.port).into();
                    from.id = name.clone();
                    if !nodes.contains_key(&name) {
                        let id = pipe_graph.add_node(NodeConfig {
                            id: name.to_string(),
                            kind: NodeKind::Input,
                            op_type: "passthrough".to_string(),
                            ..NodeConfig::default()
                        });
                        nodes.insert(name.clone(), id);
                        let op = pipe_graph
                            .raw_nodes()
                            .get(id.index())
                            .ok_or_else(|| Error::from("Error finding freshly added node."))
                            .and_then(|node| {
                                node.weight.to_op(
                                    idgen.next_id(),
                                    supported_operators,
                                    None,
                                    None,
                                    &mut helper,
                                )
                            })?;
                        pipe_ops.insert(id, op);
                        inputs.insert(name, id);
                    }
                }
                let mut into = resolve_input_port(&s.into);
                if into.id == "out" && into.port != "in" {
                    let name: Cow<'static, str> = format!("out/{}", into.port).into();
                    into.id = name.clone();
                    if !nodes.contains_key(&name) {
                        let id = pipe_graph.add_node(NodeConfig {
                            id: name.to_string(),
                            label: Some(name.to_string()),
                            kind: NodeKind::Output(into.port.clone()),
                            op_type: "passthrough".to_string(),
                            ..NodeConfig::default()
                        });
                        nodes.insert(name, id);
                        let op = pipe_graph
                            .raw_nodes()
                            .get(id.index())
                            .ok_or_else(|| Error::from("Error finding freshly added node."))
                            .and_then(|node| {
                                node.weight.to_op(
                                    idgen.next_id(),
                                    supported_operators,
                                    None,
                                    None,
                                    &mut helper,
                                )
                            })?;

                        pipe_ops.insert(id, op);
                        outputs.push(id);
                    }
                }

                links.entry(from).or_default().push(select_in.clone());
                links.entry(select_out).or_default().push(into);

                let node = NodeConfig {
                    id: select_in.id.to_string(),
                    label,
                    kind: NodeKind::Select,
                    op_type: "trickle::select".to_string(),
                    ..NodeConfig::default()
                };
                let id = pipe_graph.add_node(node.clone());

                let mut ww = HashMap::with_capacity(query_borrowed.scope.content.windows.len());
                for (name, decl) in &query_borrowed.scope.content.windows {
                    ww.insert(name.clone(), window_decl_to_impl(decl)?);
                }
                let op = node.to_op(
                    idgen.next_id(),
                    supported_operators,
                    Some(&stmt),
                    Some(ww),
                    &mut helper,
                )?;
                pipe_ops.insert(id, op);
                nodes.insert(select_in.id.clone(), id);
                outputs.push(id);
            }
            Stmt::StreamStmt(s) => {
                let name = common_cow(&s.id);
                let id = name.clone();
                if nodes.contains_key(&id) {
                    return Err(query_stream_duplicate_name_err(s, s, s.id.to_string()).into());
                }

                let node = NodeConfig {
                    id: id.to_string(),
                    kind: NodeKind::Operator,
                    op_type: "passthrough".to_string(),
                    ..NodeConfig::default()
                };
                let id = pipe_graph.add_node(node.clone());
                nodes.insert(name.clone(), id);
                let op = node.to_op(
                    idgen.next_id(),
                    supported_operators,
                    Some(&stmt),
                    None,
                    &mut helper,
                )?;
                inputs.insert(name.clone(), id);
                pipe_ops.insert(id, op);
                outputs.push(id);
            }
            Stmt::PipelineCreate(s) => {
                if let Some(mut pd) = helper.get::<PipelineDefinition>(&s.target)? {
                    let prefix = prefix_for(&s);

                    pd.apply_args(&s.params.render()?, &mut helper)?;
                    // VERY FIXME: FIXME please fixme this is a nightmare

                    let query = tremor_script::Query {
                        query: pd.query.unwrap(),
                        warnings: BTreeSet::new(),
                        locals: 0,
                    };
                    let mut from_map = HashMap::new();
                    for f in pd.from {
                        let name = from_name(&prefix, f.as_str());
                        let node = NodeConfig {
                            id: name.clone(),
                            kind: NodeKind::Operator,
                            op_type: "passthrough".to_string(),
                            ..NodeConfig::default()
                        };
                        let id = pipe_graph.add_node(node.clone());
                        from_map.insert(f.to_string(), id);
                        nodes.insert(name.clone().into(), id);
                        let op = node.to_op(
                            idgen.next_id(),
                            supported_operators,
                            Some(&stmt),
                            None,
                            &mut helper,
                        )?;
                        pipe_ops.insert(id, op);
                    }

                    let mut into_map = HashMap::new();
                    for i in pd.into {
                        let name = into_name(&prefix, i.as_str());
                        let node = NodeConfig {
                            id: name.clone(),
                            kind: NodeKind::Operator,
                            op_type: "passthrough".to_string(),
                            ..NodeConfig::default()
                        };
                        let id = pipe_graph.add_node(node.clone());
                        into_map.insert(i.to_string(), id);
                        nodes.insert(name.clone().into(), id);
                        let op = node.to_op(
                            idgen.next_id(),
                            supported_operators,
                            Some(&stmt),
                            None,
                            &mut helper,
                        )?;
                        pipe_ops.insert(id, op);
                    }
                    let name = s.alias.clone();
                    let stmts = query.query.stmts.clone(); // FIXME this should be in in to_executable graph

                    let graph = to_executable_graph(&query, stmts, idgen)?; // FIXME add source

                    if included_graphs
                        .insert(
                            name,
                            InlcudedGraph {
                                prefix,
                                graph,
                                from_map,
                                into_map,
                            },
                        )
                        .is_some()
                    {
                        return Err(Error::from(err_generic(
                            &s.extent(),
                            &s.extent(),
                            &format!("Can't create the pipeline `{}` twice", s.alias),
                        )));
                    }
                } else {
                    return Err(Error::from(err_generic(
                        &s.extent(),
                        &s.extent(),
                        &format!("Unknown pipeline `{}`", s.target),
                    )));
                }
            }
            Stmt::OperatorCreate(o) => {
                if nodes.contains_key(&common_cow(o.node_id.id())) {
                    let error_func = if has_builtin_node_name(&common_cow(o.node_id.id())) {
                        query_node_reserved_name_err
                    } else {
                        query_node_duplicate_name_err
                    };
                    return Err(error_func(o, o.node_id.id().to_string()).into());
                }

                let node = NodeConfig {
                    id: o.node_id.id().to_string(),
                    kind: NodeKind::Operator,
                    op_type: "trickle::operator".to_string(),
                    ..NodeConfig::default()
                };
                let id = pipe_graph.add_node(node.clone());

                let mut decl: OperatorDefinition =
                    helper.get(&o.target)?.ok_or("operator not found")?;
                if let Some(Stmt::OperatorCreate(o)) = query_borrowed.stmts.get(i) {
                    decl.params.ingest_creational_with(&o.params)?;
                };

                let that = Stmt::OperatorDefinition(decl);

                let op = node.to_op(
                    idgen.next_id(),
                    supported_operators,
                    Some(&that),
                    None,
                    &mut helper,
                )?;
                pipe_ops.insert(id, op);
                nodes.insert(common_cow(o.node_id.id()), id);
                outputs.push(id);
            }
            Stmt::ScriptCreate(o) => {
                if nodes.contains_key(&common_cow(o.node_id.id())) {
                    let error_func = if has_builtin_node_name(&common_cow(o.node_id.id())) {
                        query_node_reserved_name_err
                    } else {
                        query_node_duplicate_name_err
                    };
                    return Err(error_func(o, o.node_id.id().to_string()).into());
                }

                // FIXME: Better error
                let decl: ScriptDefinition = helper
                    .get(&o.target)?
                    .ok_or_else(|| format!("script not found: {}", &o.target,))?;

                let e = decl.extent();
                let mut h = Dumb::new();
                // We're trimming the code so no spaces are at the end then adding a newline
                // to ensure we're left justified (this is a dot thing, don't question it)
                let label = h
                    .highlight_range(e)
                    .ok()
                    .map(|_| format!("{}\n", h.to_string().trim_end()));
                let that_defn = Stmt::ScriptDefinition(Box::new(decl));

                let node = NodeConfig {
                    id: o.node_id.id().to_string(),
                    kind: NodeKind::Script,
                    label,
                    op_type: "trickle::script".to_string(),
                    ..NodeConfig::default()
                };
                let id = pipe_graph.add_node(node.clone());

                let op = node.to_op(
                    idgen.next_id(),
                    supported_operators,
                    Some(&that_defn),
                    None,
                    &mut helper,
                )?;

                pipe_ops.insert(id, op);
                nodes.insert(common_cow(o.node_id.id()), id);
                outputs.push(id);
            }
        };
    }

    // Link graph edges
    for (from, tos) in &links {
        for to in tos {
            let from_idx = *nodes
                .get(&from.id)
                .ok_or_else(|| query_stream_not_defined_err(from, from, from.id.to_string()))?;
            let to_idx = *nodes
                .get(&to.id)
                .ok_or_else(|| query_stream_not_defined_err(to, to, to.id.to_string()))?;

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
            pipe_graph.add_edge(
                from_idx,
                to_idx,
                Connection {
                    from: from.port.clone(),
                    to: to.port.clone(),
                },
            );
        }
    }

    for (_ig_name, ig) in included_graphs {
        let mut old_index_to_new_index = HashMap::new();

        let mut inputs = BTreeSet::new();

        for (old_idx, node) in ig.graph.graph.into_iter().enumerate() {
            if let NodeKind::Output(port) = &node.kind {
                let port: &str = &port;
                let new_idx = ig.into_map.get(port).cloned().ok_or(format!(
                    "FIXME: invalid graph bad output port {}, availabile: {:?}",
                    port,
                    ig.into_map.keys().collect::<Vec<_>>()
                ))?;
                old_index_to_new_index.insert(old_idx, new_idx);
            } else if let NodeKind::Input { .. } = &node.kind {
                inputs.insert(old_idx);
                dbg!(&node);
            } else {
                let new_idx = pipe_graph.add_node(node.config.clone());
                pipe_ops.insert(new_idx, node);
                old_index_to_new_index.insert(old_idx, new_idx);
            }
        }

        for ((from_id, from_port), tos) in ig.graph.port_indexes {
            if inputs.contains(&from_id) {
                // skip the classical inputs
                continue;
            };
            let from_id = old_index_to_new_index
                .get(&from_id)
                .copied()
                .ok_or("FIXME: invalid graph unknown from_id")?;
            for (to_id, to_port) in tos {
                let to_id = old_index_to_new_index
                    .get(&to_id)
                    .copied()
                    .ok_or("FIXME: invalid graph unknown to_id")?;
                let con = Connection {
                    from: from_port.clone(),
                    to: to_port,
                };
                pipe_graph.add_edge(from_id, to_id, con);
            }
        }
        for (input, target) in ig.graph.inputs {
            let to_id = old_index_to_new_index
                .get(&target)
                .copied()
                .ok_or("FIXME: invalid graph unknown input target")?;
            let input: &str = &input;
            let from_id = ig
                .from_map
                .get(input)
                .cloned()
                .ok_or("FIXME: invalid graph unknown input from_id")?;
            // FIXME: what is the to port here?
            let con = Connection {
                from: "out".into(),
                to: "in".into(),
            };
            pipe_graph.add_edge(from_id, to_id, con);
        }
    }

    let dot = petgraph::dot::Dot::with_attr_getters(
        &pipe_graph,
        &[],
        &|_g, _r| "".to_string(),
        &|_g, (_i, c)| match c {
            NodeConfig {
                kind: NodeKind::Input,
                ..
            } => r#"shape = "rarrow""#.to_string(),
            NodeConfig {
                kind: NodeKind::Output(_),
                ..
            } => r#"shape = "larrow""#.to_string(),
            NodeConfig {
                kind: NodeKind::Select,
                ..
            } => r#"shape = "box""#.to_string(),
            NodeConfig {
                kind: NodeKind::Script,
                ..
            } => r#"shape = "note""#.to_string(),
            NodeConfig {
                kind: NodeKind::Operator,
                ..
            } => "".to_string(),
        },
    );

    // iff cycles, fail and bail
    if is_cyclic_directed(&pipe_graph) {
        Err(ErrorKind::CyclicGraphError(format!("{}", dot)).into())
    } else {
        let mut i2pos = HashMap::new();
        let mut graph = Vec::new();
        // Nodes that handle contraflow
        let mut contraflow = Vec::new();
        // Nodes that handle signals
        let mut signalflow = Vec::new();
        for (i, nx) in pipe_graph.node_indices().enumerate() {
            let op = pipe_ops
                .remove(&nx)
                .ok_or_else(|| format!("Invalid pipeline can't find node {:?}", &nx))?;
            i2pos.insert(nx, i);
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

        let mut port_indexes2 = HashMap::new();
        for ((i1, s1), connections) in &port_indexes {
            let connections = connections
                .iter()
                .filter_map(|(i, s)| Some((*i2pos.get(i)?, s.clone())))
                .collect();
            let k = *i2pos.get(i1).ok_or_else(|| Error::from("Invalid graph"))?;
            port_indexes2.insert((k, s1.clone()), connections);
        }

        let mut inputs2: HashMap<beef::Cow<'static, str>, usize> = HashMap::new();
        for (k, idx) in &inputs {
            let v = *i2pos.get(idx).ok_or_else(|| Error::from("Invalid graph"))?;
            inputs2.insert(k.clone(), v);
        }

        let mut exec = ExecutableGraph {
            metrics: iter::repeat(NodeMetrics::default())
                .take(graph.len())
                .collect(),
            stack: Vec::with_capacity(graph.len()),
            id: pipeline_id.to_string(), // TODO make configurable
            last_metrics: 0,
            state: State::new(iter::repeat(Value::null()).take(graph.len()).collect()),
            graph,
            inputs: inputs2,
            port_indexes: port_indexes2,
            contraflow,
            signalflow,
            metric_interval,
            insights: Vec::new(),
            dot: format!("{}", dot),
            metrics_channel: METRICS_CHANNEL.tx(),
        };
        exec.optimize();

        Ok(exec)
    }
}

fn prefix_for(s: &PipelineCreate) -> String {
    format!("FIXME: this should be random {}", s.alias)
}
fn from_name(prefix: &str, port: &str) -> String {
    format!("{prefix}-from/{port}")
}
fn into_name(prefix: &str, port: &str) -> String {
    format!("{prefix}-into/{port}")
}

fn select(
    operator_uid: u64,
    config: &NodeConfig,
    node: &ast::SelectStmt<'static>,
    windows: Option<HashMap<String, window::Impl>>,
) -> Result<Box<dyn Operator>> {
    let select_type = node.complexity();
    match select_type {
        SelectType::Passthrough => {
            let op = PassthroughFactory::new_boxed();
            op.from_node(operator_uid, config)
        }
        SelectType::Simple => Ok(Box::new(SimpleSelect::with_stmt(config.id.clone(), node)?)),
        SelectType::Normal => {
            let windows = windows.ok_or_else(|| {
                ErrorKind::MissingOpConfig("select operators require a window mapping".into())
            })?;
            let windows: Result<Vec<(String, window::Impl)>> = node
                .stmt
                .windows
                .iter()
                .map(|w| {
                    let fqwn = w.id.fqn();
                    Ok(windows
                        .get(&fqwn)
                        .map(|imp| (w.id.id().to_string(), imp.clone()))
                        .ok_or_else(|| {
                            ErrorKind::BadOpConfig(format!(
                                "Unknown window: {} available: {}",
                                &fqwn,
                                windows.keys().cloned().collect::<Vec<_>>().join(", ")
                            ))
                        })?)
                })
                .collect();

            Ok(Box::new(Select::with_stmt(
                operator_uid,
                config.id.clone(),
                windows?,
                node,
            )?))
        }
    }
}

fn operator(
    operator_uid: u64,
    node: &ast::OperatorDefinition<'static>,
    helper: &mut Helper,
) -> Result<Box<dyn Operator>> {
    Ok(Box::new(TrickleOperator::with_stmt(
        operator_uid,
        node,
        helper,
    )?))
}

pub(crate) fn supported_operators(
    config: &NodeConfig,
    uid: u64,
    node: Option<&ast::Stmt<'static>>,
    windows: Option<HashMap<String, window::Impl>>,
    helper: &mut Helper,
) -> Result<OperatorNode> {
    let op: Box<dyn op::Operator> = match node {
        Some(ast::Stmt::ScriptDefinition(script)) => Box::new(op::trickle::script::Script {
            id: format!("FIXME: {uid}"),
            script: tremor_script::Script {
                script: script.script.clone(),
                aid: script.aid(),
                warnings: BTreeSet::new(),
            },
        }),
        Some(tremor_script::ast::Stmt::SelectStmt(s)) => select(uid, config, s, windows)?,
        Some(ast::Stmt::OperatorDefinition(o)) => operator(uid, o, helper)?,
        _ => crate::operator(uid, config)?,
    };

    Ok(OperatorNode {
        uid,
        id: config.id.clone(),
        kind: config.kind.clone(),
        op_type: config.op_type.clone(),
        op,
        config: config.clone(),
    })
}

pub(crate) fn make_builtin_node_name_checker() -> impl Fn(&Cow<'static, str>) -> bool {
    // saving these names for reuse
    let builtin_node_names: Vec<Cow<'static, str>> =
        BUILTIN_NODES.iter().map(|(n, _)| n.clone()).collect();
    move |name| builtin_node_names.contains(name)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn query() {
        let aggr_reg = tremor_script::aggr_registry();

        let src = "select event from in into out;";
        let query =
            Query::parse(src, &*tremor_script::FN_REGISTRY.read().unwrap(), &aggr_reg).unwrap();
        assert!(query.id().is_none());

        // check that we can overwrite the id with a config variable
        let src = "#!config id = \"test\"\nselect event from in into out;";
        let query =
            Query::parse(src, &*tremor_script::FN_REGISTRY.read().unwrap(), &aggr_reg).unwrap();
        assert_eq!(query.id().unwrap(), "test");
    }

    #[test]
    fn custom_port() {
        let aggr_reg = tremor_script::aggr_registry();

        let src = "select event from in/test_in into out/test_out;";
        let q = Query::parse(src, &*tremor_script::FN_REGISTRY.read().unwrap(), &aggr_reg).unwrap();

        let mut idgen = OperatorIdGen::new();
        let first = idgen.next_id();
        let g = q.to_pipe(&mut idgen).unwrap();
        assert!(g.inputs.contains_key("in/test_in"));
        assert_eq!(idgen.next_id(), first + g.graph.len() as u64 + 1);
        let out = g.graph.get(4).unwrap();
        assert_eq!(out.id, "out/test_out");
        assert_eq!(out.kind, NodeKind::Output("test_out".into()));
    }

    #[test]
    fn builtin_nodes() {
        let has_builtin_node_name = make_builtin_node_name_checker();
        assert!(has_builtin_node_name(&"in".into()));
        assert!(has_builtin_node_name(&"out".into()));
        assert!(has_builtin_node_name(&"err".into()));
        assert!(!has_builtin_node_name(&"snot".into()));
        assert!(!has_builtin_node_name(&"badger".into()));
    }
}
