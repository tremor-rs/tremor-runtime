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
    ConfigGraph, Connection, ExecPortIndexMap, ExecutableGraph, NodeConfig, NodeKind, NodeMetrics,
    Operator, OperatorNode, State, METRICS_CHANNEL,
};
use beef::Cow;
use halfbrown::HashMap;
use indexmap::IndexMap;
use petgraph::{
    algo::is_cyclic_directed,
    graph::NodeIndex,
    EdgeDirection::{Incoming, Outgoing},
    Graph,
};
use std::collections::BTreeSet;
use std::iter;
use tremor_common::ids::OperatorIdGen;
use tremor_script::{
    ast::{
        self,
        visitors::{ArgsRewriter, ConstFolder},
        walkers::QueryWalker,
        Helper, Ident, OperatorDefinition, PipelineCreate, PipelineDefinition, ScriptDefinition,
        SelectType, Stmt, WindowDefinition, WindowKind,
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
        let aggr_reg = tremor_script::aggr_registry();
        let reg = tremor_script::FN_REGISTRY.read()?;
        let mut helper = Helper::new(&reg, &aggr_reg);
        helper.scope = self.0.query.scope.clone();
        let mut pipe_graph = ConfigGraph::new();
        let mut nodes_by_name: HashMap<Cow<'static, str>, _> = HashMap::new();
        let mut links: IndexMap<OutputPort, Vec<InputPort>> = IndexMap::new();
        let metric_interval = self
            .0
            .query
            .config
            .get("metrics_interval_s")
            .and_then(Value::as_u64)
            .map(|i| i * 1_000_000_000);

        let pipeline_id = self
            .0
            .query
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
            nodes_by_name.insert(name.clone(), id);
        }

        let mut select_num = 0;

        let has_builtin_node_name = make_builtin_node_name_checker();

        let mut included_graphs: HashMap<String, InlcudedGraph> = HashMap::new();
        for stmt in &self.0.query.stmts {
            match stmt {
                // Ignore the definitions
                Stmt::WindowDefinition(_)
                | Stmt::ScriptDefinition(_)
                | Stmt::OperatorDefinition(_)
                | Stmt::PipelineDefinition(_) => {}

                Stmt::SelectStmt(ref select) => {
                    // Rewrite pipeline/port to their internal streams
                    let mut select = select.clone();
                    // Note we connecto from stmt.from to graph.into
                    // and stmt.into to graph.from
                    // FIXME: error handling
                    let (node, port) = &mut select.stmt.from;
                    if let Some(g) = included_graphs.get(node.as_str()) {
                        let name = into_name(&g.prefix, port.as_str());
                        node.id = name.into();
                        // port.id = "out".into();
                    }
                    let (node, port) = &mut select.stmt.into;
                    if let Some(g) = included_graphs.get(node.as_str()) {
                        let name = from_name(&g.prefix, port.as_str());
                        node.id = name.into();
                        // port.id = "in".into();
                    }

                    let s: &ast::Select<'_> = &select.stmt;

                    if !nodes_by_name.contains_key(&s.from.0.id) {
                        return Err(query_stream_not_defined_err(
                            s,
                            &s.from.0,
                            s.from.0.to_string(),
                            s.from.1.to_string(),
                        )
                        .into());
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
                        if !nodes_by_name.contains_key(&name) {
                            let id = pipe_graph.add_node(NodeConfig {
                                id: name.to_string(),
                                kind: NodeKind::Input,
                                op_type: "passthrough".to_string(),
                                ..NodeConfig::default()
                            });
                            nodes_by_name.insert(name.clone(), id);
                        }
                    }
                    let mut into = resolve_input_port(&s.into);
                    if into.id == "out" && into.port != "in" {
                        let name: Cow<'static, str> = format!("out/{}", into.port).into();
                        into.id = name.clone();
                        if !nodes_by_name.contains_key(&name) {
                            let id = pipe_graph.add_node(NodeConfig {
                                id: name.to_string(),
                                label: Some(name.to_string()),
                                kind: NodeKind::Output(into.port.clone()),
                                op_type: "passthrough".to_string(),
                                ..NodeConfig::default()
                            });
                            nodes_by_name.insert(name, id);
                        }
                    }

                    links.entry(from).or_default().push(select_in.clone());
                    links.entry(select_out).or_default().push(into);

                    let node = NodeConfig {
                        id: select_in.id.to_string(),
                        label,
                        kind: NodeKind::Select,
                        op_type: "trickle::select".to_string(),
                        stmt: Some(stmt.clone()),
                        ..NodeConfig::default()
                    };
                    let id = pipe_graph.add_node(node.clone());

                    nodes_by_name.insert(select_in.id.clone(), id);
                }
                Stmt::StreamStmt(s) => {
                    let name = common_cow(&s.id);
                    let id = name.clone();
                    if nodes_by_name.contains_key(&id) {
                        return Err(query_stream_duplicate_name_err(s, s, s.id.to_string()).into());
                    }

                    let node = NodeConfig {
                        id: id.to_string(),
                        kind: NodeKind::Operator,
                        op_type: "passthrough".to_string(),
                        stmt: Some(stmt.clone()),
                        ..NodeConfig::default()
                    };
                    let id = pipe_graph.add_node(node.clone());
                    nodes_by_name.insert(name.clone(), id);
                }
                Stmt::PipelineCreate(s) => {
                    let name = s.alias.clone();
                    if nodes_by_name.contains_key(name.as_str()) {
                        let error_func = if has_builtin_node_name(name.as_str()) {
                            query_node_reserved_name_err
                        } else {
                            query_node_duplicate_name_err
                        };
                        return Err(error_func(s, name).into());
                    }
                    // This is just a placeholder!
                    nodes_by_name.insert(name.clone().into(), NodeIndex::default());

                    if let Some(pd) = helper.get::<PipelineDefinition>(&s.target)? {
                        let prefix = prefix_for(&s);

                        let query = Query(tremor_script::Query::from_query(
                            pd.to_query(&s.params, &mut helper)?,
                        ));

                        let mut from_map = HashMap::new();
                        for f in pd.from {
                            let name = from_name(&prefix, f.as_str());
                            let node = NodeConfig {
                                id: name.clone(),
                                kind: NodeKind::Operator,
                                op_type: "passthrough".to_string(),
                                stmt: Some(stmt.clone()),
                                ..NodeConfig::default()
                            };
                            let id = pipe_graph.add_node(node.clone());
                            from_map.insert(f.to_string(), id);
                            nodes_by_name.insert(name.clone().into(), id);
                        }

                        let mut into_map = HashMap::new();
                        for i in pd.into {
                            let name = into_name(&prefix, i.as_str());
                            let node = NodeConfig {
                                id: name.clone(),
                                kind: NodeKind::Operator,
                                op_type: "passthrough".to_string(),
                                stmt: Some(stmt.clone()),
                                ..NodeConfig::default()
                            };
                            let id = pipe_graph.add_node(node.clone());
                            into_map.insert(i.to_string(), id);
                            nodes_by_name.insert(name.clone().into(), id);
                        }

                        let mut graph = query.to_pipe(idgen)?;
                        graph.optimize();

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
                    if nodes_by_name.contains_key(&common_cow(o.node_id.id())) {
                        let error_func = if has_builtin_node_name(&common_cow(o.node_id.id())) {
                            query_node_reserved_name_err
                        } else {
                            query_node_duplicate_name_err
                        };
                        return Err(error_func(o, o.node_id.id().to_string()).into());
                    }

                    let mut decl: OperatorDefinition =
                        helper.get(&o.target)?.ok_or("operator not found")?;

                    decl.params.ingest_creational_with(&o.params)?;

                    let that = Stmt::OperatorDefinition(decl);
                    let node = NodeConfig {
                        id: o.node_id.id().to_string(),
                        kind: NodeKind::Operator,
                        op_type: "trickle::operator".to_string(),
                        stmt: Some(that),
                        ..NodeConfig::default()
                    };
                    let id = pipe_graph.add_node(node.clone());

                    nodes_by_name.insert(common_cow(o.node_id.id()), id);
                }
                Stmt::ScriptCreate(o) => {
                    if nodes_by_name.contains_key(&common_cow(o.node_id.id())) {
                        let error_func = if has_builtin_node_name(&common_cow(o.node_id.id())) {
                            query_node_reserved_name_err
                        } else {
                            query_node_duplicate_name_err
                        };
                        return Err(error_func(o, o.node_id.id().to_string()).into());
                    }

                    // FIXME: Better error
                    let mut decl: ScriptDefinition = helper
                        .get(&o.target)?
                        .ok_or_else(|| format!("script not found: {}", &o.target,))?;
                    decl.params.ingest_creational_with(&o.params)?;
                    let inner_args = decl.params.render()?;
                    ArgsRewriter::new(inner_args, &mut helper).walk_script_decl(&mut decl)?;
                    ConstFolder::new(&mut helper).walk_script_decl(&mut decl)?;

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
                        stmt: Some(that_defn),
                        ..NodeConfig::default()
                    };
                    let id = pipe_graph.add_node(node.clone());

                    nodes_by_name.insert(common_cow(o.node_id.id()), id);
                }
            };
        }

        // Link graph edges
        for (from, tos) in &links {
            for to in tos {
                let from_idx = *nodes_by_name.get(&from.id).ok_or_else(|| {
                    query_stream_not_defined_err(
                        from,
                        from,
                        from.id.to_string(),
                        from.port.to_string(),
                    )
                })?;
                let to_idx = *nodes_by_name.get(&to.id).ok_or_else(|| {
                    query_stream_not_defined_err(to, to, to.id.to_string(), to.port.to_string())
                })?;

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

            for (old_idx, mut node) in ig.graph.graph.into_iter().enumerate() {
                let new_idx = if let NodeKind::Output(port) = &node.kind {
                    let port: &str = &port;
                    node.config.kind = NodeKind::Operator;
                    let new_idx = pipe_graph.add_node(node.config);
                    let to_id = ig.into_map.get(port).ok_or(format!(
                        "FIXME: invalid sub graph bad output port {}, availabile: {:?}",
                        port,
                        ig.into_map.keys().collect::<Vec<_>>()
                    ))?;
                    let con = Connection {
                        from: "out".into(),
                        to: "in".into(),
                    };
                    pipe_graph.add_edge(new_idx, *to_id, con);
                    new_idx
                } else if let NodeKind::Input { .. } = &node.kind {
                    node.config.kind = NodeKind::Operator;
                    let port = node.id;
                    let new_idx = pipe_graph.add_node(node.config);
                    let from_id = ig.from_map.get(&port).ok_or(format!(
                        "FIXME: invalid sub graph bad input port {}, availabile: {:?}",
                        port,
                        ig.from_map.keys().collect::<Vec<_>>()
                    ))?;
                    let con = Connection {
                        from: "out".into(),
                        to: "in".into(),
                    };
                    pipe_graph.add_edge(*from_id, new_idx, con);
                    new_idx
                } else {
                    pipe_graph.add_node(node.config)
                };
                old_index_to_new_index.insert(old_idx, new_idx);
            }

            for ((from_id, from_port), tos) in ig.graph.port_indexes {
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
        }

        loop {
            let mut unused = Vec::new();
            for idx in pipe_graph
                .externals(Incoming)
                .chain(pipe_graph.externals(Outgoing))
            {
                if !matches!(
                    pipe_graph.node_weight(idx),
                    Some(NodeConfig {
                        kind: NodeKind::Input | NodeKind::Output(_),
                        ..
                    })
                ) {
                    unused.push(idx);
                }
            }
            if unused.is_empty() {
                break;
            }
            for idx in unused.drain(..) {
                pipe_graph.remove_node(idx);
            }
        }

        let dot = petgraph::dot::Dot::with_attr_getters(
            &pipe_graph,
            &[],
            &|_g, _r| "".to_string(),
            &node_to_dot,
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
                let op = pipe_graph
                    .node_weight(nx)
                    .ok_or_else(|| {
                        Error::from(format!("Invalid pipeline can't find node {:?}", &nx))
                    })
                    .and_then(|node| {
                        node.to_op(idgen.next_id(), supported_operators, &mut helper)
                    })?;
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

            let mut port_indexes: ExecPortIndexMap = HashMap::new();
            for idx in pipe_graph.edge_indices() {
                let (from, to) = pipe_graph.edge_endpoints(idx).ok_or("invalid edge")?;
                let ports = pipe_graph.edge_weight(idx).cloned().ok_or("invalid edge")?;
                let from = *i2pos
                    .get(&from)
                    .ok_or_else(|| Error::from("Invalid graph - failed to build connections"))?;
                let to = *i2pos
                    .get(&to)
                    .ok_or_else(|| Error::from("Invalid graph - failed to build connections"))?;
                let from = (from, ports.from);
                let to = (to, ports.to);
                if let Some(connections) = port_indexes.get_mut(&from) {
                    connections.push(to);
                } else {
                    port_indexes.insert(from, vec![to]);
                }
            }

            let mut inputs: HashMap<beef::Cow<'static, str>, usize> = HashMap::new();
            for idx in pipe_graph.externals(Incoming) {
                if let Some(NodeConfig {
                    kind: NodeKind::Input,
                    id,
                    ..
                }) = pipe_graph.node_weight(idx)
                {
                    let v = *i2pos
                        .get(&idx)
                        .ok_or_else(|| Error::from("Invalid graph - failed to build inputs"))?;
                    inputs.insert(id.clone().into(), v);
                }
            }

            Ok(ExecutableGraph {
                metrics: iter::repeat(NodeMetrics::default())
                    .take(graph.len())
                    .collect(),
                stack: Vec::with_capacity(graph.len()),
                id: pipeline_id.to_string(), // TODO make configurable
                last_metrics: 0,
                state: State::new(iter::repeat(Value::null()).take(graph.len()).collect()),
                graph,
                inputs,
                port_indexes,
                contraflow,
                signalflow,
                metric_interval,
                insights: Vec::new(),
                dot: format!("{}", dot),
                metrics_channel: METRICS_CHANNEL.tx(),
            })
        }
    }
}

struct InlcudedGraph {
    prefix: String,
    graph: ExecutableGraph,
    from_map: HashMap<String, NodeIndex>,
    into_map: HashMap<String, NodeIndex>,
}

fn node_to_dot(_g: &Graph<NodeConfig, Connection>, (_, c): (NodeIndex, &NodeConfig)) -> String {
    match c {
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
    helper: &Helper,
) -> Result<Box<dyn Operator>> {
    let select_type = node.complexity();
    match select_type {
        SelectType::Passthrough => {
            let op = PassthroughFactory::new_boxed();
            op.from_node(operator_uid, config)
        }
        SelectType::Simple => Ok(Box::new(SimpleSelect::with_stmt(config.id.clone(), node)?)),
        SelectType::Normal => {
            let windows: Result<Vec<(String, window::Impl)>> = node
                .stmt
                .windows
                .iter()
                .map(|w| {
                    Ok(helper
                        .get::<WindowDefinition>(&w.id)?
                        .ok_or_else(|| {
                            ErrorKind::BadOpConfig(format!("Unknown window: {} available", &w.id,))
                        })
                        .and_then(|imp| Ok((w.id.id().to_string(), window_decl_to_impl(&imp)?)))?)
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
        Some(tremor_script::ast::Stmt::SelectStmt(s)) => select(uid, config, s, helper)?,
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

pub(crate) fn make_builtin_node_name_checker() -> impl Fn(&str) -> bool {
    // saving these names for reuse

    move |name| BUILTIN_NODES.iter().any(|k| k.0 == name)
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
