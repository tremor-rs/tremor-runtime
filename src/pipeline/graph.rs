// Copyright 2018, Wayfair GmbH
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

use super::onramp::*;
use super::op::*;
use super::step::*;
use super::types::*;
use crate::errors::*;
use actix;
use actix::prelude::*;
use dot;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::fs::File;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub type GraphLink = Arc<Mutex<Graph>>;

enum ActorOrOp {
    Op(Box<Op>),
    Actor(Addr<Step>),
}

#[derive(Debug, Clone)]
pub struct NodeTypes {
    in_type: ValueType,
    out_type: ValueType,
}

#[derive(Clone, PartialEq, Eq, PartialOrd)]
struct GraphEdge {
    source: GraphNode,
    target: GraphNode,
    id: Option<usize>,
    thread_boundary: bool,
}
impl Ord for GraphEdge {
    fn cmp(&self, other: &GraphEdge) -> Ordering {
        if self.source == other.source {
            self.target.cmp(&other.target)
        } else {
            self.source.cmp(&other.source)
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd)]
struct GraphNode {
    uuid: Uuid,
    title: String,
}
impl Ord for GraphNode {
    fn cmp(&self, other: &GraphNode) -> Ordering {
        self.uuid.cmp(&other.uuid)
    }
}

pub enum Graph {
    OnRamp {
        name: String,
        outputs: Vec<GraphLink>,
        uuid: Uuid,
    },
    Leaf,
    Pipeline {
        actor: Addr<Step>,
        head: GraphLink,
        name: String,
        onramp: Addr<OnRampActor>,
        types: NodeTypes,
        uuid: Uuid,
    },
    Node {
        actor: Option<Addr<Step>>,
        err: GraphLink,
        op: OpSpec,
        out: GraphLink,
        outputs: Vec<GraphLink>,
        types: NodeTypes,
        uuid: Uuid,
    },
}

impl<'a> dot::Labeller<'a, GraphNode, GraphEdge> for Graph {
    fn graph_id(&'a self) -> dot::Id<'a> {
        dot::Id::new("graph_id").unwrap()
    }

    fn node_id(&'a self, n: &GraphNode) -> dot::Id<'a> {
        let id = format!("NODE{}", n.uuid.to_string().replace("-", ""));
        dot::Id::new(id).unwrap()
    }
    fn node_label<'b>(&'b self, n: &GraphNode) -> dot::LabelText<'b> {
        dot::LabelText::LabelStr(Cow::Owned(n.title.clone()))
    }
    fn edge_label<'b>(&'b self, e: &GraphEdge) -> dot::LabelText<'b> {
        match e.id {
            Some(1) => dot::LabelText::LabelStr("std".into()),
            Some(2) => dot::LabelText::LabelStr("err".into()),
            Some(n) => dot::LabelText::LabelStr(format!("output-{}", n).into()),
            None => dot::LabelText::LabelStr("".into()),
        }
    }
    fn edge_style(&'a self, e: &GraphEdge) -> dot::Style {
        if e.thread_boundary {
            dot::Style::Dashed
        } else {
            dot::Style::Solid
        }
    }
}

impl<'a> dot::GraphWalk<'a, GraphNode, GraphEdge> for Graph {
    fn nodes(&self) -> dot::Nodes<'a, GraphNode> {
        let nodes = self.serialize_nodes(Vec::new());
        Cow::Owned(nodes)
    }

    fn edges(&'a self) -> dot::Edges<'a, GraphEdge> {
        let edges = self.serialize_edges(Vec::new());
        Cow::Owned(edges)
    }

    fn source(&self, e: &GraphEdge) -> GraphNode {
        e.source.clone()
    }

    fn target(&self, e: &GraphEdge) -> GraphNode {
        e.target.clone()
    }
}

impl fmt::Debug for Graph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Graph::Leaf => write!(f, "Graph::Leaf"),
            Graph::OnRamp { name, .. } => write!(f, "OnRamp::{}", name),
            Graph::Pipeline { name, .. } => write!(f, "Pipeline::{}", name),
            Graph::Node { out, err, op, .. } => write!(f, "{:?} => {:?} / {:?}", op, out, err),
        }
    }
}

impl Graph {
    pub fn write_dot(&self, file: &str) {
        let mut f = File::create(file).unwrap();
        dot::render(self, &mut f).unwrap()
    }
    fn is_pipeline(&self) -> bool {
        match self {
            Graph::Pipeline { .. } => true,
            _ => false,
        }
    }
    fn uuid(&self) -> Option<Uuid> {
        match self {
            Graph::Leaf => None,
            Graph::OnRamp { uuid, .. } => Some(*uuid),
            Graph::Pipeline { uuid, .. } => Some(*uuid),
            Graph::Node { uuid, .. } => Some(*uuid),
        }
    }
    fn title(&self) -> String {
        match self {
            Graph::Leaf => "Leaf".to_string(),
            Graph::OnRamp { name, .. } => format!("OnRamp: {}", name),
            Graph::Pipeline { name, .. } => format!("Pipeline: {}", name),
            Graph::Node { op, .. } => format!("{}::{}", op.optype, op.name),
        }
    }
    fn serialize_nodes(&self, nodes: Vec<GraphNode>) -> Vec<GraphNode> {
        match self {
            Graph::Leaf => nodes,
            Graph::OnRamp { uuid, outputs, .. } => {
                let mut nodes = nodes;
                let n = GraphNode {
                    uuid: *uuid,
                    title: self.title(),
                };
                nodes.push(n);

                for o in outputs {
                    let mut nodes2 = o.lock().unwrap().serialize_nodes(Vec::new());
                    nodes.append(&mut nodes2);
                }
                nodes.sort();
                nodes.dedup();
                nodes
            }
            Graph::Pipeline { uuid, head, .. } => {
                let n = GraphNode {
                    uuid: *uuid,
                    title: self.title(),
                };
                let mut nodes = nodes;
                nodes.push(n);
                let mut nodes = head.lock().unwrap().serialize_nodes(nodes);
                nodes.sort();
                nodes.dedup();
                nodes
            }
            Graph::Node {
                uuid,
                outputs,
                out,
                err,
                ..
            } => {
                let mut nodes = nodes;
                let n = GraphNode {
                    uuid: *uuid,
                    title: self.title(),
                };
                nodes.push(n);

                let mut outputs = outputs.clone();
                outputs.reverse();
                outputs.push(err.clone());
                outputs.push(out.clone());
                outputs.reverse();

                for o in outputs {
                    let mut nodes2 = o.lock().unwrap().serialize_nodes(Vec::new());
                    nodes.append(&mut nodes2);
                }
                nodes.sort();
                nodes.dedup();
                nodes
            }
        }
    }

    fn serialize_edges(&self, edges: Vec<GraphEdge>) -> Vec<GraphEdge> {
        match self {
            Graph::Leaf => edges,
            Graph::OnRamp { uuid, outputs, .. } => {
                let mut edges = edges;
                let n = GraphNode {
                    uuid: *uuid,
                    title: self.title(),
                };

                for o in outputs {
                    let o = o.lock().unwrap();
                    if let Some(o_uuid) = o.uuid() {
                        let n2 = GraphNode {
                            uuid: o_uuid,
                            title: o.title(),
                        };
                        edges.push(GraphEdge {
                            source: n.clone(),
                            target: n2,
                            id: None,
                            thread_boundary: o.is_pipeline(),
                        });
                    }
                    let mut edges2 = o.serialize_edges(Vec::new());
                    edges.append(&mut edges2);
                }
                edges.sort();
                edges.dedup();
                edges
            }
            Graph::Pipeline { uuid, head, .. } => {
                let head = head.lock().unwrap();
                let mut edges = edges;
                if let Some(head_uuid) = head.uuid() {
                    let n = GraphNode {
                        uuid: *uuid,
                        title: self.title(),
                    };
                    let n2 = GraphNode {
                        uuid: head_uuid,
                        title: head.title(),
                    };
                    edges.push(GraphEdge {
                        source: n.clone(),
                        target: n2,
                        id: Some(1),
                        thread_boundary: head.is_pipeline(),
                    });
                }
                let mut edges2 = head.serialize_edges(Vec::new());
                edges.append(&mut edges2);
                edges.sort();
                edges.dedup();
                edges
            }
            Graph::Node {
                uuid,
                outputs,
                out,
                err,
                ..
            } => {
                let mut edges = edges;
                let n = GraphNode {
                    uuid: *uuid,
                    title: self.title(),
                };

                let mut outputs = outputs.clone();
                outputs.reverse();
                outputs.push(err.clone());
                outputs.push(out.clone());
                outputs.reverse();
                let mut i = 1;
                for o in outputs {
                    let o = o.lock().unwrap();
                    if let Some(o_uuid) = o.uuid() {
                        let n2 = GraphNode {
                            uuid: o_uuid,
                            title: o.title(),
                        };
                        edges.push(GraphEdge {
                            source: n.clone(),
                            target: n2,
                            id: Some(i),
                            thread_boundary: o.is_pipeline(),
                        });
                    }
                    i += 1;
                    let mut edges2 = o.serialize_edges(Vec::new());
                    edges.append(&mut edges2);
                }
                edges.sort();
                edges.dedup();
                edges
            }
        }
    }
    pub fn leaf() -> GraphLink {
        Arc::new(Mutex::new(Graph::Leaf))
    }
    pub fn node(op: OpSpec, out: GraphLink, err: GraphLink, outputs: Vec<GraphLink>) -> GraphLink {
        Arc::new(Mutex::new(Graph::Node {
            uuid: op.uuid,
            actor: None,
            types: NodeTypes {
                in_type: ValueType::Any,
                out_type: ValueType::Any,
            },
            op,
            out,
            err,
            outputs,
        }))
    }
    pub fn make_actor(&mut self) -> Result<(Option<Addr<Step>>, NodeTypes)> {
        match self {
            Graph::Pipeline { actor, types, .. } => Ok((Some(actor.clone()), types.clone())),
            Graph::Node {
                actor: Some(act),
                types,
                ..
            } => Ok((Some(act.clone()), types.clone())),
            Graph::Node {
                actor: actor @ None,
                types,
                out,
                err,
                op: op_spec,
                outputs,
                ..
            } => {
                let mut outv = Vec::new();
                let (out, out_types) = out.lock().unwrap().make_actor()?;
                let (err, err_types) = err.lock().unwrap().make_actor()?;

                outv.push(out);
                outv.push(err);

                let op = op_spec.to_op()?;

                let in_type = match op.input_type() {
                    ValueType::Same => out_types.in_type,
                    t => t,
                };
                let out_type = match op.output_type() {
                    ValueType::Same => out_types.in_type,
                    t => t,
                };
                let my_types = NodeTypes { in_type, out_type };

                if outputs.is_empty() && outv.len() < 2 {
                    return Err("Can not specify additional outputs without specifying both the standard and the error output.".into());
                }
                for o in outputs.iter() {
                    let (o, o_types) = o.lock().unwrap().make_actor()?;
                    if my_types.out_type != o_types.in_type && o_types.in_type != ValueType::Any {
                        return type_error!(
                            format!("{}::{} (output)", op_spec.optype, op_spec.name),
                            my_types.out_type,
                            o_types.out_type
                        );
                    };
                    outv.push(o);
                }

                // If our in and output types are set to `ValueType::Same` we pass through the types
                // of the next step in the pipeline, this means our step doesn't require specific types
                // and doesn't affect them either

                if my_types.out_type != out_types.in_type && out_types.in_type != ValueType::Any {
                    return type_error!(
                        format!("{}::{} (output)", op_spec.optype, op_spec.name),
                        my_types.out_type,
                        out_types.out_type
                    );
                };
                // We'll not have finished our step so the error has to work on the input type
                // not the output type.
                if my_types.in_type != err_types.in_type && err_types.in_type != ValueType::Any {
                    return type_error!(
                        format!("{}::{} (output)", op_spec.optype, op_spec.name),
                        my_types.out_type,
                        err_types.out_type
                    );
                };
                let step = Step::actor(op, outv);
                *types = my_types.clone();
                *actor = Some(step.clone());
                Ok((Some(step), my_types))
            }
            _ => Ok((
                None,
                NodeTypes {
                    in_type: ValueType::Any,
                    out_type: ValueType::Any,
                },
            )),
        }
    }
}
