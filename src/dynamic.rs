#![allow(dead_code)]
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

use crate::config;
use crate::errors::*;
use actix;
use actix::prelude::*;
use petgraph::algo::is_cyclic_directed;
use petgraph::dot::{Config, Dot};
use petgraph::graphmap;
use std::cmp::Ord;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug)]
pub struct RampSet {
    config: config::RampSet,
    // ...
}

pub type RampSetVec = Vec<RampSet>;
pub type BindingVec = config::BindingVec;

#[derive(Debug)]
pub struct Pipeline {
    config: config::Pipeline,
    graph: (Graph, bool),
}

pub type PipelineVec = Vec<Pipeline>;

#[derive(Debug)]
pub struct Todo {
    ramps: RampSetVec,
    bindings: BindingVec,
    pipes: PipelineVec,
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
enum NodeKind {
    Input,
    Output,
    Operator,
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
struct Stream {
    id: u32,
    kind: NodeKind,
}

#[derive(Debug)]
pub enum Edge {
    Link(config::Port, config::Port),
    InputStreamLink(String, config::Port),
    OutputStreamLink(config::Port, String),
    PassthroughStream(String, String),
}

type Graph = graphmap::DiGraphMap<Stream, u32>;

pub fn incarnate(config: config::Config) -> Todo {
    let ramps = incarnate_ramps(config.ramps.clone());
    let bindings = incarnate_links(config.bindings);
    let pipes = incarnate_pipes(config.pipelines);
    // validate links ...
    // ... registry
    // check conflicts ( deploys, pipes )
    // check deps ( links )
    // push deploys, pipes, .... links ( always last )
    Todo {
        ramps,
        bindings,
        pipes,
    }
}

fn incarnate_ramps(config: config::RampSetVec) -> RampSetVec {
    config
        .iter()
        .map(|d| RampSet { config: d.clone() })
        .collect()
}

fn incarnate_links(config: config::BindingVec) -> BindingVec {
    config.clone()
}

fn pipeline_to_graph(config: config::Pipeline) -> (Graph, bool) {
    let mut graph = Graph::new(); // <u32, ()>
    let mut nodes = HashMap::new(); // <u32, Stream>
    let mut sedon = HashMap::new(); // <String, u32>

    let mut id = 0;
    for stream in &config.interface.inputs {
        nodes.insert(
            id,
            Stream {
                id,
                kind: NodeKind::Input,
            },
        );
        sedon.insert(stream.clone(), id);
        graph.add_node(nodes[&sedon[stream]]);
        id += 1;
    }

    for node in &config.nodes {
        nodes.insert(
            id,
            Stream {
                id,
                kind: NodeKind::Operator,
            },
        );
        sedon.insert(node.id.clone(), id);
        graph.add_node(nodes[&sedon[&node.id]]);
        id += 1;
    }

    for stream in &config.interface.outputs {
        nodes.insert(
            id,
            Stream {
                id,
                kind: NodeKind::Output,
            },
        );
        sedon.insert(stream.clone(), id);
        graph.add_node(nodes[&sedon[stream]]);
        id += 1;
    }

    for (from, tos) in &config.links {
        for to in tos {
            graph.add_edge(
                nodes[&sedon[&from.original]],
                nodes[&sedon[&to.original]],
                0,
            ); // ? weights
        }
    }

    let cyclic = is_cyclic_directed(&graph);
    println!(
        "{:?}, CYCLIC: {}",
        Dot::with_config(&graph, &[Config::EdgeNoLabel]),
        cyclic
    );

    (graph, cyclic)
}

fn incarnate_pipes(config: config::PipelineVec) -> PipelineVec {
    config
        .iter()
        .map(|p| Pipeline {
            config: p.clone(),
            graph: pipeline_to_graph(p.clone()),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use serde_yaml;
    use std::fs::File;
    use std::io::BufReader;

    fn slurp(file: &str) -> config::Config {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).unwrap()
    }

    #[test]
    fn load_simple_deploys() {
        let config = slurp("tests/deploy.simple.yaml");
        println!("{:?}", config);
        let runtime = incarnate(config);
        assert_eq!(1, runtime.ramps.len());
        assert_eq!(1, runtime.ramps[0].config.onramps.len());
        assert_eq!(1, runtime.ramps[0].config.offramps.len());
        assert_eq!(0, runtime.bindings.len());
        assert_eq!(0, runtime.pipes.len());
    }

    #[test]
    fn load_simple_links() {
        let config = slurp("tests/link.simple.yaml");
        let runtime = incarnate(config);
        assert_eq!(0, runtime.ramps.len());
        assert_eq!(2, runtime.bindings[0].links.len());
        // assert_eq!(
        //     "/deployments/bench-001/blaster",
        //     runtime.links.iter().next().unwrap().0.original
        // );
        // assert_eq!(
        //     "/pipelines/main/in",
        //     runtime.links.iter().next().unwrap().1.original
        // );
        assert_eq!(0, runtime.pipes.len());
    }

    #[test]
    fn load_simple_pipes() {
        let config = slurp("tests/pipe.simple.yaml");
        println!("{:?}", &config);
        let runtime = incarnate(config);
        assert_eq!(0, runtime.ramps.len());
        assert_eq!(0, runtime.bindings.len());
        assert_eq!(1, runtime.pipes.len());
        let _l = runtime.pipes[0].config.links.iter().next().unwrap();
        // assert_eq!(
        //     (&"in".to_string(), &"out".to_string()),
        //     (&l.0.original, &l.1.original)
        // ); FIXME
    }

    #[test]
    fn load_passthrough_stream() {
        let config = slurp("tests/ut.passthrough.yaml");
        println!("{:?}", &config);
        let runtime = incarnate(config);
        assert_eq!(1, runtime.ramps.len());
        assert_eq!(2, runtime.bindings[0].links.len());
        assert_eq!(1, runtime.pipes.len());
    }

    #[test]
    fn load_passthrough_op() {
        let config = slurp("tests/ut.single-op.yaml");
        println!("{:?}", &config);
        let _runtime = incarnate(config);
    }

    #[test]
    fn load_branch_op() {
        let config = slurp("tests/ut.branch-op.yaml");
        println!("{:?}", &config);
        let _runtime = incarnate(config);
    }

    #[test]
    fn load_combine_op() {
        let config = slurp("tests/ut.combine-op.yaml");
        println!("{:?}", &config);
        let _runtime = incarnate(config);
    }

    #[test]
    fn load_combine2_op() {
        let config = slurp("tests/ut.combine2-op.yaml");
        println!("{:?}", &config);
        let _runtime = incarnate(config);
    }

}
