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

//! Tremor event processing pipeline

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde;

use crate::errors::{ErrorKind, Result};
use executable_graph::NodeConfig;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use petgraph::graph;
use std::borrow::Borrow;
use std::fmt;
use std::fmt::Display;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tremor_common::{ids::OperatorId, ports::Port};
use tremor_script::{
    ast::{self, Helper},
    prelude::*,
};

/// Pipeline Errors
pub mod errors;
mod executable_graph;

/// Common metrics related code - metrics message formats etc
/// Placed here because we need it here and in tremor-runtime, but also depend on tremor-value inside of it
pub mod metrics;

#[macro_use]
mod macros;
pub(crate) mod op;

/// Tools to turn tremor query into pipelines
pub mod query;
pub use crate::executable_graph::{ExecutableGraph, OperatorNode};
pub(crate) use crate::executable_graph::{NodeMetrics, State};
pub use op::{InitializableOperator, Operator};
pub use tremor_script::prelude::EventOriginUri;
pub(crate) type ExecPortIndexMap = HashMap<(usize, Port<'static>), Vec<(usize, Port<'static>)>>;

/// A lookup function to used to look up operators
pub type NodeLookupFn = fn(
    config: &NodeConfig,
    uid: OperatorId,
    node: Option<&ast::Stmt<'static>>,
    helper: &mut Helper<'static, '_>,
) -> Result<OperatorNode>;

/// A channel used to send metrics betwen different parts of the system
#[derive(Clone, Debug)]
pub struct MetricsChannel {
    tx: Sender<MetricsMsg>,
}

impl MetricsChannel {
    pub(crate) fn new(qsize: usize) -> Self {
        let (tx, _) = broadcast::channel(qsize);
        Self { tx }
    }

    /// Get the sender
    #[must_use]
    pub fn tx(&self) -> Sender<MetricsMsg> {
        self.tx.clone()
    }
    /// Get the receiver
    #[must_use]
    pub fn rx(&self) -> Receiver<MetricsMsg> {
        self.tx.subscribe()
    }
}
/// Metrics message
#[derive(Debug, Clone)]
pub struct MetricsMsg {
    /// The payload
    pub payload: EventPayload,
    /// The origin
    pub origin_uri: Option<EventOriginUri>,
}

impl MetricsMsg {
    /// creates a new message
    #[must_use]
    pub fn new(payload: EventPayload, origin_uri: Option<EventOriginUri>) -> Self {
        Self {
            payload,
            origin_uri,
        }
    }
}

/// Sender for metrics
pub type MetricsSender = Sender<MetricsMsg>;

lazy_static! {
    /// TODO do we want to change this number or can we make it configurable?
    pub static ref METRICS_CHANNEL: MetricsChannel = MetricsChannel::new(128);
}

pub(crate) fn common_cow(s: &str) -> beef::Cow<'static, str> {
    macro_rules! cows {
        ($target:expr, $($cow:expr),*) => {
            match $target {
                $($cow => $cow.into()),*,
                _ => beef::Cow::from($target.to_string()),
            }
        };
    }
    cows!(s, "in", "out", "err", "main")
}

/// Type of nodes
#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum NodeKind {
    /// An input, this is the one end of the graph
    Input,
    /// An output, this is the other end of the graph
    Output(Port<'static>),
    /// An operator
    Operator,
    /// A select statement
    Select,
    /// A Script statement
    Script,
}

impl NodeKind {
    fn skippable(&self) -> bool {
        matches!(self, Self::Operator | Self::Select | Self::Script)
    }
}

impl Default for NodeKind {
    fn default() -> Self {
        Self::Operator
    }
}

// We ignore this since it's a simple lookup table
// #[cfg_attr(coverage, no_coverage)]
fn factory(node: &NodeConfig) -> Result<Box<dyn InitializableOperator>> {
    #[cfg(feature = "bert")]
    use op::bert::{SequenceClassificationFactory, SummerizationFactory};
    use op::debug::EventHistoryFactory;
    use op::generic::{BatchFactory, CounterFactory};
    use op::grouper::BucketGrouperFactory;
    use op::identity::PassthroughFactory;
    use op::qos::{BackpressureFactory, PercentileFactory, RoundRobinFactory};
    let name_parts: Vec<&str> = node.op_type.split("::").collect();
    let factory = match name_parts.as_slice() {
        ["passthrough"] => PassthroughFactory::new_boxed(),
        ["debug", "history"] => EventHistoryFactory::new_boxed(),
        ["grouper", "bucket"] => BucketGrouperFactory::new_boxed(),
        ["generic", "batch"] => BatchFactory::new_boxed(),
        ["generic", "backpressure"] => {
            error!("The generic::backpressure operator is depricated, please use qos::backpressure instread.");
            BackpressureFactory::new_boxed()
        }
        ["generic", "counter"] => CounterFactory::new_boxed(),
        ["qos", "backpressure"] => BackpressureFactory::new_boxed(),
        ["qos", "roundrobin"] => RoundRobinFactory::new_boxed(),
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

fn operator(uid: OperatorId, node: &NodeConfig) -> Result<Box<dyn Operator + 'static>> {
    factory(node)?.node_to_operator(uid, node)
}

#[derive(Debug, Clone)]
struct Connection {
    from: Port<'static>,
    to: Port<'static>,
}
impl Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let from: &str = self.from.borrow();
        let to: &str = self.to.borrow();
        match (from, to) {
            ("out", "in") => write!(f, ""),
            ("out", to) => write!(f, "{to}"),
            (from, "in") => write!(f, "{from} "),
            (from, to) => write!(f, "{from} -> {to}"),
        }
    }
}

pub(crate) type ConfigGraph = graph::DiGraph<NodeConfig, Connection>;
