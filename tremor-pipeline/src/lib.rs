// Copyright 2020, The Tremor Team
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

#![forbid(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::forget_copy)] // TODO needed for simd json derive
#![allow(clippy::missing_errors_doc)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rental;

use crate::errors::{ErrorKind, Result};
use crate::op::prelude::*;
use executable_graph::NodeConfig;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use op::trickle::select::WindowImpl;
use petgraph::graph::{self, NodeIndex};
use simd_json::{json, OwnedValue};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::iter::Iterator;
use std::str::FromStr;
use std::{fmt, sync::Mutex};
use tremor_script::prelude::*;
use tremor_script::query::StmtRentalWrapper;

/// Pipeline Errors
pub mod errors;
mod event;
mod executable_graph;

#[macro_use]
mod macros;
pub(crate) mod op;
/// Tools to turn tremor query into pipelines
pub mod query;
pub use crate::event::{Event, ValueIter, ValueMetaIter};
pub(crate) use crate::executable_graph::State;
pub use crate::executable_graph::{ExecutableGraph, OperatorNode};
pub use op::{ConfigImpl, InitializableOperator, Operator};
pub use tremor_script::prelude::EventOriginUri;
pub(crate) type PortIndexMap =
    HashMap<(NodeIndex, Cow<'static, str>), Vec<(NodeIndex, Cow<'static, str>)>>;
pub(crate) type ExecPortIndexMap =
    HashMap<(usize, Cow<'static, str>), Vec<(usize, Cow<'static, str>)>>;

/// A configuration map
pub type ConfigMap = Option<serde_yaml::Value>;

/// A lookup function to used to look up operators
pub type NodeLookupFn = fn(
    config: &NodeConfig,
    uid: u64,
    defn: Option<StmtRentalWrapper>,
    node: Option<StmtRentalWrapper>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode>;

/// Stringified numeric key
/// from <https://github.com/serde-rs/json-benchmark/blob/master/src/prim_str.rs>
#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct PrimStr<T>(T)
where
    T: Copy + Ord + Display + FromStr;

impl<T> simd_json_derive::SerializeAsKey for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "\"{}\"", self.0)
    }
}

impl<T> simd_json_derive::Serialize for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "\"{}\"", self.0)
    }
}

impl<'input, T> simd_json_derive::Deserialize<'input> for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    #[inline]
    fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: std::marker::Sized + 'input,
    {
        if let Some(simd_json::Node::String(s)) = tape.next() {
            Ok(PrimStr(FromStr::from_str(s).map_err(|_e| {
                simd_json::Error::generic(simd_json::ErrorType::Serde("not a number".into()))
            })?))
        } else {
            Err(simd_json::Error::generic(
                simd_json::ErrorType::ExpectedNull,
            ))
        }
    }
}

/// Operator metadata
#[derive(
    Clone, Debug, Default, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct OpMeta(BTreeMap<PrimStr<u64>, OwnedValue>);

impl OpMeta {
    /// inserts a value
    pub fn insert<V>(&mut self, key: u64, value: V) -> Option<OwnedValue>
    where
        OwnedValue: From<V>,
    {
        self.0.insert(PrimStr(key), OwnedValue::from(value))
    }
    /// reads a value
    pub fn get(&mut self, key: u64) -> Option<&OwnedValue> {
        self.0.get(&PrimStr(key))
    }
    /// checks existance of a key
    #[must_use]
    pub fn contains_key(&self, key: u64) -> bool {
        self.0.contains_key(&PrimStr(key))
    }

    /// Merges two op meta maps, overwriting values with `other` on duplicates
    pub fn merge(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
    }
}

lazy_static! {
    /// Function registory for the pipeline to look up functions
    // We wrap the registry in a mutex so that we can add functions from the outside
    // if required.
    pub static ref FN_REGISTRY: Mutex<Registry> = {
        let registry: Registry = tremor_script::registry();
        Mutex::new(registry)
    };
}

pub(crate) fn common_cow(s: &str) -> Cow<'static, str> {
    macro_rules! cows {
        ($target:expr, $($cow:expr),*) => {
            match $target {
                $($cow => $cow.into()),*,
                _ => Cow::Owned($target.to_string()),
            }
        };
    }
    cows!(s, "in", "out", "err", "main")
}

/// Type of nodes
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum NodeKind {
    /// An input, this is the one end of the graph
    Input,
    /// An output, this is the other end of the graph
    Output,
    /// An operator
    Operator,
}

/// A circuit breaker action
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub enum CBAction {
    /// Nothing of note
    None,
    /// The circuit breaker is triggerd and should break
    Close,
    /// The circuit breaker is restored and should work again
    Open,
    /// Acknowledge delivery of messages up to a given ID.
    /// All messages prior to and including  this will be considered delivered.
    Ack,
    /// Fail backwards to a given ID
    /// All messages after and including this will be considered non delivered
    Fail,
}
impl Default for CBAction {
    fn default() -> Self {
        Self::None
    }
}

impl From<bool> for CBAction {
    fn from(success: bool) -> Self {
        if success {
            CBAction::Ack
        } else {
            CBAction::Fail
        }
    }
}

impl CBAction {
    /// This is a Circuit Breaker related message
    #[must_use]
    pub fn is_cb(self) -> bool {
        self == CBAction::Close || self == CBAction::Open
    }
    /// This is a Guaranteed Delivery related message
    #[must_use]
    pub fn is_gd(self) -> bool {
        self == CBAction::Ack || self == CBAction::Fail
    }
}
/// IDs for covering multiple event sources. We use a vector to represent
/// this as this:
/// * Simplifies cloning (represented  inconsecutive memory) :sob:
/// * Allows easier serialisation/deserialisation
/// * We don't ever expect a huge number of sources at the same time
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct Ids(Vec<(u64, u64)>);

impl fmt::Display for Ids {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut i = self.0.iter();
        if let Some((oid, eid)) = i.next() {
            write!(f, "{}: {}", oid, eid)?
        }
        for (oid, eid) in i {
            write!(f, ", {}: {}", oid, eid)?
        }
        Ok(())
    }
}

impl Ids {
    /// Fetches the registered eid for a given source
    #[must_use]
    pub fn get(&self, uid: u64) -> Option<u64> {
        self.0
            .iter()
            .find_map(|(u, v)| if *u == uid { Some(*v) } else { None })
    }
    /// Adds a Id to the id map
    pub fn add(&mut self, uri: Option<tremor_script::EventOriginUri>, e_id: u64) {
        if let Some(uri) = uri {
            self.add_id(uri.uid, e_id)
        }
    }

    fn add_id(&mut self, u_id: u64, e_id: u64) {
        for (cur_uid, cur_eid) in &mut self.0 {
            if u_id == *cur_uid {
                if *cur_eid < e_id {
                    *cur_eid = e_id;
                }
                return;
            }
        }
        self.0.push((u_id, e_id))
    }
    /// Creates a new Id map
    #[must_use]
    pub fn new(u_id: u64, e_id: u64) -> Self {
        let mut m = Vec::with_capacity(4);
        m.push((u_id, e_id));
        Self(m)
    }
    /// Merges two id sets, ensures we only ever track the largest id for each
    /// source
    pub fn merge(&mut self, other: &Self) {
        for (uid, eid) in &other.0 {
            self.add_id(*uid, *eid)
        }
    }
}

#[cfg(test)]
impl From<u64> for Ids {
    fn from(eid: u64) -> Self {
        Self::new(0, eid)
    }
}

/// The kind of signal this is
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub enum SignalKind {
    // Lifecycle
    /// Init singnal
    Init,
    /// Shutdown Signal
    Shutdown,
    // Pause, TODO debug trace
    // Resume, TODO debug trace
    // Step, TODO ( into, over, to next breakpoint )
    /// Control
    Control,
    /// Periodic Tick
    Tick,
}

fn factory(node: &NodeConfig) -> Result<Box<dyn InitializableOperator>> {
    #[cfg(feature = "bert")]
    use op::bert::{SequenceClassificationFactory, SummerizationFactory};
    use op::debug::EventHistoryFactory;
    use op::generic::{BatchFactory, CounterFactory};
    use op::grouper::BucketGrouperFactory;
    use op::identity::PassthroughFactory;
    use op::qos::{BackpressureFactory, PercentileFactory, RoundRobinFactory, WalFactory};
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
        ["qos", "wal"] => WalFactory::new_boxed(),
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

fn operator(node: &NodeConfig) -> Result<Box<dyn Operator + 'static>> {
    factory(&node)?.from_node(node)
}

pub(crate) type ConfigGraph = graph::DiGraph<NodeConfig, u8>;

#[derive(Debug, Default, Clone)]
pub(crate) struct NodeMetrics {
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
            tags.insert("port".into(), Value::from(k.clone()));
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
            tags.insert("port".into(), Value::from(k.clone()));
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

#[cfg(test)]
mod test {
    use super::*;
    use simd_json_derive::{Deserialize, Serialize};
    #[test]
    fn prim_str() {
        let p = PrimStr(42);
        let fourtytwo = r#""42""#;
        let mut fourtytwo_s = fourtytwo.to_string();
        let mut fourtytwo_i = "42".to_string();
        assert_eq!(fourtytwo, p.json_string().unwrap());
        assert_eq!(
            p,
            PrimStr::from_slice(unsafe { fourtytwo_s.as_bytes_mut() }).unwrap()
        );
        assert!(PrimStr::<i32>::from_slice(unsafe { fourtytwo_i.as_bytes_mut() }).is_err());
    }

    #[test]
    fn op_meta_merge() {
        let mut m1 = OpMeta::default();
        let mut m2 = OpMeta::default();
        m1.insert(1, 1);
        m1.insert(2, 1);
        m2.insert(1, 2);
        m2.insert(3, 2);
        m1.merge(m2);

        assert!(m1.contains_key(1));
        assert!(m1.contains_key(2));
        assert!(m1.contains_key(3));

        assert_eq!(m1.get(1).unwrap(), &2);
        assert_eq!(m1.get(2).unwrap(), &1);
        assert_eq!(m1.get(3).unwrap(), &2);
    }

    #[test]
    fn cbaction_creation() {
        assert_eq!(CBAction::default(), CBAction::None);
        assert_eq!(CBAction::from(true), CBAction::Ack);
        assert_eq!(CBAction::from(false), CBAction::Fail);
    }

    #[test]
    fn cbaction_is_gd() {
        assert_eq!(CBAction::None.is_gd(), false);

        assert_eq!(CBAction::Fail.is_gd(), true);
        assert_eq!(CBAction::Ack.is_gd(), true);

        assert_eq!(CBAction::Open.is_gd(), false);
        assert_eq!(CBAction::Close.is_gd(), false);
    }

    #[test]
    fn cbaction_is_cb() {
        assert_eq!(CBAction::None.is_cb(), false);

        assert_eq!(CBAction::Fail.is_cb(), false);
        assert_eq!(CBAction::Ack.is_cb(), false);

        assert_eq!(CBAction::Open.is_cb(), true);
        assert_eq!(CBAction::Close.is_cb(), true);
    }

    #[test]
    fn ids() {
        let mut ids1 = Ids::new(1, 1);
        let mut ids2 = Ids::new(1, 2);
        ids1.add(
            Some(EventOriginUri {
                uid: 2,
                ..EventOriginUri::default()
            }),
            1,
        );

        ids2.add(
            Some(EventOriginUri {
                uid: 2,
                ..EventOriginUri::default()
            }),
            3,
        );

        assert_eq!(ids1.get(1), Some(1));
        assert_eq!(ids1.get(2), Some(1));

        assert_eq!(ids2.get(1), Some(2));
        assert_eq!(ids2.get(2), Some(3));

        ids1.merge(&ids2);

        assert_eq!(ids1.get(1), Some(2));
        assert_eq!(ids1.get(2), Some(3));

        let ids = Ids::from(42u64);
        assert_eq!(ids.get(0), Some(42));
    }
}
