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
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
use crate::errors::{ErrorKind, Result};
use async_broadcast::{broadcast, Receiver, Sender};
use async_std::task::block_on;
use beef::Cow;
use executable_graph::NodeConfig;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use log4rs::append::Append;
use petgraph::graph;
use simd_json::OwnedValue;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::iter::Iterator;
use std::str::FromStr;
//use std::{fmt::Debug, mem, pin::Pin, sync::Arc};
use tremor_common::ids::{Id, OperatorId, SinkId, SourceId};
use tremor_script::{
    ast::{self, Helper},
    prelude::*,
};

/// Pipeline Errors
pub mod errors;
mod event;
mod executable_graph;

/// Library functions for pluggable-logging
pub mod logging;

/// Common metrics related code - metrics message formats etc
/// Placed here because we need it here and in tremor-runtime, but also depend on tremor-value inside of it
pub mod metrics;

#[macro_use]
mod macros;
pub(crate) mod op;

/// Tools to turn tremor query into pipelines
pub mod query;
pub use crate::event::{Event, ValueIter, ValueMetaIter};
pub use crate::executable_graph::{ExecutableGraph, OperatorNode};
pub(crate) use crate::executable_graph::{NodeMetrics, State};
pub use op::{ConfigImpl, InitializableOperator, Operator};
pub use tremor_script::prelude::EventOriginUri;
pub(crate) type ExecPortIndexMap =
    HashMap<(usize, Cow<'static, str>), Vec<(usize, Cow<'static, str>)>>;
#[derive(Debug)]
///Pluggable-logging Appender
pub struct PluggableLoggingAppender {
    /// async Sender
    pub tx: Sender<LoggingMsg>,
}

impl Append for PluggableLoggingAppender {
    fn append(&self, record: &log::Record) -> anyhow::Result<()> {
        let vec = (r#"{"level": ""#.to_owned()
            + &record.level().to_string()
            + r#"", "args": ""#
            + &record.args().to_string()
            + r#"", "path": ""#
            + record.module_path().expect("")
            + r#"", "file": ""#
            + record.file().expect("")
            + r#"", "line": ""#
            + &record.line().expect("").to_string()
            + r#""}"#)
            .as_bytes()
            .to_vec();

        let e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).expect("").into());
        let msg = LoggingMsg {
            language: LanguageKind::Rust,
            payload: e,
            origin_uri: None,
        };

        block_on(self.tx.broadcast(msg))?;
        Ok(())
    }

    fn flush(&self) {
        todo!()
    }
}
trait Encode {}

/// A configuration map
pub type ConfigMap = Option<tremor_value::Value<'static>>;

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
    rx: Receiver<MetricsMsg>,
}
/// Channel for plugging-logging messages
pub struct LoggingChannel {
    /// tx Sender
    pub tx: Sender<LoggingMsg>,
    /// rx Serveur
    pub rx: Receiver<LoggingMsg>,
}

impl MetricsChannel {
    pub(crate) fn new(qsize: usize) -> Self {
        let (mut tx, rx) = broadcast(qsize);
        // We use overflow so that non collected messages can be removed
        // For Metrics it should be good enough we consume them quickly
        // and if not we got bigger problems
        tx.set_overflow(true);
        Self { tx, rx }
    }

    /// Get the sender
    #[must_use]
    pub fn tx(&self) -> Sender<MetricsMsg> {
        self.tx.clone()
    }
    /// Get the receiver
    #[must_use]
    pub fn rx(&self) -> Receiver<MetricsMsg> {
        self.rx.clone()
    }
}

impl LoggingChannel {
    pub(crate) fn new(qsize: usize) -> Self {
        let (mut tx, rx) = broadcast(qsize);
        // We use overflow so that non collected messages can be removed
        // For Logging it should be good enough we consume them quickly
        // and if not we got bigger problems
        tx.set_overflow(true);
        Self { tx, rx }
    }

    /// Get the sender
    #[must_use]
    pub fn tx(&self) -> Sender<LoggingMsg> {
        self.tx.clone()
    }
    /// Get the receiver
    #[must_use]
    pub fn rx(&self) -> Receiver<LoggingMsg> {
        self.rx.clone()
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

#[derive(Debug, Clone)]
/// Language from which the logs are coming (Rust, Tremor, etc.)
pub enum LanguageKind {
    /// The Rust language
    Rust,
    /// Tremor language
    Tremor,
}

#[derive(Debug, Clone)]
/// Playload for pluggable logging
pub struct LoggingMsg {
    /// The payload
    pub payload: EventPayload,
    /// The origin
    pub origin_uri: Option<EventOriginUri>,
    /// The language
    pub language: LanguageKind,
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
impl LoggingMsg {
    /// creates a new message
    #[must_use]
    pub fn new(
        payload: EventPayload,
        origin_uri: Option<EventOriginUri>,
        language: LanguageKind,
    ) -> Self {
        Self {
            payload,
            origin_uri,
            language,
        }
    }
}

/// Sender for metrics
pub type MetricsSender = Sender<MetricsMsg>;

// TODO FIX ME NOTE Add LogsSender
/// Sender for plugging logging messagers
pub type LoggingSender = Sender<LoggingMsg>;

lazy_static! {
    /// TODO do we want to change this number or can we make it configurable?
    pub static ref METRICS_CHANNEL: MetricsChannel = MetricsChannel::new(128);
    /// TODO do we want to change this number or can we make it configurable?
    pub static ref LOGGING_CHANNEL: LoggingChannel = LoggingChannel::new(128);
}

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
// TODO: optimization: - use two Vecs, one for operator ids, one for operator metadata (Values)
//                     - make it possible to trace operators with and without metadata
//                     - insert with bisect (numbers of operators tracked will be low single digit numbers most of the time)
pub struct OpMeta(BTreeMap<PrimStr<OperatorId>, OwnedValue>);

impl OpMeta {
    /// inserts a value
    pub fn insert<V>(&mut self, key: OperatorId, value: V) -> Option<OwnedValue>
    where
        OwnedValue: From<V>,
    {
        self.0.insert(PrimStr(key), OwnedValue::from(value))
    }
    /// reads a value
    pub fn get(&mut self, key: OperatorId) -> Option<&OwnedValue> {
        self.0.get(&PrimStr(key))
    }
    /// checks existance of a key
    #[must_use]
    pub fn contains_key(&self, key: OperatorId) -> bool {
        self.0.contains_key(&PrimStr(key))
    }

    /// Merges two op meta maps, overwriting values with `other` on duplicates
    pub fn merge(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
    }
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
    Output(Cow<'static, str>),
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

/// A circuit breaker action
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub enum CbAction {
    /// Nothing of note
    None,
    /// The circuit breaker is triggerd and should break
    Trigger,
    /// The circuit breaker is restored and should work again
    Restore,
    // TODO: add stream based CbAction variants once their use manifests
    /// Acknowledge delivery of messages up to a given ID.
    /// All messages prior to and including  this will be considered delivered.
    Ack,
    /// Fail backwards to a given ID
    /// All messages after and including this will be considered non delivered
    Fail,
    /// Notify all upstream sources that this sink has started, notifying them of its existence.
    /// Will be used for tracking for which sinks to wait during Drain.
    SinkStart(SinkId),
    /// answer to a `SignalKind::Drain(uid)` signal from a connector with the same uid
    Drained(SourceId, SinkId),
}
impl Default for CbAction {
    fn default() -> Self {
        Self::None
    }
}

impl From<bool> for CbAction {
    fn from(success: bool) -> Self {
        if success {
            CbAction::Ack
        } else {
            CbAction::Fail
        }
    }
}

impl CbAction {
    /// This message should always be delivered and not filtered out
    #[must_use]
    pub fn always_deliver(self) -> bool {
        self.is_cb() || matches!(self, CbAction::Drained(_, _) | CbAction::SinkStart(_))
    }
    /// This is a Circuit Breaker related message
    #[must_use]
    pub fn is_cb(self) -> bool {
        matches!(self, CbAction::Trigger | CbAction::Restore)
    }
    /// This is a Guaranteed Delivery related message
    #[must_use]
    pub fn is_gd(self) -> bool {
        matches!(self, CbAction::Ack | CbAction::Fail)
    }
}

/// Event identifier
///
/// Events are identified first by their source and within the source by the stream that originated the given event.
/// Then we have a `pull_counter` identifying the `pull` with which the given event was introduced into the system
/// and an `event_id` that is unique only within the same stream and might deviate from the `pull_counter`.
///
/// `EventId` also tracks min and max event ids for other events in order to support batched and grouped events
/// and facilitate CB mechanics
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct EventId {
    /// can be a `SourceId` or an `OperatorId`
    source_id: u64,
    stream_id: u64,
    event_id: u64,
    pull_id: u64,
    tracked_pull_ids: Vec<TrackedPullIds>,
}

/// default stream id if streams dont make sense
pub const DEFAULT_STREAM_ID: u64 = 0;
/// default pull id if pulls arent tracked
pub const DEFAULT_PULL_ID: u64 = 0;

impl EventId {
    #[must_use]
    /// create a new `EventId` from numeric ids
    pub fn new(source_id: u64, stream_id: u64, event_id: u64, pull_id: u64) -> Self {
        Self {
            source_id,
            stream_id,
            event_id,
            pull_id,
            tracked_pull_ids: Vec::with_capacity(0),
        }
    }

    /// create a new `EventId` with `pull_id` being equal to `event_id`
    #[must_use]
    pub fn from_id(source_id: u64, stream_id: u64, event_id: u64) -> Self {
        Self::new(source_id, stream_id, event_id, event_id)
    }

    #[must_use]
    /// return the `source_id` of this event
    /// the unique id of the source/onramp/pipeline-node where this event came from
    pub fn source_id(&self) -> u64 {
        self.source_id
    }

    /// setter for `source_id`
    pub fn set_source_id(&mut self, source_id: u64) {
        self.source_id = source_id;
    }

    #[must_use]
    /// return the `stream_id` of this event
    /// the unique id of the stream within a source/onramp/pipeline-node where this event came from
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// setter for `stream_id`
    pub fn set_stream_id(&mut self, stream_id: u64) {
        self.stream_id = stream_id;
    }

    #[must_use]
    /// return the `event_id` of this event
    /// the unique id of the event within its stream
    pub fn event_id(&self) -> u64 {
        self.event_id
    }

    /// setter for `event_id`
    pub fn set_event_id(&mut self, event_id: u64) {
        self.event_id = event_id;
    }

    #[must_use]
    /// return the `pull_id` of this event.
    /// the identifier of the pull operation with which this event was `pulled` from its origin source.
    pub fn pull_id(&self) -> u64 {
        self.pull_id
    }

    /// setter for `pull_id`
    pub fn set_pull_id(&mut self, pull_id: u64) {
        self.pull_id = pull_id;
    }

    /// track the min and max of the given `event_id`
    /// and also include all event ids `event_id` was tracking
    pub fn track(&mut self, other: &EventId) {
        self.track_ids(
            other.source_id,
            other.stream_id,
            other.pull_id,
            other.pull_id,
        );

        for other_tracked in &other.tracked_pull_ids {
            match self
                .tracked_pull_ids
                .binary_search_by(|probe| probe.compare(other_tracked))
            {
                Ok(idx) => {
                    // ALLOW: binary_search_by verified this idx exists
                    unsafe { self.tracked_pull_ids.get_unchecked_mut(idx) }
                        .track_ids(other_tracked.min_pull_id, other_tracked.max_pull_id);
                }
                Err(idx) => self.tracked_pull_ids.insert(idx, other_tracked.clone()),
            }
        }
    }

    /// track the given event id by its raw numeric ids
    pub fn track_id(&mut self, source_id: u64, stream_id: u64, pull_id: u64) {
        self.track_ids(source_id, stream_id, pull_id, pull_id);
    }

    fn track_ids(&mut self, source_id: u64, stream_id: u64, min_pull_id: u64, max_pull_id: u64) {
        // track our own id upon first track call, so we can keep resolving min and max simpler
        if self.tracked_pull_ids.is_empty() {
            self.tracked_pull_ids.push(TrackedPullIds::new(
                self.source_id,
                self.stream_id,
                self.pull_id,
                self.pull_id,
            ));
        }
        match self
            .tracked_pull_ids
            .binary_search_by(|probe| probe.compare_ids(source_id, stream_id))
        {
            Ok(idx) => {
                unsafe { self.tracked_pull_ids.get_unchecked_mut(idx) }
                    .track_ids(min_pull_id, max_pull_id);
            }
            Err(idx) => self.tracked_pull_ids.insert(
                idx,
                TrackedPullIds::new(source_id, stream_id, min_pull_id, max_pull_id),
            ),
        }
    }

    #[must_use]
    /// get minimum pull id for a given source and stream, if it is tracked
    ///
    /// This also always checks the actual eventId, not only the tracked ones, this way we can save allocations when used within insights
    pub fn get_min_by_stream(&self, source_id: u64, stream_id: u64) -> Option<u64> {
        if self.tracked_pull_ids.is_empty()
            && self.source_id == source_id
            && self.stream_id == stream_id
        {
            Some(self.pull_id)
        } else {
            self.tracked_pull_ids.iter().find_map(|teid| {
                if (source_id, stream_id) == (teid.source_id, teid.stream_id) {
                    Some(teid.min_pull_id)
                } else {
                    None
                }
            })
        }
    }

    #[must_use]
    /// checks if the given `EventId` is tracked by this one.
    /// Also returns true, if the `event_id` has the same id as `self`.
    pub fn is_tracking(&self, event_id: &EventId) -> bool {
        let is_same = self.source_id() == event_id.source_id()
            && self.stream_id() == event_id.stream_id()
            && self.pull_id == event_id.pull_id
            && self.event_id == event_id.event_id;
        is_same
            || match self.tracked_pull_ids.binary_search_by(|probe| {
                probe.compare_ids(event_id.source_id(), event_id.stream_id())
            }) {
                Ok(idx) => {
                    let entry = unsafe { self.tracked_pull_ids.get_unchecked(idx) };
                    // this is only a heuristic, but is good enough for now
                    (entry.min_pull_id <= event_id.pull_id)
                        && (event_id.pull_id <= entry.max_pull_id)
                }
                Err(_) => false,
            }
    }

    #[must_use]
    /// get maximum pull id for a given source and stream if we have it here
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_max_by_stream(&self, source_id: u64, stream_id: u64) -> Option<u64> {
        if self.tracked_pull_ids.is_empty()
            && self.source_id == source_id
            && self.stream_id == stream_id
        {
            Some(self.pull_id)
        } else {
            self.tracked_pull_ids.iter().find_map(|teid| {
                if (source_id, stream_id) == (teid.source_id, teid.stream_id) {
                    Some(teid.max_pull_id)
                } else {
                    None
                }
            })
        }
    }

    #[must_use]
    /// get the minimum tracked (`stream_id`, `pull_id`)
    /// by chosing events with smaller stream id
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_min_by_source(&self, source_id: u64) -> Option<(u64, u64)> {
        // TODO: change the return type to an iterator, so we make sure to return all values for all streams
        if self.tracked_pull_ids.is_empty() && self.source_id == source_id {
            Some((self.stream_id, self.pull_id))
        } else {
            self.tracked_pull_ids
                .iter()
                .filter(|teid| teid.source_id == source_id)
                .min_by(|teid1, teid2| teid1.stream_id.cmp(&teid2.stream_id))
                .map(|teid| (teid.stream_id, teid.min_pull_id))
        }
    }

    #[must_use]
    /// get the maximum tracked (`stream_id`, `pull_id`)
    /// by chosing events with bigger stream id
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_max_by_source(&self, source_id: u64) -> Option<(u64, u64)> {
        // TODO: change the return type to an iterator, so we make sure to return all values for all streams
        if self.tracked_pull_ids.is_empty() && self.source_id == source_id {
            Some((self.stream_id, self.pull_id))
        } else {
            self.tracked_pull_ids
                .iter()
                .filter(|teid| teid.source_id == source_id)
                .max_by(|teid1, teid2| teid1.stream_id.cmp(&teid2.stream_id))
                .map(|teid| (teid.stream_id, teid.max_pull_id))
        }
    }

    /// get all streams for a source id
    #[must_use]
    pub fn get_streams(&self, source_id: u64) -> HashSet<u64> {
        let mut v = HashSet::new();
        if self.source_id == source_id {
            v.insert(self.stream_id);
        }
        let iter = self
            .tracked_pull_ids
            .iter()
            .filter(|tids| tids.source_id == source_id)
            .map(|tids| tids.stream_id);
        v.extend(iter);
        v
    }

    /// get a stream id for the given `source_id`
    /// will favor the events own stream id, will also look into tracked event ids and return the first it finds
    #[must_use]
    pub fn get_stream(&self, source_id: u64) -> Option<u64> {
        if self.source_id == source_id {
            Some(self.stream_id)
        } else {
            self.tracked_pull_ids
                .iter()
                .find(|tids| tids.source_id == source_id)
                .map(|tids| tids.stream_id)
        }
    }
}

impl From<(u64, u64, u64)> for EventId {
    fn from(x: (u64, u64, u64)) -> Self {
        EventId::new(x.0, x.1, x.2, x.2)
    }
}

impl From<(u64, u64, u64, u64)> for EventId {
    fn from(x: (u64, u64, u64, u64)) -> Self {
        EventId::new(x.0, x.1, x.2, x.3)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.source_id, self.stream_id, self.event_id, self.pull_id
        )?;
        if !self.tracked_pull_ids.is_empty() {
            let mut iter = self.tracked_pull_ids.iter();
            if let Some(ids) = iter.next() {
                write!(f, " {}", ids)?;
            }
            for ids in iter {
                write!(f, ", {}", ids)?;
            }
        }
        Ok(())
    }
}

#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
/// tracked min and max pull id for a given source and stream
///
/// We are only interested in pull ids as they are the units that need to be tracked
/// for correct acknowledments at the sources.
/// And this is the sole reason we are tracking anything in the first place.
pub struct TrackedPullIds {
    /// uid of the source (can be a connector-source or a pipeline operator) this event originated from
    pub source_id: u64,
    /// uid of the stream within the source this event originated from
    pub stream_id: u64,
    /// tracking min pull id
    pub min_pull_id: u64,
    /// tracking max pull id
    pub max_pull_id: u64,
}

impl TrackedPullIds {
    #[must_use]
    /// create a new instance with min and max pull id
    pub fn new(source_id: u64, stream_id: u64, min_pull_id: u64, max_pull_id: u64) -> Self {
        Self {
            source_id,
            stream_id,
            min_pull_id,
            max_pull_id,
        }
    }

    #[must_use]
    /// create tracked ids from a single `pull_id`
    pub fn from_id(source_id: u64, stream_id: u64, pull_id: u64) -> Self {
        Self {
            source_id,
            stream_id,
            min_pull_id: pull_id,
            max_pull_id: pull_id,
        }
    }

    #[must_use]
    /// returns true if this struct tracks the given source and stream ids
    pub fn tracks_id(&self, source_id: u64, stream_id: u64) -> bool {
        self.source_id == source_id && self.stream_id == stream_id
    }

    #[must_use]
    /// compares against the given source and stream ids, using simple numeric ordering
    pub fn compare_ids(&self, source_id: u64, stream_id: u64) -> Ordering {
        (self.source_id, self.stream_id).cmp(&(source_id, stream_id))
    }

    #[must_use]
    /// compare source and stream ids against the ones given in `other`
    pub fn compare(&self, other: &TrackedPullIds) -> Ordering {
        (self.source_id, self.stream_id).cmp(&(other.source_id, other.stream_id))
    }

    /// track everything from the given `event_id`
    pub fn track(&mut self, event_id: &EventId) {
        #[cfg(test)]
        {
            debug_assert!(
                self.source_id == event_id.source_id,
                "incompatible source ids"
            );
            debug_assert!(
                self.stream_id == event_id.stream_id,
                "incompatible stream ids"
            );
        }
        self.track_ids(event_id.pull_id, event_id.pull_id);
    }

    /// track a single event id
    pub fn track_id(&mut self, pull_id: u64) {
        self.track_ids(pull_id, pull_id);
    }

    /// track a min and max event id
    pub fn track_ids(&mut self, min_pull_id: u64, max_pull_id: u64) {
        self.min_pull_id = self.min_pull_id.min(min_pull_id);
        self.max_pull_id = self.max_pull_id.max(max_pull_id);
    }

    /// merge the other `ids` into this one
    pub fn merge(&mut self, ids: &TrackedPullIds) {
        // TODO: once https://github.com/rust-lang/rust-clippy/issues/6970 is fixed comment those in again
        #[cfg(test)]
        {
            debug_assert!(self.source_id == ids.source_id, "incompatible source ids");
            debug_assert!(self.stream_id == ids.stream_id, "incompatible stream ids");
        }
        self.track_ids(ids.min_pull_id, ids.max_pull_id);
    }
}

impl From<&EventId> for TrackedPullIds {
    fn from(e: &EventId) -> Self {
        Self::from_id(e.source_id, e.stream_id, e.pull_id)
    }
}

impl From<(u64, u64, u64)> for TrackedPullIds {
    fn from(x: (u64, u64, u64)) -> Self {
        Self::from_id(x.0, x.1, x.2)
    }
}

impl From<(u64, u64, u64, u64)> for TrackedPullIds {
    fn from(x: (u64, u64, u64, u64)) -> Self {
        Self::new(x.0, x.1, x.2, x.3)
    }
}

impl fmt::Display for TrackedPullIds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[min={}:{}:{}, max={}:{}:{}]",
            self.source_id,
            self.stream_id,
            self.min_pull_id,
            self.source_id,
            self.stream_id,
            self.max_pull_id
        )
    }
}

#[derive(Debug, Clone, Copy, Default)]
/// For generating consecutive unique event ids for a single stream.
pub struct EventIdGenerator(u64, u64, u64);
impl EventIdGenerator {
    /// generate the next event id for this stream
    /// with equivalent pull id
    pub fn next_id(&mut self) -> EventId {
        let event_id = self.2;
        self.2 = self.2.wrapping_add(1);
        EventId::new(self.0, self.1, event_id, event_id)
    }

    /// generate the next event id for this stream with the given `pull_id`
    pub fn next_with_pull_id(&mut self, pull_id: u64) -> EventId {
        let event_id = self.2;
        self.2 = self.2.wrapping_add(1);
        EventId::new(self.0, self.1, event_id, pull_id)
    }

    #[must_use]
    /// create a new generator for the `Source` identified by `source_id` using the default stream id
    pub fn new(source_id: SourceId) -> Self {
        Self(source_id.id(), DEFAULT_STREAM_ID, 0)
    }

    #[must_use]
    /// create a new generator for the `Operator` identified by `operator_id` using the default stream id
    pub fn for_operator(operator_id: OperatorId) -> Self {
        Self(operator_id.id(), DEFAULT_STREAM_ID, 0)
    }

    #[must_use]
    /// create a new generator using the given source and stream id
    pub fn new_with_stream(source_id: SourceId, stream_id: u64) -> Self {
        Self(source_id.id(), stream_id, 0)
    }

    /// set the source id
    pub fn set_source(&mut self, source_id: SourceId) {
        self.0 = source_id.id();
    }

    /// set the stream id
    pub fn set_stream(&mut self, stream_id: u64) {
        self.1 = stream_id;
    }

    /// reset the pull id to zero
    pub fn reset(&mut self) {
        self.2 = 0;
    }
}

/// The kind of signal this is
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub enum SignalKind {
    // Lifecycle
    /// Start signal, containing the source uid which just started
    Start(SourceId),
    /// Shutdown Signal
    Shutdown,
    // Pause, TODO debug trace
    // Resume, TODO debug trace
    // Step, TODO ( into, over, to next breakpoint )
    /// Control
    Control,
    /// Periodic Tick
    Tick,
    /// Drain Signal - this connection is being drained, there should be no events after this
    /// This signal must be answered with a Drain contraflow event containing the same uid (u64)
    /// this way a contraflow event will not be interpreted by connectors for which it isn't meant
    /// reception of such Drain contraflow event notifies the signal sender that the intermittent pipeline is drained and can be safely disconnected
    Drain(SourceId),
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

#[derive(Debug, Default, Clone)]
struct Connection {
    from: Cow<'static, str>,
    to: Cow<'static, str>,
}
impl Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let from: &str = &self.from;
        let to: &str = &self.to;
        match (from, to) {
            ("out", "in") => write!(f, ""),
            ("out", to) => write!(f, "{}", to),
            (from, "in") => write!(f, "{} ", from),
            (from, to) => write!(f, "{} -> {}", from, to),
        }
    }
}

pub(crate) type ConfigGraph = graph::DiGraph<NodeConfig, Connection>;

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
        let op_id1 = OperatorId::new(1);
        let op_id2 = OperatorId::new(2);
        let op_id3 = OperatorId::new(3);
        let mut m1 = OpMeta::default();
        let mut m2 = OpMeta::default();
        m1.insert(op_id1, 1);
        m1.insert(op_id2, 1);
        m2.insert(op_id1, 2);
        m2.insert(op_id3, 2);
        m1.merge(m2);

        assert!(m1.contains_key(op_id1));
        assert!(m1.contains_key(op_id2));
        assert!(m1.contains_key(op_id3));

        assert_eq!(m1.get(op_id1).unwrap(), &2);
        assert_eq!(m1.get(op_id2).unwrap(), &1);
        assert_eq!(m1.get(op_id3).unwrap(), &2);
    }

    #[test]
    fn cbaction_creation() {
        assert_eq!(CbAction::default(), CbAction::None);
        assert_eq!(CbAction::from(true), CbAction::Ack);
        assert_eq!(CbAction::from(false), CbAction::Fail);
    }

    #[test]
    fn cbaction_is_gd() {
        assert_eq!(CbAction::None.is_gd(), false);

        assert_eq!(CbAction::Fail.is_gd(), true);
        assert_eq!(CbAction::Ack.is_gd(), true);

        assert_eq!(CbAction::Restore.is_gd(), false);
        assert_eq!(CbAction::Trigger.is_gd(), false);
    }

    #[test]
    fn cbaction_is_cb() {
        assert_eq!(CbAction::None.is_cb(), false);

        assert_eq!(CbAction::Fail.is_cb(), false);
        assert_eq!(CbAction::Ack.is_cb(), false);

        assert_eq!(CbAction::Restore.is_cb(), true);
        assert_eq!(CbAction::Trigger.is_cb(), true);
    }

    #[test]
    fn event_ids() {
        let mut ids1 = EventId::from_id(1, 1, 1);
        assert_eq!(Some(1), ids1.get_max_by_stream(1, 1));
        assert_eq!(None, ids1.get_max_by_stream(1, 2));
        assert_eq!(None, ids1.get_max_by_stream(2, 1));

        let mut ids2 = EventId::from_id(1, 1, 2);
        assert_eq!(ids2.get_max_by_stream(1, 1), Some(2));
        assert_eq!(ids2.get_max_by_stream(1, 3), None);
        assert_eq!(ids2.get_max_by_stream(2, 1), None);

        ids1.track_id(2, DEFAULT_STREAM_ID, 1);
        ids2.track_id(2, DEFAULT_STREAM_ID, 3);

        assert_eq!(ids1.get_max_by_source(1), Some((1, 1)));
        assert_eq!(ids1.get_max_by_source(2), Some((DEFAULT_STREAM_ID, 1)));

        assert_eq!(ids2.get_min_by_source(1), Some((1, 2)));
        assert_eq!(ids2.get_min_by_source(2), Some((DEFAULT_STREAM_ID, 3)));

        ids1.track(&ids2);

        assert_eq!(ids1.get_max_by_source(1), Some((1, 2)));
        assert_eq!(ids1.get_min_by_source(1), Some((1, 1)));
        assert_eq!(ids1.get_min_by_source(3), None);
        assert_eq!(ids1.get_max_by_source(3), None);
        assert_eq!(ids1.get_max_by_source(2), Some((DEFAULT_STREAM_ID, 3)));
        assert_eq!(ids1.get_min_by_source(2), Some((DEFAULT_STREAM_ID, 1)));

        assert_eq!(ids1.get_max_by_stream(1, 1), Some(2));
        assert_eq!(ids1.get_max_by_stream(1, 2), None);
        assert_eq!(ids1.get_min_by_stream(2, DEFAULT_STREAM_ID), Some(1));
        assert_eq!(ids1.get_min_by_stream(2, 42), None);

        let id = EventId::from((1, DEFAULT_STREAM_ID, 42_u64));
        assert_eq!(id.get_max_by_stream(1, DEFAULT_STREAM_ID), Some(42));
        assert_eq!(id.get_max_by_stream(5, DEFAULT_STREAM_ID), None);
    }

    #[test]
    fn tracked_pull_ids() {
        let teid1 = TrackedPullIds::default();
        assert_eq!(
            (
                teid1.source_id,
                teid1.stream_id,
                teid1.min_pull_id,
                teid1.max_pull_id
            ),
            (0, 0, 0, 0)
        );

        let mut teid2 = TrackedPullIds::new(1, 2, 3, 4);
        let eid1 = EventId::from_id(1, 2, 6);
        let eid2 = EventId::from_id(1, 2, 1);
        teid2.track(&eid1);
        assert_eq!(teid2.max_pull_id, eid1.event_id);
        assert_eq!(teid2.min_pull_id, 3);

        teid2.track(&eid2);
        assert_eq!(teid2.min_pull_id, eid2.event_id);
        assert_eq!(teid2.max_pull_id, eid1.event_id);

        let teid3 = TrackedPullIds::from((1, 2, 19));
        teid2.merge(&teid3);

        assert_eq!(teid2.min_pull_id, 1);
        assert_eq!(teid2.max_pull_id, 19);

        teid2.track_id(0);
        assert_eq!(teid2.min_pull_id, 0);
    }

    #[test]
    fn stream_ids() {
        let source = 1_u64;
        let mut eid = EventId::new(source, 1, 0, 0);
        assert_eq!(
            vec![1_u64],
            eid.get_streams(source).into_iter().collect::<Vec<_>>()
        );
        assert!(eid.get_streams(2).is_empty());

        eid.track_id(source, 2, 1);
        eid.track_id(2, 1, 42);
        let mut streams = eid.get_streams(source).into_iter().collect::<Vec<_>>();
        streams.sort();

        assert_eq!(vec![1_u64, 2], streams);
        assert_eq!(
            vec![1_u64],
            eid.get_streams(2).into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn event_id_pull_id_tracking() {
        let source = 1_u64;
        let stream = 2_u64;
        let mut e1 = EventId::from((source, stream, 0, 0));
        let e2 = EventId::from((source, stream, 1, 0));

        e1.track(&e2);
        assert_eq!(Some(0), e1.get_max_by_stream(source, stream)); // we track pull id, not event id

        let e3 = EventId::from((source, stream, 2, 1));
        let e4 = EventId::from((source, stream + 1, 3, 2));
        e1.track(&e3);
        e1.track(&e4);

        assert_eq!(Some(1), e1.get_max_by_stream(source, stream));
        assert_eq!(Some((stream + 1, 2)), e1.get_max_by_source(source));
    }

    #[test]
    fn get_stream_from_self() {
        let event_id = EventId::from_id(12, 34, 56);

        assert_eq!(event_id.get_stream(12), Some(34));
    }

    #[test]
    fn get_stream_from_tracked() {
        let mut event_id = EventId::from_id(12, 34, 56);
        event_id.track(&EventId::from_id(99, 75, 1024));

        assert_eq!(event_id.get_stream(99), Some(75));
    }
}
