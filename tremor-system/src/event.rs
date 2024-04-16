// Copyright 2020-2024, The Tremor Team
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

use either::Either;
use std::{cmp::Ordering, collections::HashSet, mem::swap};
use tremor_common::ids::{Id, OperatorId, SourceId};
use tremor_common::time::nanotime;
use tremor_script::prelude::*;
pub use tremor_script::EventOriginUri as OriginUri;

use crate::{controlplane::CbAction, dataplane::SignalKind, pipeline::OpMeta};

/// A tremor event
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct Event {
    /// The event ID
    pub id: EventId,
    /// The event Data
    pub data: EventPayload,
    /// Nanoseconds at when the event was ingested
    pub ingest_ns: u64,
    /// URI to identify the origin of the event
    pub origin_uri: Option<EventOriginUri>,
    /// The kind of the event
    pub kind: Option<SignalKind>,
    /// If this event is batched (containing multiple events itself)
    pub is_batch: bool,

    /// Circuit breaker action
    pub cb: CbAction,
    /// Metadata for operators
    pub op_meta: OpMeta,
    /// this needs transactional data
    pub transactional: bool,
}

impl Event {
    /// create a tick signal event
    #[must_use]
    pub fn signal_tick() -> Self {
        Self {
            ingest_ns: nanotime(),
            kind: Some(SignalKind::Tick),
            ..Self::default()
        }
    }

    /// create a drain signal event originating at the connector with the given `source_id`
    #[must_use]
    pub fn signal_drain(source_id: SourceId) -> Self {
        Self {
            ingest_ns: nanotime(),
            kind: Some(SignalKind::Drain(source_id)),
            ..Self::default()
        }
    }

    /// create start signal for the given `SourceId`
    #[must_use]
    pub fn signal_start(uid: SourceId) -> Self {
        Self {
            ingest_ns: nanotime(),
            kind: Some(SignalKind::Start(uid)),
            ..Self::default()
        }
    }

    /// turns the event in an insight given it's success
    #[must_use]
    pub fn insight(cb: CbAction, id: EventId, ingest_ns: u64, op_meta: OpMeta) -> Event {
        Event {
            id,
            ingest_ns,
            cb,
            op_meta,
            ..Event::default()
        }
    }

    /// Creates either a restore or trigger event
    #[must_use]
    pub fn restore_or_break(restore: bool, ingest_ns: u64) -> Self {
        if restore {
            Event::cb_restore(ingest_ns)
        } else {
            Event::cb_trigger(ingest_ns)
        }
    }

    /// Creates either a ack or fail event
    #[must_use]
    pub fn ack_or_fail(ack: bool, ingest_ns: u64, ids: EventId, op_meta: OpMeta) -> Self {
        if ack {
            Event::cb_ack(ingest_ns, ids, op_meta)
        } else {
            Event::cb_fail(ingest_ns, ids, op_meta)
        }
    }

    /// Creates a new ack insight from the event
    #[must_use]
    pub fn insight_ack(&self) -> Event {
        Event::cb_ack(self.ingest_ns, self.id.clone(), self.op_meta.clone())
    }

    /// produce a `CBAction::Ack` insight event with the given time (in nanoseconds) in the metadata
    #[must_use]
    pub fn insight_ack_with_timing(&mut self, processing_time: u64) -> Event {
        let mut e = self.insight_ack();
        e.data = (Value::null(), literal!({ "time": processing_time })).into();
        e
    }

    /// Creates a new fail insight from the event, consumes the `op_meta` of the
    /// event
    #[must_use]
    pub fn insight_fail(&self) -> Event {
        Event::cb_fail(self.ingest_ns, self.id.clone(), self.op_meta.clone())
    }

    /// Creates a restore insight from the event, consumes the `op_meta` of the
    /// event
    #[must_use]
    pub fn insight_restore(&mut self) -> Event {
        let mut e = Event::cb_restore(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        e
    }

    /// Creates a trigger insight from the event, consums the `op_meta` of the
    /// event
    #[must_use]
    pub fn insight_trigger(&mut self) -> Event {
        let mut e = Event::cb_trigger(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        e
    }

    /// Creates a new event to restore a CB
    #[must_use]
    pub fn cb_restore(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CbAction::Restore,
            ..Event::default()
        }
    }

    /// Creates a new event to restore/open a CB
    ///
    /// For those CB events we don't need an explicit `EventId`.
    /// Sources should react properly upon any CB message, no matter the `EventId`
    /// operators can use the `op_meta` for checking if they are affected
    #[must_use]
    pub fn cb_open(ingest_ns: u64, op_meta: OpMeta) -> Self {
        Self {
            ingest_ns,
            op_meta,
            cb: CbAction::Restore,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger/close a CB
    ///
    /// For those CB events we don't need an explicit `EventId`.
    /// Sources should react properly upon any CB message, no matter the `EventId`
    /// operators can use the `op_meta` for checking if they are affected
    #[must_use]
    pub fn cb_close(ingest_ns: u64, op_meta: OpMeta) -> Self {
        Self {
            ingest_ns,
            op_meta,
            cb: CbAction::Trigger,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_trigger(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CbAction::Trigger,
            ..Event::default()
        }
    }

    /// Creates a new contraflow event delivery acknowledge message
    #[must_use]
    pub fn cb_ack(ingest_ns: u64, id: EventId, op_meta: OpMeta) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CbAction::Ack,
            op_meta,
            ..Event::default()
        }
    }

    /// Creates a new contraflow event delivery acknowledge message with timing in the metadata
    #[must_use]
    pub fn cb_ack_with_timing(ingest_ns: u64, id: EventId, op_meta: OpMeta, duration: u64) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CbAction::Ack,
            op_meta,
            data: (Value::null(), literal!({ "time": duration })).into(),
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_fail(ingest_ns: u64, id: EventId, op_meta: OpMeta) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CbAction::Fail,
            op_meta,
            ..Event::default()
        }
    }

    #[must_use]
    /// return the number of events contained within this event
    /// normally 1, but for batched events possibly > 1
    pub fn len(&self) -> usize {
        if self.is_batch {
            self.data.suffix().value().as_array().map_or(0, Vec::len)
        } else {
            1
        }
    }

    /// returns true if this event is batched but has no wrapped events
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.is_batch
            && self
                .data
                .suffix()
                .value()
                .as_array()
                .map_or(true, Vec::is_empty)
    }

    /// Extracts the `$correlation` metadata into a `Vec` of `Option<Value<'static>>`.
    /// We use a `Vec` to account for possibly batched events and `Option`s because single events might not have a value there.
    /// We use `Value<'static>`, which requires a clone, as we need to pass the values on to another event anyways.
    #[must_use]
    pub fn correlation_metas(&self) -> Vec<Option<Value<'static>>> {
        let mut res = Vec::with_capacity(self.len());
        for (_, meta) in self.value_meta_iter() {
            res.push(meta.get("correlation").map(Value::clone_static));
        }
        res
    }

    /// get the correlation metadata as a single value, if present
    /// creates an array value for batched events
    #[must_use]
    pub fn correlation_meta(&self) -> Option<Value<'static>> {
        if self.is_batch {
            let cms = self.correlation_metas();
            if cms.is_empty() {
                None
            } else {
                Some(Value::from(cms))
            }
        } else {
            self.data
                .suffix()
                .meta()
                .get("correlation")
                .map(Value::clone_static)
        }
    }
}

impl Event {
    /// allows to iterate over the values and metadatas
    /// in an event, if it is batched this can be multiple
    /// otherwise it's a singular event
    #[must_use]
    pub fn value_meta_iter(&self) -> ValueMetaIter {
        ValueMetaIter {
            event: self,
            idx: 0,
        }
    }
}
/// Iterator over the event value and metadata
/// if the event is a batch this will allow iterating
/// over all the batched events
pub struct ValueMetaIter<'value> {
    event: &'value Event,
    idx: usize,
}

impl<'value> ValueMetaIter<'value> {
    fn extract_batched_value_meta(
        batched_value: &'value Value<'value>,
    ) -> Option<(&'value Value<'value>, &'value Value<'value>)> {
        batched_value
            .get("data")
            .and_then(|last_data| last_data.get("value").zip(last_data.get("meta")))
    }

    /// Split off the last value and meta in this iter and return all previous values and metas inside a slice.
    /// Returns `None` if there are no values in this Event, which shouldn't happen, tbh.
    ///
    /// This is efficient because all the values are in a slice already.
    pub fn split_last(
        &mut self,
    ) -> Option<(
        (&'value Value<'value>, &'value Value<'value>),
        impl Iterator<Item = (&'value Value<'value>, &'value Value<'value>)>,
    )> {
        if self.event.is_batch {
            self.event
                .data
                .suffix()
                .value()
                .as_array()
                .and_then(|vec| vec.split_last())
                .and_then(|(last, rest)| {
                    let last_option = Self::extract_batched_value_meta(last);
                    let rest_option =
                        Some(rest.iter().filter_map(Self::extract_batched_value_meta));
                    last_option.zip(rest_option)
                })
        } else {
            let v = self.event.data.suffix();
            let vs: &[Value<'value>] = &[];
            Some((
                (v.value(), v.meta()),
                // only use the extract method to end up with the same type as the if branch above
                vs.iter().filter_map(Self::extract_batched_value_meta),
            ))
        }
    }
}

// TODO: descend recursively into batched events in batched events ...
impl<'value> Iterator for ValueMetaIter<'value> {
    type Item = (&'value Value<'value>, &'value Value<'value>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .get_idx(self.idx)
                .and_then(Self::extract_batched_value_meta);
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = self.event.data.suffix();
            self.idx += 1;
            Some((v.value(), v.meta()))
        } else {
            None
        }
    }
}

impl Event {
    /// Iterate over the values in an event
    /// this will result in multiple entries
    /// if the event was batched otherwise
    /// have only a single element
    #[must_use]
    pub fn value_iter(&self) -> ValueIter {
        ValueIter {
            event: self,
            idx: 0,
        }
    }
}
/// Iterator over the values of an event
pub struct ValueIter<'value> {
    event: &'value Event,
    idx: usize,
}

impl<'value> ValueIter<'value> {
    fn extract_batched_value(
        batched_value: &'value Value<'value>,
    ) -> Option<&'value Value<'value>> {
        batched_value
            .get("data")
            .and_then(|last_data| last_data.get("value"))
    }
    /// Split off the last value in this iter and return all previous values inside an iterator.
    /// Returns `None` if there are no values in this Event, which shouldn't happen, tbh.
    ///
    /// This is useful if we don't want to clone stuff on the last value.
    pub fn split_last(
        &mut self,
    ) -> Option<(
        &'value Value<'value>,
        impl Iterator<Item = &'value Value<'value>>,
    )> {
        if self.event.is_batch {
            self.event
                .data
                .suffix()
                .value()
                .as_array()
                .and_then(|vec| vec.split_last())
                .and_then(|(last, rest)| {
                    let last_option = Self::extract_batched_value(last);
                    let rest_option = Some(rest.iter().filter_map(Self::extract_batched_value));
                    last_option.zip(rest_option)
                })
        } else {
            let v = self.event.data.suffix().value();
            let vs: &[Value<'value>] = &[];
            // only use the extract method to end up with the same type as the if branch above
            Some((v, vs.iter().filter_map(Self::extract_batched_value)))
        }
    }
}

impl<'value> Iterator for ValueIter<'value> {
    type Item = &'value Value<'value>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .get_idx(self.idx)
                .and_then(Self::extract_batched_value);
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = self.event.data.suffix().value();
            self.idx += 1;
            Some(v)
        } else {
            None
        }
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
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize, Eq,
)]
#[allow(clippy::module_name_repetitions)]
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
        // if self.source_id and self.stream_id are the same, calculate the max from the given pull_id and the one in other
        // and insert it with binary search
        // this makes searching the tracked id easier
        // as we only need to look at the tracked_pull_ids then
        let (min, max) = if self.is_same_stream(other.source_id(), other.stream_id()) {
            (
                self.pull_id().min(other.pull_id()),
                self.pull_id().max(other.pull_id()),
            )
        } else {
            (other.pull_id(), other.pull_id())
        };
        self.track_ids(other.source_id, other.stream_id, min, max);

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
                Err(idx) => self.tracked_pull_ids.insert(idx, *other_tracked),
            }
        }
    }

    #[cfg(test)]
    /// track the given event id by its raw numeric ids
    fn track_id(&mut self, source_id: u64, stream_id: u64, pull_id: u64) {
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
                // binary_search_by guarantees us that this index exists
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
    /// checks if the given `EventId` is tracked by this one.
    /// Also returns true, if the `event_id` has the same id as `self`.
    pub fn is_tracking(&self, event_id: &EventId) -> bool {
        // this logic works, because the api of `.track` is garuanteeing us that
        // if we track something in tracked_pull_ids, and it is the same source_id, stream_id as the event id
        // it contains the min/max and we don't need to consider self.pull_id anymore
        if self.tracked_pull_ids.is_empty() {
            self.is_same_stream(event_id.source_id(), event_id.stream_id())
                && self.pull_id == event_id.pull_id
                && self.event_id == event_id.event_id
        } else {
            match self.tracked_pull_ids.binary_search_by(|probe| {
                probe.compare_ids(event_id.source_id(), event_id.stream_id())
            }) {
                Ok(idx) => {
                    // ALLOW: binary search ensures this index is available
                    let entry = unsafe { self.tracked_pull_ids.get_unchecked(idx) };
                    // this is only a heuristic, but is good enough for now
                    (entry.min_pull_id..=entry.max_pull_id).contains(&event_id.pull_id)
                }
                Err(_) => false,
            }
        }
    }

    /// returns true if the event is from the same source and stream
    #[must_use]
    fn is_same_stream(&self, source_id: u64, stream_id: u64) -> bool {
        self.source_id() == source_id && self.stream_id() == stream_id
    }

    #[must_use]
    /// get minimum pull id for a given source and stream, if it is tracked
    ///
    /// This also always checks the actual eventId, not only the tracked ones, this way we can save allocations when used within insights
    pub fn get_min_by_stream(&self, source_id: u64, stream_id: u64) -> Option<u64> {
        if self.tracked_pull_ids.is_empty() {
            self.is_same_stream(source_id, stream_id)
                .then_some(self.pull_id())
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
    /// get maximum pull id for a given source and stream if we have it here
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_max_by_stream(&self, source_id: u64, stream_id: u64) -> Option<u64> {
        if self.tracked_pull_ids.is_empty() {
            self.is_same_stream(source_id, stream_id)
                .then_some(self.pull_id())
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

    /// get the minimum tracked `pull_id` for each stream of the source with `source_id`
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_min_streams_by_source(
        &self,
        source_id: u64,
    ) -> impl Iterator<Item = (u64, u64)> + '_ {
        if self.tracked_pull_ids.is_empty() {
            Either::Left(
                (self.source_id() == source_id)
                    .then_some((self.stream_id(), self.pull_id()))
                    .into_iter(),
            )
        } else {
            Either::Right(
                self.tracked_pull_ids
                    .as_slice()
                    .iter()
                    .filter_map(move |teid| {
                        (teid.source_id == source_id).then_some((teid.stream_id, teid.min_pull_id))
                    }),
            )
        }
    }

    /// get the maximum tracked `pull_id` for each stream of the source with `source_id`
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_max_streams_by_source(
        &self,
        source_id: u64,
    ) -> impl Iterator<Item = (u64, u64)> + '_ {
        if self.tracked_pull_ids.is_empty() {
            Either::Left(
                (self.source_id() == source_id)
                    .then_some((self.stream_id(), self.pull_id()))
                    .into_iter(),
            )
        } else {
            Either::Right(
                self.tracked_pull_ids
                    .as_slice()
                    .iter()
                    .filter_map(move |teid| {
                        (teid.source_id == source_id).then_some((teid.stream_id, teid.max_pull_id))
                    }),
            )
        }
    }

    /// get all streams for a source id
    #[must_use]
    pub fn get_streams(&self, source_id: u64) -> HashSet<u64> {
        let mut v = HashSet::new();
        if self.tracked_pull_ids.is_empty() {
            if self.source_id() == source_id {
                v.insert(self.stream_id);
            }
        } else {
            let iter = self
                .tracked_pull_ids
                .iter()
                .filter_map(|tids| (tids.source_id == source_id).then_some(tids.stream_id));
            v.extend(iter);
        }
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

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.source_id, self.stream_id, self.event_id, self.pull_id
        )?;
        if !self.tracked_pull_ids.is_empty() {
            let mut iter = self.tracked_pull_ids.iter();
            if let Some(ids) = iter.next() {
                write!(f, " {ids}")?;
            }
            for ids in iter {
                write!(f, ", {ids}")?;
            }
        }
        Ok(())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Default,
    simd_json_derive::Serialize,
    simd_json_derive::Deserialize,
    Eq,
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

impl std::fmt::Display for TrackedPullIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
pub struct IdGenerator(u64, u64, u64);
impl IdGenerator {
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
    /// create a new generator for the `Operator` identified by `operator_id` with `stream_id`
    pub fn for_operator_with_stream(operator_id: OperatorId, stream_id: u64) -> Self {
        Self(operator_id.id(), stream_id, 0)
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

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use super::*;
    use simd_json::{ObjectHasher, OwnedValue};
    use tremor_common::ids::{Id, OperatorId};
    use tremor_script::ValueAndMeta;
    use tremor_value::Object;

    #[allow(clippy::unnecessary_wraps)] // as this is a function that gets passed as an argument and needs to fulful the bounds
    fn merge<'head>(
        this: &mut ValueAndMeta<'head>,
        other: ValueAndMeta<'head>,
    ) -> Result<(), Infallible> {
        if let Some(ref mut a) = this.value_mut().as_array_mut() {
            let mut e = Object::with_capacity_and_hasher(7, ObjectHasher::default());
            let mut data = Object::with_capacity_and_hasher(2, ObjectHasher::default());
            let (value, meta) = other.into_parts();
            data.insert_nocheck("value".into(), value);
            data.insert_nocheck("meta".into(), meta);
            e.insert_nocheck("data".into(), Value::from(data));
            e.insert_nocheck("ingest_ns".into(), 1.into());
            // kind is always null on events
            e.insert_nocheck("kind".into(), Value::null());
            e.insert_nocheck("is_batch".into(), false.into());
            a.push(Value::from(e));
        };
        Ok(())
    }
    #[test]
    fn value_iters() {
        let mut b = Event {
            data: (Value::array(), 2).into(),
            is_batch: true,
            ..Event::default()
        };
        let e1 = Event {
            data: (1, 2).into(),
            ..Event::default()
        };
        let e2 = Event {
            data: (3, 4).into(),
            ..Event::default()
        };

        assert!(b.data.consume(e1.data, merge).is_ok());
        assert!(b.data.consume(e2.data, merge).is_ok());

        let mut vi = b.value_iter();
        assert_eq!(vi.next().expect("no value"), &1);
        assert_eq!(vi.next().expect("no value"), &3);
        assert!(vi.next().is_none());
        let mut vmi = b.value_meta_iter();
        assert_eq!(vmi.next().expect("no value"), (&1.into(), &2.into()));
        assert_eq!(vmi.next().expect("no value"), (&3.into(), &4.into()));
        assert!(vmi.next().is_none());
    }

    #[test]
    fn value_iters_split_last() {
        let mut b = Event {
            data: (Value::array(), 2).into(),
            is_batch: true,
            ..Event::default()
        };

        assert!(b.value_iter().split_last().is_none());
        assert!(b.value_meta_iter().split_last().is_none());

        let e1 = Event {
            data: (1, 2).into(),
            ..Event::default()
        };
        {
            let splitted = e1.value_iter().split_last();
            let (last, mut rest) = splitted.expect("no value");
            assert_eq!(last, &1);
            assert!(rest.next().is_none());

            let splitted_meta = e1.value_meta_iter().split_last();
            let ((last_value, last_meta), mut rest) = splitted_meta.expect("no value");
            assert_eq!(last_value, &1);
            assert_eq!(last_meta, &2);
            assert!(rest.next().is_none());
        }
        assert!(b.data.consume(e1.data, merge).is_ok());
        {
            let splitted = b.value_iter().split_last();
            let (last, mut rest) = splitted.expect("no value");
            assert_eq!(last, &1);
            assert!(rest.next().is_none());

            let splitted_meta = b.value_meta_iter().split_last();
            let ((last_value, last_meta), mut rest) = splitted_meta.expect("no value");
            assert_eq!(last_value, &1);
            assert_eq!(last_meta, &2);
            assert!(rest.next().is_none());
        }
        let e2 = Event {
            data: (3, 4).into(),
            ..Event::default()
        };
        assert!(b.data.consume(e2.data, merge).is_ok());
        {
            let splitted = b.value_iter().split_last();
            let (last, mut rest) = splitted.expect("no value");
            assert_eq!(last, &3);
            let first = rest.next();
            assert_eq!(first.expect("no value"), &1);

            let splitted_meta = b.value_meta_iter().split_last();
            let ((last_value, last_meta), mut rest) = splitted_meta.expect("no value");
            assert_eq!(last_value, &3);
            assert_eq!(last_meta, &4);
            let first = rest.next();
            let (value, meta) = first.expect("no value");
            assert_eq!(value, &1);
            assert_eq!(meta, &2);
        }
    }

    #[test]
    fn cb() {
        let mut e = Event::default();
        assert_eq!(CbAction::from(true), CbAction::Ack);
        assert_eq!(CbAction::from(false), CbAction::Fail);

        assert_eq!(
            Event::ack_or_fail(true, 0, EventId::default(), OpMeta::default()).cb,
            CbAction::Ack
        );
        assert_eq!(
            Event::cb_ack(0, EventId::default(), OpMeta::default()).cb,
            CbAction::Ack
        );
        assert_eq!(e.insight_ack().cb, CbAction::Ack);

        assert_eq!(
            Event::ack_or_fail(false, 0, EventId::default(), OpMeta::default()).cb,
            CbAction::Fail
        );
        assert_eq!(
            Event::cb_fail(0, EventId::default(), OpMeta::default()).cb,
            CbAction::Fail
        );
        assert_eq!(e.insight_fail().cb, CbAction::Fail);

        let mut clone = e.clone();
        let op_id = OperatorId::new(1);
        clone.op_meta.insert(op_id, OwnedValue::null());
        let ack_with_timing = clone.insight_ack_with_timing(100);
        assert_eq!(ack_with_timing.cb, CbAction::Ack);
        assert!(ack_with_timing.op_meta.contains_key(op_id));
        let (_, m) = ack_with_timing.data.parts();
        assert_eq!(Some(100), m.get_u64("time"));

        e.op_meta.insert(OperatorId::new(42), OwnedValue::null());
    }

    #[test]
    fn gd() {
        let mut e = Event::default();

        assert_eq!(Event::restore_or_break(true, 0).cb, CbAction::Restore);
        assert_eq!(Event::cb_restore(0).cb, CbAction::Restore);
        assert_eq!(e.insight_restore().cb, CbAction::Restore);

        assert_eq!(Event::restore_or_break(false, 0).cb, CbAction::Trigger);
        assert_eq!(Event::cb_trigger(0).cb, CbAction::Trigger);
        assert_eq!(e.insight_trigger().cb, CbAction::Trigger);
    }

    #[test]
    fn len() -> Result<(), Box<dyn std::error::Error>> {
        // default non-batched event
        let mut e = Event::default();
        assert_eq!(1, e.len());
        // batched event with 2 elements
        e.is_batch = true;
        let mut value = Value::array_with_capacity(2);
        value.push(Value::from(true))?; // dummy events
        value.push(Value::from(false))?;
        e.data = (value, Value::object_with_capacity(0)).into();
        assert_eq!(2, e.len());

        // batched event with non-array value
        e.data = (Value::null(), Value::object_with_capacity(0)).into();
        assert_eq!(0, e.len());
        // batched array with empty array value
        e.data = (
            Value::array_with_capacity(0),
            Value::object_with_capacity(0),
        )
            .into();
        assert_eq!(0, e.len());

        Ok(())
    }

    #[test]
    fn is_empty() -> Result<(), Box<dyn std::error::Error>> {
        let mut e = Event::default();
        assert!(!e.is_empty());

        e.is_batch = true;
        e.data = (Value::null(), Value::object()).into();
        assert!(e.is_empty());

        e.data = (Value::array(), Value::object()).into();
        assert!(e.is_empty());

        let mut value = Value::array_with_capacity(2);
        value.push(Value::from(true))?; // dummy events
        value.push(Value::from(false))?;
        e.data = (value, Value::object()).into();
        assert!(!e.is_empty());
        Ok(())
    }
    #[test]
    fn correlation_meta() -> Result<(), Box<dyn std::error::Error>> {
        let mut e = Event::default();
        assert!(e.correlation_meta().is_none());
        let mut m = literal!({
            "correlation": 1
        });
        e.data = (Value::null(), m.clone()).into();

        assert_eq!(e.correlation_meta().ok_or("no value")?, 1);
        let mut e2 = Event {
            is_batch: true,
            data: (Value::array(), m.clone()).into(),
            ..Default::default()
        };

        e2.data.consume(e.data.clone(), merge)?;
        m.try_insert("correlation", 2);
        e.data = (Value::null(), m.clone()).into();
        e2.data.consume(e.data, merge)?;
        assert_eq!(
            e2.correlation_meta().ok_or("no value")?,
            Value::from(vec![1, 2])
        );

        Ok(())
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

        assert_eq!(
            ids1.get_max_streams_by_source(1).collect::<Vec<_>>(),
            vec![(1, 1)]
        );
        assert_eq!(
            ids1.get_max_streams_by_source(2).collect::<Vec<_>>(),
            vec![(DEFAULT_STREAM_ID, 1)]
        );

        assert_eq!(
            ids2.get_min_streams_by_source(1).collect::<Vec<_>>(),
            vec![(1, 2)]
        );
        assert_eq!(
            ids2.get_min_streams_by_source(2).collect::<Vec<_>>(),
            vec![(DEFAULT_STREAM_ID, 3)]
        );

        ids1.track(&ids2);

        assert_eq!(
            ids1.get_max_streams_by_source(1).collect::<Vec<_>>(),
            vec![(1, 2)]
        );
        assert_eq!(
            ids1.get_min_streams_by_source(1).collect::<Vec<_>>(),
            vec![(1, 1)]
        );
        assert_eq!(
            ids1.get_min_streams_by_source(3).collect::<Vec<_>>(),
            vec![]
        );
        assert_eq!(
            ids1.get_max_streams_by_source(3).collect::<Vec<_>>(),
            vec![]
        );
        assert_eq!(
            ids1.get_max_streams_by_source(2).collect::<Vec<_>>(),
            vec![(DEFAULT_STREAM_ID, 3)]
        );
        assert_eq!(
            ids1.get_min_streams_by_source(2).collect::<Vec<_>>(),
            vec![(DEFAULT_STREAM_ID, 1)]
        );

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
        let eid_1 = EventId::from_id(1, 2, 6);
        teid2.track(&eid_1);
        assert_eq!(teid2.max_pull_id, eid_1.event_id);
        assert_eq!(teid2.min_pull_id, 3);

        let eid_2 = EventId::from_id(1, 2, 1);
        teid2.track(&eid_2);
        assert_eq!(teid2.min_pull_id, eid_2.event_id);
        assert_eq!(teid2.max_pull_id, eid_1.event_id);

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
        streams.sort_unstable();

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
        let stream2 = 3_u64;
        let mut e1 = EventId::from((source, stream, 0, 0));
        let e2 = EventId::from((source, stream, 1, 0));

        e1.track(&e2);
        assert_eq!(Some(0), e1.get_max_by_stream(source, stream)); // we track pull id, not event id

        let e3 = EventId::from((source, stream, 2, 1));
        let e4 = EventId::from((source, stream2, 3, 2));
        e1.track(&e3);
        e1.track(&e4);

        assert_eq!(Some(1), e1.get_max_by_stream(source, stream));
        assert_eq!(
            vec![(stream, 1), (stream2, 2)],
            e1.get_max_streams_by_source(source).collect::<Vec<_>>()
        );
    }

    #[test]
    fn event_id_is_tracking() {
        let source = 1_u64;
        let source2 = 5_u64;
        let stream = 2_u64;
        let mut e1 = EventId::from((source, stream, 0, 1));

        // every id tracks itself by definition
        assert!(e1.is_tracking(&e1));

        let mut e2 = EventId::from((source, stream, 1, 2));
        let e3 = EventId::from((source2, stream, 5, 5));
        e2.track(&e3);

        e1.track(&e2);

        assert!(e1.is_tracking(&e1));
        assert!(e1.is_tracking(&e2));
        assert!(e1.is_tracking(&e3));
    }

    #[test]
    fn get_min_streams_by_source() {
        let source = 1;
        let source2 = 2;
        let source3 = 3;
        let stream = 42;
        let stream2 = 69;
        let mut e1 = EventId::new(source, stream, 1, 1);
        // get its own pull id when nothing is tracked
        assert_eq!(
            vec![(stream, 1)],
            e1.get_min_streams_by_source(source).collect::<Vec<_>>()
        );
        // different source
        let e2 = EventId::new(source2, DEFAULT_STREAM_ID, 42, 42);
        e1.track(&e2);
        assert_eq!(
            vec![(stream, 1)],
            e1.get_min_streams_by_source(source).collect::<Vec<_>>()
        );
        assert_eq!(
            vec![(DEFAULT_STREAM_ID, 42)],
            e1.get_min_streams_by_source(source2).collect::<Vec<_>>()
        );
        assert_eq!(
            e1.get_min_streams_by_source(source3).collect::<Vec<_>>(),
            vec![]
        );

        // same source, different stream
        let e3 = EventId::new(source, stream2, 1000, 1000);
        e1.track(&e3);

        assert_eq!(
            vec![(stream, 1), (stream2, 1000)],
            e1.get_min_streams_by_source(source).collect::<Vec<_>>()
        );

        // same stream, bigger pull_id
        let e4 = EventId::new(source, stream, 9, 7);
        e1.track(&e4);

        assert_eq!(
            vec![(stream, 1), (stream2, 1000)],
            e1.get_min_streams_by_source(source).collect::<Vec<_>>()
        );

        // same stream, smaller pull_id
        let e5 = EventId::new(source, stream, 0, 0);
        e1.track(&e5);

        assert_eq!(
            vec![(stream, 0), (stream2, 1000)],
            e1.get_min_streams_by_source(source).collect::<Vec<_>>()
        );

        // same source, different stream, smaller pull_id
        let e6 = EventId::new(source, stream2, 999, 999);
        e1.track(&e6);
        assert_eq!(
            vec![(stream, 0), (stream2, 999)],
            e1.get_min_streams_by_source(source).collect::<Vec<_>>()
        );
    }

    #[test]
    fn get_max_streams_by_source() {
        let source = 1;
        let source2 = 2;
        let source3 = 3;
        let stream = 42;
        let stream2 = 69;
        let mut e1 = EventId::new(source, stream, 1, 1);
        // get its own pull id when nothing is tracked
        assert_eq!(
            vec![(stream, 1)],
            e1.get_max_streams_by_source(source).collect::<Vec<_>>()
        );
        // different source
        let e2 = EventId::new(source2, DEFAULT_STREAM_ID, 42, 42);
        e1.track(&e2);
        assert_eq!(
            vec![(stream, 1)],
            e1.get_max_streams_by_source(source).collect::<Vec<_>>()
        );
        assert_eq!(
            vec![(DEFAULT_STREAM_ID, 42)],
            e1.get_max_streams_by_source(source2).collect::<Vec<_>>()
        );
        assert_eq!(
            e1.get_max_streams_by_source(source3).collect::<Vec<_>>(),
            vec![]
        );

        // same source, different stream
        let e3 = EventId::new(source, stream2, 1000, 1000);
        e1.track(&e3);

        assert_eq!(
            vec![(stream, 1), (stream2, 1000)],
            e1.get_max_streams_by_source(source).collect::<Vec<_>>()
        );

        // same stream, smaller pull_id
        let e4 = EventId::new(source, stream, 0, 0);
        e1.track(&e4);

        assert_eq!(
            vec![(stream, 1), (stream2, 1000)],
            e1.get_max_streams_by_source(source).collect::<Vec<_>>()
        );

        // same stream, bigger pull_id
        let e5 = EventId::new(source, stream, 2, 2);
        e1.track(&e5);

        assert_eq!(
            vec![(stream, 2), (stream2, 1000)],
            e1.get_max_streams_by_source(source).collect::<Vec<_>>()
        );

        // same source, different stream, bigger pull_id
        let e6 = EventId::new(source, stream2, 888, 1001);
        e1.track(&e6);
        assert_eq!(
            vec![(stream, 2), (stream2, 1001)],
            e1.get_max_streams_by_source(source).collect::<Vec<_>>()
        );
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
