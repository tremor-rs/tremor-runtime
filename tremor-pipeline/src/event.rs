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

use crate::{CbAction, EventId, OpMeta, SignalKind};
use std::mem::swap;
use tremor_common::ids::SourceId;
use tremor_common::time::nanotime;
use tremor_script::prelude::*;
use tremor_script::{EventOriginUri, EventPayload};

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

#[cfg(test)]
mod test {
    use super::*;
    use crate::Result;
    use simd_json::OwnedValue;
    use tremor_common::ids::{Id, OperatorId};
    use tremor_script::ValueAndMeta;
    use tremor_value::Object;

    #[allow(clippy::unnecessary_wraps)] // as this is a function that gets passed as an argument and needs to fulful the bounds
    fn merge<'head>(this: &mut ValueAndMeta<'head>, other: ValueAndMeta<'head>) -> Result<()> {
        if let Some(ref mut a) = this.value_mut().as_array_mut() {
            let mut e = Object::with_capacity(7);
            let mut data = Object::with_capacity(2);
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
    fn value_iters() -> Result<()> {
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
        assert_eq!(vi.next().ok_or("no value")?, &1);
        assert_eq!(vi.next().ok_or("no value")?, &3);
        assert!(vi.next().is_none());
        let mut vmi = b.value_meta_iter();
        assert_eq!(vmi.next().ok_or("no value")?, (&1.into(), &2.into()));
        assert_eq!(vmi.next().ok_or("no value")?, (&3.into(), &4.into()));
        assert!(vmi.next().is_none());
        Ok(())
    }

    #[test]
    fn value_iters_split_last() -> Result<()> {
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
            let (last, mut rest) = splitted.ok_or("no value")?;
            assert_eq!(last, &1);
            assert!(rest.next().is_none());

            let splitted_meta = e1.value_meta_iter().split_last();
            let ((last_value, last_meta), mut rest) = splitted_meta.ok_or("no value")?;
            assert_eq!(last_value, &1);
            assert_eq!(last_meta, &2);
            assert!(rest.next().is_none());
        }
        assert!(b.data.consume(e1.data, merge).is_ok());
        {
            let splitted = b.value_iter().split_last();
            let (last, mut rest) = splitted.ok_or("no value")?;
            assert_eq!(last, &1);
            assert!(rest.next().is_none());

            let splitted_meta = b.value_meta_iter().split_last();
            let ((last_value, last_meta), mut rest) = splitted_meta.ok_or("no value")?;
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
            let (last, mut rest) = splitted.ok_or("no value")?;
            assert_eq!(last, &3);
            let first = rest.next();
            assert_eq!(first.ok_or("no value")?, &1);

            let splitted_meta = b.value_meta_iter().split_last();
            let ((last_value, last_meta), mut rest) = splitted_meta.ok_or("no value")?;
            assert_eq!(last_value, &3);
            assert_eq!(last_meta, &4);
            let first = rest.next();
            let (value, meta) = first.ok_or("no value")?;
            assert_eq!(value, &1);
            assert_eq!(meta, &2);
        }
        Ok(())
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
    fn len() -> Result<()> {
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
    fn is_empty() -> Result<()> {
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
    fn correlation_meta() -> Result<()> {
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
}
