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

use crate::{CBAction, Ids, OpMeta, SignalKind};
use simd_json::prelude::*;
use simd_json::BorrowedValue;
use std::mem::swap;
use tremor_script::{EventOriginUri, LineValue};

/// A tremor event
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct Event {
    /// The event ID
    pub id: Ids,
    /// The event Data
    pub data: LineValue,
    /// Nanoseconds at when the event was ingested
    pub ingest_ns: u64,
    /// URI to identify the origin of th event
    pub origin_uri: Option<EventOriginUri>,
    /// The kind of the event
    pub kind: Option<SignalKind>,
    /// If this event is batched (containing multiple events itself)
    pub is_batch: bool,
    /// Circuit breaker action
    pub cb: CBAction,
    /// Metadata for operators
    pub op_meta: OpMeta,
    /// this needs transactional data
    pub transactional: bool,
}

impl Event {
    /// turns the event in an insight given it's success
    #[must_use]
    pub fn insight(self, success: bool) -> Event {
        Event {
            cb: success.into(),
            ingest_ns: self.ingest_ns,
            id: self.id,
            op_meta: self.op_meta,
            origin_uri: self.origin_uri,
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
    pub fn ack_or_fail(ack: bool, ingest_ns: u64, ids: Ids) -> Self {
        if ack {
            Event::cb_ack(ingest_ns, ids)
        } else {
            Event::cb_fail(ingest_ns, ids)
        }
    }
    /// Creates a new ack insight from the event, consums the `op_meta` and
    /// `origin_uri` of the event may return None if no insight is needed
    #[must_use]
    pub fn insight_ack(&mut self) -> Event {
        let mut e = Event::cb_ack(self.ingest_ns, self.id.clone());
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// Creates a new fail insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    #[must_use]
    pub fn insight_fail(&mut self) -> Event {
        let mut e = Event::cb_fail(self.ingest_ns, self.id.clone());
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// Creates a restore insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    #[must_use]
    pub fn insight_restore(&mut self) -> Event {
        let mut e = Event::cb_restore(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// Creates a trigger insight from the event, consums the `op_meta` of the
    /// event may return None if no insight is needed
    #[must_use]
    pub fn insight_trigger(&mut self) -> Event {
        let mut e = Event::cb_trigger(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

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
    /// Creates a new event to restore a CB
    #[must_use]
    pub fn cb_restore(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CBAction::Open,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_trigger(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CBAction::Close,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_ack(ingest_ns: u64, id: Ids) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CBAction::Ack,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_fail(ingest_ns: u64, id: Ids) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CBAction::Fail,
            ..Event::default()
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

impl<'value> Iterator for ValueMetaIter<'value> {
    type Item = (&'value BorrowedValue<'value>, &'value BorrowedValue<'value>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .as_array()
                .and_then(|arr| arr.get(self.idx))
                .and_then(|e| Some((e.get("data")?.get("value")?, e.get("data")?.get("meta")?)));
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = self.event.data.suffix();
            self.idx += 1;
            Some((&v.value(), &v.meta()))
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

impl<'value> Iterator for ValueIter<'value> {
    type Item = &'value BorrowedValue<'value>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .as_array()
                .and_then(|arr| arr.get(self.idx))
                .and_then(|e| e.get("data")?.get("value"));
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = &self.event.data.suffix().value();
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
    use simd_json::value::borrowed::Object;
    use tremor_script::ValueAndMeta;

    #[test]
    fn valeu_iters() {
        let mut b = Event {
            data: (BorrowedValue::array(), 2).into(),
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

        let merge = move |this: &mut ValueAndMeta<'static>,
                          other: ValueAndMeta<'static>|
              -> crate::errors::Result<()> {
            if let Some(ref mut a) = this.value_mut().as_array_mut() {
                let mut e = Object::with_capacity(7);
                // {"id":1,
                // e.insert_nocheck("id".into(), id.into());
                //  "data": {
                //      "value": "snot", "meta":{}
                //  },
                let mut data = Object::with_capacity(2);
                let (value, meta) = other.into_parts();
                data.insert_nocheck("value".into(), value);
                data.insert_nocheck("meta".into(), meta);
                e.insert_nocheck("data".into(), BorrowedValue::from(data));
                //  "ingest_ns":1,
                e.insert_nocheck("ingest_ns".into(), 1.into());
                //  "kind":null,
                // kind is always null on events
                e.insert_nocheck("kind".into(), BorrowedValue::null());
                //  "is_batch":false
                e.insert_nocheck("is_batch".into(), false.into());
                // }
                a.push(BorrowedValue::from(e))
            };
            Ok(())
        };
        assert!(b.data.consume(e1.data, merge).is_ok());
        assert!(b.data.consume(e2.data, merge).is_ok());

        let mut vi = b.value_iter();
        assert_eq!(vi.next().unwrap(), &1);
        assert_eq!(vi.next().unwrap(), &3);
        assert!(vi.next().is_none());
        let mut vmi = b.value_meta_iter();
        assert_eq!(vmi.next().unwrap(), (&1.into(), &2.into()));
        assert_eq!(vmi.next().unwrap(), (&3.into(), &4.into()));
        assert!(vmi.next().is_none());
    }

    #[test]
    fn cb() {
        let mut e = Event::default();
        assert_eq!(e.clone().insight(true).cb, CBAction::Ack);
        assert_eq!(e.clone().insight(false).cb, CBAction::Fail);

        assert_eq!(
            Event::ack_or_fail(true, 0, Ids::default()).cb,
            CBAction::Ack
        );
        assert_eq!(Event::cb_ack(0, Ids::default()).cb, CBAction::Ack);
        assert_eq!(e.insight_ack().cb, CBAction::Ack);

        assert_eq!(
            Event::ack_or_fail(false, 0, Ids::default()).cb,
            CBAction::Fail
        );
        assert_eq!(Event::cb_fail(0, Ids::default()).cb, CBAction::Fail);
        assert_eq!(e.insight_fail().cb, CBAction::Fail);
    }

    #[test]
    fn gd() {
        let mut e = Event::default();

        assert_eq!(Event::restore_or_break(true, 0).cb, CBAction::Open);
        assert_eq!(Event::cb_restore(0).cb, CBAction::Open);
        assert_eq!(e.insight_restore().cb, CBAction::Open);

        assert_eq!(Event::restore_or_break(false, 0).cb, CBAction::Close);
        assert_eq!(Event::cb_trigger(0).cb, CBAction::Close);
        assert_eq!(e.insight_trigger().cb, CBAction::Close);
    }
}
