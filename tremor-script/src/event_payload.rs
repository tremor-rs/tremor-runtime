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

use crate::prelude::*;
use std::{fmt::Debug, mem, pin::Pin, sync::Arc};
pub use tremor_common::stry;

/// Combined struct for an event value and metadata
#[derive(
    Clone, Debug, PartialEq, Serialize, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct ValueAndMeta<'event> {
    v: Value<'event>,
    m: Value<'event>,
}

impl simd_json_derive::Serialize for EventPayload {
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        self.rent(|d| d.json_write(writer))
    }
}

impl<'input> simd_json_derive::Deserialize<'input> for EventPayload {
    fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: Sized + 'input,
    {
        let ValueAndMeta { v, m } = simd_json_derive::Deserialize::from_tape(tape)?;

        Ok(Self::new(vec![], |_| {
            ValueAndMeta::from_parts(v.into_static(), m.into_static())
        }))
    }
}

impl<'event> ValueAndMeta<'event> {
    /// A value from it's parts
    #[must_use]
    pub fn from_parts(v: Value<'event>, m: Value<'event>) -> Self {
        Self { v, m }
    }
    /// Event value
    #[must_use]
    pub fn value(&self) -> &Value<'event> {
        &self.v
    }

    /// Event value
    #[must_use]
    pub fn value_mut(&mut self) -> &mut Value<'event> {
        &mut self.v
    }
    /// Event metadata
    #[must_use]
    pub fn meta(&self) -> &Value<'event> {
        &self.m
    }
    /// Deconstruicts the value into it's parts
    #[must_use]
    pub fn into_parts(self) -> (Value<'event>, Value<'event>) {
        (self.v, self.m)
    }
    /// borrows both parts as mutalbe
    #[must_use]
    pub fn parts_mut(&mut self) -> (&mut Value<'event>, &mut Value<'event>) {
        (&mut self.v, &mut self.m)
    }
}

impl<'event> Default for ValueAndMeta<'event> {
    fn default() -> Self {
        ValueAndMeta {
            v: Value::object(),
            m: Value::object(),
        }
    }
}

impl<'v> From<Value<'v>> for ValueAndMeta<'v> {
    fn from(v: Value<'v>) -> ValueAndMeta<'v> {
        ValueAndMeta {
            v,
            m: Value::object(),
        }
    }
}

impl<'v, T1, T2> From<(T1, T2)> for ValueAndMeta<'v>
where
    Value<'v>: From<T1> + From<T2>,
{
    fn from((v, m): (T1, T2)) -> Self {
        ValueAndMeta {
            v: Value::from(v),
            m: Value::from(m),
        }
    }
}

/// FIXME
#[derive(Clone, Default)]
pub struct EventPayload {
    /// - We have a vector to hold multiple raw input values
    /// - Each input value is a Vec<u8>
    /// - Each Vec is pinned to ensure the underlying data isn't moved
    /// - Each Pin is in a Arc so we can clone the data without with both clones
    ///   still pointing to the underlying pin.
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ValueAndMeta<'static>,
}

impl std::fmt::Debug for EventPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

impl EventPayload {
    /// FIXME
    #[must_use]
    pub fn suffix(&self) -> &ValueAndMeta {
        &self.structured
    }

    /// FIXME
    #[must_use]
    pub fn new<F>(raw: Vec<u8>, f: F) -> Self
    where
        F: for<'head> FnOnce(&'head mut [u8]) -> ValueAndMeta<'head>,
    {
        let mut raw = Pin::new(raw);
        let structured = f(raw.as_mut().get_mut());
        // This is where the magic happens
        let structured: ValueAndMeta<'static> = unsafe { mem::transmute(structured) };
        let raw = vec![Arc::new(raw)];
        Self { raw, structured }
    }

    /// FIXME
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(raw: Vec<u8>, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut [u8]) -> std::result::Result<ValueAndMeta<'head>, E>,
    {
        let mut raw = Pin::new(raw);
        let structured = f(raw.as_mut().get_mut())?;
        // This is where the magic happens
        let structured: ValueAndMeta<'static> = unsafe { mem::transmute(structured) };
        let raw = vec![Arc::new(raw)];
        Ok(Self { raw, structured })
    }

    /// FIXME
    pub fn rent<F, R>(&self, f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(&'iref ValueAndMeta<'head>) -> R,
    {
        f(&self.structured)
    }

    /// FIXME
    pub fn rent_mut<F, R>(&mut self, f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(&'iref mut ValueAndMeta<'head>) -> R,
    {
        f(&mut self.structured)
    }

    /// Borrow the parts (event and metadata) from a rental.
    #[must_use]
    pub fn parts<'value, 'borrow>(&'borrow self) -> (&'borrow Value<'value>, &'borrow Value<'value>)
    where
        'borrow: 'value,
    {
        let ValueAndMeta { ref v, ref m } = self.structured;
        (v, m)
    }

    /// Consumes an event into another
    /// This function works around a rental limitation that is meant
    /// to protect its users: Rental does not allow you to get both
    /// the owned and borrowed part at the same time.
    ///
    /// The reason for that is that once those are taken out of the
    /// rental the link of lifetimes between them would be broken
    /// and you'd risk invalid pointers or memory leaks.
    /// We however do not really want to take them out, all we want
    /// is combine two rentals. We can do this since:
    /// 1) the owned values are inside a vector, while the vector
    ///    itself may be relocated by adding to it, the values
    ///    in it will stay in the same location.
    /// 2) we are only ever adding / extending never deleting
    ///    or modifying.
    ///
    /// So what this function does it is crowbars the content
    /// from a rental into an accessible struct then uses this
    /// to modify its content by adding the owned parts of
    /// `other` into the owned part `self` and the running
    /// a merge function on the borrowed parts
    ///
    /// # Errors
    /// if `join_f` errors
    pub fn consume<E, F>(&mut self, mut other: Self, join_f: F) -> Result<(), E>
    where
        E: std::error::Error,
        F: Fn(&mut ValueAndMeta<'static>, ValueAndMeta<'static>) -> Result<(), E>,
    {
        self.raw.append(&mut other.raw);
        join_f(&mut self.structured, other.structured)?;

        Ok(())
    }
}

impl<T> From<T> for EventPayload
where
    ValueAndMeta<'static>: From<T>,
{
    fn from(vm: T) -> Self {
        Self {
            raw: Vec::new(),
            structured: vm.into(),
        }
    }
}

/// An error occurred while deserializing
/// a value into an Event.
pub enum DeserError {
    /// The value was missing the `value` key
    ValueMissing,
    /// The value was missing the `metadata` key
    MetaMissing,
}

impl PartialEq for EventPayload {
    fn eq(&self, other: &Self) -> bool {
        self.structured.eq(&other.structured)
    }
}
