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
use std::{mem, pin::Pin};
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

rental! {
    /// Tremor script rentals to work around lifetime
    /// issues
    pub(crate) mod rentals {
        use super::*;
        use std::pin::Pin;

        /// Rental wrapped value with the data it was parsed
        /// from from
        #[rental_mut(covariant, debug)]
        pub struct Value {
            raw: Vec<Pin<Vec<u8>>>,
            parsed: ValueAndMeta<'raw>
        }

    }
}
pub use rentals::Value as EventPayload;

impl EventPayload {
    /// Borrow the parts (event and metadata) from a rental.
    #[must_use]
    pub fn parts<'value, 'borrow>(&'borrow self) -> (&'borrow Value<'value>, &'borrow Value<'value>)
    where
        'borrow: 'value,
    {
        let ValueAndMeta { v, m } = self.suffix();
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
    pub fn consume<'run, E, F>(&'run mut self, other: Self, join_f: F) -> Result<(), E>
    where
        E: std::error::Error,
        F: Fn(&mut ValueAndMeta<'static>, ValueAndMeta<'static>) -> Result<(), E>,
    {
        struct ScrewRental {
            pub parsed: ValueAndMeta<'static>,
            pub raw: Vec<Pin<Vec<u8>>>,
        }
        // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1031
        #[allow(clippy::transmute_ptr_to_ptr)]
        unsafe {
            // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1031
            let self_unrent: &'run mut ScrewRental = mem::transmute(self);
            // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1031
            let mut other_unrent: ScrewRental = mem::transmute(other);
            self_unrent.raw.append(&mut other_unrent.raw);
            join_f(&mut self_unrent.parsed, other_unrent.parsed)?;
        }
        Ok(())
    }
}

impl From<Value<'static>> for EventPayload {
    fn from(v: Value<'static>) -> Self {
        Self::new(vec![], |_| ValueAndMeta::from(v))
    }
}

impl<T1, T2> From<(T1, T2)> for EventPayload
where
    Value<'static>: From<T1> + From<T2>,
{
    fn from((v, m): (T1, T2)) -> Self {
        Self::new(vec![], |_| ValueAndMeta {
            v: Value::from(v),
            m: Value::from(m),
        })
    }
}

impl Clone for EventPayload {
    fn clone(&self) -> Self {
        // The only safe way to clone a line value is to clone
        // the data and then turn it into a owned value
        // then turn this owned value back into a borrowed value
        // we need to do this dance to 'free' value from the
        // linked lifetime.
        // An alternative would be keeping the raw data in an ARC
        // instead of a Box.
        Self::new(vec![], |_| {
            let v = self.suffix();
            ValueAndMeta::from_parts(v.value().clone_static(), v.meta().clone_static())
        })
    }
}
impl Default for EventPayload {
    fn default() -> Self {
        Self::new(vec![], |_| ValueAndMeta::default())
    }
}

/// An error occurred while deserializing
/// a value into an Event.
pub enum EventPayloadDeserError {
    /// The value was missing the `value` key
    ValueMissing,
    /// The value was missing the `metadata` key
    MetaMissing,
}

impl PartialEq for EventPayload {
    fn eq(&self, other: &Self) -> bool {
        self.rent(|s| other.rent(|o| s == o))
    }
}
