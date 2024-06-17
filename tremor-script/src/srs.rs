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

use tremor_codec::Codec;

use crate::prelude::*;
use std::{fmt::Debug, mem, pin::Pin, sync::Arc};

/*
=========================================================================
*/

/// A event payload in form of two borrowed Value's with a vector of source binaries.
///
/// We have a vector to hold multiple raw input values
///   - Each input value is a Vec<u8>
///   - Each Vec is pinned to ensure the underlying data isn't moved
///   - Each Pin is in a Arc so we can clone the data without with both clones
///     still pointing to the underlying pin.
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///
#[derive(Clone, Default)]
pub struct EventPayload {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    data: ValueAndMeta<'static>,
}

// #[cfg_attr(coverage, no_coverage)] // this is a simple Debug implementation
impl Debug for EventPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.fmt(f)
    }
}

impl EventPayload {
    /// Gets the suffix
    ///
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = {
    ///       let s = e.suffix();
    ///       s.value()["key"].clone()
    ///   };
    ///   drop(e);
    ///   println!("v: {}", v)
    /// ```

    #[must_use]
    pub fn suffix(&self) -> &ValueAndMeta {
        &self.data
    }

    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    #[must_use]
    pub fn new<F>(raw: Vec<u8>, f: F) -> Self
    where
        F: for<'head> FnOnce(&'head mut [u8]) -> ValueAndMeta<'head>,
    {
        let mut raw = Pin::new(raw);
        let data = f(raw.as_mut().get_mut());
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<ValueAndMeta<'_>, ValueAndMeta<'static>>(data) };
        let raw = vec![Arc::new(raw)];
        Self {
            raw,
            data: structured,
        }
    }

    /// The function tries to turn an array of raw data into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_from_multiple<F>(mut raw: Vec<Vec<u8>>, f: F) -> Result<Self, crate::errors::Error>
    where
        F: for<'head> FnOnce(
            &'head mut [Vec<u8>],
        ) -> Result<ValueAndMeta<'head>, crate::errors::Error>,
    {
        let data = f(&mut raw)?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured = unsafe { mem::transmute::<ValueAndMeta<'_>, ValueAndMeta<'static>>(data) };
        let raw = raw
            .into_iter()
            .map(|r| Arc::new(Pin::new(r)))
            .collect::<Vec<_>>();
        Ok(Self {
            raw,
            data: structured,
        })
    }

    /// Creates a new Payload with a given byte vector and
    /// a codec to decode it.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub async fn from_codec(
        raw: Vec<u8>,
        meta: Value<'static>,
        ingest_ns: &mut u64,
        codec: &mut Box<dyn Codec>,
    ) -> std::result::Result<Option<Self>, crate::errors::Error> {
        let mut raw = Pin::new(raw);
        // We are keeping the Vector pinnd and as part of the same struct, and the decoded data
        // can't leak this function aside of inside this struct.
        // ALLOW: See above explenation
        let r = unsafe { mem::transmute::<&mut [u8], &'static mut [u8]>(raw.as_mut().get_mut()) };
        let res = codec.decode(r, *ingest_ns, meta).await?;
        Ok(res.map(|(decoded, meta)| {
            let data = ValueAndMeta::from_parts(decoded, meta);
            let raw = vec![Arc::new(raw)];
            Self { raw, data }
        }))
    }

    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// and calls the provided function with a reference to it
    ///
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = e.rent(|s| {
    ///       s.value()["key"].clone()
    ///   });
    ///   println!("v: {}", v)
    /// ```
    pub fn rent<'iref, F, R>(&'iref self, f: F) -> R
    where
        F: for<'head> FnOnce(&'head ValueAndMeta<'head>) -> R,
    {
        // we are turning a longer lifetime into a shorter one for a covariant
        // type ValueAndMeta. &mut is invariant over it's lifetime, but we are
        // choosing a shorter one and passing it down not up. So a user
        // should not be able to choose an inappropriate lifetime for it, plus
        // they don't control the owner here.
        f(unsafe {
            // ALLOW: See above explenation
            mem::transmute::<&'iref ValueAndMeta<'static>, &'iref ValueAndMeta<'iref>>(&self.data)
        })
    }

    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// mutably and calls the provided function with a mutatable reference to it
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let mut e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = e.rent_mut(|s| {
    ///       s.value()["key"].clone()
    ///   });
    ///   println!("v: {}", v)
    /// ```
    pub fn rent_mut<'iref, F, R>(&'iref mut self, f: F) -> R
    where
        F: for<'head> FnOnce(&'head mut ValueAndMeta<'head>) -> R,
    {
        // we are turning a longer lifetime into a shorter one for a covariant
        // type ValueAndMeta. &mut is invariant over it's lifetime, but we are
        // choosing a shorter one and passing it down not up. So a user should
        // not be able to choose an inappropriate lifetime for it, plus they
        // don't control the owner here.
        f(unsafe {
            // ALLOW: See above explenation
            mem::transmute::<&'iref mut ValueAndMeta<'static>, &'iref mut ValueAndMeta<'iref>>(
                &mut self.data,
            )
        })
    }

    /// Borrow the parts (event and metadata) from a rental.
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   let vec = br#"{"key": "value"}"#.to_vec();
    ///   let e = EventPayload::new(vec, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let v: Value = {
    ///       let (s, _) = e.parts();
    ///       s["key"].clone()
    ///   };
    ///   drop(e);
    ///   println!("v: {}", v)
    /// ```
    #[must_use]
    pub fn parts<'value, 'borrow>(&'borrow self) -> (&'borrow Value<'value>, &'borrow Value<'value>)
    where
        'borrow: 'value,
    {
        let ValueAndMeta { ref v, ref m } = self.data;
        (v, m)
    }

    /// Consumes one payload into another
    ///
    /// ```compile_fail
    ///   use tremor_script::prelude::*;
    ///   use tremor_script::errors::Error;
    ///   let vec1 = br#"{"key": "value"}"#.to_vec();
    ///   let mut e1 = EventPayload::new(vec1, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let vec2 = br#"{"snot": "badger"}"#.to_vec();
    ///   let e2 = EventPayload::new(vec2, |d| tremor_value::parse_to_value(d).unwrap().into());
    ///   let mut v = Value::null();
    ///   // We try to move the data ov v2 outside of this closure to trick the borrow checker
    ///   // into letting us have it even if it's referenced data no longer exist
    ///   e1.consume::<Error,_>(e2, |v1, v2| {
    ///     let (v2,_) = v2.into_parts();
    ///     v = v2;
    ///     Ok(())
    ///   }).unwrap();
    ///   drop(e1);
    ///   println!("v: {}", v);
    /// ```
    ///
    /// # Errors
    /// if `join_f` errors
    pub fn consume<'iref, E, F>(
        &'iref mut self,
        mut other: EventPayload,
        join_f: F,
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error,
        F: for<'head> FnOnce(
            &'head mut ValueAndMeta<'head>,
            ValueAndMeta<'head>,
        ) -> std::result::Result<(), E>,
    {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        // READ: ORDER MATTERS!
        self.raw.append(&mut other.raw);

        //  we are turning a longer lifetime into a shorter one for a covariant
        // type ValueAndMeta. &mut is invariant over it's lifetime, but we are
        // choosing a shorter one and passing it down not up. So a user should
        // not be able to choose an inappropriate lifetime for it, plus they
        // don't control the owner here.
        join_f(
            unsafe {
                // ALLOW: See above for explenation
                mem::transmute::<&'iref mut ValueAndMeta<'static>, &'iref mut ValueAndMeta<'iref>>(
                    &mut self.data,
                )
            },
            // ALLOW: See above for explenation
            unsafe { mem::transmute::<ValueAndMeta<'static>, ValueAndMeta<'iref>>(other.data) },
        )
    }
}

impl<T> From<T> for EventPayload
where
    ValueAndMeta<'static>: From<T>,
{
    fn from(vm: T) -> Self {
        Self {
            raw: Vec::new(),
            data: vm.into(),
        }
    }
}

impl PartialEq for EventPayload {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
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

/*
=========================================================================
*/

/// Combined struct for an event value and metadata
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    simd_json_derive::Serialize,
    simd_json_derive::Deserialize,
    Eq,
)]
pub struct ValueAndMeta<'event> {
    v: Value<'event>,
    m: Value<'event>,
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

    /// borrows both parts as mutalbe
    #[must_use]
    pub fn parts(&self) -> (&Value<'event>, &Value<'event>) {
        (&self.v, &self.m)
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_event_payload() {
        let vec = br#"{"key": "value"}"#.to_vec();
        let e = EventPayload::new(vec, |d| {
            tremor_value::parse_to_value(d)
                .expect("failed to parse")
                .into()
        });
        let v: String = e.rent(|s| {
            s.value()["key"]
                .as_str()
                .expect("failed to parse")
                .to_string()
        });
        assert_eq!(v, "value");
    }

    #[test]
    fn test_event_payload_try_from_multiple() -> Result<(), crate::errors::Error> {
        let vec1 = br#"{"key": "value1"}"#.to_vec();
        let vec2 = br#"{"key": "value2"}"#.to_vec();
        let e = EventPayload::try_from_multiple(vec![vec1, vec2], |d| {
            let mut value = Value::array_with_capacity(2);
            for v in d.iter_mut() {
                value.try_push(tremor_value::parse_to_value(v)?);
            }
            Ok((value, Value::null()).into())
        })?;
        let v: String = e.rent(|s| s.value()[0]["key"].as_str().expect("no string").to_string());
        assert_eq!(v, "value1");
        let v: String = e.rent(|s| s.value()[1]["key"].as_str().expect("no string").to_string());
        assert_eq!(v, "value2");

        Ok(())
    }
}
