#![warn(unused_extern_crates)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]
#![deny(missing_docs)]
// We might want to revisit inline_always
#![allow(clippy::module_name_repetitions, clippy::inline_always)]
// TODO: remove this when https://github.com/rust-lang/rust-clippy/issues/9076 is fixed
#![allow(clippy::trait_duplication_in_bounds)]
// TODO: remove this when https://github.com/rust-lang/rust-clippy/issues/8772 is fixed
#![allow(clippy::type_repetition_in_bounds)]
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

// This code takes the simd-json borrowed value as a baseline and copies a good part of it's content
// the original can be found at: https://github.com/simd-lite/simd-json
//
// Changs here should be evaluated if they make sense publish back upstream

//! A tremor value to represent data

extern crate serde as serde_ext;

mod error;
mod known_key;
mod macros;
/// Prelude module
pub mod prelude;
mod serde;
/// The value modules defines a structural module of tremor supported types
pub mod value;
pub use crate::serde::structurize;
pub use error::*;
pub use known_key::{Error as KnownKeyError, KnownKey};
pub use simd_json::{json, json_typed, AlignedBuf, StaticNode};
pub use value::from::*;
pub use value::{parse_to_value, parse_to_value_with_buffers, to_value, Object, Value};

use simd_json::{Node, ObjectHasher};
use simd_json_derive::{Deserialize, Serialize, Tape};
use value_trait::{ValueAccess, ValueInto, Writable};

/// Maximum size for a vector object
pub const VEC_LIMIT_UPPER: usize = 32;

impl<'value> Serialize for Value<'value> {
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        self.write(writer)
    }
}

impl<'value> ValueInto for Value<'value> {
    type String = String;

    fn into_string(self) -> Option<<Value<'value> as ValueInto>::String> {
        match self {
            Self::String(s) => Some(s.into_owned()),
            _ => None,
        }
    }

    fn into_array(self) -> Option<<Value<'value> as ValueAccess>::Array> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    fn into_object(self) -> Option<<Value<'value> as ValueAccess>::Object> {
        match self {
            Self::Object(o) => Some(*o),
            _ => None,
        }
    }
}

struct ValueDeser<'input, 'tape>(&'tape mut Tape<'input>);

impl<'input, 'tape> ValueDeser<'input, 'tape> {
    #[inline(always)]
    fn parse(&mut self) -> simd_json::Result<Value<'input>> {
        match self.0.next() {
            Some(Node::Static(s)) => Ok(Value::Static(s)),
            Some(Node::String(s)) => Ok(Value::from(s)),
            Some(Node::Array(len, _)) => Ok(self.parse_array(len)),
            Some(Node::Object(len, _)) => Ok(self.parse_map(len)),
            None => Err(simd_json::Error::generic(simd_json::ErrorType::Eof)),
        }
    }
    #[inline(always)]
    #[allow(clippy::unwrap_used, clippy::uninit_vec)]
    fn parse_array(&mut self, len: usize) -> Value<'input> {
        // Rust doesn't optimize the normal loop away here
        // so we write our own avoiding the length
        // checks during push
        let mut res = Vec::with_capacity(len);
        unsafe {
            res.set_len(len);
            for i in 0..len {
                // ALLOW: we know the values will be OK
                std::ptr::write(res.get_unchecked_mut(i), self.parse().unwrap());
            }
        }
        Value::Array(res)
    }

    #[inline(always)]
    #[allow(clippy::unwrap_used)]
    fn parse_map(&mut self, len: usize) -> Value<'input> {
        let mut res = Object::with_capacity_and_hasher(len, ObjectHasher::default());

        // Since we checked if it's empty we know that we at least have one
        // element so we eat this
        for _ in 0..len {
            // ALLOW: we know the values will be OK
            if let Node::String(key) = self.0.next().unwrap() {
                // ALLOW: we know it will parse correctly
                res.insert_nocheck(key.into(), self.parse().unwrap());
            } else {
                // ALLOW: We check against this in tape
                unreachable!();
            }
        }
        Value::from(res)
    }
}

impl<'input> Deserialize<'input> for Value<'input> {
    fn from_tape(tape: &mut crate::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: Sized + 'input,
    {
        ValueDeser(tape).parse()
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use simd_json_derive::{Deserialize, Serialize};
    #[test]
    fn parse() {
        #[derive(Deserialize, Serialize)]
        struct TestStruct<'test> {
            value: Value<'test>,
        }
        let mut v = br#"{"value":{"array":[1,1.0,true,null],"string":"badger"}}"#.to_vec();
        let orig = String::from_utf8(v.clone()).unwrap();
        let s = TestStruct::from_slice(&mut v).unwrap();
        assert_eq!(
            s.value,
            literal!({"array": [1, 1.0,true,null], "string": "badger"})
        );
        assert_eq!(s.json_string().unwrap(), orig);
    }
}
