// Copyright 2022, The Tremor Team and Mario Ortiz Manero
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

use abi_stable::std_types::{RCowSlice, RCowStr};

/// The error type used for the PDK, from `abi_stable`
pub type RError = abi_stable::std_types::SendRBoxError;
/// The result type used for the PDK, from `abi_stable`
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;
/// The KnownKey optimization currently relies on a deterministic hasher. Thus,
/// we must ensure that `RHashMap` always uses `halfbrown`'s hasher.
///
/// This can be removed after
/// https://github.com/tremor-rs/tremor-runtime/issues/1812 is fixed.
pub type RHashMap<K, V> = abi_stable::std_types::RHashMap<K, V, halfbrown::DefaultHashBuilder>;

/// This can be used alongside [`abi_stable::rtry`] for error handling. Its
/// difference is that it does an implicit conversion to `RResult`, which is
/// useful sometimes to reduce boilerplate.
///
/// These macros are a workaround until `?` can be used with functions that
/// return `RResult`: https://github.com/rust-lang/rust/issues/84277.
#[macro_export]
macro_rules! ttry {
    ($e:expr) => {
        match $e {
            ::std::result::Result::Ok(val) => val,
            ::std::result::Result::Err(err) => {
                return ::abi_stable::std_types::RResult::RErr(err.into())
            }
        }
    };
}

/// Converts a `RCow` to a `beef::Cow` by turning it into a `std::borrow::Cow`
/// as an intermediate step.
#[must_use]
pub fn rcow_to_beef_str(cow: RCowStr<'_>) -> beef::Cow<'_, str> {
    let cow: std::borrow::Cow<'_, str> = cow.into();
    cow.into()
}

/// Converts a `RCow` to a `beef::Cow` by turning it into a `std::borrow::Cow`
/// as an intermediate step.
#[must_use]
pub fn beef_to_rcow_str(cow: beef::Cow<'_, str>) -> RCowStr<'_> {
    let cow: std::borrow::Cow<'_, str> = cow.into();
    cow.into()
}

/// Converts a `RCow` to a `beef::Cow` by turning it into a `std::borrow::Cow`
/// as an intermediate step.
#[must_use]
pub fn rcow_to_beef_u8(cow: RCowSlice<'_, u8>) -> beef::Cow<'_, [u8]> {
    let cow: std::borrow::Cow<'_, [u8]> = cow.into();
    cow.into()
}

/// Converts a `RCow` to a `beef::Cow` by turning it into a `std::borrow::Cow`
/// as an intermediate step.
#[must_use]
pub fn beef_to_rcow_u8(cow: beef::Cow<'_, [u8]>) -> RCowSlice<'_, u8> {
    let cow: std::borrow::Cow<'_, [u8]> = cow.into();
    cow.into()
}
