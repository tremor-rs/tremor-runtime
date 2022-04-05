//! The `Value` type is too complex to make `#[repr(C)]` from scratch. This is
//! why this module declares `PdkValue` as the FFI-safe alternative to
//! communicate with the plugins. It's a copy from the original one, but the
//! collections have been replaced with `abi_stable`'s (e.g. `Box` for `RBox`).
//!
//! `PdkValue` is only meant to be used for the PDK interface. A plugin or the
//! runtime can convert between `PdkValue` and `Value` at a relatively small
//! cost, and use its full functionality that way.
//!
//! Although this decision might sound like a considerable performance overhead,
//! it would be even worse to try to make `Value` fully `#[repr(C)]` as it is
//! right now. The collections in `abi_stable` are much more basic than what the
//! original `Value` uses, such as `beef::Cow` or `halfbrown::HashMap`. Not only
//! are these types by themselves more efficient for Tremor's use-case, but also
//! their functionality is consierably improved, making it impossible to for
//! example implement the known-key optimization for a `Value` if we were to use
//! `RHashMap`.

use crate::Value;

use abi_stable::{
    std_types::{RBox, RCow, RHashMap, RVec, Tuple2},
    StableAbi,
};
use value_trait::StaticNode;

/// Representation of a JSON object
pub type Object<'value> = RHashMap<RCow<'value, str>, PdkValue<'value>>;
/// Bytes
pub type Bytes<'value> = RCow<'value, [u8]>;

// There are no direct conversions between `beef::Cow` and `RCow`, so the type
// has to be converted to std as the intermediate. These conversions are cheap
// and they shouldn't be a performance issue.
//
// FIXME: clean up after creation of `tremor-pdk`, this is repeated in other
// crates.
fn conv_str(cow: beef::Cow<str>) -> RCow<str> {
    let cow: std::borrow::Cow<str> = cow.into();
    cow.into()
}
fn conv_u8(cow: beef::Cow<[u8]>) -> RCow<[u8]> {
    let cow: std::borrow::Cow<[u8]> = cow.into();
    cow.into()
}
fn conv_str_inv(cow: RCow<str>) -> beef::Cow<str> {
    let cow: std::borrow::Cow<str> = cow.into();
    cow.into()
}
fn conv_u8_inv(cow: RCow<[u8]>) -> beef::Cow<[u8]> {
    let cow: std::borrow::Cow<[u8]> = cow.into();
    cow.into()
}

/// Temporary type to represent a [`Value`] in the PDK interface. It's meant to
/// be converted to the original `Value` whenever its full functionality is
/// needed, and then back to `PdkValue` in order to pass it through the FFI
/// boundary.
///
/// Refer to the [`crate::pdk`] top-level documentation for more information.
///
/// [`Value`]: [`crate::Value`]
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub enum PdkValue<'value> {
    /// Static values
    Static(StaticNode),
    /// string type
    String(RCow<'value, str>),
    /// array type
    Array(RVec<PdkValue<'value>>),
    /// object type
    Object(RBox<Object<'value>>),
    /// A binary type
    Bytes(Bytes<'value>),
}

/// Easily converting the original type to the PDK one to pass it through the
/// FFI boundary.
impl<'value> From<Value<'value>> for PdkValue<'value> {
    fn from(original: Value<'value>) -> Self {
        match original {
            // No conversion needed; `StaticNode` implements `StableAbi`
            Value::Static(s) => PdkValue::Static(s),
            // This conversion is cheap
            Value::String(s) => PdkValue::String(conv_str(s)),
            // This unfortunately requires iterating the array
            Value::Array(a) => {
                let a = a.into_iter().map(Into::into).collect();
                PdkValue::Array(a)
            }
            // This unfortunately requires iterating the map and a new
            // allocation
            Value::Object(m) => {
                let m = m
                    .into_iter()
                    .map(|(k, v)| (conv_str(k), v.into()))
                    .collect();
                PdkValue::Object(RBox::new(m))
            }
            // This conversion is cheap
            Value::Bytes(b) => PdkValue::Bytes(conv_u8(b)),
        }
    }
}

/// Easily converting the PDK type to the original one to access its full
/// functionality.
impl<'value> From<PdkValue<'value>> for Value<'value> {
    fn from(original: PdkValue<'value>) -> Self {
        match original {
            // No conversion needed; `StaticNode` implements `StableAbi`
            PdkValue::Static(s) => Value::Static(s),
            // This conversion is cheap
            PdkValue::String(s) => Value::String(conv_str_inv(s)),
            // This unfortunately requires iterating the array
            PdkValue::Array(a) => {
                let a = a.into_iter().map(Into::into).collect();
                Value::Array(a)
            }
            // This unfortunately requires iterating the map and a new
            // allocation
            PdkValue::Object(m) => {
                // Note that `into_inner` is only necessary for `RBox`'s case,
                // because `Box` has magic that makes it possible to move
                // instead of borrow when dereferencing. For more information,
                // look for `DerefMove`.
                // TODO: call properly after merge of:
                // https://github.com/rodrimati1992/abi_stable_crates/pull/74/files
                let m: RHashMap<_, _> = RBox::into_inner(m);
                let m = m
                    .into_iter()
                    .map(|Tuple2(k, v)| (conv_str_inv(k), v.into()))
                    .collect();
                Value::Object(Box::new(m))
            }
            // This conversion is cheap
            PdkValue::Bytes(b) => Value::Bytes(conv_u8_inv(b)),
        }
    }
}
