//! This module implements a FFI-safe `Value` type to communicate with the
//! plugins.

use abi_stable::{
    std_types::{RBox, RCow, RHashMap, RVec},
    StableAbi,
};
use value_trait::StaticNode;

/// Representation of a JSON object
pub type Object<'value> = RHashMap<RCow<'value, str>, Value<'value>>;
/// Bytes
pub type Bytes<'value> = RCow<'value, [u8]>;

#[repr(C)]
#[derive(StableAbi)]
pub enum Value<'value> {
    /// Static values
    Static(StaticNode),
    /// string type
    String(RCow<'value, str>),
    /// array type
    Array(RVec<Value<'value>>),
    /// object type
    Object(RBox<Object<'value>>),
    /// A binary type
    Bytes(Bytes<'value>),
}

/// Easy conversions from the regular value to the PDK value
impl<'value> From<tremor_value::Value<'value>> for Value<'value> {
    fn from(original: tremor_value::Value) -> Self {
        match original {
            tremor_value::Value::Static(s) => Value::Static(s.into()),
            tremor_value::Value::String(s) => Value::String(s.into()),
            tremor_value::Value::Array(a) => Value::Array(a.into()),
            tremor_value::Value::Object(m) => Value::Object(m.into()),
            tremor_value::Value::Bytes(b) => Value::Bytes(b.into()),
        }
    }
}
