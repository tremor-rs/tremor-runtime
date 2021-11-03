use abi_stable::{
    std_types::{RBox, RCow, RHashMap, RVec},
    StableAbi,
};
use value_trait::StaticNode;

/// Representation of a JSON object
pub type Object<'value> = RHashMap<RCow<'value, str>, Value<'value>>;
/// Bytes
pub type Bytes<'value> = RCow<'value, [u8]>;

/// FFI-safe `Value` type to communicate with the plugins. It's meant to be
/// converted to/from the original `tremor_value::Value` type and back so that
/// it can be passed through the plugin interface.
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

/// Easily converting the regular value into the PDK value and back.
impl<'value> From<tremor_value::Value<'value>> for Value<'value> {
    fn from(original: tremor_value::Value<'value>) -> Self {
        match original {
            // No conversion needed; `StaticNode` implements `StableAbi`
            tremor_value::Value::Static(s) => Value::Static(s),
            // This conversion is cheap
            tremor_value::Value::String(s) => Value::String(conv_str(s)),
            // This unfortunately requires iterating the array
            tremor_value::Value::Array(a) => {
                let a = a.into_iter().map(Into::into).collect();
                Value::Array(a)
            }
            // This unfortunately requires iterating the map and a new
            // allocation
            tremor_value::Value::Object(m) => {
                let m = m
                    .into_iter()
                    .map(|(k, v)| (conv_str(k), v.into()))
                    .collect();
                Value::Object(RBox::new(m))
            }
            // This conversion is cheap
            tremor_value::Value::Bytes(b) => Value::Bytes(conv_u8(b)),
        }
    }
}

/// There are no direct conversions between `beef::Cow` and `RCow`, so the type
/// has to be converted to std as the intermediate. These conversions are cheap
/// and they shouldn't be a performance issue.
fn conv_str(cow: beef::Cow<str>) -> RCow<str> {
    let cow: std::borrow::Cow<str> = cow.into();
    cow.into()
}
fn conv_u8(cow: beef::Cow<[u8]>) -> RCow<[u8]> {
    let cow: std::borrow::Cow<[u8]> = cow.into();
    cow.into()
}
