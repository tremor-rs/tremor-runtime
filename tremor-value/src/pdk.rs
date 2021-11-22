use abi_stable::{
    std_types::{RBox, RCow, RHashMap, RVec, Tuple2},
    StableAbi,
};
use value_trait::StaticNode;

/// Representation of a JSON object
pub type Object<'value> = RHashMap<RCow<'value, str>, Value<'value>>;
/// Bytes
pub type Bytes<'value> = RCow<'value, [u8]>;

/// FFI-safe `Value` type to communicate with the plugins. It's meant to be
/// converted to/from the original `crate::Value` type and back so that it can
/// can be passed through the plugin interface. Thus, no functionality is
/// implemented other than the conversion from and to the original type.
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
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

/// Easily converting the PDK value to the original one.
impl<'value> From<crate::Value<'value>> for Value<'value> {
    fn from(original: crate::Value<'value>) -> Self {
        match original {
            // No conversion needed; `StaticNode` implements `StableAbi`
            crate::Value::Static(s) => Value::Static(s),
            // This conversion is cheap
            crate::Value::String(s) => Value::String(conv_str(s)),
            // This unfortunately requires iterating the array
            crate::Value::Array(a) => {
                let a = a.into_iter().map(Into::into).collect();
                Value::Array(a)
            }
            // This unfortunately requires iterating the map and a new
            // allocation
            crate::Value::Object(m) => {
                let m: halfbrown::HashMap<_, _> = *m;
                let m = m
                    .into_iter()
                    .map(|(k, v)| (conv_str(k), v.into()))
                    .collect();
                Value::Object(RBox::new(m))
            }
            // This conversion is cheap
            crate::Value::Bytes(b) => Value::Bytes(conv_u8(b)),
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
fn conv_str_inv(cow: RCow<str>) -> beef::Cow<str> {
    let cow: std::borrow::Cow<str> = cow.into();
    cow.into()
}
fn conv_u8_inv(cow: RCow<[u8]>) -> beef::Cow<[u8]> {
    let cow: std::borrow::Cow<[u8]> = cow.into();
    cow.into()
}

/// Easily converting the original value to the PDK one.
impl<'value> From<Value<'value>> for crate::Value<'value> {
    fn from(original: Value<'value>) -> Self {
        match original {
            // No conversion needed; `StaticNode` implements `StableAbi`
            Value::Static(s) => crate::Value::Static(s),
            // This conversion is cheap
            Value::String(s) => crate::Value::String(conv_str_inv(s)),
            // This unfortunately requires iterating the array
            Value::Array(a) => {
                let a = a.into_iter().map(Into::into).collect();
                crate::Value::Array(a)
            }
            // This unfortunately requires iterating the map and a new
            // allocation
            Value::Object(m) => {
                let m = (*m).clone(); // FIXME: remove this super ugly clone
                let m = m
                    .into_iter()
                    .map(|Tuple2(k, v)| (conv_str_inv(k), v.into()))
                    .collect();
                crate::Value::Object(Box::new(m))
            }
            // This conversion is cheap
            Value::Bytes(b) => crate::Value::Bytes(conv_u8_inv(b)),
        }
    }
}
