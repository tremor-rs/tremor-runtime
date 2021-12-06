//! Similarly to [`PdkValue`], the types defined in this module are only meant
//! to be used temporarily for the plugin interface. They can be converted to
//! their original types for full functionality, and back to the PDK version in
//! order to pass them to the runtime.
//!
//! [`PdkValue`]: [`tremor_value::pdk::PdkValue`]

use crate::{EventPayload, ValueAndMeta};

use std::pin::Pin;

use abi_stable::{
    std_types::{RArc, RVec},
    StableAbi,
};
use tremor_value::pdk::PdkValue;

/// Temporary type to represent [`ValueAndMeta`] in the PDK interface. Refer to
/// the [`crate::pdk`] top-level documentation for more information.
///
/// [`ValueAndMeta`]: [`crate::ValueAndMeta`]
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct PdkValueAndMeta<'event> {
    v: PdkValue<'event>,
    m: PdkValue<'event>,
}

/// Easily converting the original type to the PDK one to pass it through the
/// FFI boundary.
impl<'event> From<ValueAndMeta<'event>> for PdkValueAndMeta<'event> {
    fn from(original: ValueAndMeta<'event>) -> Self {
        PdkValueAndMeta {
            v: original.v.into(),
            m: original.m.into(),
        }
    }
}

/// Easily converting the PDK type to the original one to access its full
/// functionality.
impl<'event> From<PdkValueAndMeta<'event>> for ValueAndMeta<'event> {
    fn from(original: PdkValueAndMeta<'event>) -> Self {
        ValueAndMeta {
            v: original.v.into(),
            m: original.m.into(),
        }
    }
}

/// Temporary type to represent [`EventPayload`] in the PDK interface. Refer to
/// the [`crate::pdk`] top-level documentation for more information.
///
/// [`EventPayload`]: [`crate::EventPayload`]
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct PdkEventPayload {
    /// The vector of raw input values
    raw: RVec<RArc<Pin<RVec<u8>>>>,
    data: PdkValueAndMeta<'static>,
}

/// Easily converting the original type to the PDK one to pass it through the
/// FFI boundary.
impl From<EventPayload> for PdkEventPayload {
    fn from(original: EventPayload) -> Self {
        let raw = original
            .raw
            .into_iter()
            .map(|x| {
                // FIXME: this conversion could probably be simpler
                let x: RArc<Pin<RVec<u8>>> = RArc::new(Pin::new((**x).into()));
                x
            })
            .collect();
        PdkEventPayload {
            raw,
            data: original.data.into(),
        }
    }
}

/// Easily converting the PDK type to the original one to access its full
/// functionality.
impl From<PdkEventPayload> for EventPayload {
    fn from(original: PdkEventPayload) -> Self {
        EventPayload {
            // Note that there is no conversion for the raw field because it's a
            // self-referential type, and modifying its data would be unsound.
            raw: original.raw,
            data: original.data.into(),
        }
    }
}
