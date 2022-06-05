use abi_stable::std_types::{RCowSlice, RCowStr};

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
