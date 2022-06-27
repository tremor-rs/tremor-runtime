use abi_stable::std_types::{RCowSlice, RCowStr};

/// The error type used for the PDK, from `abi_stable`
pub type RError = abi_stable::std_types::SendRBoxError;
/// The result type used for the PDK, from `abi_stable`
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;

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
