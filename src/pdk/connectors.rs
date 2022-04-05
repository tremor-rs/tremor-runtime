use crate::{
    config::Connector as ConnectorConfig,
    connectors::{BoxedRawConnector, ConnectorType},
    pdk::RResult,
};
use tremor_value::Value;

use std::fmt;

use abi_stable::{
    declare_root_module_statics,
    library::RootModule,
    package_version_strings,
    sabi_types::VersionStrings,
    std_types::{ROption, RStr, RString},
    StableAbi,
};
use async_ffi::BorrowingFfiFuture;

/// This type represents a connector plugin that has been loaded with
/// `abi_stable`. It serves as a builder, making it possible to construct a
/// trait object of `RawConnector`.
///
/// Note that its interface may change heavily in the future in order to support
/// more kinds of plugins. It's also important to keep the plugin interface as
/// simple as possible, so with time this may be worked on.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix))]
pub struct ConnectorMod {
    /// the type of the connector
    pub connector_type: extern "C" fn() -> ConnectorType,

    /// create a connector from the given `id` and `config`
    ///
    /// # Errors
    ///  * If the config is invalid for the connector
    #[sabi(last_prefix_field)]
    pub from_config: for<'a> extern "C" fn(
        alias: RStr<'a>,
        config: &'a ConnectorConfig,
    )
        -> BorrowingFfiFuture<'a, RResult<BoxedRawConnector>>,
}

// Marking `MinMod` as the main module in this plugin. Note that `MinMod_Ref` is
// a pointer to the prefix of `MinMod`.
impl RootModule for ConnectorMod_Ref {
    // The name of the dynamic library
    const BASE_NAME: &'static str = "connector";
    // The name of the library for logging and similars
    const NAME: &'static str = "connector";
    // The version of this plugin's crate
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    // Implements the `RootModule::root_module_statics` function, which is the
    // only required implementation for the `RootModule` trait.
    declare_root_module_statics! {ConnectorMod_Ref}
}

impl fmt::Debug for ConnectorMod_Ref {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "reference to connector plugin '{}'",
            self.connector_type()()
        )
    }
}
