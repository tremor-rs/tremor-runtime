use crate::{
    config::Connector as ConnectorConfig,
    connectors::{BoxedRawConnector, ConnectorType},
    pdk::RResult,
};

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

#[repr(C)]
#[derive(StableAbi)]
// TODO: rename to follow PascalCase
#[sabi(kind(Prefix(prefix_ref = ConnectorPlugin_Ref)))]
pub struct ConnectorPlugin {
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

/// `ConnectorPlugin_Ref` is the main module in this plugin declaration.
impl RootModule for ConnectorPlugin_Ref {
    /// The name of the dynamic library
    const BASE_NAME: &'static str = "connector";
    /// The name of the library for logging and similars
    const NAME: &'static str = "connector";
    /// The version of this plugin's crate
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    /// Implements the `RootModule::root_module_statics` function, which is the
    /// only required implementation for the `RootModule` trait.
    declare_root_module_statics! {ConnectorPlugin_Ref}
}

impl fmt::Debug for ConnectorPlugin_Ref {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connector plugin '{}'", self.connector_type()())
    }
}
