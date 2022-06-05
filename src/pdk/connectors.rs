use crate::{
    config::Connector as ConnectorConfig,
    connectors::{BoxedRawConnector, ConnectorType},
    pdk::RResult,
};

use std::fmt;

use abi_stable::{
    std_types::{ROption, RStr, RString},
    StableAbi,
};
use async_ffi::BorrowingFfiFuture;

#[repr(C)]
#[derive(StableAbi)]
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

impl fmt::Debug for ConnectorPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connector plugin '{}'", self.connector_type()())
    }
}
