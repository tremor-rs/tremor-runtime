// Copyright 2022, The Tremor Team and Mario Ortiz Manero
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    config::Connector as ConnectorConfig,
    connectors::{BoxedRawConnector, ConnectorType},
    system::BoxedKillSwitch,
};
use tremor_common::pdk::RResult;
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

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = ConnectorPluginRef)))]
pub struct ConnectorPlugin {
    /// the type of the connector
    pub connector_type: extern "C" fn() -> ConnectorType,

    /// create a connector from the given `id` and `config`
    ///
    /// # Errors
    ///  * If the config is invalid for the connector
    #[sabi(last_prefix_field)]
    pub from_config: for<'a> extern "C" fn(
        id: RStr<'a>,
        raw_config: &'a ConnectorConfig,
        config: &'a Value,
        kill_switch: &'a BoxedKillSwitch,
    )
        -> BorrowingFfiFuture<'a, RResult<BoxedRawConnector>>,
}

/// `ConnectorPluginRef` is the main module in this plugin declaration.
impl RootModule for ConnectorPluginRef {
    /// The name of the dynamic library
    const BASE_NAME: &'static str = "connector";
    /// The name of the library for logging and similars
    const NAME: &'static str = "connector";
    /// The version of this plugin's crate
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    /// Implements the `RootModule::root_module_statics` function, which is the
    /// only required implementation for the `RootModule` trait.
    declare_root_module_statics! {ConnectorPluginRef}
}

impl fmt::Debug for ConnectorPluginRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connector plugin '{}'", self.connector_type()())
    }
}
