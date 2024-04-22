// Copyright 2021-2024, The Tremor Team
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

//! Tremor AWS connectors

#![deny(warnings)]
#![deny(missing_docs)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

use auth::AWSAuth;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_types::{region::Region, SdkConfig};
use serde::Deserialize;
use tremor_common::url::{HttpsDefaults, Url};
use tremor_connectors::ConnectorBuilder;

/// AWS S3 connector
pub mod s3;

/// AWS authentication
pub(crate) mod auth;

/// builtin connector types
#[must_use]
pub fn builtin_connector_types() -> Vec<Box<dyn ConnectorBuilder + 'static>> {
    vec![
        Box::<s3::streamer::Builder>::default(),
        Box::<s3::reader::Builder>::default(),
    ]
}

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct EndpointConfig {
    pub(crate) aws_region: Option<String>,
    pub(crate) url: Option<Url<HttpsDefaults>>,

    #[serde(default = "Default::default")]
    auth: AWSAuth,

    /// Enable path-style access
    /// So e.g. creating a bucket is done using:
    ///
    /// PUT http://<host>:<port>/<bucket>
    ///
    /// instead of
    ///
    /// PUT http://<bucket>.<host>:<port>/
    ///
    /// Set this to `true` for accessing s3 compatible backends
    /// that do only support path style access, like e.g. minio.
    /// Defaults to `true` for backward compatibility.
    #[serde(default = "tremor_common::default_true")]
    pub(crate) path_style_access: bool,
}

pub(crate) async fn make_config(config: &EndpointConfig) -> SdkConfig {
    let region_provider =
        RegionProviderChain::first_try(config.aws_region.clone().map(Region::new))
            .or_default_provider();

    let region: Option<Region> = region_provider.region().await;

    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).region(region);

    let sdk_config = auth::resolve(config, sdk_config);

    sdk_config.load().await
}

#[cfg(test)]
mod tests {}
