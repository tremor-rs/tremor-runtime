// Copyright 2020-2024, The Tremor Team
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

//! Tremor runtime

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

#[macro_use]
pub(crate) mod macros;
/// Tremor runtime configuration
pub mod config;
/// Tremor runtime errors
pub mod errors;
/// Tremor function library
pub mod functions;

/// pipelines
pub mod pipeline;

/// Tremor runtime system
pub mod system;

/// Tremor runtime version tools
pub mod version;

use system::Runtime;
use tremor_script::highlighter::Dumb as ToStringHighlighter;

/// Operator Config
pub type OpConfig = tremor_value::Value<'static>;

pub(crate) mod channel;

/// registering builtin and debug connector types
///
/// # Errors
///  * If a builtin connector couldn't be registered
pub(crate) async fn register_builtin_connector_types(runtime: &Runtime) -> anyhow::Result<()> {
    for builder in tremor_connectors::builtin_connector_types() {
        runtime.register_connector(builder).await?;
    }
    #[cfg(feature = "connector-gcp")]
    for builder in tremor_connectors_gcp::builtin_connector_types() {
        runtime.register_connector(builder).await?;
    }
    #[cfg(feature = "connector-aws")]
    for builder in tremor_connectors_aws::builtin_connector_types() {
        runtime.register_connector(builder).await?;
    }
    #[cfg(feature = "connector-azure")]
    for builder in tremor_connectors_azure::builtin_connector_types() {
        runtime.register_connector(builder).await?;
    }
    #[cfg(feature = "connector-otel")]
    for builder in tremor_connectors_otel::builtin_connector_types() {
        runtime.register_connector(builder).await?;
    }

    Ok(())
}

/// Logs but ignores an error
#[macro_export]
#[doc(hidden)]
macro_rules! log_error {
    ($maybe_error:expr,  $($args:tt)+) => (
        if let Err(e) = $maybe_error {
            error!($($args)+, e = e);
            true
        } else {
            false
        }
    )
}

#[cfg(test)]
mod tests {
    use tremor_system::killswitch::ShutdownMode;

    use super::*;
    use std::io::Write;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_troy_file() -> crate::errors::Result<()> {
        let (runtime, handle) = Runtime::builder()
            .default_include_connectors()
            .build()
            .await?;
        let troy_file = tempfile::NamedTempFile::new()?;
        troy_file.as_file().write_all(
            r"
        define flow my_flow
        flow
            define pipeline foo
            pipeline
                select event from in into out;
            end;
        end;
        "
            .as_bytes(),
        )?;
        troy_file.as_file().flush()?;
        let path = troy_file.path().display().to_string();
        let num_deploys = runtime.load_troy_file(&path).await?;
        assert_eq!(0, num_deploys);
        runtime.stop(ShutdownMode::Graceful).await?;
        handle.abort();
        troy_file.close()?;
        Ok(())
    }
}
