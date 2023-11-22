// Copyright 2020-2021, The Tremor Team
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
extern crate serde;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

#[cfg(test)]
extern crate test_case;

#[macro_use]
pub(crate) mod macros;
/// Tremor runtime configuration
pub mod config;
/// Tremor runtime errors
pub mod errors;
/// Tremor function library
pub mod functions;

pub(crate) mod primerge;

/// pipelines
pub mod pipeline;

/// Tremor connector extensions
pub mod connectors;
/// Tremor runtime system
pub mod system;
/// Utility functions
pub mod utils;
/// Tremor runtime version tools
pub mod version;

/// Instance management
pub mod instance;

/// Clustering / Consensus code
pub mod raft;

/// Metrics instance name
pub static mut INSTANCE: &str = "tremor";

pub(crate) use crate::config::Connector;
use crate::{
    errors::{Error, Result},
    system::Runtime,
};
use std::{io::Read, sync::atomic::AtomicUsize};
pub use tremor_pipeline::Event;
use tremor_script::highlighter::Dumb as ToStringHighlighter;

/// Operator Config
pub type OpConfig = tremor_value::Value<'static>;

pub(crate) mod channel;

/// Default Q Size
static QSIZE: AtomicUsize = AtomicUsize::new(128);

pub(crate) fn qsize() -> usize {
    QSIZE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Loads a Troy file
///
/// # Errors
/// Fails if the file can not be loaded
pub async fn load_troy_file(world: &Runtime, file_name: &str) -> Result<usize> {
    info!("Loading troy from {file_name}");

    let mut file = tremor_common::file::open(&file_name)?;
    let mut src = String::new();

    file.read_to_string(&mut src)
        .map_err(|e| anyhow::Error::from(e).context(format!("Could not open file {file_name}")))?;
    world.load_troy(file_name, &src).await
}

/// Logs but ignores an error
#[macro_export]
#[doc(hidden)]
macro_rules! log_error {
    ($maybe_error:expr,  $($args:tt)+) => (
        if let Err(e) = &$maybe_error {
            error!($($args)+, e = e);
            true
        } else {
            false
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::{ShutdownMode, WorldConfig};
    use std::io::Write;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_troy_file() -> Result<()> {
        let (world, handle) = Runtime::start(WorldConfig::default()).await?;
        let troy_file = tempfile::NamedTempFile::new()?;
        troy_file.as_file().write_all(
            r#"
        define flow my_flow
        flow
            define pipeline foo
            pipeline
                select event from in into out;
            end;
        end;
        "#
            .as_bytes(),
        )?;
        troy_file.as_file().flush()?;
        let path = troy_file.path().display().to_string();
        let num_deploys = load_troy_file(&world, &path).await?;
        assert_eq!(0, num_deploys);
        world.stop(ShutdownMode::Graceful).await?;
        handle.abort();
        troy_file.close()?;
        Ok(())
    }
}
