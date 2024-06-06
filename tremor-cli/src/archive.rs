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

use tremor_common::asy::file;
use tremor_runtime::system::{World, WorldConfig};
use tremor_value::Value;

use crate::{cli::ArchiveCommand, errors::Result};

impl ArchiveCommand {
    /// Run the cluster command
    pub(crate) async fn run(self) -> Result<()> {
        match self {
            ArchiveCommand::Package {
                name,
                out,
                entrypoint,
            } => {
                let mut out = file::open(&out).await?;
                tremor_archive::package(&mut out, &entrypoint, name.clone()).await?;
            }
            ArchiveCommand::Run {
                archive,
                flow,
                config,
            } => {
                let world_config = WorldConfig {
                    debug_connectors: true,
                };
                let (world, handle) = World::start(world_config).await?;

                let config = config
                    .map(|f| tremor_common::file::read(&f))
                    .transpose()?
                    .map(|mut c| simd_json::from_slice::<Value>(&mut c).map(Value::into_static))
                    .transpose()?;
                let flow = flow.unwrap_or_else(|| "main".to_string());
                let mut archive = tremor_common::asy::file::open(&archive).await?;

                tremor_runtime::load_archive(&world, &mut archive, &flow, config).await?;
                handle.await??;
            }
        }
        Ok(())
    }
}
