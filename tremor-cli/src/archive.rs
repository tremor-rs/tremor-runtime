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

use tokio::fs::OpenOptions;
use tremor_runtime::system::Runtime;
use tremor_value::Value;

use crate::{cli::ArchiveCommand, errors::Result};

impl ArchiveCommand {
    /// Run the cluster command
    pub(crate) async fn run(self) -> Result<()> {
        match self {
            ArchiveCommand::Package {
                name,
                out,
                input,
                entrypoint,
            } => {
                let mut out = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&out)
                    .await?;

                tremor_archive::package(&mut out, &input, name, entrypoint).await?;
            }
            ArchiveCommand::Run {
                archive,
                flow,
                config,
            } => {
                let (runtime, handle) = Runtime::builder()
                    .default_include_connectors()
                    .build()
                    .await?;

                let config = if let Some(f) = config {
                    let mut c = tremor_common::asy::file::read(&f).await?;
                    let v = simd_json::from_slice::<Value>(&mut c)?;
                    Some(v.into_static())
                } else {
                    None
                };
                let mut archive = tremor_common::asy::file::open(&archive).await?;

                runtime
                    .load_archive(&mut archive, flow.as_deref(), config)
                    .await?;
                handle.await??;
            }
        }
        Ok(())
    }
}
