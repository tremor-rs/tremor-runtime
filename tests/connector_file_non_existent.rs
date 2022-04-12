// Copyright 2021, The Tremor Team
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

mod connectors;

use async_std::path::Path;
use async_std::task;
use connectors::ConnectorHarness;
use std::time::Duration;
use tremor_runtime::errors::Result;

#[macro_use]
extern crate log;

#[async_std::test]
async fn file_connector() -> Result<()> {
    let _ = env_logger::try_init();

    let input_path = Path::new(file!())
        .parent()
        .unwrap()
        .join("data")
        .join("non_existent.txt");
    let connector_yaml = format!(
        r#"
id: my_file
type: file
codec: string
preprocessors:
  - lines
config:
  path: "{}"
  mode: read
"#,
        input_path.display()
    );
    let harness = ConnectorHarness::new(connector_yaml).await?;
    assert!(harness.start().await.is_err());

    let (out_events, err_events) = harness.stop().await?;
    assert!(out_events.is_empty());
    assert!(err_events.is_empty());
    Ok(())
}
