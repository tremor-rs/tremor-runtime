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

use super::ConnectorHarness;
use crate::{connectors::impls::file, errors::Result};
use async_std::path::Path;
use tremor_value::prelude::*;

#[async_std::test]
async fn file_connector() -> Result<()> {
    let _ = env_logger::try_init();

    let input_path = Path::new(file!())
        .parent()
        .unwrap()
        .join("data")
        .join("non_existent.txt");
    let defn = literal!({
        "codec": "string",
        "preprocessors": ["separate"],
        "config": {
            "path": input_path.display().to_string(),
            "mode": "read"
        }
    });

    let harness = ConnectorHarness::new(function_name!(), &file::Builder::default(), &defn).await?;
    assert!(harness.start().await.is_err());

    let (out_events, err_events) = harness.stop().await?;
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);
    Ok(())
}
