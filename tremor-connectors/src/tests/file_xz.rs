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
use std::path::Path;
use tremor_value::literal;
use value_trait::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn file_connector_xz() -> Result<()> {
    let input_path = Path::new(file!())
        .parent()
        .ok_or("bad path")?
        .join("../../..")
        .join("tests")
        .join("data")
        .join("input.xz");
    let defn = literal!({
        "codec": "string",
        "preprocessors": ["separate"],
        "config": {
            "path": input_path.display().to_string(),
            "mode": "read"
        }
    });

    let mut harness =
        ConnectorHarness::new(function_name!(), &file::Builder::default(), &defn).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    let event = harness.out()?.get_event().await?;
    assert_eq!(1, event.len());
    let value = event.data.suffix().value();
    let meta = event.data.suffix().meta();

    // value
    assert_eq!(Some("snot"), value.as_str());
    // meta
    assert_eq!(
        literal!({
            "file": {
                "path": "src/connectors/tests/../../../tests/data/input.xz"
            }
        }),
        meta
    );

    let event2 = harness.out()?.get_event().await?;
    assert_eq!(1, event2.len());
    let data = event2.data.suffix().value();
    assert_eq!(Some("badger"), data.as_str());

    let (out_events, err_events) = harness.stop().await?;
    assert!(
        out_events.is_empty(),
        "got some events on OUT port: {err_events:?}",
    );

    assert!(
        err_events.is_empty(),
        "got some events on ERR port: {err_events:?}",
    );

    Ok(())
}
