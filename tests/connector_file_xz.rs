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
use connectors::ConnectorHarness;
use std::time::Duration;
use tremor_runtime::errors::Result;
use tremor_value::literal;
use value_trait::ValueAccess;

#[macro_use]
extern crate log;

#[async_std::test]
async fn file_connector_xz() -> Result<()> {
    let _ = env_logger::try_init();

    let input_path = Path::new(file!())
        .parent()
        .unwrap()
        .join("data")
        .join("input.xz");
    let defn = literal!({
        "codec": "string",
        "preprocessors": ["lines"],
        "config": {
            "path": input_path.display().to_string(),
            "mode": "read"
        }
    });

    let harness = ConnectorHarness::new("file", defn).await?;
    let out = harness.out().expect("No out pipeline");
    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;

    let event = out.get_event().await?;
    assert_eq!(1, event.len());
    let value = event.data.suffix().value();
    let meta = event.data.suffix().meta();

    // value
    assert_eq!("snot", value.as_str().unwrap());
    // meta
    assert_eq!(
        literal!({
            "file": {
                "path": "tests/data/input.xz"
            }
        }),
        meta
    );

    let event2 = out.get_event().await?;
    assert_eq!(1, event2.len());
    let data = event2.data.suffix().value();
    assert_eq!("badger", data.as_str().unwrap());

    let (out_events, err_events) = harness.stop().await?;
    assert!(
        out_events.is_empty(),
        "got some events on OUT port: {:?}",
        err_events
    );

    assert!(
        err_events.is_empty(),
        "got some events on ERR port: {:?}",
        err_events
    );

    Ok(())
}
