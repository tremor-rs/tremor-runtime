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
use simd_json::ValueAccess;
use std::time::Duration;
use tremor_runtime::errors::Result;
use tremor_value::literal;

#[macro_use]
extern crate log;

#[async_std::test]
async fn file_connector() -> Result<()> {
    let _ = env_logger::try_init();

    let input_path = Path::new(file!())
        .parent()
        .unwrap()
        .join("data")
        .join("input.txt");
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
    harness.start().await?;

    task::yield_now().await;
    // give it some time to read the file
    task::sleep(Duration::from_millis(100)).await;

    println!("stopping the harness...");
    let (mut out_events, err_events) = harness.stop().await?;
    // check the out and err channels
    assert_eq!(
        2,
        out_events.len(),
        "didn't receive 2 events on out, but {:?}",
        out_events
    );
    // get 1 event
    let event = out_events.remove(0);
    assert_eq!(1, event.len());
    let value = event.data.suffix().value();
    let meta = event.data.suffix().meta();

    // value
    assert_eq!("snot", value.as_str().unwrap());
    // meta
    assert_eq!(
        literal!({
            "file": {
                "path": "tests/data/input.txt"
            }
        }),
        meta
    );
    let event2 = out_events.remove(0);
    assert_eq!(1, event2.len());
    let data = event2.data.suffix().value();
    assert_eq!("badger", data.as_str().unwrap());

    assert!(
        err_events.is_empty(),
        "got some events on ERR port: {:?}",
        err_events
    );

    Ok(())
}
