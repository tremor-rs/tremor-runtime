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

#[macro_use]
extern crate log;

use connectors::ConnectorHarness;
use std::{thread::sleep, time::Duration};
use tremor_runtime::errors::Result;
use value_trait::ValueAccess;

#[async_std::test]
async fn connector_crononome_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let connector_yaml = format!(
        r#"
id: my_cron
type: crononome
config:
  entries:
    - name: test
      expr: "* * * * * * *"
      payload: 
        "snot badger"
      
"#,
    );

    let harness = ConnectorHarness::new(connector_yaml).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'in' port of ws_server connector");

    harness.start().await?;
    harness
        .wait_for_connected(Duration::from_millis(100))
        .await?;

    sleep(Duration::from_secs(1));

    let event = out_pipeline.get_event().await?;
    let (data, _meta) = event.data.parts();
    let trigger = data.get("trigger").unwrap();
    let name = trigger.get_str("name");
    assert_eq!(Some("test"), name);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
