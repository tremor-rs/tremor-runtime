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
use crate::errors::Result;
use async_std::task;
use std::time::Duration;
use tremor_value::prelude::*;

#[async_std::test]
async fn connector_metronome_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let defn = literal!({
      "config": {
        "interval": 10000000 // millis
      }
    });
    let epoch = tremor_common::time::nanotime();

    let harness = ConnectorHarness::new("metronome", &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'in' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected(None).await?;

    task::sleep(Duration::from_secs(1)).await;

    let event = out_pipeline.get_event().await?;
    let (data, _meta) = event.data.parts();

    let at = data.get_u64("ingest_ns").unwrap();
    assert!(at >= epoch);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
