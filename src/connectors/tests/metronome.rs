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
use crate::{connectors::impls::metronome, errors::Result};
use std::time::Duration;
use tremor_value::prelude::*;

#[allow(clippy::cast_possible_truncation)]
#[async_std::test]
async fn connector_metronome_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let defn = literal!({
      "config": {
        "interval": Duration::from_secs(1).as_nanos() as u64
      }
    });
    let epoch = tremor_common::time::nanotime();

    let harness =
        ConnectorHarness::new(function_name!(), &metronome::Builder::default(), &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'in' port of ws_server connector");

    dbg!();
    harness.start().await?;
    dbg!();
    harness.wait_for_connected().await?;
    dbg!();

    let event = out_pipeline.get_event().await?;
    let (data, _meta) = event.data.parts();

    let at = data.get_u64("ingest_ns").unwrap_or_default();
    assert!(at >= epoch);

    //cleanup
    dbg!();
    let (_out, err) = harness.stop().await?;
    dbg!();
    assert!(err.is_empty());
    Ok(())
}
