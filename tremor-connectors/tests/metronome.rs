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

#![cfg(feature = "integration-tests-metronome")]

use std::time::Duration;
use tremor_connectors::harness::Harness;
use tremor_connectors::impls::metronome;
use tremor_value::prelude::*;

#[allow(clippy::cast_possible_truncation)]
#[tokio::test(flavor = "multi_thread")]
async fn connector_metronome_routing() -> anyhow::Result<()> {
    let defn = literal!({
      "config": {
        "interval": Duration::from_secs(1).as_nanos() as u64
      }
    });
    let epoch = tremor_common::time::nanotime();

    let mut harness = Harness::new("metronome", &metronome::Builder::default(), &defn).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    let event = harness.out()?.get_event().await?;
    let (data, _meta) = event.data.parts();

    let at = data.get_u64("ingest_ns").unwrap_or_default();
    assert!(at >= epoch);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
