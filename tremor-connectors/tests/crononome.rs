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
#![cfg(feature = "crononome-integration")]

use tremor_connectors::harness::Harness;
use tremor_connectors::impls::crononome;
use tremor_value::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn connector_crononome_routing() -> anyhow::Result<()> {
    let defn = literal!({
    "config": {
        "entries":[{
          "name": "test",
          "expr": "* * * * * * *",
          "payload": "snot badger"
        }],
      },
    });

    let mut harness = Harness::new("crononome", &crononome::Builder::default(), &defn).await?;

    harness.start().await?;
    harness.wait_for_connected().await?;

    let event = harness.out()?.get_event().await?;
    let (data, _meta) = event.data.parts();

    assert_eq!(Some("test"), data.get("trigger").get_str("name"));

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
