// Copyright 2022, The Tremor Team
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
// See the License for the specific language governing perm
#![cfg(feature = "integration-tests-bench")]

use log::{error, info};
use std::{io::Write, time::Duration};
use tempfile::NamedTempFile;
use tokio::{sync::mpsc::channel, time::timeout};
use tremor_common::ports::IN;
use tremor_connectors::{harness::Harness, impls::bench};
use tremor_system::killswitch::KillSwitch;
use tremor_value::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn stop_after_events() -> anyhow::Result<()> {
    let mut file = NamedTempFile::new()?;
    file.write_all(b"{}\n")?;
    file.write_all(b"\"snot\"\n")?;
    file.write_all(b"\"badger\"\n")?;
    file.flush()?;
    let path = file.into_temp_path();

    let defn = literal!({
      "codec": "binary",
      "config": {
        "path": path.display().to_string(),
        "iters": 2
      }
    });
    // let (world, world_handle) = World::start(mWorldConfig::default()).await?;
    let (kill_tx, mut kill_rx) = channel(1);
    let kill_switch = KillSwitch::new(kill_tx);
    let mut harness =
        Harness::new_with_kill_switch("bench", &bench::Builder::default(), &defn, kill_switch)
            .await?;

    harness.start().await?;
    harness.wait_for_connected().await?;

    let handle = tokio::task::spawn(async move {
        // echo pipeline
        for _ in 0..6 {
            let event = harness.out()?.get_event().await?;
            harness.send_to_sink(event, IN).await?;
        }
        anyhow::Ok(())
    });

    // the bench connector should shut the world down
    kill_rx.recv().await.expect("failed to recv");
    handle.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_after_secs() -> anyhow::Result<()> {
    let mut file = NamedTempFile::new()?;
    file.write_all(b"{}\n")?;
    file.write_all(b"\"snot\"\n")?;
    file.write_all(b"\"badger\"\n")?;
    file.flush()?;
    let path = file.into_temp_path();

    let defn = literal!({
      "codec": "string",
      "config": {
        "path": path.display().to_string(),
        "stop_after_secs": 1
      }
    });

    let (tx, mut rx) = channel(1);
    let kill_switch = KillSwitch::new(tx);
    let mut harness =
        Harness::new_with_kill_switch("bench", &bench::Builder::default(), &defn, kill_switch)
            .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    // echo pipeline
    let handle = tokio::task::spawn(async move {
        // echo pipeline
        loop {
            let event = match harness.out()?.get_event().await {
                Ok(r) => r,
                Err(e) => return anyhow::Result::<()>::Err(e),
            };
            if let Err(e) = harness.send_to_sink(event, IN).await {
                error!("Error sending event to sink: {e}");
            }
        }
    });

    // the bench connector should trigger the kill switch
    let two_secs = Duration::from_secs(2);
    let msg = timeout(two_secs, rx.recv()).await?.expect("failed to recv");
    assert!(matches!(msg, tremor_system::killswitch::Msg::Stop));
    info!("Flow supervisor finished");
    info!("Harness stopped");
    handle.abort(); // stopping the pipeline after the connector to ensure it is draining the source
    info!("Echo pipeline finished");

    Ok(())
}
