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
use super::ConnectorHarness;
use crate::{
    connectors::{impls::bench, sink::SinkMsg},
    errors::Result,
    system::{World, WorldConfig},
};
use std::{
    io::Write,
    time::{Duration, Instant},
};
use tempfile::NamedTempFile;
use tremor_common::ports::IN;
use tremor_value::prelude::*;

#[async_std::test]
async fn stop_after_events() -> Result<()> {
    let _ = env_logger::try_init();

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
    let (world, world_handle) = World::start(WorldConfig::default()).await?;
    let harness = ConnectorHarness::new_with_kill_switch(
        function_name!(),
        &bench::Builder::default(),
        &defn,
        world.kill_switch,
    )
    .await?;
    let out = harness.out().expect("No out pipeline connected");
    harness.start().await?;
    harness.wait_for_connected().await?;

    let bg_out = out.clone();
    let bg_addr = harness.addr.clone();
    let handle = async_std::task::spawn::<_, Result<()>>(async move {
        // echo pipeline
        for _ in 0..6 {
            let event = bg_out.get_event().await?;
            bg_addr
                .send_sink(SinkMsg::Event { event, port: IN })
                .await?;
        }
        Ok(())
    });

    // the bench connector should shut the world down
    world_handle.await?;
    handle.cancel().await;
    Ok(())
}

#[async_std::test]
async fn stop_after_secs() -> Result<()> {
    let _ = env_logger::try_init();

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

    let (world, world_handle) = World::start(WorldConfig::default()).await?;
    let harness = ConnectorHarness::new_with_kill_switch(
        function_name!(),
        &bench::Builder::default(),
        &defn,
        world.kill_switch,
    )
    .await?;
    let out = harness.out().expect("No out pipeline connected");
    harness.start().await?;
    harness.wait_for_connected().await?;

    let one_sec = Duration::from_secs(1);
    let start = Instant::now();
    // echo pipeline
    let bg_out = out.clone();
    let bg_addr = harness.addr.clone();
    let handle = async_std::task::spawn::<_, Result<()>>(async move {
        // echo pipeline
        while start.elapsed() < one_sec {
            let event = bg_out.get_event().await?;
            bg_addr
                .send_sink(SinkMsg::Event { event, port: IN })
                .await?;
        }
        Ok(())
    });

    // the bench connector should shut the world down
    world_handle.await?;
    info!("Flow supervisor finished");
    handle.cancel().await;
    info!("Echo pipeline finished");
    let (_out, err) = harness.stop().await?;
    info!("Harness stopped");
    assert!(err.is_empty());

    Ok(())
}
