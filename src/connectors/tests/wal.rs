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
// See the License for the specific language governing permissions and
// limitations under the License.
use super::ConnectorHarness;
use crate::{connectors::impls::wal, errors::Result};
use std::time::Duration;
use tremor_common::{
    ids::{Id, SourceId},
    ports::IN,
};
use tremor_pipeline::{CbAction, Event, EventIdGenerator};
use tremor_value::{literal, prelude::*, Value};

#[async_std::test]
async fn wal() -> Result<()> {
    let _ = env_logger::try_init();
    let temp_dir = tempfile::Builder::new().tempdir()?;
    let temp_path = temp_dir.into_path();

    let config = literal!({
        "config": {
            "path": temp_path.display().to_string(),
            "chunk_size": 1024,
            "max_chunks": 100
        }
    });
    let harness =
        ConnectorHarness::new(function_name!(), &wal::Builder::default(), &config).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    let out = harness.out().expect("No pipeline connected to WAL out.");
    let in_pipe = harness
        .get_pipe(IN)
        .expect("No pipeline connected to WAL in.");

    let source_id = SourceId::new(1);
    let mut id_gen = EventIdGenerator::new(source_id);
    let value = Value::from(42_u64);
    let meta = Value::object();
    let first_id = id_gen.next_id();
    let event = Event {
        id: first_id.clone(),
        data: (value, meta).into(),
        transactional: false,
        ..Event::default()
    };
    harness.send_to_sink(event, IN).await?;
    let event = out.get_event().await?;
    // event is now transactional
    let ack_id = event.id.clone();
    assert!(event.transactional);
    assert_eq!(&Value::from(42_u64), event.data.suffix().value());

    // send another event without acking
    let another_id = id_gen.next_id();
    let event = Event {
        id: another_id.clone(),
        data: (Value::from("snot"), Value::object()).into(),
        transactional: true,
        ..Event::default()
    };
    harness.send_to_sink(event, IN).await?;

    // check that we got an ack for the event
    let cf = in_pipe.get_contraflow().await?;
    assert_eq!(CbAction::Ack, cf.cb);
    assert!(
        cf.id.is_tracking(&another_id),
        "{:?} does not track {:?}",
        cf.id,
        another_id
    );

    // now we get the next event
    let event = out.get_event().await?;
    assert!(event.transactional);
    assert_eq!(&Value::from("snot"), event.data.suffix().value());

    harness
        .send_contraflow(CbAction::Ack, ack_id.clone())
        .await?;
    async_std::task::sleep(Duration::from_secs(2)).await;

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());

    // start harness again with same config, expect the second event to be re-emitted
    let harness =
        ConnectorHarness::new(function_name!(), &wal::Builder::default(), &config).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    let out = harness.out().expect("No pipeline connected to WAL out.");

    // now we get the next one
    let event = out.get_event().await?;
    assert!(event.transactional);
    assert_eq!(&Value::from("snot"), event.data.suffix().value());

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());

    Ok(())
}
