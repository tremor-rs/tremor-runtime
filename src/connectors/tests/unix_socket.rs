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
use crate::errors::Result;
use async_std::os::unix::net::UnixStream;
use async_std::prelude::*;
use std::time::Duration;
use tremor_common::url::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_value::{literal, Value};
use value_trait::{Builder, ValueAccess};

#[async_std::test]
async fn connector_unix_socket_event_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let temp_file = tempfile::Builder::new().tempfile()?;
    let temp_path = temp_file.into_temp_path();
    let socket_path = temp_path.to_path_buf();
    temp_path.close()?;

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["lines"],
      "config": {
          "path": socket_path.display().to_string(),
          "permissions": "=777",
          "buf_size": 4096
      }
    });

    let harness = ConnectorHarness::new("unix_socket_server", &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of unix_socket_server connector");

    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;

    // connect 2 client sockets
    let mut socket1 = UnixStream::connect(&socket_path).await?;
    let mut socket2 = UnixStream::connect(&socket_path).await?;

    socket1.write_all("snot\n".as_bytes()).await?;
    let event = out_pipeline
        .get_event()
        .timeout(Duration::from_millis(400))
        .await??;
    let (_data, meta) = event.data.parts();

    let socket1_meta = meta.get("unix_socket_server");

    let socket1_id: u64 = socket1_meta.get_u64("peer").unwrap();

    // lets send an event and route it via metadata to socket 1
    let meta = literal!({
        "unix_socket_server": {
            "peer": socket1_id
        }
    });
    let event1 = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(event1, IN).await?;

    let mut buf = vec![0_u8; 8192];
    let bytes_read = socket1
        .read(&mut buf)
        .timeout(Duration::from_millis(500))
        .await??;
    let data = &buf[0..bytes_read];
    assert_eq!("badger", &String::from_utf8_lossy(data));
    debug!("Received event 1 via socket1");

    // send something to socket 2
    socket2.write_all("carfuffle\n".as_bytes()).await?;

    let event = out_pipeline
        .get_event()
        .timeout(Duration::from_millis(400))
        .await??;
    // send an event and route it via eventid to socket 2
    let mut id2 = EventId::default();
    id2.track(&event.id);
    let event2 = Event {
        id: id2,
        data: (Value::String("fleek".into()), Value::object()).into(),
        ..Event::default()
    };
    harness.send_to_sink(event2, IN).await?;
    let bytes_read = socket2
        .read(&mut buf)
        .timeout(Duration::from_millis(500))
        .await??;
    let data = &buf[0..bytes_read];
    assert_eq!("fleek", &String::from_utf8_lossy(data));
    debug!("Received event 2 via socket1");

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
