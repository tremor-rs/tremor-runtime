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

#![cfg(feature = "integration-tests-unix-socket")]

use log::debug;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};
use tremor_connectors::harness::Harness;
use tremor_connectors::impls::unix_socket;
use tremor_system::event::{Event, EventId};
use tremor_value::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn unix_socket() -> anyhow::Result<()> {
    let temp_file = tempfile::Builder::new().tempfile()?;
    let temp_path = temp_file.into_temp_path();
    let socket_path = temp_path.to_path_buf();
    temp_path.close()?;

    let server_defn = literal!({
      "codec": "string",
      "preprocessors": ["separate"],
      "postprocessors": ["separate"],
      "config": {
          "path": socket_path.display().to_string(),
          "permissions": "=777",
          "buf_size": 4096
      }
    });
    let client_defn = literal!({
      "codec": "string",
      "preprocessors": ["separate"],
      "postprocessors": ["separate"],
      "config": {
          "path": socket_path.display().to_string(),
          "buf_size": 4096
      }
    });

    let mut server_harness = Harness::new(
        "unix_socket_server",
        &unix_socket::server::Builder::default(),
        &server_defn,
    )
    .await?;
    server_harness.start().await?;
    server_harness.wait_for_connected().await?;

    let mut client_harness = Harness::new(
        "unix_socket_client",
        &unix_socket::client::Builder::default(),
        &client_defn,
    )
    .await?;
    client_harness.start().await?;
    client_harness.wait_for_connected().await?;

    // connect 2 client sockets
    let mut socket1 = UnixStream::connect(&socket_path).await?;

    socket1.write_all("snot\n".as_bytes()).await?;
    let event = server_harness.out()?.get_event().await?;
    let (_data, meta) = event.data.parts();

    let socket1_meta = meta.get("unix_socket_server");

    let socket1_id: u64 = socket1_meta.get_u64("peer").unwrap_or_default();

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
    server_harness.send_to_sink(event1).await?;

    let mut buf = vec![0_u8; 8192];
    let bytes_read = socket1.read(&mut buf).await?;
    let data = &buf[0..bytes_read];
    assert_eq!("badger\n", &String::from_utf8_lossy(data));
    debug!("Received event 1 via socket1");

    let id = EventId::from_id(1, 1, 1);
    let event = Event {
        id: id.clone(),
        data: (Value::from("carfuffle"), Value::object()).into(),
        transactional: true,
        ..Event::default()
    };

    client_harness.send_to_sink(event).await?;
    // send something to socket 2
    let server_event = server_harness.out()?.get_event().await?;
    // send an event and route it via eventid to socket 2
    let mut id2 = EventId::default();
    id2.track(&server_event.id);
    let event2 = Event {
        id: id2,
        data: (Value::String("fleek".into()), Value::object()).into(),
        ..Event::default()
    };
    server_harness.send_to_sink(event2).await?;
    let client_event = client_harness.out()?.get_event().await?;
    assert_eq!("fleek", client_event.data.parts().0.to_string());
    debug!("Received event 2 via client");

    //cleanup
    let (_out, err) = server_harness.stop().await?;
    assert!(err.is_empty());
    let (_out, err) = client_harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
