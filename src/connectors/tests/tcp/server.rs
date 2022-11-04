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

use std::time::Duration;

use crate::connectors::impls::tcp;
use crate::connectors::tests::{free_port, ConnectorHarness};
use crate::errors::Result;
use async_std::{io::WriteExt, net::TcpStream, prelude::*};
use tremor_common::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_value::{literal, prelude::*, Value};
use value_trait::Builder;

#[async_std::test]
async fn server_event_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = free_port::find_free_tcp_port().await?;

    let server_addr = format!("127.0.0.1:{}", free_port);

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["separate"],
      "config": {
        "url": format!("tcp://127.0.0.1:{free_port}"),
        "buf_size": 4096
      }
    });
    let harness =
        ConnectorHarness::new(function_name!(), &tcp::server::Builder::default(), &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");
    harness.start().await?;
    harness.wait_for_connected().await?;
    // connect 2 client sockets
    let mut socket1 = TcpStream::connect(&server_addr).await?;
    let mut socket2 = TcpStream::connect(&server_addr).await?;
    socket1.write_all("snot\n".as_bytes()).await?;
    let event = out_pipeline.get_event().await?;
    let (_data, meta) = event.data.parts();
    let tcp_server_meta = meta.get("tcp_server");
    assert_eq!(Some(false), tcp_server_meta.get_bool("tls"));

    let peer = tcp_server_meta.get("peer");
    assert!(peer.contains_key("host"));
    assert!(peer.contains_key("port"));
    // lets send an event and route it via metadata to socket 1
    let meta = literal!({
        "tcp_server": {
            "peer": {
                "host": peer.get("host").map(Value::clone_static),
                "port": peer.get("port").map(Value::clone_static)
            }
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
        .timeout(Duration::from_secs(2))
        .await??;
    let data = &buf[0..bytes_read];
    assert_eq!("badger", &String::from_utf8_lossy(data));
    debug!("Received event 1 via socket1");

    // send something to socket 2
    socket2.write_all("carfuffle\n".as_bytes()).await?;

    let event = out_pipeline.get_event().await?;
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
        .timeout(Duration::from_secs(5))
        .await??;
    let data = &buf[0..bytes_read];
    assert_eq!("fleek", &String::from_utf8_lossy(data));
    debug!("Received event 2 via socket1");

    //cleanup
    let (_out, err) = harness.stop().await?;

    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn client_disconnect() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = free_port::find_free_tcp_port().await?;

    let server_addr = format!("127.0.0.1:{}", free_port);

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["separate"],
      "config": {
        "url": format!("tcp://127.0.0.1:{free_port}"),
        "buf_size": 4096
      }
    });
    let harness =
        ConnectorHarness::new(function_name!(), &tcp::server::Builder::default(), &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");
    harness.start().await?;
    harness.wait_for_connected().await?;

    let num_events = 129_usize;
    for i in 0..num_events {
        debug!("{i}");
        let mut socket = TcpStream::connect(&server_addr).await?;
        let msg = format!("snot{i}\n");
        socket.write_all(msg.as_bytes()).await?;

        let event = out_pipeline
            .get_event()
            .timeout(Duration::from_secs(1))
            .await??;
        let (data, _meta) = event.data.parts();
        assert_eq!(&Value::from(msg.trim_end()), data);

        // the sink needs to clear out the incoming queue for managing channels
        // so we simulate the tick here
        harness.signal_tick_to_sink().await?;
    }
    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
