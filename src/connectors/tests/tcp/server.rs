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

use crate::connectors::tests::{free_port, ConnectorHarness};
use crate::errors::Result;
// use async_std::{io::WriteExt, prelude::*};
use std::{
    io::{Read, Write},
    net::TcpStream,
};
use tremor_common::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_value::{literal, prelude::*, Value};
use value_trait::Builder;

#[tokio::test]
async fn server_event_routing() -> Result<()> {
    dbg!(num_cpus::get());
    let _ = env_logger::try_init();

    let free_port = free_port::find_free_tcp_port().await?;

    let server_addr = format!("127.0.0.1:{}", free_port);

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["lines"],
      "config": {
        "url": format!("tcp://127.0.0.1:{free_port}"),
        "buf_size": 4096
      }
    });
    dbg!();
    let harness = ConnectorHarness::new("tcp_server", &defn).await?;
    dbg!();
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");
    dbg!();
    harness.start().await?;
    dbg!();
    harness.wait_for_connected().await?;
    dbg!();
    // connect 2 client sockets
    let mut socket1 = TcpStream::connect(&server_addr)?;
    let mut socket2 = TcpStream::connect(&server_addr)?;
    dbg!();
    socket1.write_all("snot\n".as_bytes())?;
    let event = out_pipeline.get_event().await?;
    dbg!(&event);
    let (_data, meta) = event.data.parts();
    dbg!();
    let tcp_server_meta = meta.get("tcp_server");
    assert_eq!(Some(false), tcp_server_meta.get_bool("tls"));

    let peer_obj = tcp_server_meta.get_object("peer").unwrap();
    assert!(peer_obj.contains_key("host"));
    assert!(peer_obj.contains_key("port"));
    dbg!();
    // lets send an event and route it via metadata to socket 1
    let meta = literal!({
        "tcp_server": {
            "peer": {
                "host": peer_obj.get("host").unwrap().clone_static(),
                "port": peer_obj.get("port").unwrap().clone_static()
            }
        }
    });
    dbg!();
    let event1 = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(event1, IN).await?;
    dbg!();
    let mut buf = vec![0_u8; 8192];
    let bytes_read = socket1.read(&mut buf)?;
    dbg!();
    let data = &buf[0..bytes_read];
    dbg!();
    assert_eq!("badger", &String::from_utf8_lossy(data));
    dbg!();
    debug!("Received event 1 via socket1");
    dbg!();

    // send something to socket 2
    socket2.write_all("carfuffle\n".as_bytes())?;

    let event = out_pipeline.get_event().await?;
    // send an event and route it via eventid to socket 2
    let mut id2 = EventId::default();
    id2.track(&event.id);
    let event2 = Event {
        id: id2,
        data: (Value::String("fleek".into()), Value::object()).into(),
        ..Event::default()
    };
    dbg!();

    harness.send_to_sink(event2, IN).await?;
    let bytes_read = socket2.read(&mut buf).unwrap();
    let data = &buf[0..bytes_read];
    assert_eq!("fleek", &String::from_utf8_lossy(data));
    debug!("Received event 2 via socket1");

    dbg!();

    //cleanup
    let (_out, err) = harness.stop().await?;
    dbg!();

    assert!(err.is_empty());
    Ok(())
}
