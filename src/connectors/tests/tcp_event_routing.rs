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

use super::{ConnectorHarness, TIMEOUT};
use crate::errors::Result;
use async_std::{
    io::WriteExt,
    net::{TcpListener, TcpStream},
    prelude::*,
};
use tremor_common::url::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_value::{literal, Value};
use value_trait::{Builder, ValueAccess};

#[async_std::test]
async fn event_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener);
        port
    };

    let server_addr = format!("127.0.0.1:{}", free_port);

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["lines"],
      "config": {
        "url": format!("tcp://127.0.0.1:{free_port}"),
        "buf_size": 4096
      }
    });

    let harness = ConnectorHarness::new("tcp_server", &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");

    harness.start().await?;
    harness.wait_for_connected(None).await?;

    // connect 2 client sockets
    let mut socket1 = TcpStream::connect(&server_addr).await?;
    let mut socket2 = TcpStream::connect(&server_addr).await?;

    socket1.write_all("snot\n".as_bytes()).await?;
    let event = out_pipeline.get_event().timeout(TIMEOUT).await??;
    let (_data, meta) = event.data.parts();

    let tcp_server_meta = meta.get("tcp_server");
    assert_eq!(Some(false), tcp_server_meta.get_bool("tls"));

    let peer_obj = tcp_server_meta.get_object("peer").unwrap();
    assert!(peer_obj.contains_key("host"));
    assert!(peer_obj.contains_key("port"));

    // lets send an event and route it via metadata to socket 1
    let meta = literal!({
        "tcp_server": {
            "peer": {
                "host": peer_obj.get("host").unwrap().clone_static(),
                "port": peer_obj.get("port").unwrap().clone_static()
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
    let bytes_read = socket1.read(&mut buf).timeout(TIMEOUT).await??;
    let data = &buf[0..bytes_read];
    assert_eq!("badger", &String::from_utf8_lossy(data));
    debug!("Received event 1 via socket1");

    // send something to socket 2
    socket2.write_all("carfuffle\n".as_bytes()).await?;

    let event = out_pipeline.get_event().timeout(TIMEOUT).await??;
    // send an event and route it via eventid to socket 2
    let mut id2 = EventId::default();
    id2.track(&event.id);
    let event2 = Event {
        id: id2,
        data: (Value::String("fleek".into()), Value::object()).into(),
        ..Event::default()
    };
    harness.send_to_sink(event2, IN).await?;
    let bytes_read = socket2.read(&mut buf).timeout(TIMEOUT).await??;
    let data = &buf[0..bytes_read];
    assert_eq!("fleek", &String::from_utf8_lossy(data));
    debug!("Received event 2 via socket1");

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
