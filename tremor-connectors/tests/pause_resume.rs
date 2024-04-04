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

use log::debug;
#[allow(unused_imports)]
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket},
};
use tremor_connectors::{
    harness::{
        free_port::{find_free_tcp_port, find_free_udp_port},
        Harness,
    },
    impls::{tcp, udp},
};
use tremor_system::{connector::source, instance::State};
use tremor_value::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn udp_pause_resume() -> anyhow::Result<()> {
    let free_port = find_free_udp_port().await?;

    let server_addr = format!("127.0.0.1:{free_port}");

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["separate"],
      "config": {
          "url": format!("udp://127.0.0.1:{free_port}"),
          "buf_size": 4096
      }
    });

    let mut harness = Harness::new("test", &udp::server::Builder::default(), &defn).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    // connect client socket
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket.connect(&server_addr).await?;
    // send data
    let data: &str = "Foo\nBar\nBaz\nSnot\nBadger";
    socket.send(data.as_bytes()).await?;
    // expect data being received
    for expected in data.split('\n').take(4) {
        let event = harness.out()?.get_event().await?;
        let content = event.data.suffix().value().as_str();
        assert_eq!(Some(expected), content);
    }
    // pause connector
    harness.pause().await?;
    harness.wait_for_state(State::Paused).await?;
    // ensure the source has applied the state change
    let (tx, rx) = oneshot::channel();
    harness.send_to_source(source::Msg::Synchronize(tx))?;
    rx.await?;
    // send some more data
    let data2 = "Connectors\nsuck\nwho\nthe\nhell\ncame\nup\nwith\nthat\nshit\n";
    socket.send(data2.as_bytes()).await?;

    // ensure nothing is received (pause is actually doing the right thing)
    let res = harness
        .out()?
        .expect_no_event_for(std::time::Duration::from_secs(1))
        .await;
    assert!(res.is_ok(), "We got an event during pause: {res:?}");
    // resume connector
    harness.resume().await?;
    harness.wait_for_state(State::Running).await?;

    // ensure the source has applied the state change
    let (tx, rx) = oneshot::channel();
    harness.send_to_source(source::Msg::Synchronize(tx))?;
    rx.await?;
    // receive the data sent during pause
    // first line, continueing the stuff from last send
    assert_eq!(
        Some(
            format!(
                "{}{}",
                data.split('\n').last().unwrap_or_default(),
                data2.split('\n').next().unwrap_or_default()
            )
            .as_str()
        ),
        harness
            .out()?
            .get_event()
            .await?
            .data
            .suffix()
            .value()
            .as_str()
    );
    for expected in data2[..data2.len() - 1].split('\n').skip(1) {
        debug!("expecting '{}'", expected);
        let event = harness.out()?.get_event().await?;
        let content = event.data.suffix().value().as_str();
        debug!("got '{:?}'", content);
        assert_eq!(Some(expected), content);
    }

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_server_pause_resume() -> anyhow::Result<()> {
    let free_port = find_free_tcp_port().await?;

    let server_addr = format!("127.0.0.1:{free_port}");

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["separate"],
      "config": {
          "url": format!("tcp://127.0.0.1:{free_port}"),
          "buf_size": 4096
      }
    });
    let mut harness = Harness::new("test", &tcp::server::Builder::default(), &defn).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    debug!("Connected.");
    // connect client socket
    let mut socket = TcpStream::connect(&server_addr).await?;
    // send data
    let data: &str = "Foo\nBar\nBaz\nSnot\nBadger";
    socket.write_all(data.as_bytes()).await?;
    // expect data being received, the last item is not received yet
    for expected in data.split('\n').take(4) {
        debug!("expecting: '{}'", expected);
        let event = harness.out()?.get_event().await?;
        let content = event.data.suffix().value().as_str();
        debug!("received '{:?}'", content);
        assert_eq!(Some(expected), content);
    }
    // pause connector
    harness.pause().await?;
    harness.wait_for_state(State::Paused).await?;

    // ensure the source has applied the state change
    let (tx, rx) = oneshot::channel();
    harness.send_to_source(source::Msg::Synchronize(tx))?;
    rx.await?;

    // send some more data
    let data2 = "Connectors\nsuck\nwho\nthe\nhell\ncame\nup\nwith\nthat\nshit\n";
    socket.write_all(data2.as_bytes()).await?;

    // ensure nothing is received (pause is actually doing the right thing)
    assert!(harness
        .out()?
        .expect_no_event_for(Duration::from_millis(500))
        .await
        .is_ok());
    // resume connector
    harness.resume().await?;
    harness.wait_for_state(State::Running).await?;

    // ensure the source has applied the state change
    let (tx, rx) = oneshot::channel();
    harness.send_to_source(source::Msg::Synchronize(tx))?;
    rx.await?;

    // receive the data sent during pause
    assert_eq!(
        Some(
            format!(
                "{}{}",
                data.split('\n').last().unwrap_or_default(),
                data2.split('\n').next().unwrap_or_default()
            )
            .as_str()
        ),
        harness
            .out()?
            .get_event()
            .await?
            .data
            .suffix()
            .value()
            .as_str()
    );
    drop(socket); // closing the socket, ensuring the last bits are flushed from preprocessors etc
    for expected in data2[..data2.len() - 1].split('\n').skip(1) {
        debug!("expecting: '{}'", expected);
        let event = harness.out()?.get_event().await?;
        let content = event.data.suffix().value().as_str();
        debug!("received '{:?}'", content);
        assert_eq!(Some(expected), content);
    }
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
