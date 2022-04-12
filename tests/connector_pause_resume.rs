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

mod connectors;

#[macro_use]
extern crate log;

use async_std::{
    io::WriteExt,
    net::{TcpListener, TcpStream, UdpSocket},
    prelude::FutureExt,
};
use connectors::ConnectorHarness;
use std::time::Duration;
use tremor_runtime::{errors::Result, instance::InstanceState};
use tremor_value::prelude::*;

#[async_std::test]
async fn connector_udp_pause_resume() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let port = socket.local_addr()?.port();
        drop(socket);
        port
    };

    let server_addr = format!("127.0.0.1:{}", free_port);

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["lines"],
      "config": {
          "host": "127.0.0.1",
          "port": free_port,
          "buf_size": 4096
      }
    });

    let harness = ConnectorHarness::new("udp_server", defn).await?;

    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of udp_server");
    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;

    // connect client socket
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket.connect(&server_addr).await?;
    // send data
    let data: &str = "Foo\nBar\nBaz\nSnot\nBadger";
    socket.send(data.as_bytes()).await?;
    // expect data being received
    for expected in data.split('\n').take(4) {
        let event = out_pipeline.get_event().await?;
        let content = event.data.suffix().value().as_str().unwrap();
        assert_eq!(expected, content);
    }
    // pause connector
    harness.pause().await?;
    harness
        .wait_for_state(InstanceState::Paused, Duration::from_secs(5))
        .await?;
    // send some more data
    let data2 = "Connectors\nsuck\nwho\nthe\nhell\ncame\nup\nwith\nthat\nshit\n";
    socket.send(data2.as_bytes()).await?;

    // ensure nothing is received (pause is actually doing the right thing)
    assert!(
        async_std::future::timeout(Duration::from_millis(500), out_pipeline.get_event())
            .await
            .is_err()
    );
    // resume connector
    harness.resume().await?;
    harness
        .wait_for_state(InstanceState::Running, Duration::from_secs(5))
        .await?;
    // receive the data sent during pause
    // first line, continueing the stuff from last send
    assert_eq!(
        format!(
            "{}{}",
            data.split('\n').last().unwrap(),
            data2.split('\n').next().unwrap()
        )
        .as_str(),
        out_pipeline
            .get_event()
            .await?
            .data
            .suffix()
            .value()
            .as_str()
            .unwrap()
    );
    for expected in data2[..data2.len() - 1].split('\n').skip(1) {
        debug!("expecting '{}'", expected);
        let event = out_pipeline.get_event().await?;
        let content = event.data.suffix().value().as_str().unwrap();
        debug!("got '{}'", content);
        assert_eq!(expected, content);
    }
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn connector_tcp_server_pause_resume() -> Result<()> {
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
          "host": "127.0.0.1",
          "port": free_port,
          "buf_size": 4096
      }
    });

    let harness = ConnectorHarness::new("tcp_server", defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");
    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;
    debug!("Connected.");
    // connect client socket
    let mut socket = TcpStream::connect(&server_addr).await?;
    // send data
    let data: &str = "Foo\nBar\nBaz\nSnot\nBadger";
    socket.write_all(data.as_bytes()).await?;
    // expect data being received, the last item is not received yet
    for expected in data.split('\n').take(4) {
        debug!("expecting: '{}'", expected);
        let event = out_pipeline
            .get_event()
            .timeout(Duration::from_secs(2))
            .await??;
        let content = event.data.suffix().value().as_str().unwrap();
        debug!("received '{}'", content);
        assert_eq!(expected, content);
    }
    // pause connector
    harness.pause().await?;
    harness
        .wait_for_state(InstanceState::Paused, Duration::from_secs(5))
        .await?;
    // send some more data
    let data2 = "Connectors\nsuck\nwho\nthe\nhell\ncame\nup\nwith\nthat\nshit\n";
    socket.write_all(data2.as_bytes()).await?;

    // ensure nothing is received (pause is actually doing the right thing)
    assert!(out_pipeline
        .get_event()
        .timeout(Duration::from_millis(500))
        .await
        .is_err());
    // resume connector
    harness.resume().await?;
    harness
        .wait_for_state(InstanceState::Running, Duration::from_secs(5))
        .await?;
    // receive the data sent during pause
    assert_eq!(
        format!(
            "{}{}",
            data.split('\n').last().unwrap(),
            data2.split('\n').next().unwrap()
        )
        .as_str(),
        out_pipeline
            .get_event()
            .await?
            .data
            .suffix()
            .value()
            .as_str()
            .unwrap()
    );
    drop(socket); // closing the socket, ensuring the last bits are flushed from preprocessors etc
    for expected in data2[..data2.len() - 1].split('\n').skip(1) {
        debug!("expecting: '{}'", expected);
        let event = out_pipeline
            .get_event()
            .timeout(Duration::from_secs(2))
            .await??;
        let content = event.data.suffix().value().as_str().unwrap();
        debug!("received '{}'", content);
        assert_eq!(expected, content);
    }
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
