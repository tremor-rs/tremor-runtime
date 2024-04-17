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

use crate::EchoServer;
use std::time::Duration;
use tokio::net::lookup_host;
use tremor_connectors::{harness::Harness, impls::tcp};
use tremor_connectors_test_helpers::{free_port, setup_for_tls};
use tremor_system::{
    controlplane::CbAction,
    event::{Event, EventId},
};
use tremor_value::{literal, Value};
use value_trait::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn tls_client() -> anyhow::Result<()> {
    setup_for_tls();
    tcp_client_test(true).await
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_client() -> anyhow::Result<()> {
    tcp_client_test(false).await
}

async fn tcp_client_test(use_tls: bool) -> anyhow::Result<()> {
    let free_port = free_port::find_free_tcp_port().await?;

    let server_addr = format!("localhost:{free_port}");

    // simple echo server
    let mut echo_server = EchoServer::new(server_addr.clone(), use_tls);
    echo_server.run()?;
    let tls_config = if use_tls {
        literal!({
            "cafile": "./tests/localhost.cert",
            "domain": "localhost"
        })
    } else {
        Value::from(false)
    };

    let config = literal!({
        "reconnect": {
           "retry": {
               "interval_ms": 500,
               "growth_rate": 2.0,
               "max_retries": 10
           }
        },
        "codec": {"name": "json", "config": {"mode": "sorted"}},
        "preprocessors": ["separate"],
        "postprocessors": ["separate"],
        "config": {
            "url": server_addr,
            "socket_options": {
                "TCP_NODELAY": true
            },
            "buf_size": 1024,
            "tls": tls_config
        }
    });
    let mut connector = Harness::new("test", &tcp::client::Builder::default(), &config).await?;
    connector.start().await?;
    connector.wait_for_connected().await?;
    connector.consume_initial_sink_contraflow().await?;

    let id = EventId::from_id(1, 1, 1);
    let event = Event {
        id: id.clone(),
        data: (Value::from("snot badger"), Value::object()).into(),
        transactional: true,
        ..Event::default()
    };
    connector.send_to_sink(event).await?;
    let response = connector.out()?.get_event().await?;
    let localhost_ip = lookup_host(("localhost", 0))
        .await?
        .next()
        .expect("Expected an ip")
        .ip()
        .to_string();
    assert_eq!(Some("snot badger"), response.data.suffix().value().as_str());
    assert_eq!(
        &literal!({
            "tcp_client": {
                "tls": use_tls,
                "peer": {
                    "host": localhost_ip,
                    "port": free_port
                }
            }
        }),
        response.data.suffix().meta()
    );

    // check for ack for transactional event
    let cf = connector.get_pipe("in")?.get_contraflow().await?;
    assert_eq!(CbAction::Ack, cf.cb);
    assert_eq!(id, cf.id);

    // stop server to see how is behaves when sending to closed port
    echo_server.stop().await?;

    // check that we will fail writing eventually after server close
    let id = EventId::from_id(1, 1, 2);
    let event = Event {
        id: id.clone(),
        data: (Value::from(42.24), Value::object()).into(),
        transactional: true,
        ..Event::default()
    };
    connector.send_to_sink(event.clone()).await?;
    let mut cf = connector.get_pipe("in")?.get_contraflow().await?;
    while matches!(cf.cb, CbAction::Ack) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        connector.send_to_sink(event.clone()).await?;
        cf = connector.get_pipe("in")?.get_contraflow().await?;
    }
    assert_eq!(CbAction::Fail, cf.cb);
    assert_eq!(id, cf.id);

    // as the connection is closed we expect a CB close some time after the fail
    let cf = connector.get_pipe("in")?.get_contraflow().await?;
    assert_eq!(CbAction::Trigger, cf.cb);

    let (out, err) = connector.stop().await?;
    assert!(out.is_empty());
    assert!(err.is_empty());

    Ok(())
}
