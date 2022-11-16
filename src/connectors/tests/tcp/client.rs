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

use crate::{
    connectors::{
        impls::tcp,
        tests::{free_port, setup_for_tls, tcp::EchoServer, ConnectorHarness},
    },
    errors::Result,
};
use tremor_common::ports::IN;
use tremor_pipeline::{CbAction, Event, EventId};
use tremor_value::{literal, Value};
use value_trait::{Builder, ValueAccess};

#[async_std::test]
async fn tls_client() -> Result<()> {
    setup_for_tls();
    tcp_client_test(true).await
}

#[async_std::test]
async fn tcp_client() -> Result<()> {
    tcp_client_test(false).await
}

async fn tcp_client_test(use_tls: bool) -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = free_port::find_free_tcp_port().await?;

    let server_addr = format!("localhost:{}", free_port);

    // simple echo server
    let mut echo_server = EchoServer::new(server_addr.clone(), use_tls);
    echo_server.run().await?;
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
        "codec": "json-sorted",
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
    let connector =
        ConnectorHarness::new(function_name!(), &tcp::client::Builder::default(), &config).await?;
    let out = connector
        .out()
        .expect("No pipeline connected to tcp_client OUT port.");
    let in_pipe = connector
        .get_pipe(IN)
        .expect("No pipeline connected to tcp_client IN port");
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
    connector.send_to_sink(event, IN).await?;
    let response = out.get_event().await?;
    assert_eq!(Some("snot badger"), response.data.suffix().value().as_str());
    assert_eq!(
        &literal!({
            "tcp_client": {
                "tls": use_tls,
                "peer": {
                    "host": "localhost",
                    "port": free_port
                }
            }
        }),
        response.data.suffix().meta()
    );

    // check for ack for transactional event
    let cf = in_pipe.get_contraflow().await?;
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
    connector.send_to_sink(event.clone(), IN).await?;
    let mut cf = in_pipe.get_contraflow().await?;
    while matches!(cf.cb, CbAction::Ack) {
        async_std::task::sleep(Duration::from_millis(100)).await;
        connector.send_to_sink(event.clone(), IN).await?;
        cf = in_pipe.get_contraflow().await?;
    }
    assert_eq!(CbAction::Fail, cf.cb);
    assert_eq!(id, cf.id);

    // as the connection is closed we expect a CB close some time after the fail
    let cf = in_pipe.get_contraflow().await?;
    assert_eq!(CbAction::Trigger, cf.cb);

    let (out, err) = connector.stop().await?;
    assert!(out.is_empty());
    assert!(err.is_empty());

    Ok(())
}
