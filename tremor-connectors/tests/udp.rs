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
#![cfg(feature = "integration-tests-udp")]

use tremor_common::ports::IN;
use tremor_connectors::harness::Harness;
use tremor_connectors::impls::udp;
use tremor_system::event::Event;
use tremor_value::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn udp_no_bind() -> anyhow::Result<()> {
    let server_defn = literal!({
      "codec": "string",
      "config": {
          "url": "127.0.0.1:4242",
      }
    });

    let mut server_harness =
        Harness::new("udp_server", &udp::server::Builder::default(), &server_defn).await?;
    server_harness.start().await?;
    server_harness.wait_for_connected().await?;

    let client_defn = literal!({
      "codec": "string",
      "config": {
          "url": "127.0.0.1:4242",
      }
    });

    let client_harness =
        Harness::new("udp_client", &udp::client::Builder::default(), &client_defn).await?;
    client_harness.start().await?;
    client_harness.wait_for_connected().await?;

    let event1 = Event {
        data: (Value::String("badger".into()), literal!({})).into(),
        ..Event::default()
    };
    client_harness.send_to_sink(event1, IN).await?;
    // send something to socket 2
    let server_event = server_harness.out()?.get_event().await?;
    // send an event and route it via eventid to socket 2

    assert_eq!(server_event.data.parts().0.as_str(), Some("badger"));

    let (_out, err) = server_harness.stop().await?;
    assert!(err.is_empty());
    let (_out, err) = client_harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn udp_bind() -> anyhow::Result<()> {
    let server_defn = literal!({
      "codec": "string",
      "config": {
          "url": "127.0.0.1:4243",
      }
    });

    let mut server_harness =
        Harness::new("udp_server", &udp::server::Builder::default(), &server_defn).await?;

    server_harness.start().await?;
    server_harness.wait_for_connected().await?;

    let client_defn = literal!({
      "codec": "string",
      "config": {
          "url": "127.0.0.1:4243",
          "bind": "127.0.0.1:4244",
      }
    });

    let client_harness =
        Harness::new("udp_client", &udp::client::Builder::default(), &client_defn).await?;
    client_harness.start().await?;
    client_harness.wait_for_connected().await?;

    let event1 = Event {
        data: (Value::String("badger".into()), literal!({})).into(),
        ..Event::default()
    };
    client_harness.send_to_sink(event1, IN).await?;
    // send something to socket 2
    let server_event = server_harness.out()?.get_event().await?;
    // send an event and route it via eventid to socket 2

    assert_eq!(server_event.data.parts().0.as_str(), Some("badger"));

    let (_out, err) = server_harness.stop().await?;
    assert!(err.is_empty());
    let (_out, err) = client_harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bind_connect_ipv4_ipv6() -> anyhow::Result<()> {
    let config = literal!({
        "codec": "string",
        "config": {
            "bind": "[::1]:12345",
            "url": "127.0.0.1:12345"
        }
    });
    let client_harness =
        Harness::new("udp_client", &udp::client::Builder::default(), &config).await?;
    let res = client_harness.start().await;
    assert!(res.is_err(), "Not an error: {res:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_ipv4() -> anyhow::Result<()> {
    let config = literal!({
        "codec": "string",
        "config": {
            "url": "127.0.0.1:12345"
        }
    });
    let client_harness =
        Harness::new("udp_client", &udp::client::Builder::default(), &config).await?;
    client_harness.start().await?;
    client_harness.wait_for_connected().await?;
    // ensure we were able to connect given the config above
    client_harness.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_ipv6() -> anyhow::Result<()> {
    let config = literal!({
        "codec": "string",
        "config": {
            "url": "[::1]:12345"
        }
    });
    let client_harness =
        Harness::new("udp_client", &udp::client::Builder::default(), &config).await?;
    client_harness.start().await?;
    client_harness.wait_for_connected().await?;
    // ensure we were able to connect given the config above
    client_harness.stop().await?;
    Ok(())
}
