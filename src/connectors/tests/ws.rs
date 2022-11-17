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

use super::{free_port::find_free_tcp_port, setup_for_tls, ConnectorHarness};
use crate::connectors::impls::ws;
use crate::connectors::{impls::ws::WsDefaults, utils::url::Url};
use crate::errors::{Error, Result, ResultExt};
use async_std::{
    channel::{bounded, Receiver, Sender, TryRecvError},
    net::{TcpListener, TcpStream},
    path::Path,
    prelude::StreamExt,
    task,
};
use async_tls::TlsConnector;
use async_tungstenite::{
    accept_async, client_async,
    tungstenite::{stream::MaybeTlsStream, Message, WebSocket},
    WebSocketStream,
};
use futures::SinkExt;
use rustls::ClientConfig;
use std::time::{Duration, Instant};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tremor_common::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_value::{literal, prelude::*, Value};
use tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};

/// A minimal websocket test client harness
struct TestClient<S> {
    client: S,
}

#[derive(Debug, PartialEq)]
enum ExpectMessage {
    Text(String),
    Binary(Vec<u8>),
    Unexpected(Message),
}

impl TestClient<WebSocketStream<async_tls::client::TlsStream<async_std::net::TcpStream>>> {
    async fn new_tls(url: &str, port: u16) -> Result<Self> {
        let mut config = ClientConfig::new();

        let cafile = Path::new("./tests/localhost.cert");
        let file = std::fs::File::open(cafile)?;
        let mut pem = std::io::BufReader::new(file);
        let (certs, _) = config
            .root_store
            .add_pem_file(&mut pem)
            .map_err(|_e| Error::from("Error adding pem file to root store"))?;
        assert_eq!(1, certs);
        let tcp_stream = TcpStream::connect(("localhost", port)).await?;
        let tls_connector = TlsConnector::from(Arc::new(config));
        let tls_stream = tls_connector.connect("localhost", tcp_stream).await?;
        let (client, _http_response) = client_async(url, tls_stream).await?;

        Ok(Self { client })
    }
    async fn send(&mut self, data: &str) -> Result<()> {
        let status = self.client.send(Message::Text(data.to_string())).await;
        if status.is_err() {
            Err("Failed to send to ws server".into())
        } else {
            Ok(())
        }
    }

    fn port(&mut self) -> Result<u16> {
        Ok(self.client.get_ref().get_ref().local_addr()?.port())
    }

    async fn expect(&mut self) -> Result<ExpectMessage> {
        match self.client.next().await {
            Some(Ok(Message::Text(data))) => Ok(ExpectMessage::Text(data)),
            Some(Ok(Message::Binary(data))) => Ok(ExpectMessage::Binary(data)),
            Some(Ok(other)) => Ok(ExpectMessage::Unexpected(other)),
            Some(Err(e)) => Err(e.into()),
            None => Err("EOF".into()), // stream end
        }
    }

    async fn close(&mut self) -> Result<()> {
        info!("Closing Test client...");
        self.client.flush().await?; // ignore errors
        self.client
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "Test client closing.".into(),
            }))
            .await?;
        info!("Test client closed.");
        Ok(())
    }
}

impl TestClient<WebSocket<MaybeTlsStream<std::net::TcpStream>>> {
    fn new(url: &str) -> Result<Self> {
        use async_tungstenite::tungstenite::connect;

        let (client, _http_response) = connect(Url::<WsDefaults>::parse(url)?.url())?;
        Ok(Self { client })
    }

    #[cfg(feature = "flaky-test")]
    fn ping(&mut self) -> Result<()> {
        self.client
            .write_message(Message::Ping(vec![1, 2, 3, 4]))
            .chain_err(|| "Failed to send ping to ws server")
    }
    #[cfg(feature = "flaky-test")]
    fn pong(&mut self) -> Result<()> {
        self.client
            .write_message(Message::Pong(vec![5, 6, 7, 8]))
            .chain_err(|| "Failed to send pong to ws server")
    }
    #[cfg(feature = "flaky-test")]
    fn recv(&mut self) -> Result<Message> {
        self.client.read_message().map_err(Error::from)
    }

    fn send(&mut self, data: &str) -> Result<()> {
        self.client
            .write_message(Message::Text(data.into()))
            .chain_err(|| "Failed to send to ws server")
    }

    fn port(&mut self) -> Result<u16> {
        match self.client.get_ref() {
            MaybeTlsStream::Plain(client) => Ok(client.local_addr()?.port()),
            _otherwise => Err("Unable to retrieve local port".into()),
        }
    }

    fn expect(&mut self) -> Result<ExpectMessage> {
        match self.client.read_message() {
            Ok(Message::Text(data)) => Ok(ExpectMessage::Text(data)),
            Ok(Message::Binary(data)) => Ok(ExpectMessage::Binary(data)),
            Ok(other) => Ok(ExpectMessage::Unexpected(other)),
            Err(e) => Err(e.into()),
        }
    }

    fn close(&mut self) -> Result<()> {
        info!("Closing WS test client...");
        self.client.close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "WS Test client closing.".into(),
        }))?;
        // finish closing handshake
        self.client.write_pending()?;
        info!("WS test client closed.");
        Ok(())
    }
}

/// A minimal websocket server endpoint test harness
struct TestServer {
    endpoint: String,
    tx: Sender<Message>,
    rx: Receiver<Message>,
    stopped: Arc<AtomicBool>,
}

impl TestServer {
    fn new(host: &str, port: u16) -> Self {
        let (tx, rx) = bounded(128);
        Self {
            endpoint: format!("{}:{}", host, port),
            tx,
            rx,
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn handle_connection(
        sender: Sender<Message>,
        stream: TcpStream,
        _addr: SocketAddr,
        stopped: Arc<AtomicBool>,
    ) {
        let mut ws = accept_async(stream)
            .await
            .expect("Error during WS handshake sequence");

        while !stopped.load(Ordering::Acquire) {
            let msg = match ws.next().await {
                Some(Ok(message)) => message,
                Some(Err(_)) | None => break,
            };
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    }

    async fn start(&mut self) -> Result<()> {
        let endpoint = self.endpoint.clone();
        let tx = self.tx.clone();
        let stopped = self.stopped.clone();
        task::spawn(async move {
            let acceptor = TcpListener::bind(&endpoint)
                .await
                .expect("Could not start test server");
            while let (Ok((stream, addr)), false) =
                (acceptor.accept().await, stopped.load(Ordering::Acquire))
            {
                task::spawn(TestServer::handle_connection(
                    tx.clone(),
                    stream,
                    addr,
                    stopped.clone(),
                ));
            }
            info!("Test Server stopped.");
        });

        Ok(())
    }

    fn stop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        info!("Stopping test server...");
    }

    fn expect(&mut self) -> Result<ExpectMessage> {
        loop {
            match self.rx.try_recv() {
                Ok(Message::Text(data)) => return Ok(ExpectMessage::Text(data)),
                Ok(Message::Binary(data)) => return Ok(ExpectMessage::Binary(data)),
                Ok(other) => return Ok(ExpectMessage::Unexpected(other)),
                Err(TryRecvError::Empty) => continue,
                Err(_e) => return Err("Failed to receive text message".into()),
            }
        }
    }
}

#[async_std::test]
async fn ws_client_bad_config() -> Result<()> {
    setup_for_tls();

    let defn = literal!({
      "codec": "string",
      "config": {
          "snot": "ws://127.0.0.1:8080",
          "tls": {
              "cafile": "./tests/localhost.cert",
              "domain": "localhost"
          }
      }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &ws::client::Builder::default(), &defn).await;
    assert!(harness.is_err());
    Ok(())
}

#[async_std::test]
async fn ws_server_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await?;
    let url = format!("ws://localhost:{free_port}");
    info!("url: {url}");
    let defn = literal!({
      "codec": "json",
      "config": {
        "url": url.clone(),
        "backlog": 64,
        "socket_options": {
            "TCP_NODELAY": true,
            "SO_REUSEPORT": false
        }
      }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &ws::server::Builder::default(), &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected().await?;

    let start = Instant::now();
    let timeout = Duration::from_secs(30);
    let mut c1 = loop {
        match TestClient::new(&format!("localhost:{free_port}")) {
            Err(e) => {
                if start.elapsed() > timeout {
                    return Err(format!(
                        "Timeout waiting for the ws server to start listening: {e}."
                    )
                    .into());
                }
                async_std::task::sleep(Duration::from_secs(1)).await;
            }
            Ok(client) => {
                break client;
            }
        }
    };

    //
    // Send from ws client to server and check received event
    //
    c1.send("\"Hello WebSocket Server\"")?;

    let event = out_pipeline.get_event().await?;
    let (data, meta) = event.data.parts();
    assert_eq!("Hello WebSocket Server", &data.to_string());

    let ws_server_meta = meta.get("ws_server");
    let peer_obj = ws_server_meta.get("peer");

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
        "ws_server": {
            "peer": {
                "host": peer_obj.get("host").map(Value::clone_static),
                "port": c1.port()?,
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;
    assert_eq!(ExpectMessage::Text("\"badger\"".into()), c1.expect()?);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    c1.close()?;
    Ok(())
}

#[async_std::test]
async fn ws_client_binary_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await?;
    let mut ts = TestServer::new("127.0.0.1", free_port);
    ts.start().await?;

    let defn = literal!({
      "codec": "json",
      "config": {
          "url": format!("ws://127.0.0.1:{}", free_port),
          "socket_options": {} // enforcing defaults during serialization
      }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &ws::client::Builder::default(), &defn).await?;
    harness.start().await?;
    harness.wait_for_connected().await?;

    let _out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");

    let _in_pipeline = harness
        .in_port()
        .expect("No pipeline connected to 'in' port of tcp_server connector");

    let meta = literal!({
        "binary": true,
        "ws_client": {
            "peer": {
                "host": "127.0.0.1",
                "port": free_port,
                "url": format!("ws://127.0.0.1:{}", free_port),
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;

    let data: Vec<u8> = br#""badger""#.to_vec();
    assert_eq!(ExpectMessage::Binary(data), ts.expect()?);

    ts.stop();
    drop(ts);

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn ws_client_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await?;
    let mut ts = TestServer::new("127.0.0.1", free_port);
    ts.start().await?;

    let defn = literal!({
      "codec": "json",
      "config": {
          "url": format!("ws://127.0.0.1:{}", free_port),
      }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &ws::client::Builder::default(), &defn).await?;

    harness.start().await?;
    harness.wait_for_connected().await?;

    let _out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");

    let _in_pipeline = harness
        .in_port()
        .expect("No pipeline connected to 'in' port of tcp_server connector");

    let meta = literal!({
        "ws_client": {
            "peer": {
                "host": "127.0.0.1",
                "port": free_port,
                "url": format!("ws://127.0.0.1:{}", free_port),
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;

    assert_eq!(ExpectMessage::Text(r#""badger""#.to_string()), ts.expect()?);

    ts.stop();
    drop(ts);

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn wss_server_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    setup_for_tls();

    let free_port = find_free_tcp_port().await?;
    let _server_addr = format!("localhost:{}", &free_port);

    let defn = literal!({
      "codec": "json",
      "config": {
          "url": format!("wss://localhost:{free_port}"),
          "tls": {
            "cert": "./tests/localhost.cert",
            "key": "./tests/localhost.key",
            "domain": "localhost"
          }
        }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &ws::server::Builder::default(), &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected().await?;

    let start = Instant::now();
    let timeout = Duration::from_secs(30);
    let url = format!("wss://localhost:{free_port}/");

    let mut c1 = loop {
        match TestClient::new_tls(url.as_str(), free_port).await {
            Err(e) => {
                if start.elapsed() > timeout {
                    return Err(format!(
                        "Timeout waiting for the ws server to start listening: {e}."
                    )
                    .into());
                }
                async_std::task::sleep(Duration::from_secs(1)).await;
            }
            Ok(client) => {
                break client;
            }
        }
    };

    //
    // Send from ws client to server and check received event
    //
    c1.send("\"Hello WebSocket Server\"").await?;

    let event = out_pipeline.get_event().await?;
    let (data, meta) = event.data.parts();
    assert_eq!("Hello WebSocket Server", &data.to_string());

    let ws_server_meta = meta.get("ws_server");
    let peer_obj = ws_server_meta.get("peer");

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
        "ws_server": {
            "peer": {
                "host": peer_obj.get("host").map(Value::clone_static),
                "port": c1.port()?,
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;
    assert_eq!(
        ExpectMessage::Text(r#""badger""#.to_string()),
        c1.expect().await?
    );

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    c1.close().await?;

    Ok(())
}

#[async_std::test]
async fn wss_server_binary_routing() -> Result<()> {
    let _ = env_logger::try_init();

    setup_for_tls();

    let free_port = find_free_tcp_port().await?;
    let _server_addr = format!("localhost:{}", &free_port);

    let defn = literal!({
      "codec": "json",
      "config": {
        "url": format!("wss://localhost:{free_port}"),
        "tls": {
            "cert": "./tests/localhost.cert",
            "key": "./tests/localhost.key",
            "domain": "localhost"
            }
        }
    });

    let harness =
        ConnectorHarness::new(function_name!(), &ws::server::Builder::default(), &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected().await?;

    let start = Instant::now();
    let timeout = Duration::from_secs(30);
    let url = format!("wss://localhost:{free_port}/");

    let mut c1 = loop {
        match TestClient::new_tls(url.as_str(), free_port).await {
            Err(e) => {
                if start.elapsed() > timeout {
                    return Err(format!(
                        "Timeout waiting for the ws server to start listening: {e}."
                    )
                    .into());
                }
                async_std::task::sleep(Duration::from_secs(1)).await;
            }
            Ok(client) => {
                break client;
            }
        }
    };

    //
    // Send from ws client to server and check received event
    //
    c1.send("\"Hello WebSocket Server\"").await?;

    let event = out_pipeline.get_event().await?;
    let (data, meta) = event.data.parts();
    assert_eq!("Hello WebSocket Server", &data.to_string());

    let ws_server_meta = meta.get("ws_server");
    let peer_obj = ws_server_meta.get("peer");

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
            "binary": true,
            "ws_server": {
            "peer": {
                "host": peer_obj.get("host").map(Value::clone_static),
                "port": c1.port()?,
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;
    let data = br#""badger""#.to_vec();
    assert_eq!(ExpectMessage::Binary(data), c1.expect().await?);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());

    c1.close().await?;

    Ok(())
}

#[cfg(feature = "flaky-test")]
#[async_std::test]
async fn server_control_frames() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await?;
    let url = format!("ws://0.0.0.0:{free_port}");
    let defn = literal!({
      "codec": "json",
      "config": {
        "url": url.clone()
      }
    });

    let harness = ConnectorHarness::new(function_name!(), "ws_server", &defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected().await?;

    let start = Instant::now();
    let timeout = Duration::from_secs(30);
    let mut c1 = loop {
        match TestClient::new(url.as_str()) {
            Err(e) => {
                if start.elapsed() > timeout {
                    return Err(format!(
                        "Timeout waiting for the ws server to start listening: {e}."
                    )
                    .into());
                }
                async_std::task::sleep(Duration::from_secs(1)).await;
            }
            Ok(client) => {
                break client;
            }
        }
    };

    // check ping
    c1.ping()?;
    let pong = c1.recv()?;
    assert_eq!(Message::Pong(vec![1, 2, 3, 4]), pong);

    // we ignore pings, they shouldn't get through as events
    assert!(out_pipeline
        .expect_no_event_for(Duration::from_secs(1))
        .await
        .is_ok());

    // check pong
    c1.pong()?;
    // expect no response and no event, as we ignore pong frames
    assert!(out_pipeline
        .expect_no_event_for(Duration::from_secs(1))
        .await
        .is_ok());

    // check close
    c1.close().await?;
    // expect a close frame as response
    let close = c1.recv()?;
    assert_eq!(
        Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "WS Test client closing.".into()
        })),
        close
    );
    // send a signal so the server sink can clean out the channel
    harness.signal_tick_to_sink().await?;

    // this should fail, as the server should close the connection
    assert!(matches!(
        c1.recv(),
        Err(Error(
            crate::errors::ErrorKind::WsError(async_tungstenite::Error::ConnectionClosed),
            _
        ))
    ));
    // expect no response and no event, the stream should have been closed though
    assert!(out_pipeline
        .expect_no_event_for(Duration::from_secs(1))
        .await
        .is_ok());

    Ok(())
}
