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
    channel::{bounded, Receiver, Sender, TryRecvError},
    net::{TcpListener, TcpStream},
    path::Path,
    prelude::{FutureExt, StreamExt},
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
use std::{
    net::SocketAddr,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Once,
    },
    time::Duration,
};
use tremor_common::url::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_value::{literal, Value};
use tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
use value_trait::ValueAccess;

use connectors::ConnectorHarness;
use tremor_runtime::errors::Result;

static TLS_SETUP: Once = Once::new();

fn setup_for_tls() {
    use std::process::Command;

    // create TLS cert and key only once at the beginning of the test execution to avoid
    // multiple threads stepping on each others toes
    TLS_SETUP.call_once(|| {
        let mut cmd = Command::new("./tests/refresh_tls_cert.sh")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Unable to spawn ./tests/refresh_tls_cert.sh");
        let out = cmd.wait().expect("Failed top refresh certs/keys");
        match out.code() {
            Some(0) => {}
            _ => panic!("Error creating tls certificate for connector_ws test"),
        }
    });
}

/// Find free TCP port for use in test server endpoints
async fn find_free_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await;
    let listener = match listener {
        Err(_) => return 65535, // TODO error handling
        Ok(listener) => listener,
    };
    let port = match listener.local_addr().ok() {
        Some(addr) => addr.port(),
        None => return 65535,
    };
    info!("free port: {}", port);
    port
}

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
    async fn new_tls(url: String, port: u16) -> Self {
        let mut config = ClientConfig::new();

        let cafile = Path::new("./tests/localhost.cert");
        let file = std::fs::File::open(cafile).unwrap();
        let mut pem = std::io::BufReader::new(file);
        let (certs, _) = config
            .root_store
            .add_pem_file(&mut pem)
            .expect("Unable to create configuration object.");
        assert_eq!(1, certs);
        use url::Url;
        let tcp_stream = TcpStream::connect(&format!("localhost:{}", port))
            .await
            .unwrap();
        let tls_connector = TlsConnector::from(Arc::new(config));
        let tls_stream = tls_connector
            .connect("localhost", tcp_stream)
            .await
            .expect("Expected to successfully connect using TLS.");
        let maybe_connect = client_async(Url::parse(&url).unwrap(), tls_stream).await;
        if let Ok((client, _http_response)) = maybe_connect {
            Self { client }
        } else {
            dbg!(&maybe_connect);
            panic!("Could not connect to server");
        }
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
        loop {
            match self.client.next().await {
                Some(Ok(Message::Text(data))) => return Ok(ExpectMessage::Text(data)),
                Some(Ok(Message::Binary(data))) => return Ok(ExpectMessage::Binary(data)),
                Some(Ok(other)) => return Ok(ExpectMessage::Unexpected(other)),
                Some(Err(e)) => return Err(e.into()),
                None => continue,
            }
        }
    }
    async fn close(&mut self) -> Result<()> {
        info!("Closing Test client...");
        let _ = self.client.flush().await; // ignore errors
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
    fn new(url: String) -> Self {
        use async_tungstenite::tungstenite::connect;
        use url::Url;

        let maybe_connect = connect(Url::parse(&url).unwrap());
        if let Ok((client, _http_response)) = maybe_connect {
            Self { client }
        } else {
            dbg!(&maybe_connect);
            panic!("Could not connect to server");
        }
    }

    fn send(&mut self, data: &str) -> Result<()> {
        let status = self.client.write_message(Message::Text(data.into()));
        if status.is_err() {
            Err("Failed to send to ws server".into())
        } else {
            Ok(())
        }
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
    async fn close(&mut self) -> Result<()> {
        info!("Closing TLS test client...");
        self.client.close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "TLS Test client closing.".into(),
        }))?;
        let _ = self.client.write_pending();
        info!("TLS test client closed.");
        Ok(())
    }
}

impl<S> TestClient<S> {
    // fn print_headers(&self) {
    //     println!("Response HTTP code: {}", self.http_response.status());
    //     println!("Response contains the following headers:");
    //     for (ref header, _value) in self.http_response.headers() {
    //         println!("* {}", header);
    //     }
    // }
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
            let msg = ws.next().await.unwrap().unwrap();
            let _sent = sender.send(msg).await.unwrap();
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

    fn stop(&mut self) -> Result<()> {
        self.stopped.store(true, Ordering::Release);
        info!("Stopping test server...");
        Ok(())
    }

    async fn expect(&mut self) -> Result<ExpectMessage> {
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
async fn connector_ws_client_bad_config() -> Result<()> {
    setup_for_tls();

    let defn = literal!({
      "codec": "string",
      "preprocessors": ["lines"],
      "config": {
          "snot": "ws://127.0.0.1:8080",
          "tls": {
              "cacert": "./tests/localhost.cert",
              "domain": "localhost"
          }
      }
    });

    let harness = ConnectorHarness::new("ws_client", defn).await;
    assert!(harness.is_err());
    Ok(())
}

#[async_std::test]
async fn connector_ws_server_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await;
    let server_addr = format!("0.0.0.0:{}", &free_port);

    let defn = literal!({
      "codec": "json",
      "config": {
          "host": "127.0.0.1",
          "port": free_port
      }
    });

    let harness = ConnectorHarness::new("ws_server", defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;

    //
    // Send from ws client to server and check received event
    //
    let mut c1 = TestClient::new(format!("ws://{}/", server_addr));
    c1.send("\"Hello WebSocket Server\"")?;

    let event = out_pipeline
        .get_event()
        .timeout(Duration::from_millis(400))
        .await??;
    let (data, meta) = event.data.parts();
    assert_eq!("Hello WebSocket Server", &data.to_string());

    let ws_server_meta = meta.get("ws_server");
    let peer_obj = ws_server_meta.get_object("peer").unwrap();

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
        "ws_server": {
            "peer": {
                "host": peer_obj.get("host").unwrap().clone_static(),
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
    c1.close().await?;
    Ok(())
}

#[async_std::test]
async fn connector_ws_client_binary_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await;
    let mut ts = TestServer::new("127.0.0.1", free_port);
    ts.start().await?;

    let defn = literal!({
      "codec": "json",
      "config": {
          "url": format!("ws://127.0.0.1:{}", free_port),
      }
    });

    let harness = ConnectorHarness::new("ws_client", defn).await?;
    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;

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

    let data: Vec<u8> = "\"badger\"".to_string().into_bytes();
    assert_eq!(ExpectMessage::Binary(data), ts.expect().await?);

    ts.stop()?;
    drop(ts);

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn connector_ws_client_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await;
    let mut ts = TestServer::new("127.0.0.1", free_port);
    ts.start().await?;

    let defn = literal!({
      "codec": "json",
      "config": {
          "url": format!("ws://127.0.0.1:{}", free_port),
      }
    });

    let harness = ConnectorHarness::new("ws_client", defn).await?;

    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;

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

    assert_eq!(
        ExpectMessage::Text("\"badger\"".to_string()),
        ts.expect().await?
    );

    ts.stop()?;
    drop(ts);

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn connector_wss_server_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    setup_for_tls();

    let free_port = find_free_tcp_port().await;
    let _server_addr = format!("localhost:{}", &free_port);

    let defn = literal!({
      "codec": "json",
      "config": {
          "host": "localhost",
          "port": free_port,
          "tls": {
            "cert": "./tests/localhost.cert",
            "key": "./tests/localhost.key",
            "domain": "localhost"
            }
        }
    });

    let harness = ConnectorHarness::new("ws_server", defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;
    //
    // Send from ws client to server and check received event
    //
    let mut c1 =
        TestClient::new_tls(format!("wss://localhost:{}/", free_port), free_port as u16).await;
    c1.send("\"Hello WebSocket Server\"").await?;

    let event = out_pipeline
        .get_event()
        .timeout(Duration::from_millis(400))
        .await??;
    let (data, meta) = event.data.parts();
    assert_eq!("Hello WebSocket Server", &data.to_string());

    let ws_server_meta = meta.get("ws_server");
    let peer_obj = ws_server_meta.get_object("peer").unwrap();

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
            "ws_server": {
            "peer": {
                "host": peer_obj.get("host").unwrap().clone_static(),
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
        ExpectMessage::Text("\"badger\"".to_string()),
        c1.expect().await?
    );

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    c1.close().await?;

    Ok(())
}

#[async_std::test]
async fn connector_wss_server_binary_routing() -> Result<()> {
    let _ = env_logger::try_init();

    setup_for_tls();

    let free_port = find_free_tcp_port().await;
    let _server_addr = format!("localhost:{}", &free_port);

    let defn = literal!({
      "codec": "json",
      "config": {
          "host": "localhost",
          "port": free_port,
          "tls": {
            "cert": "./tests/localhost.cert",
            "key": "./tests/localhost.key",
            "domain": "localhost"
            }
        }
    });

    let harness = ConnectorHarness::new("ws_server", defn).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;
    //
    // Send from ws client to server and check received event
    //
    let mut c1 =
        TestClient::new_tls(format!("wss://localhost:{}/", free_port), free_port as u16).await;
    c1.send("\"Hello WebSocket Server\"").await?;

    let event = out_pipeline
        .get_event()
        .timeout(Duration::from_millis(400))
        .await??;
    let (data, meta) = event.data.parts();
    assert_eq!("Hello WebSocket Server", &data.to_string());

    let ws_server_meta = meta.get("ws_server");
    let peer_obj = ws_server_meta.get_object("peer").unwrap();

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
            "binary": true,
            "ws_server": {
            "peer": {
                "host": peer_obj.get("host").unwrap().clone_static(),
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
    let data = "\"badger\"".to_string().into_bytes();
    assert_eq!(ExpectMessage::Binary(data), c1.expect().await?);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());

    c1.close().await?;

    Ok(())
}
