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

use async_std::{channel::{bounded, Receiver, Sender, TryRecvError}, net::TcpStream, path::Path, task};
use async_tls::TlsConnector;
use async_tungstenite::{WebSocketStream, client_async, tungstenite::{accept, stream::MaybeTlsStream, Message, WebSocket}};
use futures::{SinkExt};
use http::Response;
use rustls::ClientConfig;
use std::{net::SocketAddr, sync::Arc, thread, time::Duration};
use tremor_common::url::ports::IN;
use tremor_pipeline::{Event, EventId};
use tremor_value::{literal, Value};
use value_trait::ValueAccess;

use async_std::{net::TcpListener, prelude::FutureExt};
use connectors::ConnectorHarness;
use tremor_runtime::errors::Result;

fn setup_for_tls() -> Result<()> {
    use std::process::Command;

    let cmd = Command::new("./tests/refresh_tls_cert.sh").spawn()?;
    let out = cmd.wait_with_output()?;
    // println!("{}", std::str::from_utf8(&out.stderr)?);
    match out.status.code() {
        Some(0) => Ok(()),
        _ => Err("Error creating tls certificate for connector_ws test".into())
    }
}

/// Find free TCP port for use in test server endpoints
async fn find_free_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await;
    let listener = match listener {
        Err(_) => return 0, // TODO error handling
        Ok(listener) => listener,
    };
    let port = match listener.local_addr().ok() {
        Some(addr) => addr.port(),
        None => return 0,
    };
    drop(listener);
    port
}

/// A minimal websocket test client harness
struct TestClient<S> {
    client: S,
    http_response: Response<()>,
}

impl TestClient<WebSocketStream<async_tls::client::TlsStream<async_std::net::TcpStream>>> {
    async fn new_tls(url: String, port: u16) -> Self {
        let mut config = ClientConfig::new();

        let cafile = Path::new("./tests/localhost.cert");
        let file = std::fs::File::open(cafile).unwrap();
        let mut pem = std::io::BufReader::new(file);
        config
            .root_store
            .add_pem_file(&mut pem)
            .expect("Unable to create configuration object.");
        use url::Url;
        let tcp_stream = TcpStream::connect(&format!("localhost:{}", port)).await.unwrap();
        let tls_connector = TlsConnector::from(Arc::new(config));
        let tls_stream = tls_connector.connect("localhost", tcp_stream).await.unwrap();
        let maybe_connect = client_async(Url::parse(&url).unwrap(), tls_stream).await;
        if let Ok((client, http_response)) = maybe_connect {
            Self {
                client,
                http_response,
            }
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

    async fn expect_text(&mut self) -> Result<String> {
        use futures::StreamExt;
        loop {
            let message = self.client.next().await;
            if let Some(Ok(Message::Text(data))) = message {
                return Ok(data)
            } else if let None = message {
                continue;
            } else {
                return Err("Unexpected message type".into())
            }
        }
    }
}

impl TestClient<WebSocket<MaybeTlsStream<std::net::TcpStream>>> {
    fn new(url: String) -> Self {
        use async_tungstenite::tungstenite::connect;
        use url::Url;

        let maybe_connect = connect(Url::parse(&url).unwrap());
        if let Ok((client, http_response)) = maybe_connect {
            Self {
                client,
                http_response,
            }
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

    fn expect_text(&mut self) -> Result<String> {
        let message = self.client.read_message().expect("Error reading message");
        if let Message::Text(data) = message {
            Ok(data)
        } else {
            Err("Unexpected message type".into())
        }
    }
}

impl<S> TestClient<S> {
    fn print_headers(&self) {
        println!("Response HTTP code: {}", self.http_response.status());
        println!("Response contains the following headers:");
        for (ref header, _value) in self.http_response.headers() {
            println!("* {}", header);
        }
    }
}

/// A minimal websocket server endpoint test harness
struct TestServer {
    endpoint: String,
    tx: Sender<Message>,
    rx: Receiver<Message>,
}

impl TestServer {
    fn new(host: &str, port: u16) -> Self {
        let (tx, rx) = bounded(128);
        Self {
            endpoint: format!("{}:{}", host, port),
            tx,
            rx,
        }
    }

    async fn handle_connection(
        sender: Sender<Message>,
        stream: std::net::TcpStream,
        addr: SocketAddr,
    ) {
        let mut ws = accept(stream).expect("Error during WS handshake sequence");

        loop {
            let msg = ws.read_message().unwrap();
            let sent = sender.send(msg).await.unwrap();
        }
    }

    fn start(&mut self) -> Result<()> {
        let endpoint = self.endpoint.clone();
        let tx = self.tx.clone();
        thread::spawn(move || {
            let acceptor = std::net::TcpListener::bind(&endpoint).unwrap();
            while let Ok((stream, addr)) = acceptor.accept() {
                task::spawn(TestServer::handle_connection(tx.clone(), stream, addr));
            }
        });

        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    async fn expect_text(&mut self) -> Result<String> {
        loop {
            match self.rx.try_recv() {
                Ok(Message::Text(data)) => return Ok(data),
                Ok(_) => continue,
                Err(TryRecvError::Empty) => continue,
                Err(_e) => return Err("Failed to receive text message".into()),
            }
        }
    }
}

#[async_std::test]
async fn connector_ws_client_bad_config() -> Result<()> {
    setup_for_tls()?;

    let connector_yaml = format!(
        r#"
id: my_ws_client
type: ws_client
codec: string
preprocessors:
  - lines
config:
  snot: "ws://127.0.0.1:8080"
  tls:
    cacert: "./tests/localhost.cert"
    domain: "localhost"
"#,
    );

    let harness = ConnectorHarness::new(connector_yaml).await;
    assert!(harness.is_err());
    Ok(())
}

#[async_std::test]
async fn connector_ws_server_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await.to_string();
    let server_addr = format!("0.0.0.0:{}", &free_port);
    let connector_yaml = format!(
        r#"
id: my_ws_server
type: ws_server
codec: json
config:
  host: 127.0.0.1
  port: {}
"#,
        free_port
    );

    let harness = ConnectorHarness::new(connector_yaml).await?;
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

    let connector_meta = meta.get("connector");
    let ws_server_meta = connector_meta.get("ws_server");
    let peer_obj = ws_server_meta.get_object("peer").unwrap();

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
        "connector": {
            "ws_server": {
                "peer": {
                    "host": peer_obj.get("host").unwrap().clone_static(),
                    "port": c1.port()?,
                }
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;
    assert_eq!("\"badger\"", c1.expect_text()?);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn connector_ws_client_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let free_port = find_free_tcp_port().await;
    let mut ts = TestServer::new("127.0.0.1", free_port);
    ts.start()?;

    let ws_client_yaml = format!(
        r#"
id: my_ws_client
type: ws_client
codec: json
config:
  url: ws://127.0.0.1:{}
"#,
        free_port
    );
    let harness = ConnectorHarness::new(ws_client_yaml).await?;
    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;

    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of tcp_server connector");

    let in_pipeline = harness
        .in_port()
        .expect("No pipeline connected to 'in' port of tcp_server connector");

    let meta = literal!({
        "connector": {
            "ws_client": {
                "peer": {
                    "host": "127.0.0.1",
                    "port": free_port,
                    "url": format!("ws://127.0.0.1:{}", free_port),
                }
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;

    assert_eq!("\"badger\"", ts.expect_text().await?);

    ts.stop()?;
    drop(ts);

    Ok(())
}

#[async_std::test]
async fn connector_wss_server_text_routing() -> Result<()> {
    let _ = env_logger::try_init();

    setup_for_tls()?;

    let free_port = find_free_tcp_port().await;
    let server_addr = format!("localhost:{}", &free_port);
    let connector_yaml = format!(
        r#"
id: my_ws_server
type: ws_server
codec: json
config:
  host: "localhost"
  port: {}
  tls:
    cert: "./tests/localhost.cert"
    key: "./tests/localhost.key"
    domain: "localhost"
"#,
        free_port
    );

    let harness = ConnectorHarness::new(connector_yaml).await?;
    let out_pipeline = harness
        .out()
        .expect("No pipeline connected to 'out' port of ws_server connector");

    harness.start().await?;
    harness.wait_for_connected(Duration::from_secs(5)).await?;
    //
    // Send from ws client to server and check received event
    //
    let mut c1 = TestClient::new_tls(format!("wss://localhost:{}/", free_port), free_port as u16).await;
    c1.send("\"Hello WebSocket Server\"").await?;

    let event = out_pipeline
        .get_event()
        .timeout(Duration::from_millis(400))
        .await??;
    let (data, meta) = event.data.parts();
    assert_eq!("Hello WebSocket Server", &data.to_string());

    let connector_meta = meta.get("connector");
    let ws_server_meta = connector_meta.get("ws_server");
    let peer_obj = ws_server_meta.get_object("peer").unwrap();

    //
    // Send from ws server to client and check received event
    //
    let meta = literal!({
        "connector": {
            "ws_server": {
                "peer": {
                    "host": peer_obj.get("host").unwrap().clone_static(),
                    "port": c1.port()?,
                }
            }
        }
    });
    let echo_back = Event {
        id: EventId::default(),
        data: (Value::String("badger".into()), meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(echo_back, IN).await?;
    assert_eq!("\"badger\"", c1.expect_text().await?);

    //cleanup
    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}
