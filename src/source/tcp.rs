// Copyright 2018-2020, Wayfair GmbH
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

use crate::onramp::prelude::*;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use serde_yaml::Value;
use std::io::{ErrorKind, Read};
use std::net::SocketAddr;
use std::time::Duration;
use tremor_pipeline::EventOriginUri;

const ONRAMP: Token = Token(0);

// TODO expose this as config (would have to change buffer to be vector?)
const BUFFER_SIZE_BYTES: usize = 8192;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub port: u16,
    pub host: String,
}

impl ConfigImpl for Config {}

pub struct Tcp {
    pub config: Config,
    onramp_id: TremorURL,
}

pub struct Int {
    uid: u64,
    config: Config,
    poll: Poll,
    listener: Option<TcpListener>,
    events: Events,
    event_offset: usize,
    connections: Vec<Option<TremorTcpConnection>>,

    /// to keep track of tokens that are returned for re-use (after connection is terminated)
    returned_tokens: Vec<usize>,
    new_streams: Vec<usize>,
    onramp_id: TremorURL,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tcp")
    }
}
impl Int {
    fn from_config(uid: u64, onramp_id: TremorURL, config: &Config) -> Result<Self> {
        let config = config.clone();
        let poll = Poll::new()?;
        let events = Events::with_capacity(1024);
        // initializing with a single None entry, since we match the indices of this
        // vector with mio event tokens and we use 0 for the ONRAMP token
        let connections: Vec<Option<TremorTcpConnection>> = vec![None];

        // to keep track of tokens that are returned for re-use (after connection is terminated)
        let returned_tokens: Vec<usize> = vec![];

        Ok(Self {
            uid,
            config,
            poll,
            listener: None,
            events,
            connections,
            event_offset: 0,
            returned_tokens,
            new_streams: Vec::new(),
            onramp_id,
        })
    }
}

impl onramp::Impl for Tcp {
    fn from_config(id: &TremorURL, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for tcp onramp".into())
        }
    }
}

struct TremorTcpConnection {
    stream: TcpStream,
    origin_uri: EventOriginUri,
}

impl TremorTcpConnection {
    fn register(&mut self, poll: &Poll, token: Token) -> std::io::Result<()> {
        // register the socket w/ poll
        poll.registry()
            .register(&mut self.stream, token, Interest::READABLE)
    }
}

impl Int {
    // if there are any returned tokens, use it to keep track of the
    // connection. otherwise create a new one.
    fn next_token(&mut self, mut tcp_connection: TremorTcpConnection) -> Result<usize> {
        let token_num = if let Some(token_num) = self.returned_tokens.pop() {
            trace!(
                "Tracking connection with returned token number: {}",
                token_num
            );
            token_num
        } else {
            let token_num = self.connections.len();
            trace!("Tracking connection with new token number: {}", token_num);
            token_num
        };
        tcp_connection.register(&self.poll, Token(token_num))?;
        self.connections[token_num] = Some(tcp_connection);
        Ok(token_num)
    }

    fn uri(&self, client_addr: &SocketAddr) -> EventOriginUri {
        EventOriginUri {
            uid: self.uid,
            scheme: "tremor-tcp".to_string(),
            host: client_addr.ip().to_string(),
            port: Some(client_addr.port()),
            // TODO also add token_num here?
            path: vec![self.config.port.to_string()], // captures server port
        }
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }

    #[allow(unused_variables)]
    async fn read(&mut self, id: u64) -> Result<SourceReply> {
        // temporary buffer to keep data read from the tcp socket
        let mut buffer = [0; BUFFER_SIZE_BYTES];

        if let Some(id) = self.new_streams.pop() {
            return Ok(SourceReply::StartStream(id));
        }

        if let Some(event) = self.events.iter().nth(self.event_offset) {
            self.event_offset += 1;
            match event.token() {
                ONRAMP => {
                    loop {
                        if let Some(ref mut listener) = self.listener {
                            match listener.accept() {
                                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                                // end of successful accept
                                Err(e) => {
                                    error!("Failed to onboard new tcp client connection: {}", e);
                                    return Err(e.into());
                                }
                                Ok((stream, client_addr)) => {
                                    debug!("Accepted connection from client: {}", &client_addr);

                                    let origin_uri = self.uri(&client_addr);

                                    let id = self
                                        .next_token(TremorTcpConnection { stream, origin_uri })?;

                                    self.new_streams.push(id);
                                }
                            }
                        }
                    }
                    if let Some(id) = self.new_streams.pop() {
                        Ok(SourceReply::StartStream(id))
                    } else {
                        Ok(SourceReply::Empty(10))
                    }
                }
                token => {
                    if let Some(TremorTcpConnection {
                        ref mut stream,
                        ref origin_uri,
                        ..
                    }) = self.connections[token.0]
                    {
                        let mut data = Vec::with_capacity(BUFFER_SIZE_BYTES);

                        loop {
                            match stream.read(&mut buffer) {
                                Ok(0) => {
                                    // TODO test re-connections
                                    debug!(
                                        "Connection closed by client: {}",
                                        origin_uri.host_port()
                                    );
                                    self.connections[token.0] = None;

                                    // release the token for re-use. ensures that we don't run out of
                                    // tokens (eg: if we were to just keep incrementing the token number)
                                    self.returned_tokens.push(token.0);
                                    trace!("Returned token number for reuse: {}", token.0);
                                    return Ok(SourceReply::EndStream(token.0));
                                }
                                Ok(n) => {
                                    // TODO remove later
                                    trace!(
                                        "Read {} bytes: {:?}",
                                        n,
                                        String::from_utf8_lossy(&buffer[0..n])
                                    );
                                    data.extend_from_slice(&buffer[0..n])
                                }
                                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break, // end of successful read
                                Err(ref e) if e.kind() == ErrorKind::Interrupted => break,
                                Err(e) => {
                                    error!("Failed to read data from tcp client connection: {}", e);
                                    return Err(e.into());
                                }
                            }
                        }
                        Ok(SourceReply::Data {
                            origin_uri: origin_uri.clone(),
                            data,
                            stream: token.0,
                        })
                    // end of read
                    } else {
                        error!(
                            "Failed to retrieve tcp client connection for token: {}",
                            token.0
                        );
                        Ok(SourceReply::Empty(10))
                    }
                }
            }
        } else {
            self.poll
                .poll(&mut self.events, Some(Duration::from_millis(10)))?;
            self.event_offset = 0;

            Ok(SourceReply::Empty(10))
        }
    }

    async fn init(&mut self) -> Result<SourceState> {
        let server_addr = format!("{}:{}", self.config.host, self.config.port).parse()?;
        let mut listener = TcpListener::bind(server_addr)?;
        self.poll
            .registry()
            .register(&mut listener, ONRAMP, Interest::READABLE)?;
        self.listener = Some(listener);

        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Tcp {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = Int::from_config(onramp_uid, self.onramp_id.clone(), &self.config)?;
        SourceManager::start(onramp_uid, source, codec, preprocessors, metrics_reporter).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
