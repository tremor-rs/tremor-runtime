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
use mio::{Events, Poll, PollOpt, Ready, Token};
use serde_yaml::Value;
use std::io::{ErrorKind, Read};
use std::thread;
use std::time::Duration;

const ONRAMP: Token = Token(0);

// TODO expose this as config (would have to change buffer to be vector?)
const BUFFER_SIZE_BYTES: usize = 8192;
// TODO remove later. test value
//const BUFFER_SIZE_BYTES: usize = 16;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub port: u16,
    pub host: String,
    //#[serde(default = "dflt_bsize")]
    //pub buffer_size_bytes: usize,
}

/*
fn dflt_bsize() -> usize {
    8192 // in bytes
}
*/

impl ConfigImpl for Config {}

pub struct Tcp {
    pub config: Config,
}

impl onramp::Impl for Tcp {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for tcp onramp".into())
        }
    }
}

struct TremorTcpConnection {
    stream: TcpStream,
    origin_uri: tremor_pipeline::EventOriginUri,
    preprocessors: Preprocessors,
}

impl TremorTcpConnection {
    fn register(&self, poll: &Poll, token: Token) -> std::io::Result<()> {
        // register the socket w/ poll
        poll.register(&self.stream, token, Ready::readable(), PollOpt::edge())
    }
}

// We got to allow this because of the way that the onramp works
// by creating new instances during runtime.
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    preprocessors: Vec<String>,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut id = 0;

    info!("[TCP Onramp] listening on {}:{}", config.host, config.port);
    let poll = Poll::new()?;

    // Start listening for incoming connections
    let server_addr = format!("{}:{}", config.host, config.port).parse()?;
    let listener = TcpListener::bind(&server_addr)?;
    poll.register(&listener, ONRAMP, Ready::readable(), PollOpt::edge())?;

    // temporary buffer to keep data read from the tcp socket
    let mut buffer = [0; BUFFER_SIZE_BYTES];

    // initializing with a single None entry, since we match the indices of this
    // vector with mio event tokens and we use 0 for the ONRAMP token
    let mut connections: Vec<Option<TremorTcpConnection>> = vec![None];

    // to keep track of tokens that are returned for re-use (after connection is terminated)
    let mut returned_tokens: Vec<usize> = vec![];

    let mut events = Events::with_capacity(1024);
    loop {
        while pipelines.is_empty() {
            match rx.recv()? {
                onramp::Msg::Connect(ps) => {
                    for p in &ps {
                        if p.0 == *METRICS_PIPELINE {
                            metrics_reporter.set_metrics_pipeline(p.clone());
                        } else {
                            pipelines.push(p.clone());
                        }
                    }
                }
                onramp::Msg::Disconnect { tx, .. } => {
                    tx.send(true)?;
                    return Ok(());
                }
            };
        }
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(_e) => error!("Crossbream receive error"),
            Ok(onramp::Msg::Connect(mut ps)) => pipelines.append(&mut ps),
            Ok(onramp::Msg::Disconnect { id, tx }) => {
                pipelines.retain(|(pipeline, _)| pipeline != &id);
                if pipelines.is_empty() {
                    tx.send(true)?;
                    return Ok(());
                } else {
                    tx.send(false)?;
                }
            }
        }

        // wait for events and then process them
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;
        let mut ingest_ns = nanotime();
        for event in events.iter() {
            match event.token() {
                ONRAMP => loop {
                    match listener.accept() {
                        Ok((stream, client_addr)) => {
                            debug!("Accepted connection from client: {}", client_addr);

                            let origin_uri = tremor_pipeline::EventOriginUri {
                                scheme: "tremor-tcp".to_string(),
                                host: client_addr.ip().to_string(),
                                port: Some(client_addr.port()),
                                // TODO also add token_num here?
                                path: vec![config.port.to_string()], // captures server port
                            };

                            let tcp_connection = TremorTcpConnection {
                                stream,
                                origin_uri,
                                preprocessors: make_preprocessors(&preprocessors)?,
                            };

                            // if there are any returned tokens, use it to keep track of the
                            // connection. otherwise create a new one.
                            if let Some(token_num) = returned_tokens.pop() {
                                trace!(
                                    "Tracking connection with returned token number: {}",
                                    token_num
                                );
                                tcp_connection.register(&poll, Token(token_num))?;
                                connections[token_num] = Some(tcp_connection);
                            } else {
                                let token_num = connections.len();
                                trace!("Tracking connection with new token number: {}", token_num);
                                tcp_connection.register(&poll, Token(token_num))?;
                                connections.push(Some(tcp_connection));
                            };
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break, // end of successful accept
                        Err(e) => {
                            error!("Failed to onboard new tcp client connection: {}", e);
                            break;
                        }
                    }
                },
                token => {
                    if let Some(TremorTcpConnection {
                        ref mut stream,
                        ref origin_uri,
                        ref mut preprocessors,
                    }) = connections[token.0]
                    {
                        loop {
                            match stream.read(&mut buffer) {
                                Ok(0) => {
                                    // TODO test re-connections
                                    debug!(
                                        "Connection closed by client: {}",
                                        origin_uri.host_port()
                                    );
                                    connections[token.0] = None;

                                    // release the token for re-use. ensures that we don't run out of
                                    // tokens (eg: if we were to just keep incrementing the token number)
                                    returned_tokens.push(token.0);
                                    trace!("Returned token number for reuse: {}", token.0);

                                    break;
                                }
                                Ok(n) => {
                                    // TODO remove later
                                    trace!(
                                        "Read {} bytes: {:?}",
                                        n,
                                        String::from_utf8_lossy(&buffer[0..n])
                                    );
                                    send_event(
                                        &pipelines,
                                        preprocessors,
                                        &mut codec,
                                        &mut metrics_reporter,
                                        &mut ingest_ns,
                                        origin_uri,
                                        id,
                                        buffer[0..n].to_vec(),
                                    );
                                    // TODO should we bumping up this id on every partial read too?
                                    id += 1;
                                }
                                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break, // end of successful read
                                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue, // will continue read
                                Err(e) => {
                                    error!("Failed to read data from tcp client connection: {}", e);
                                    break;
                                }
                            }
                        } // end of read
                    } else {
                        error!(
                            "Failed to retrieve tcp client connection for token: {}",
                            token.0
                        );
                    }
                }
            }
        }
    }
}

impl Onramp for Tcp {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = bounded(0);
        let config = self.config.clone();
        let preprocessors = preprocessors.to_vec();
        let codec = codec::lookup(codec)?;
        thread::Builder::new()
            .name(format!("onramp-tcp-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(&rx, &config, preprocessors, codec, metrics_reporter) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
