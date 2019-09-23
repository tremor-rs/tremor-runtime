// Copyright 2018-2019, Wayfair GmbH
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
use std::collections::HashMap;
use std::io::{ErrorKind, Read};
use std::thread;
use std::time::Duration;

const ONRAMP: Token = Token(0);

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub port: u32,
    pub host: String,
}

pub struct Tcp {
    pub config: Config,
}

impl OnrampImpl for Tcp {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            //let config: Config = serde_yaml::from_value(config.clone())?;
            // hacky way of getting extra info on errors here (eg: field name on type mismatches)
            // TODO see if there's another way to achieve nicer errors here
            let config: Config = serde_yaml::from_str(&serde_yaml::to_string(config)?)?;
            Ok(Box::new(Tcp { config }))
        } else {
            Err("Missing config for tcp onramp".into())
        }
    }
}

fn onramp_loop(
    rx: Receiver<OnrampMsg>,
    config: Config,
    preprocessors: Vec<String>,
    codec: String,
) -> Result<()> {
    let mut codec = codec::lookup(&codec)?;
    let mut preprocessors = make_preprocessors(&preprocessors)?;

    // Imposed Limit of a TCP payload
    let mut buffer = [0; 65535];

    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut id = 0;

    info!("[TCP Onramp] listening on {}:{}", config.host, config.port);
    let poll = Poll::new()?;

    // Start listening for incoming connections
    let server_addr = format!("{}:{}", config.host, config.port).parse()?;
    let listener = TcpListener::bind(&server_addr)?;
    poll.register(&listener, ONRAMP, Ready::readable(), PollOpt::edge())?;

    // TODO use slab instead of hashmap here
    let mut connections: HashMap<Token, TcpStream> = HashMap::new();

    // initialize the token to keep track of each incoming connection.
    let mut connection_token_number = ONRAMP.0;

    let mut events = Events::with_capacity(1024);
    loop {
        while pipelines.is_empty() {
            match rx.recv()? {
                OnrampMsg::Connect(mut ps) => pipelines.append(&mut ps),
                OnrampMsg::Disconnect { tx, .. } => {
                    let _ = tx.send(true);
                    return Ok(());
                }
            };
        }
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(_e) => error!("Crossbream receive error"),
            Ok(OnrampMsg::Connect(mut ps)) => pipelines.append(&mut ps),
            Ok(OnrampMsg::Disconnect { id, tx }) => {
                pipelines.retain(|(pipeline, _)| pipeline != &id);
                if pipelines.is_empty() {
                    let _ = tx.send(true);
                    return Ok(());
                } else {
                    let _ = tx.send(false);
                }
            }
        }

        // wait for events and then process them
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;
        for event in events.iter() {
            match event.token() {
                ONRAMP => loop {
                    match listener.accept() {
                        Ok((stream, client_addr)) => {
                            debug!("Accepted connection from client: {}", client_addr);

                            // get the token for the socket
                            connection_token_number += 1;
                            let token = Token(connection_token_number);

                            // register the new socket w/ poll
                            poll.register(&stream, token, Ready::readable(), PollOpt::edge())?;

                            connections.insert(token, stream);
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break, // end of successful accept
                        Err(e) => {
                            error!("Failed to onboard new tcp client connection: {}", e);
                            break;
                        }
                    }
                },
                token => {
                    if let Some(stream) = connections.get_mut(&token) {
                        let mut buffer_end_index = 0;
                        loop {
                            match stream.read(&mut buffer[buffer_end_index..]) {
                                Ok(0) => {
                                    debug!("Connection closed by client: {}", stream.peer_addr()?);
                                    connections.remove(&token);
                                    break;
                                }
                                Ok(n) => buffer_end_index += n,
                                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break, // end of successful read
                                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                                Err(e) => {
                                    error!("Failed to read data from tcp client connection: {}", e);
                                    break;
                                }
                            }
                        }
                        if buffer_end_index > 0 {
                            // TODO remove later
                            trace!(
                                "Read {} bytes: {}",
                                buffer_end_index,
                                String::from_utf8_lossy(&buffer[0..buffer_end_index])
                            );
                            send_event(
                                &pipelines,
                                &mut preprocessors,
                                &mut codec,
                                id,
                                buffer[0..buffer_end_index].to_vec(),
                            );
                            id += 1;
                        }
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
    fn start(&mut self, codec: String, preprocessors: Vec<String>) -> Result<OnrampAddr> {
        let (tx, rx) = bounded(0);
        let config = self.config.clone();
        thread::Builder::new()
            .name(format!("onramp-tcp-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(rx, config, preprocessors, codec) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "string"
    }
}
