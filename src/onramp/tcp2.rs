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

use crate::dflt;
use crate::onramp::prelude::*;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Poll, PollOpt, Ready, Token};
use serde_yaml::Value;
use std::collections::HashMap;
use std::io::{ErrorKind, Read};
use std::thread;
use std::time::Duration;

// TODO can share same token as udp onramp?
const ONRAMP: Token = Token(0);

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub port: u32,
    pub host: String,
    #[serde(default = "dflt::d_ttl")]
    // TODO implement. also add a delimiter option?
    pub ttl: u32,
}

pub struct Tcp {
    pub config: Config,
}

impl OnrampImpl for Tcp {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            //let config: Config = serde_yaml::from_value(config.clone())?;
            // hacky way of getting extra info on errors here (eg: field name on type mismatches)
            // TODO see if there's another way to achieve what we want here
            let config: Config = serde_yaml::from_str(&serde_yaml::to_string(config)?)?;
            Ok(Box::new(Tcp { config }))
        } else {
            Err("Missing config for tcp onramp".into())
        }
    }
}

// TODO better way to manage this
fn next(current: &mut Token) -> Token {
    let next = current.0;
    current.0 += 1;
    Token(next)
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
    let mut buf = [0; 65535];

    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut id = 0;

    info!("[TCP Onramp] listening on {}:{}", config.host, config.port);
    let poll = Poll::new()?;

    // Start listening for incoming connections
    // TODO verify that socket is non-blocking here. also set ttl?
    let server_addr = format!("{}:{}", config.host, config.port).parse()?;
    let listener = TcpListener::bind(&server_addr)?;
    poll.register(&listener, ONRAMP, Ready::readable(), PollOpt::edge())?;

    // Unique token for each incoming connection.
    let mut unique_token = Token(ONRAMP.0 + 1);
    let mut connections: HashMap<Token, TcpStream> = HashMap::new();

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

        // TODO right value for duration?
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;
        for event in events.iter() {
            match event.token() {
                ONRAMP => match listener.accept() {
                    Ok((stream, client_addr)) => {
                        debug!("Accepted connection from: {}", client_addr);

                        // TODO another way to handle tokens?
                        let token = next(&mut unique_token);

                        // register the new socket w/ poll
                        poll.register(&stream, token, Ready::readable(), PollOpt::edge())?;

                        connections.insert(token, stream);
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::WouldBlock {
                            // TODO remove later
                            //println!("Got would block (accept)");
                            break;
                        } else {
                            error!("Failed to onboard new tcp client connection: {}", e);
                            //return Err(e.into());
                        }
                    }
                },
                token => {
                    loop {
                        match connections.get_mut(&token) {
                            Some(stream) => match stream.read(&mut buf) {
                                Ok(0) => {
                                    // TODO add client info
                                    debug!("Connection closed by client");
                                    connections.remove(&token);
                                    break;
                                }
                                Ok(n) => {
                                    // TODO remove later
                                    //println!("Read {} bytes", n);
                                    //println!("{}", String::from_utf8_lossy(&buf));

                                    send_event(
                                        &pipelines,
                                        &mut preprocessors,
                                        &mut codec,
                                        id,
                                        buf[0..n].to_vec(),
                                    );
                                    id += 1;
                                }
                                Err(e) => match e.kind() {
                                    ErrorKind::WouldBlock => {
                                        // TODO remove later
                                        //println!("Got would block (stream read)");
                                        break;
                                    }
                                    ErrorKind::Interrupted => {
                                        // TODO remove later
                                        //println!("Got interrupt (stream read)");
                                        continue;
                                    }
                                    _ => {
                                        error!("Failed to read data from tcp client connection: {}", e);
                                        //return Err(e.into());
                                    }
                                },
                            },
                            None => error!("Failed to retrieve tcp client connection for token: {}", token.0)
                        }
                    }
                }
                // TODO look into this
                //_ => unreachable!(),
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
