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
use halfbrown::HashMap;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Poll, PollOpt, Ready, Token};
use serde_yaml::Value;
use std::io::{ErrorKind, Read};
use std::thread;
use std::time::Duration;

const ONRAMP: Token = Token(0);

// TODO expose this as config (but still main the buffer on stack)
const BUFFER_SIZE_BYTES: usize = 8192;
// TODO remove later. test value
//const BUFFER_SIZE_BYTES: usize = 16;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub port: u32,
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

impl OnrampImpl for Tcp {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
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

    let mut buffer = [0; BUFFER_SIZE_BYTES];

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
        let mut ingest_ns = nanotime();
        for event in events.iter() {
            match event.token() {
                ONRAMP => loop {
                    match listener.accept() {
                        Ok((stream, client_addr)) => {
                            debug!("Accepted connection from client: {}", client_addr);

                            // make a new token for the socket to keep track of the connection
                            connection_token_number += 1;
                            let token = Token(connection_token_number);

                            // register the new socket with poll
                            poll.register(&stream, token, Ready::readable(), PollOpt::edge())?;

                            connections.insert(token, stream);

                            // TODO create actors for each connection
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
                        // TODO test re-connections
                        let client_addr = stream.peer_addr()?;

                        //let mut meta = metamap! { // TODO remove. uses macro local to the crate
                        let mut meta = tremor_pipeline::metamap! {
                            "source_id" => token.0.to_string(),
                            "source_ip" => client_addr.ip().to_string(),
                            "source_port" => client_addr.port()
                        };

                        /*
                        // TODO remove, since we do this via metamap macro now
                        let mut meta = tremor_pipeline::MetaMap::new();
                        meta.insert(
                            "source_id".to_string(),
                            // TODO see if this is the best way to achieve this
                            //simd_json::OwnedValue::from(token.0.to_string()),
                            simd_json::value::owned::Value::String(token.0.to_string()),
                        );
                        meta.insert(
                            "source_ip".to_string(),
                            //simd_json::OwnedValue::from(client_addr.ip().to_string()),
                            simd_json::value::owned::Value::String(client_addr.ip().to_string()),
                        );
                        meta.insert(
                            "source_port".to_string(),
                            simd_json::OwnedValue::from(client_addr.port()),
                            //simd_json::value::owned::Value::I64(client_addr.port() as i64),
                        );
                        // TODO figure out why object insert is not working
                        //let mut test: HashMap<String, String> = HashMap::new();
                        //test.insert("num".to_string(), "42".to_string());
                        //dbg!(&test);
                        //meta.insert(
                        //    "test".to_string(),
                        //    //simd_json::OwnedValue::from(test),
                        //    simd_json::value::owned::Value::Object(test),
                        //);
                        */

                        loop {
                            match stream.read(&mut buffer) {
                                Ok(0) => {
                                    debug!(
                                        "Connection closed by client: {}",
                                        client_addr.to_string()
                                    );
                                    connections.remove(&token);
                                    break;
                                }
                                Ok(n) => {
                                    // TODO remove later
                                    trace!(
                                        "Read {} bytes: {:?}",
                                        n,
                                        String::from_utf8_lossy(&buffer[0..n])
                                    );
                                    /*
                                    send_event(
                                        &pipelines,
                                        &mut preprocessors,
                                        &mut codec,
                                        &mut ingest_ns,
                                        id,
                                        buffer[0..n].to_vec(),
                                    );
                                    */
                                    // TODO remove later. temp code for testing
                                    send_event2(
                                        &pipelines,
                                        &mut preprocessors,
                                        &mut codec,
                                        &mut ingest_ns,
                                        &mut meta,
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
        "json"
    }
}
