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
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token};
use serde_yaml::Value;
use std::io::ErrorKind;
use std::thread;
use std::time::Duration;

const ONRAMP: Token = Token(0);

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    pub host: String,
}

impl ConfigImpl for Config {}

pub struct Udp {
    pub config: Config,
}

impl onramp::Impl for Udp {
    fn from_config(_id: &TremorURL, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

fn onramp_loop(
    uid: u64,
    rx: &Receiver<onramp::Msg>,
    config: &Config,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut metrics_reporter: RampReporter,
) -> Result<()> {
    // Limit of a UDP package
    let mut buf = [0; 65535];

    let mut pipelines: Vec<(TremorURL, pipeline::Addr)> = Vec::new();
    let mut id = 0;

    let mut origin_uri = tremor_pipeline::EventOriginUri {
        uid,
        scheme: "tremor-udp".to_string(),
        host: String::default(),
        port: None,
        path: vec![config.port.to_string()], // captures receive port
    };

    info!("[UDP Onramp] listening on {}:{}", config.host, config.port);
    let mut poll = Poll::new()?;

    let addr = format!("{}:{}", config.host, config.port).parse()?;
    let mut socket = UdpSocket::bind(addr)?;
    poll.registry()
        .register(&mut socket, ONRAMP, Interest::READABLE)?;

    let mut events = Events::with_capacity(1024);
    loop {
        match task::block_on(handle_pipelines(
            false,
            &rx,
            &mut pipelines,
            &mut metrics_reporter,
        ))? {
            PipeHandlerResult::Retry => continue,
            PipeHandlerResult::Terminate => return Ok(()),
            _ => (), // fixme .unwrap()
        }

        poll.poll(&mut events, Some(Duration::from_millis(100)))?;
        for _event in events.iter() {
            loop {
                // WARNING:
                // Since we are using an **edge** tiggered poll we need to loop
                // here since we might have more data in the socket and only
                // get a new poll event if we run out of buffers and it would block.
                // This has one important side effect, this loop might NEVER finish
                // if the buffer stays full enough to never block.
                match task::block_on(handle_pipelines(
                    false,
                    &rx,
                    &mut pipelines,
                    &mut metrics_reporter,
                ))? {
                    PipeHandlerResult::Retry => continue,
                    PipeHandlerResult::Terminate => return Ok(()),
                    _ => (), // fixme .unwrap()
                }

                let mut ingest_ns = nanotime();

                match socket.recv_from(&mut buf) {
                    Ok((n, sender_addr)) => {
                        // TODO add a method in origin_uri for changes like this?
                        origin_uri.host = sender_addr.ip().to_string();
                        origin_uri.port = Some(sender_addr.port());
                        send_event(
                            &pipelines,
                            &mut preprocessors,
                            &mut codec,
                            &mut metrics_reporter,
                            &mut ingest_ns,
                            &origin_uri,
                            id,
                            buf[0..n].to_vec(),
                        );
                        id += 1;
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::WouldBlock {
                            break;
                        } else {
                            return Err(e.into());
                        }
                    }
                }
            }
        }
    }
}
#[async_trait::async_trait]
impl Onramp for Udp {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let (tx, rx) = channel(1);
        let config = self.config.clone();
        let codec = codec::lookup(codec)?;
        let preprocessors = make_preprocessors(&preprocessors)?;
        thread::Builder::new()
            .name(format!("onramp-udp-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(
                    onramp_uid,
                    &rx,
                    &config,
                    preprocessors,
                    codec,
                    metrics_reporter,
                ) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "string"
    }
}
