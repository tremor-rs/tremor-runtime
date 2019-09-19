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
use serde_yaml::Value;
use std::io::Read;
use std::net;
use std::thread;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u32,
    pub host: String,
    #[serde(default = "dflt::d_false")]
    pub is_non_blocking: bool,
    #[serde(default = "dflt::d_ttl")]
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

    info!("[TCP Onramp] listening on {}:{}", config.host, config.port);
    let endpoint = net::TcpListener::bind(format!("{}:{}", config.host, config.port))?;
    endpoint
        .set_nonblocking(config.is_non_blocking)
        .expect("cannot set non-blocking");
    // if config.ttl > 0 {
    //    endpoint.set_ttl(config.ttl).expect("cannot set ttl");
    // }

    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut id = 0;

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

        for stream in endpoint.incoming() {
            match stream {
                Ok(mut stream) => match stream.read(&mut buf) {
                    Ok(len) => {
                        if len == 0 {
                            continue;
                        }
                        send_event(
                            &pipelines,
                            &mut preprocessors,
                            &mut codec,
                            id,
                            buf[0..len].to_vec(),
                        );
                        id += 1;
                    }
                    Err(e) => error!("Failed to read data from tcp client connection: {}", e),
                },
                Err(e) => error!("Failed to onboard new tcp client connection: {}", e),
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
