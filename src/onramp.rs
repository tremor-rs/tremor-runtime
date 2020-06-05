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

use crate::errors::Result;
use crate::metrics::RampReporter;
use crate::pipeline;
use crate::repository::ServantId;
use crate::url::TremorURL;
use serde_yaml::Value;
use std::fmt;
mod blaster;
mod crononome;
mod file;
#[cfg(feature = "gcp")]
mod gsub;
mod kafka;
mod metronome;
mod postgres;
mod prelude;
pub mod tcp;
mod udp;
use async_std::sync::{self, channel};
use async_std::task::{self, JoinHandle};
use crossbeam_channel::Sender as CbSender;
// mod rest;
mod ws;

pub(crate) type Sender = sync::Sender<ManagerMsg>;

pub(crate) trait Impl {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>>;
}

#[derive(Clone, Debug)]
pub enum Msg {
    Connect(Vec<(TremorURL, pipeline::Addr)>),
    Disconnect { id: TremorURL, tx: CbSender<bool> },
}

pub type Addr = sync::Sender<Msg>;

pub(crate) trait Onramp: Send {
    fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<Addr>;
    fn default_codec(&self) -> &str;
}

// just a lookup
#[cfg_attr(tarpaulin, skip)]
pub(crate) fn lookup(name: &str, config: &Option<Value>) -> Result<Box<dyn Onramp>> {
    match name {
        "blaster" => blaster::Blaster::from_config(config),
        "file" => file::File::from_config(config),
        #[cfg(feature = "gcp")]
        "gsub" => gsub::GSub::from_config(config),
        "kafka" => kafka::Kafka::from_config(config),
        "postgres" => postgres::Postgres::from_config(config),
        "metronome" => metronome::Metronome::from_config(config),
        "crononome" => crononome::Crononome::from_config(config),
        "udp" => udp::Udp::from_config(config),
        "tcp" => tcp::Tcp::from_config(config),
        // "rest" => rest::Rest::from_config(config),
        "ws" => ws::Ws::from_config(config),
        _ => Err(format!("Onramp {} not known", name).into()),
    }
}

pub(crate) struct Create {
    pub id: ServantId,
    pub stream: Box<dyn Onramp>,
    pub codec: String,
    pub preprocessors: Vec<String>,
    pub metrics_reporter: RampReporter,
}

impl fmt::Debug for Create {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StartOnramp({})", self.id)
    }
}

/// This is control plane
#[allow(clippy::large_enum_variant)]
pub(crate) enum ManagerMsg {
    Create(async_std::sync::Sender<Result<Addr>>, Create),
    Stop,
}

#[derive(Debug, Default)]
pub(crate) struct Manager {
    qsize: usize,
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }
    pub fn start(self) -> (JoinHandle<bool>, Sender) {
        let (tx, rx) = channel(self.qsize);

        let h = task::spawn(async move {
            info!("Onramp manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping onramps...");
                        break;
                    }
                    Ok(ManagerMsg::Create(
                        r,
                        Create {
                            codec,
                            mut stream,
                            preprocessors,
                            metrics_reporter,
                            id,
                        },
                    )) => match stream.start(&codec, &preprocessors, metrics_reporter) {
                        Ok(addr) => {
                            info!("Onramp {} started.", id);
                            r.send(Ok(addr)).await
                        }
                        Err(e) => error!("Creating an onramp failed: {}", e),
                    },
                    Err(e) => {
                        info!("Stopping onramps... {}", e);
                        break;
                    }
                }
            }
            info!("Onramp manager stopped.");
            true
        });

        (h, tx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::Binding;
    use crate::config::MappingMap;
    use crate::repository::BindingArtefact;
    use crate::repository::PipelineArtefact;
    use crate::system;
    use crate::url::TremorURL;
    use simd_json::json;
    use std::io::Write;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::net::UdpSocket;
    use std::ops::Range;

    macro_rules! b {
        ($f:expr) => {
            async_std::task::block_on($f)
        };
    }

    #[allow(dead_code)]
    fn port_is_taken(port: u16) -> bool {
        match TcpListener::bind(format!("127.0.0.1:{}", port)) {
            Ok(_) => false,
            _ => true,
        }
    }

    fn port_is_free(port: u16) -> bool {
        match TcpListener::bind(format!("127.0.0.1:{}", port)) {
            Ok(_x) => {
                dbg!(&_x);
                true
            }
            _otherwise => {
                dbg!(&_otherwise);
                false
            }
        }
    }

    fn find_free_port(mut range: Range<u16>) -> Option<u16> {
        range.find(|port| port_is_free(*port))
    }

    #[allow(dead_code)]
    struct TcpRecorder {
        port: u16,
        listener: TcpListener,
    }

    impl TcpRecorder {
        #[allow(dead_code)]
        fn new() -> Self {
            let port = find_free_port(9000..10000).expect("could not find free port");
            dbg!(&port);
            TcpRecorder {
                port,
                listener: TcpListener::bind(format!("localhost:{}", port))
                    .expect("could not bind listener"),
            }
        }
    }

    struct TcpInjector {
        stream: TcpStream,
    }

    impl TcpInjector {
        fn with_port(port: u16) -> Self {
            let hostport = format!("localhost:{}", port);
            let stream = TcpStream::connect(hostport).expect("could not connect");
            stream.set_nodelay(true).expect("could not disable nagle");
            stream
                .set_nonblocking(true)
                .expect("could not set non-blocking");
            TcpInjector { stream }
        }
    }

    struct UdpInjector {
        datagram: UdpSocket,
    }

    impl UdpInjector {
        fn with_port(port: u16) -> Self {
            let ephemeral_port = format!(
                "localhost:{}",
                find_free_port(30000..31000).expect("no free ports in range")
            );
            let hostport = format!("localhost:{}", port);
            let datagram = UdpSocket::bind(ephemeral_port).expect("could not bind");
            datagram.connect(hostport).expect("could not connect");
            datagram
                .set_nonblocking(true)
                .expect("could not set non-blocking");
            UdpInjector { datagram }
        }
    }

    macro_rules! rampercize {
        ($onramp_config:expr, $offramp_config:expr, $test:tt) => {
            let storage_directory = Some("./storage".to_string());
            let (world, _handle) = b!(system::World::start(50, storage_directory))?;
            let config = serde_yaml::to_value($onramp_config).expect("json to yaml not ok");

            let onramp: crate::config::OnRamp = serde_yaml::from_value(config)?;
            let onramp_url = TremorURL::from_onramp_id("test").expect("bad url");
            b!(world.repo.publish_onramp(&onramp_url, false, onramp))?;

            let config2 = serde_yaml::to_value($offramp_config).expect("json to yaml not ok");

            let offramp: crate::config::OffRamp = serde_yaml::from_value(config2)?;
            let offramp_url = TremorURL::from_offramp_id("test").expect("bad url");
            b!(world.repo.publish_offramp(&offramp_url, false, offramp))?;

            let id = TremorURL::parse(&format!("/pipeline/{}", "test"))?;

            let test_pipeline_config: tremor_pipeline::config::Pipeline = serde_yaml::from_str(
                r#"
id: test
description: 'Test pipeline'
interface:
  inputs: [ in ]
  outputs: [ out ]
links:
  in: [ out ]
"#,
            )?;
            let artefact = PipelineArtefact::Pipeline(Box::new(tremor_pipeline::build_pipeline(
                test_pipeline_config,
            )?));
            b!(world.repo.publish_pipeline(&id, false, artefact))?;

            let binding: Binding = serde_yaml::from_str(
                r#"
id: test
links:
  '/onramp/test/{instance}/out': [ '/pipeline/test/{instance}/in' ]
  '/pipeline/test/{instance}/out': [ '/offramp/test/{instance}/in' ]
"#,
            )?;

            b!(world.repo.publish_binding(
                &TremorURL::parse(&format!("/binding/{}", "test"))?,
                false,
                BindingArtefact {
                    binding,
                    mapping: None,
                },
            ))?;

            let mapping: MappingMap = serde_yaml::from_str(
                r#"
/binding/test/01:
  instance: "01"
"#,
            )?;

            let id = TremorURL::parse(&format!("/binding/{}/01", "test"))?;
            b!(world.link_binding(&id, mapping[&id].clone()))?;

            std::thread::sleep(std::time::Duration::from_millis(1000));

            $test;

            std::thread::sleep(std::time::Duration::from_millis(1000));

            b!(world.stop());
        };
    }

    #[allow(unused_macros)] // KEEP Useful for developing tests
    macro_rules! rampercize_with_logs {
        ($onramp_config:expr, $offramp_config:expr, $test:tt) => {
            env_logger::init();
            rampercize!($onramp_config, $offramp_config, $test)
        };
    }

    #[test]
    fn tcp_onramp() -> Result<()> {
        let port = find_free_port(9000..9099).expect("no free port");
        rampercize!(
            // onramp config
            json!({
                "id": "test",
                "type": "tcp",
                "codec": "json",
                "preprocessors": [ "lines" ],
                "config": {
                  "host": "127.0.0.1",
                  "port": port,
                  "is_non_blocking": true,
                  "ttl": 32,
                }
            }),
            // offramp config
            json!({
                "id": "test",
                "type": "stdout",
                "codec": "json",
            }),
            {
                for _ in 0..3 {
                    let mut inject = TcpInjector::with_port(port);
                    inject
                        .stream
                        .write_all(r#"{"snot": "badger"}\n"#.as_bytes())
                        .expect("something bad happened in cli injector");
                    inject.stream.flush().expect("");
                    drop(inject);
                }
            }
        );
        Ok(())
    }

    #[test]
    fn udp_onramp() -> Result<()> {
        let port = find_free_port(9100..9199).expect("no free port");
        rampercize!(
            // onramp config
            json!({
                "id": "test",
                "type": "udp",
                "codec": "json",
                "preprocessors": [ "lines" ],
                "config": {
                  "host": "127.0.0.1",
                  "port": port,
                  "is_non_blocking": true,
                  "ttl": 32,
                }
            }),
            // offramp config
            json!({
                "id": "test",
                "type": "stdout",
                "codec": "json",
            }),
            {
                for _ in 0..3 {
                    let inject = UdpInjector::with_port(port);
                    inject
                        .datagram
                        .send(r#"{"snot": "badger"}\n"#.as_bytes())
                        .expect("something bad happened in cli injector");
                    drop(inject);
                }
            }
        );
        Ok(())
    }

    #[ignore]
    #[test]
    fn rest_onramp() -> Result<()> {
        let port = find_free_port(9200..9299).expect("no free port");
        rampercize!(
            // onramp config
            json!({
                "id": "test",
                "type": "rest",
                "codec": "json",
                "preprocessors": [ "lines" ],
                "config": {
                  "host": "127.0.0.1",
                  "port": port,
                  "is_non_blocking": true,
                  "ttl": 32,
                  "resources": [

                  ],
                }
            }),
            // offramp config
            json!({
                "id": "test",
                "type": "stdout",
                "codec": "json",
            }),
            {
                for _ in 0..3 {
                    let mut inject = TcpInjector::with_port(port);
                    inject
                        .stream
                        .write_all(
                            r#"{"HTTP/1.1\nContent-type: application\nContent-Length: 3\n\n{}\n"#
                                .as_bytes(),
                        )
                        .expect("something bad happened in cli injector");
                    inject.stream.flush().expect("");
                    drop(inject);
                }
            }
        );
        Ok(())
    }
}
