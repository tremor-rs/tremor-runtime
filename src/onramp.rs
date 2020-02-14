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

use crate::errors::*;
use crate::metrics::RampReporter;
use crate::repository::ServantId;
use crate::system::{PipelineAddr, Stop};
use crate::url::TremorURL;
use actix::prelude::*;
use crossbeam_channel::Sender;
use serde_yaml::Value;
use std::fmt;
mod blaster;
mod crononome;
mod file;
mod gsub;
mod kafka;
mod metronome;
mod prelude;
mod rest;
pub mod tcp;
mod udp;
mod ws;

pub(crate) trait Impl {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>>;
}

#[derive(Clone, Debug)]
pub enum Msg {
    Connect(Vec<(TremorURL, PipelineAddr)>),
    Disconnect { id: TremorURL, tx: Sender<bool> },
}

pub type Addr = Sender<Msg>;

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
        "gsub" => gsub::GSub::from_config(config),
        "kafka" => kafka::Kafka::from_config(config),
        "metronome" => metronome::Metronome::from_config(config),
        "crononome" => crononome::Crononome::from_config(config),
        "udp" => udp::Udp::from_config(config),
        "tcp" => tcp::Tcp::from_config(config),
        "rest" => rest::Rest::from_config(config),
        "ws" => ws::Ws::from_config(config),
        _ => Err(format!("Onramp {} not known", name).into()),
    }
}

#[derive(Debug, Default)]
pub struct Manager {}

impl Actor for Manager {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Onramp manager started");
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

impl Message for Create {
    type Result = Result<Addr>;
}

impl Handler<Create> for Manager {
    type Result = Result<Addr>;
    fn handle(&mut self, mut req: Create, _ctx: &mut Context<Self>) -> Self::Result {
        req.stream
            .start(&req.codec, &req.preprocessors, req.metrics_reporter)
    }
}

impl Handler<Stop> for Manager {
    type Result = ();
    fn handle(&mut self, _req: Stop, _ctx: &mut Self::Context) -> Self::Result {
        // TODO: Propper shutdown needed?
        info!("Stopping onramps");
        System::current().stop();
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
            let (world, _handle) = system::World::start(50, storage_directory)?;
            let config = serde_yaml::to_value($onramp_config).expect("json to yaml not ok");

            let onramp: crate::config::OnRamp = serde_yaml::from_value(config)?;
            let onramp_url = TremorURL::from_onramp_id("test").expect("bad url");
            world.repo.publish_onramp(&onramp_url, false, onramp)?;

            let config2 = serde_yaml::to_value($offramp_config).expect("json to yaml not ok");

            let offramp: crate::config::OffRamp = serde_yaml::from_value(config2)?;
            let offramp_url = TremorURL::from_offramp_id("test").expect("bad url");
            world.repo.publish_offramp(&offramp_url, false, offramp)?;

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
            world.repo.publish_pipeline(&id, false, artefact)?;

            let binding: Binding = serde_yaml::from_str(
                r#"
id: test
links:
  '/onramp/test/{instance}/out': [ '/pipeline/test/{instance}/in' ]
  '/pipeline/test/{instance}/out': [ '/offramp/test/{instance}/in' ]
"#,
            )?;

            world.repo.publish_binding(
                &TremorURL::parse(&format!("/binding/{}", "test"))?,
                false,
                BindingArtefact {
                    binding,
                    mapping: None,
                },
            )?;

            let mapping: MappingMap = serde_yaml::from_str(
                r#"
/binding/test/01:
  instance: "01"
"#,
            )?;

            let id = TremorURL::parse(&format!("/binding/{}/01", "test"))?;
            world.link_binding(&id, mapping[&id].clone())?;

            std::thread::sleep(std::time::Duration::from_millis(1000));

            $test();

            std::thread::sleep(std::time::Duration::from_millis(1000));

            world.stop();
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
