use actix::*;
use simd_json::json;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::net::UdpSocket;
use std::ops::Range;
use tremor_runtime::errors::*;
use tremor_runtime::url::TremorURL;
use tremor_runtime::system;
use tremor_runtime::repository::PipelineArtefact;
use tremor_runtime::repository::BindingArtefact;
use tremor_runtime::config::Binding;
use tremor_runtime::config::MappingMap;


#[cfg(test)]
mod test {
    use super::*;

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

    struct TcpRecorder {
        port: u16,
        listener: TcpListener,
    }

    impl TcpRecorder {
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
            let ephemeral_port = format!("localhost:{}", find_free_port(30000..31000).expect("no free ports in range"));
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
            let config = serde_yaml::to_value($onramp_config)
                .expect("json to yaml not ok");

            let onramp: tremor_runtime::config::OnRamp = serde_yaml::from_value(config)?;
            let onramp_url = TremorURL::from_onramp_id("test").expect("bad url");
            world.repo.publish_onramp(&onramp_url, false, onramp)?;

            let config2 = serde_yaml::to_value($offramp_config)
                .expect("json to yaml not ok");

            let offramp: tremor_runtime::config::OffRamp = serde_yaml::from_value(config2)?;
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
            let artefact =
                PipelineArtefact::Pipeline(Box::new(tremor_pipeline::build_pipeline(test_pipeline_config)?));
                world.repo.publish_pipeline(&id, false, artefact)?;

            let binding: Binding = serde_yaml::from_str(r#"
id: test
links:
  '/onramp/test/{instance}/out': [ '/pipeline/test/{instance}/in' ]
  '/pipeline/test/{instance}/out': [ '/offramp/test/{instance}/in' ]
"#)?;

            world.repo.publish_binding(
                &TremorURL::parse(&format!("/binding/{}", "test"))?,
                false,
                BindingArtefact {
                    binding,
                    mapping: None,
                },
            )?;

            let mapping: MappingMap = serde_yaml::from_str(r#"
/binding/test/01:
  instance: "01"
"#)?;

            let id = TremorURL::parse(&format!("/binding/{}/01", "test"))?;
            world.link_binding(&id, mapping[&id].clone())?;

            std::thread::sleep(std::time::Duration::from_millis(1000));

            $test();

            std::thread::sleep(std::time::Duration::from_millis(1000));

            world.stop(); 
        };
    }

    macro_rules! rampercize_with_logs {
        ($onramp_config:expr, $offramp_config:expr, $test:tt) => {
            env_logger::init();
            rampercize!($onramp_config, $offramp_config, $test)      
        };
    }

    #[test]
    fn tcp_onramp() -> Result<()> {
        let port = find_free_port(9000..10000).expect("no free port");
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
        let port = find_free_port(9000..10000).expect("no free port");
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
        let port = find_free_port(9000..10000).expect("no free port");
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
                        .write_all(r#"{"HTTP/1.1\nContent-type: application\nContent-Length: 3\n\n{}\n"#.as_bytes())
                        .expect("something bad happened in cli injector");
                        inject.stream.flush().expect("");
                    drop(inject);
                }
            }
        );
        Ok(())
    }

}
