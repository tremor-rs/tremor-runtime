// Copyright 2021, The Tremor Team
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

use async_std::channel::bounded;
use async_std::channel::Receiver;
use async_std::path::Path;
use async_std::task;
use simd_json::ValueAccess;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_runtime::config;
use tremor_runtime::connectors;
use tremor_runtime::errors::Result;
use tremor_runtime::lifecycle::InstanceState;
use tremor_runtime::pipeline;
use tremor_runtime::system::{ShutdownMode, World};
use tremor_runtime::url::ports::{ERR, IN, OUT};
use tremor_runtime::url::TremorUrl;
use tremor_runtime::Event;
use tremor_runtime::QSIZE;
use tremor_value::literal;

#[async_std::test]
async fn file_connector() -> Result<()> {
    env_logger::init();
    let (world, handle) = World::start(None).await?;

    let id = TremorUrl::from_connector_instance("my_file", "01")?;
    let out_pipeline_id = TremorUrl::from_pipeline_instance("out_pipeline", "01")?.with_port(&IN);
    let err_pipeline_id = TremorUrl::from_pipeline_instance("err_pipeline", "01")?.with_port(&IN);

    let input_path = Path::new(file!())
        .parent()
        .unwrap()
        .join("data")
        .join("input.txt");
    let connector_yaml = format!(
        r#"
id: my_file
type: file
codec: string
preprocessors:
  - lines
config:
  path: "{}"
  mode: read
"#,
        input_path.display()
    );
    println!("{}", &connector_yaml);
    // instantiate the connector
    let config = serde_yaml::from_slice::<config::Connector>(connector_yaml.as_bytes())?;
    let _connector_config = world.repo.publish_connector(&id, false, config).await?;
    let res = world.bind_connector(&id).await?;
    let connector_addr = world.reg.find_connector(&id).await?.unwrap();
    assert_eq!(InstanceState::Initialized, res);

    // connect a fake pipeline to OUT
    let (link_tx, link_rx) = async_std::channel::bounded(1);
    let (out_pipeline, pipeline_addr) = TestPipeline::new(out_pipeline_id.clone());
    connector_addr
        .send(connectors::Msg::Link {
            port: OUT,
            pipelines: vec![(out_pipeline_id, pipeline_addr.clone())],
            result_tx: link_tx.clone(),
        })
        .await?;
    link_rx.recv().await??;

    // connect a fake pipeline to ERR
    let (err_pipeline, err_pipeline_addr) = TestPipeline::new(err_pipeline_id.clone());
    connector_addr
        .send(connectors::Msg::Link {
            port: ERR,
            pipelines: vec![(err_pipeline_id, err_pipeline_addr.clone())],
            result_tx: link_tx.clone(),
        })
        .await?;
    link_rx.recv().await??;

    // start the connector, let it read the file
    assert_eq!(
        InstanceState::Running,
        world.reg.start_connector(&id).await?
    );
    task::sleep(Duration::from_millis(100)).await;

    world
        .stop(ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        })
        .await?;
    handle.cancel().await;

    // check the out and err channels
    assert!(
        out_pipeline.rx.len() > 0,
        "didn't receive 2 events on out, but {}",
        out_pipeline.rx.len()
    );
    // get 1 event
    let event = out_pipeline.get_event()?;
    assert_eq!(1, event.len());
    let value = event.data.suffix().value();
    let meta = event.data.suffix().meta();

    // value
    assert_eq!("snot", value.as_str().unwrap());
    // meta
    assert_eq!(
        literal!({
            "path": "tests/data/input.txt"
        }),
        meta
    );
    let event2 = out_pipeline.get_event()?;
    assert_eq!(1, event2.len());
    let data = event.data.suffix().value();
    assert_eq!("badger", data.as_str().unwrap());

    assert!(
        err_pipeline.get_event().is_err(),
        "got some events on ERR port"
    );

    Ok(())
}

struct TestPipeline {
    rx: Receiver<pipeline::Msg>,
    //rx_cf: Receiver<pipeline::CfMsg>,
    //rx_mgmt: Receiver<pipeline::MgmtMsg>,
}

impl TestPipeline {
    fn new(id: TremorUrl) -> (Self, pipeline::Addr) {
        let qsize = QSIZE.load(Ordering::Relaxed);
        let (tx, rx) = bounded(qsize);
        let (tx_cf, _rx_cf) = bounded(qsize);
        let (tx_mgmt, _rx_mgmt) = bounded(qsize);
        let addr = pipeline::Addr::new(tx, tx_cf, tx_mgmt, id.clone());
        (
            Self {
                rx,
                //rx_cf,
                //rx_mgmt,
                //id,
            },
            addr,
        )
    }

    fn get_event(&self) -> Result<Event> {
        loop {
            let msg = self.rx.try_recv()?;
            match msg {
                pipeline::Msg::Event { event, .. } => return Ok(event.clone()),
                pipeline::Msg::Signal(signal) => {
                    eprintln!("Received signal: {:?}", signal.kind)
                }
            }
        }
    }
}
