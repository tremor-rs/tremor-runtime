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
use async_std::task::JoinHandle;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_runtime::config;
use tremor_runtime::connectors;
use tremor_runtime::errors::Result;
use tremor_runtime::lifecycle::InstanceState;
use tremor_runtime::pipeline;
use tremor_runtime::system::ShutdownMode;
use tremor_runtime::system::World;
use tremor_runtime::url::ports::{ERR, IN, OUT};
use tremor_runtime::url::TremorUrl;
use tremor_runtime::Event;
use tremor_runtime::QSIZE;

pub(crate) struct TestHarness {
    connector_id: TremorUrl,
    world: World,
    handle: JoinHandle<Result<()>>,
    //config: config::Connector,
    //addr: connectors::Addr,
    out_pipeline: TestPipeline,
    err_pipeline: TestPipeline,
}

impl TestHarness {
    pub(crate) async fn new(config: String) -> Result<Self> {
        let (world, handle) = World::start(None).await?;
        let raw_config = serde_yaml::from_slice::<config::Connector>(config.as_bytes())?;
        let id = TremorUrl::from_connector_instance(raw_config.id.as_str(), "test")?;
        let _connector_config = world.repo.publish_connector(&id, false, raw_config).await?;
        let res = world.bind_connector(&id).await?;
        let connector_addr = world.reg.find_connector(&id).await?.unwrap();
        assert_eq!(InstanceState::Initialized, res);

        // connect a fake pipeline to OUT
        let out_pipeline_id =
            TremorUrl::from_pipeline_instance("out_pipeline", "01")?.with_port(&IN);
        let (link_tx, link_rx) = async_std::channel::bounded(1);
        let out_pipeline = TestPipeline::new(out_pipeline_id.clone());
        connector_addr
            .send(connectors::Msg::Link {
                port: OUT,
                pipelines: vec![(out_pipeline_id, out_pipeline.addr.clone())],
                result_tx: link_tx.clone(),
            })
            .await?;
        link_rx.recv().await??;

        // connect a fake pipeline to ERR
        let err_pipeline_id =
            TremorUrl::from_pipeline_instance("err_pipeline", "01")?.with_port(&IN);
        let err_pipeline = TestPipeline::new(err_pipeline_id.clone());
        connector_addr
            .send(connectors::Msg::Link {
                port: ERR,
                pipelines: vec![(err_pipeline_id, err_pipeline.addr.clone())],
                result_tx: link_tx.clone(),
            })
            .await?;
        link_rx.recv().await??;

        Ok(Self {
            connector_id: id,
            world,
            handle,
            //config: connector_config,
            //addr: connector_addr,
            out_pipeline,
            err_pipeline,
        })
    }

    pub(crate) async fn start(&self) -> Result<()> {
        // start the connector
        assert_eq!(
            InstanceState::Running,
            self.world.reg.start_connector(&self.connector_id).await?
        );
        Ok(())
    }

    pub(crate) async fn stop(self, timeout_s: u64) -> Result<(Vec<Event>, Vec<Event>)> {
        self.world
            .stop(ShutdownMode::Graceful {
                timeout: Duration::from_secs(timeout_s),
            })
            .await?;
        self.handle.cancel().await;
        let out_events = self.out_pipeline.get_events()?;
        let err_events = self.err_pipeline.get_events()?;
        Ok((out_events, err_events))
    }
}

pub(crate) struct TestPipeline {
    rx: Receiver<pipeline::Msg>,
    addr: pipeline::Addr,
}

impl TestPipeline {
    pub(crate) fn new(id: TremorUrl) -> Self {
        let qsize = QSIZE.load(Ordering::Relaxed);
        let (tx, rx) = bounded(qsize);
        let (tx_cf, _rx_cf) = bounded(qsize);
        let (tx_mgmt, _rx_mgmt) = bounded(qsize);
        let addr = pipeline::Addr::new(tx, tx_cf, tx_mgmt, id);
        Self { rx, addr }
    }

    pub(crate) fn get_events(&self) -> Result<Vec<Event>> {
        let mut events = Vec::with_capacity(self.rx.len());
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                pipeline::Msg::Event { event, .. } => {
                    events.push(event);
                }
                pipeline::Msg::Signal(signal) => {
                    debug!("Received signal: {:?}", signal.kind)
                }
            }
        }
        Ok(events)
    }
}
