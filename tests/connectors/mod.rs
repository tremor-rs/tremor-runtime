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

// some tests don't use everything and this would generate warnings for those
// which it shouldn't
#![allow(dead_code)]

use async_std::channel::bounded;
use async_std::channel::Receiver;
use async_std::task::JoinHandle;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tremor_runtime::config;
use tremor_runtime::connectors;
use tremor_runtime::connectors::{Connectivity, ConnectorState, StatusReport};
use tremor_runtime::errors::Result;
use tremor_runtime::lifecycle::InstanceState;
use tremor_runtime::pipeline;
use tremor_runtime::system::ShutdownMode;
use tremor_runtime::system::World;
use tremor_runtime::url::ports::{ERR, IN, OUT};
use tremor_runtime::url::TremorUrl;
use tremor_runtime::Event;
use tremor_runtime::QSIZE;

pub(crate) struct ConnectorHarness {
    connector_id: TremorUrl,
    world: World,
    handle: JoinHandle<Result<()>>,
    //config: config::Connector,
    addr: connectors::Addr,
    out_pipeline: TestPipeline,
    err_pipeline: TestPipeline,
}

impl ConnectorHarness {
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
            TremorUrl::from_pipeline_instance("TEST__out_pipeline", "01")?.with_port(&IN);
        let (link_tx, link_rx) = async_std::channel::unbounded();
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
            TremorUrl::from_pipeline_instance("TEST__err_pipeline", "01")?.with_port(&IN);
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
            addr: connector_addr,
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

    pub(crate) async fn pause(&self) -> Result<()> {
        Ok(self.addr.send(connectors::Msg::Pause).await?)
    }

    pub(crate) async fn resume(&self) -> Result<()> {
        Ok(self.addr.send(connectors::Msg::Resume).await?)
    }

    pub(crate) async fn stop(self) -> Result<(Vec<Event>, Vec<Event>)> {
        self.world.stop(ShutdownMode::Graceful).await?;
        //self.handle.cancel().await;
        let out_events = self.out_pipeline.get_events()?;
        let err_events = self.err_pipeline.get_events()?;
        Ok((out_events, err_events))
    }

    pub(crate) async fn status(&self) -> Result<StatusReport> {
        let (report_tx, report_rx) = bounded(1);
        self.addr.send(connectors::Msg::Report(report_tx)).await?;
        Ok(report_rx.recv().await?)
    }

    /// Wait for the connector to be connected.
    ///
    /// # Errors
    ///
    /// If communication with the connector fails or we time out without reaching connected state.
    pub(crate) async fn wait_for_connected(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        while self.status().await?.connectivity != Connectivity::Connected {
            // TODO create my own future here that succeeds on poll when status is connected
            async_std::task::sleep(Duration::from_millis(100)).await;
            if start.elapsed() >= timeout {
                return Err(format!(
                    "Connector {} didn't reach connected within {:?}",
                    self.connector_id, timeout
                )
                .into());
            }
        }
        Ok(())
    }

    /// Wait for the connecte to reach the given `state`.
    ///
    /// # Errors
    ///
    /// If communication with the connector fails or we time out without reaching the desired state
    pub(crate) async fn wait_for_state(
        &self,
        state: ConnectorState,
        timeout: Duration,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        while self.status().await?.status != state {
            async_std::task::sleep(Duration::from_millis(100)).await;
            if start.elapsed() >= timeout {
                return Err(format!(
                    "Connector {} didn't reach state {} within {:?}",
                    self.connector_id, state, timeout
                )
                .into());
            }
        }
        Ok(())
    }

    /// get the out pipeline
    pub(crate) fn out(&self) -> &TestPipeline {
        &self.out_pipeline
    }

    /// get the err pipeline
    pub(crate) fn err(&self) -> &TestPipeline {
        &self.err_pipeline
    }
}

pub(crate) struct TestPipeline {
    rx: Receiver<Box<pipeline::Msg>>,
    rx_cf: Receiver<pipeline::CfMsg>,
    rx_mgmt: Receiver<pipeline::MgmtMsg>,
    addr: pipeline::Addr,
}

impl TestPipeline {
    pub(crate) fn new(id: TremorUrl) -> Self {
        let qsize = QSIZE.load(Ordering::Relaxed);
        let (tx, rx) = bounded(qsize);
        let (tx_cf, rx_cf) = bounded(qsize);
        let (tx_mgmt, rx_mgmt) = bounded(qsize);
        let addr = pipeline::Addr::new(tx, tx_cf, tx_mgmt, id);
        Self {
            rx,
            rx_cf,
            rx_mgmt,
            addr,
        }
    }

    // get all currently available events from the pipeline
    pub(crate) fn get_events(&self) -> Result<Vec<Event>> {
        let mut events = Vec::with_capacity(self.rx.len());
        while let Ok(msg) = self.rx.try_recv() {
            match *msg {
                pipeline::Msg::Event { event, .. } => {
                    events.push(event.clone());
                }
                pipeline::Msg::Signal(signal) => {
                    debug!("Received signal: {:?}", signal.kind)
                }
            }
        }
        Ok(events)
    }

    /// get a single event from the pipeline
    pub(crate) async fn get_event(&self) -> Result<Event> {
        loop {
            match *self.rx.recv().await? {
                pipeline::Msg::Event { event, .. } => break Ok(event),
                // filter out signals
                pipeline::Msg::Signal(signal) => {
                    debug!("Received signal: {:?}", signal.kind)
                }
            }
        }
    }
}
