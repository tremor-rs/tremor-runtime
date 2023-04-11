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

//!
//! Connector testing framework
//!
//! ....

#[cfg(feature = "clickhouse-integration")]
mod clickhouse;
#[cfg(feature = "crononome-integration")]
mod crononome;
#[cfg(feature = "es-integration")]
mod elastic;
#[cfg(feature = "file-integration")]
mod file;
#[cfg(feature = "file-integration")]
mod file_non_existent;
#[cfg(feature = "file-integration")]
mod file_xz;
#[cfg(feature = "gcp-integration")]
mod gpubsub;
#[cfg(feature = "http-integration")]
mod http;
#[cfg(feature = "kafka-integration")]
mod kafka;
#[cfg(feature = "metronome-integration")]
mod metronome;
mod pause_resume;
#[cfg(feature = "s3-integration")]
mod s3;
#[cfg(feature = "net-integration")]
mod tcp;
#[cfg(feature = "net-integration")]
mod udp;
#[cfg(feature = "socket-integration")]
mod unix_socket;
#[cfg(feature = "wal-integration")]
mod wal;
#[cfg(feature = "ws-integration")]
mod ws;

mod bench;

// some tests don't use everything and this would generate warnings for those
// which it shouldn't

use super::{prelude::KillSwitch, sink::SinkMsg};
use crate::{
    channel::{bounded, unbounded, Receiver, UnboundedReceiver},
    errors::empty_error,
    raft,
};
use crate::{
    config,
    connectors::{
        self, builtin_connector_types, source::SourceMsg, Alias as ConnectorAlias, Connectivity,
        StatusReport,
    },
    errors::Result,
    ids::AppFlowInstanceId,
    instance::State,
    pipeline, qsize, Event,
};
use log::{debug, info};
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use tokio::{sync::oneshot, task, time::timeout};
use tremor_common::{
    ports::{Port, ERR, IN, OUT},
    uids::{ConnectorUIdGen, SourceUId, UId},
};
use tremor_pipeline::{CbAction, EventId};
use tremor_script::{ast::DeployEndpoint, lexer::Location, NodeMeta};
use tremor_value::Value;

pub(crate) struct ConnectorHarness {
    addr: connectors::Addr,
    pipes: HashMap<Port<'static>, TestPipeline>,
}

impl ConnectorHarness {
    pub(crate) async fn new_with_ports(
        alias: &str,
        builder: &dyn connectors::ConnectorBuilder,
        defn: &Value<'static>,
        kill_switch: KillSwitch,
        input_ports: Vec<Port<'static>>,
        output_ports: Vec<Port<'static>>,
    ) -> Result<Self> {
        let alias = ConnectorAlias::new(AppFlowInstanceId::new("app", "test"), alias);
        let mut connector_id_gen = ConnectorUIdGen::new();
        let mut known_connectors = HashMap::new();

        for builder in builtin_connector_types() {
            known_connectors.insert(builder.connector_type(), builder);
        }
        let raw_config = config::Connector::from_config(&alias, builder.connector_type(), defn)?;
        let connector_addr = connectors::spawn(
            openraft::NodeId::default(),
            &alias,
            &mut connector_id_gen,
            builder,
            raw_config,
            &kill_switch,
            raft::Manager::default(),
        )
        .await?;
        let mut pipes = HashMap::new();

        let (link_tx, mut link_rx) = bounded(qsize());
        let mid = NodeMeta::new(Location::yolo(), Location::yolo());
        for port in input_ports {
            // try to connect a fake pipeline outbound
            let pipeline_id = DeployEndpoint::new(&format!("TEST__{port}_pipeline"), IN, &mid);
            // connect pipeline to connector
            let pipeline = TestPipeline::new(pipeline_id.alias().to_string());
            connector_addr
                .send(connectors::Msg::LinkInput {
                    port: port.clone(),
                    pipelines: vec![(pipeline_id, pipeline.addr.clone())],
                    result_tx: link_tx.clone(),
                })
                .await?;

            if link_rx.recv().await.is_none() {
                info!("Error connecting fake pipeline to port {port} of connector {alias}",);
            } else {
                pipes.insert(port, pipeline);
            }
        }
        for port in output_ports {
            // try to connect a fake pipeline outbound
            let pipeline_id = DeployEndpoint::new(&format!("TEST__{port}_pipeline"), IN, &mid);
            let pipeline = TestPipeline::new(pipeline_id.alias().to_string());
            connector_addr
                .send(connectors::Msg::LinkOutput {
                    port: port.clone(),
                    pipeline: (pipeline_id, pipeline.addr.clone()),
                    result_tx: link_tx.clone(),
                })
                .await?;

            if link_rx.recv().await.is_none() {
                info!("Error connecting fake pipeline to port {port} of connector {alias}",);
            } else {
                pipes.insert(port, pipeline);
            }
        }

        Ok(Self {
            addr: connector_addr,
            pipes,
        })
    }

    pub(crate) async fn new(
        id: &str,
        builder: &dyn connectors::ConnectorBuilder,
        defn: &Value<'static>,
    ) -> Result<Self> {
        Self::new_with_kill_switch(id, builder, defn, KillSwitch::dummy()).await
    }

    pub(crate) async fn new_with_kill_switch(
        id: &str,
        builder: &dyn connectors::ConnectorBuilder,
        defn: &Value<'static>,
        kill_switch: KillSwitch,
    ) -> Result<Self> {
        Self::new_with_ports(id, builder, defn, kill_switch, vec![IN], vec![OUT, ERR]).await
    }

    pub(crate) async fn start(&self) -> Result<()> {
        // start the connector
        let (tx, mut rx) = bounded(1);
        self.addr.start(tx).await?;
        let cr = rx.recv().await.ok_or_else(empty_error)?;
        cr.res?;

        // send a `CBAction::Restore` to the connector, so it starts pulling data
        self.send_to_source(SourceMsg::Cb(CbAction::Restore, EventId::default()))?;
        // We introduce a synchronisation step to ensure that the restore has been processed
        // and the sink is treated as connected
        if self.addr.source.is_some() {
            let (tx, rx) = oneshot::channel();
            self.send_to_source(SourceMsg::Synchronize(tx))?;
            rx.await?;
        }

        // ensure we notify the connector that its sink part is connected
        self.addr
            .send_sink(SinkMsg::Signal {
                signal: Event::signal_start(SourceUId::new(1)),
            })
            .await?;

        Ok(())
    }

    pub(crate) async fn pause(&self) -> Result<()> {
        self.addr.send(connectors::Msg::Pause).await
    }

    pub(crate) async fn resume(&self) -> Result<()> {
        self.addr.send(connectors::Msg::Resume).await
    }

    pub(crate) async fn stop(mut self) -> Result<(Vec<Event>, Vec<Event>)> {
        let (tx, mut rx) = bounded(qsize());
        debug!("Stopping harness...");
        self.addr.stop(tx).await?;
        debug!("Waiting for stop result...");
        let cr = rx.recv().await.ok_or_else(empty_error)?;
        debug!("Stop result received.");
        cr.res?;
        //self.handle.abort();
        let out_events = self
            .pipes
            .get_mut(&OUT)
            .map_or(vec![], TestPipeline::get_events);
        let err_events = self
            .pipes
            .get_mut(&ERR)
            .map_or(vec![], TestPipeline::get_events);
        for (port, p) in self.pipes {
            debug!("stopping pipeline connected to {port}");
            p.stop().await?;
        }
        debug!("Pipes stopped");
        Ok((out_events, err_events))
    }

    pub(crate) async fn status(&self) -> Result<StatusReport> {
        let (report_tx, report_rx) = oneshot::channel();
        self.addr.send(connectors::Msg::Report(report_tx)).await?;
        Ok(report_rx.await?)
    }

    /// Wait for the connector to be connected.
    ///
    /// # Errors
    ///
    /// If communication with the connector fails or we time out without reaching connected state.
    pub(crate) async fn wait_for_connected(&self) -> Result<()> {
        while self.status().await?.connectivity != Connectivity::Connected {
            // TODO create my own future here that succeeds on poll when status is connected
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// everytime we boot up and start a sink connector
    /// the runtime will emit a Cb Open and a Cb `SinkStart` message. We consume those here to clear out the pipe.
    ///
    /// # Errors
    /// If we receive different bootup contraflow messages
    #[cfg(any(
        feature = "kafka-integration",
        feature = "es-integration",
        feature = "s3-integration",
        feature = "net-integration",
        feature = "http-integration",
        feature = "gcp-integration"
    ))]
    pub(crate) async fn consume_initial_sink_contraflow(&mut self) -> Result<()> {
        for cf in [
            self.get_pipe(IN)?.get_contraflow().await?,
            self.get_pipe(IN)?.get_contraflow().await?,
        ] {
            assert!(
                matches!(cf.cb, CbAction::SinkStart(_) | CbAction::Restore),
                "Expected SinkStart or Open Contraflow message, got: {cf:?}"
            );
        }

        Ok(())
    }

    /// Wait for the connector to reach the given `state`.
    ///
    /// # Errors
    ///
    /// If communication with the connector fails or we time out without reaching the desired state
    pub(crate) async fn wait_for_state(&self, state: State) -> Result<()> {
        while self.status().await?.status != state {
            // TODO create my own future here that succeeds on poll when status is connected
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    pub(crate) fn get_pipe<T>(&mut self, port: T) -> Result<&mut TestPipeline>
    where
        T: Into<Port<'static>>,
    {
        let port = port.into();
        Ok(self
            .pipes
            .get_mut(&port)
            .ok_or_else(|| format!("No pipeline connected to port {port}"))?)
    }

    /// get the out pipeline - if any
    pub(crate) fn out(&mut self) -> Result<&mut TestPipeline> {
        self.get_pipe(OUT)
    }

    #[cfg(any(
        feature = "http-integration",
        feature = "es-integration",
        feature = "kafka-integration"
    ))]
    /// get the err pipeline - if any
    pub(crate) fn err(&mut self) -> Result<&mut TestPipeline> {
        self.get_pipe(ERR)
    }

    #[cfg(any(
        feature = "http-integration",
        feature = "es-integration",
        feature = "socket-integration",
        feature = "net-integration",
        feature = "ws-integration",
        feature = "s3-integration",
        feature = "gcp-integration"
    ))]
    pub(crate) async fn send_to_sink(&self, event: Event, port: Port<'static>) -> Result<()> {
        self.addr.send_sink(SinkMsg::Event { event, port }).await
    }

    pub(crate) fn send_to_source(&self, msg: SourceMsg) -> Result<()> {
        self.addr.send_source(msg)
    }

    // this is only used in integration tests,
    // otherwise this throws an error when compiled for non-integration tests
    #[allow(dead_code)]
    pub(crate) async fn signal_tick_to_sink(&self) -> Result<()> {
        self.addr
            .send_sink(SinkMsg::Signal {
                signal: Event::signal_tick(),
            })
            .await
    }

    #[cfg(any(
        feature = "kafka-integration",
        feature = "wal-integration",
        feature = "gcp-integration"
    ))]
    pub(crate) fn send_contraflow(&self, cb: CbAction, id: EventId) -> Result<()> {
        self.addr.send_source(SourceMsg::Cb(cb, id))
    }
}

#[derive(Debug)]
pub(crate) struct TestPipeline {
    rx: Receiver<Box<pipeline::Msg>>,
    // this is only used in some integration tests
    #[allow(dead_code)]
    rx_cf: UnboundedReceiver<pipeline::CfMsg>,
    addr: pipeline::Addr,
}

impl TestPipeline {
    pub(crate) async fn stop(&self) -> Result<()> {
        self.addr.send_mgmt(pipeline::MgmtMsg::Stop).await
    }
    pub(crate) fn new(alias: String) -> Self {
        let flow_id = AppFlowInstanceId::new("TEST", "test");
        let qsize = qsize();
        let (tx, rx) = bounded(qsize);
        let (tx_cf, rx_cf) = unbounded();
        let (tx_mgmt, mut rx_mgmt) = bounded(qsize);
        let pipeline_id = pipeline::Alias::new(flow_id, alias);
        let addr = pipeline::Addr::new(tx, tx_cf, tx_mgmt, pipeline_id);

        task::spawn(async move {
            while let Some(msg) = rx_mgmt.recv().await {
                match msg {
                    pipeline::MgmtMsg::Stop => {
                        break;
                    }
                    pipeline::MgmtMsg::ConnectInput { tx, .. } => {
                        if let Err(e) = tx.send(Ok(())).await {
                            error!("Oh no error in test: {e}");
                        }
                    }
                    _ => {}
                }
            }
        });

        Self { rx, rx_cf, addr }
    }

    // get all available contraflow events
    #[cfg(any(feature = "s3-integration", feature = "kafka-integration"))]
    pub(crate) fn get_contraflow_events(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Ok(pipeline::CfMsg::Insight(event)) = self.rx_cf.try_recv() {
            events.push(event);
        }
        events
    }

    // wait for a contraflow
    #[cfg(any(
        feature = "kafka-integration",
        feature = "es-integration",
        feature = "s3-integration",
        feature = "net-integration",
        feature = "http-integration",
        feature = "gcp-integration"
    ))]
    pub(crate) async fn get_contraflow(&mut self) -> Result<Event> {
        match timeout(Duration::from_secs(20), self.rx_cf.recv())
            .await?
            .ok_or("No contraflow")?
        {
            pipeline::CfMsg::Insight(event) => Ok(event),
        }
    }

    // get all currently available events from the pipeline
    pub(crate) fn get_events(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Ok(msg) = self.rx.try_recv() {
            match *msg {
                pipeline::Msg::Event { event, .. } => {
                    events.push(event.clone());
                }
                pipeline::Msg::Signal(signal) => {
                    debug!("Received signal: {:?}", signal.kind);
                }
            }
        }
        events
    }

    /// get a single event from the pipeline
    /// wait for an event to arrive
    pub(crate) async fn get_event(&mut self) -> Result<Event> {
        const TIMEOUT: Duration = Duration::from_secs(120);
        let start = Instant::now();
        loop {
            match timeout(TIMEOUT, self.rx.recv()).await {
                Ok(Some(msg)) => {
                    match *msg {
                        pipeline::Msg::Event { event, .. } => break Ok(event),
                        // filter out signals
                        pipeline::Msg::Signal(signal) => {
                            debug!("Received signal: {:?}", signal.kind);
                        }
                    }
                }
                Ok(None) => {
                    return Err(empty_error());
                }
                Err(_) => {
                    return Err(format!("Did not receive an event for {TIMEOUT:?}").into());
                }
            }
            if start.elapsed() > TIMEOUT {
                return Err(format!("Did not receive an event for {TIMEOUT:?}").into());
            }
        }
    }

    pub(crate) async fn expect_no_event_for(&mut self, duration: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            match timeout(duration, self.rx.recv()).await {
                Err(_timeout_error) => {
                    return Ok(());
                }
                Ok(Some(msg)) => match *msg {
                    pipeline::Msg::Signal(_signal) => (),
                    pipeline::Msg::Event { event, .. } => {
                        return Err(
                            format!("Expected no event for {duration:?}, got: {event:?}").into(),
                        );
                    }
                },
                Ok(None) => {
                    return Err(empty_error());
                }
            }
            if start.elapsed() > duration {
                return Ok(());
            }
        }
    }
}

pub(crate) mod free_port {

    use crate::errors::Result;
    use std::ops::RangeInclusive;
    use tokio::net::{TcpListener, UdpSocket};
    use tokio::sync::Mutex;

    struct FreePort {
        port: u16,
    }

    impl FreePort {
        const RANGE: RangeInclusive<u16> = 10000..=65535;

        const fn new() -> Self {
            Self {
                port: *Self::RANGE.start(),
            }
        }

        async fn next(&mut self) -> Result<u16> {
            let mut candidate = self.port;
            self.port = self.port.wrapping_add(1);
            loop {
                if let Ok(listener) = TcpListener::bind(("127.0.0.1", candidate)).await {
                    let port = listener.local_addr()?.port();
                    assert_eq!(candidate, port);
                    drop(listener);
                    return Ok(port);
                }
                candidate = self.port;
                self.port = self.port.wrapping_add(1).min(*Self::RANGE.end());
            }
        }
    }

    lazy_static! {
        static ref FREE_PORT: Mutex<FreePort> = Mutex::new(FreePort::new());
    }
    /// Find free TCP port for use in test server endpoints
    pub(crate) async fn find_free_tcp_port() -> Result<u16> {
        FREE_PORT.lock().await.next().await
    }
    pub(crate) async fn find_free_udp_port() -> Result<u16> {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let port = socket.local_addr()?.port();
        drop(socket);
        Ok(port)
    }
}

pub(crate) fn setup_for_tls() {
    use std::process::Command;
    use std::process::Stdio;
    use std::sync::Once;

    static TLS_SETUP: Once = Once::new();

    // create TLS cert and key only once at the beginning of the test execution to avoid
    // multiple threads stepping on each others toes
    TLS_SETUP.call_once(|| {
        warn!("Refreshing TLS Cert/Key...");
        let mut cmd = Command::new("./tests/refresh_tls_cert.sh")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Unable to spawn ./tests/refresh_tls_cert.sh");
        let out = cmd.wait().expect("Failed to refresh certs/keys");
        match out.code() {
            Some(0) => {
                println!("Done refreshing TLS Cert/Key.");
                warn!("Done refreshing TLS Cert/Key.");
            }
            _ => panic!("Error creating tls certificate and key"),
        }
    });
}
