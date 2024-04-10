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

// some tests don't use everything and this would generate warnings for those
// which it shouldn't

use crate::{
    builtin_connector_types,
    channel::{bounded, unbounded, Receiver, UnboundedReceiver},
    config, pipeline, qsize,
    sink::prelude::GenericImplementationError,
    Connectivity,
};
use anyhow::bail;
use log::{debug, info};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc::error::TryRecvError, oneshot},
    task,
    time::timeout,
};
use tremor_common::{
    alias,
    ids::{ConnectorIdGen, Id, SourceId},
    ports::{Port, ERR, IN, OUT},
};
use tremor_script::{ast::DeployEndpoint, lexer::Location, NodeMeta};
use tremor_system::{
    connector::{self, sink, source, StatusReport},
    controlplane::{self, CbAction},
    dataplane::{self, contraflow},
    event::{Event, EventId},
    instance::State,
    killswitch::KillSwitch,
};
use tremor_value::Value;

/// A connector harnes to run single connectors without the need for a full tremor runtime
pub struct Harness {
    addr: connector::Addr,
    pipes: HashMap<Port<'static>, TestPipeline>,
}

impl Harness {
    pub(crate) async fn new_with_ports(
        alias: &str,
        builder: &dyn crate::ConnectorBuilder,
        defn: &Value<'static>,
        kill_switch: KillSwitch,
        input_ports: Vec<Port<'static>>,
        output_ports: Vec<Port<'static>>,
    ) -> anyhow::Result<Self> {
        let alias = alias::Connector::new("harness", alias);
        let mut connector_id_gen = ConnectorIdGen::new();
        let mut known_connectors = HashMap::new();

        for builder in builtin_connector_types() {
            known_connectors.insert(builder.connector_type(), builder);
        }
        let raw_config = config::Connector::from_config(&alias, builder.connector_type(), defn)?;
        let connector_addr = crate::spawn(
            &alias,
            &mut connector_id_gen,
            builder,
            raw_config,
            &kill_switch,
        )
        .await?;
        let mut pipes = HashMap::new();

        let (link_tx, mut link_rx) = bounded(qsize());
        let mid = NodeMeta::new(Location::yolo(), Location::yolo());
        for port in input_ports {
            // try to connect a fake pipeline outbound
            let pipeline_id = DeployEndpoint::new(&format!("HARNESS__{port}_pipeline"), IN, &mid);
            // connect pipeline to connector
            let pipeline = TestPipeline::new(pipeline_id.alias().to_string());
            connector_addr
                .send(crate::Msg::LinkInput {
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
            let pipeline_id = DeployEndpoint::new(&format!("HARNESS__{port}_pipeline"), IN, &mid);
            let pipeline = TestPipeline::new(pipeline_id.alias().to_string());
            connector_addr
                .send(crate::Msg::LinkOutput {
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

    ///
    /// Create a new connector harness
    /// # Errors
    ///  - If the connector harnes could not be created or the linking failed
    pub async fn new(
        id: &str,
        builder: &dyn crate::ConnectorBuilder,
        defn: &Value<'static>,
    ) -> anyhow::Result<Self> {
        Self::new_with_kill_switch(id, builder, defn, KillSwitch::dummy()).await
    }

    /// Create a new connector harness with a kill switch
    /// # Errors
    /// - If the connector harnes could not be created or the linking failed
    pub async fn new_with_kill_switch(
        id: &str,
        builder: &dyn crate::ConnectorBuilder,
        defn: &Value<'static>,
        kill_switch: KillSwitch,
    ) -> anyhow::Result<Self> {
        Self::new_with_ports(id, builder, defn, kill_switch, vec![IN], vec![OUT, ERR]).await
    }

    /// Start the connector harness
    ///
    /// # Errors
    ///  - If the connector could not be started
    ///  - If the sink could not be connected

    pub async fn start(&self) -> anyhow::Result<()> {
        // start the connector
        let (tx, mut rx) = bounded(1);
        self.addr.start(tx).await?;
        let cr = rx
            .recv()
            .await
            .ok_or(GenericImplementationError::ChannelEmpty)?;
        cr.res?;

        // send a `CBAction::Restore` to the connector, so it starts pulling data
        self.send_to_source(source::Msg::Cb(CbAction::Restore, EventId::default()))?;
        // We introduce a synchronisation step to ensure that the restore has been processed
        // and the sink is treated as connected
        if self.addr.has_source() {
            let (tx, rx) = oneshot::channel();
            self.send_to_source(source::Msg::Synchronize(tx))?;
            rx.await?;
        }

        // ensure we notify the connector that its sink part is connected
        self.addr
            .send_sink(sink::Msg::Signal {
                signal: Event::signal_start(SourceId::new(1)),
            })
            .await?;

        Ok(())
    }

    /// Pause the connector harness
    /// # Errors
    /// - If the connector could not be paused
    pub async fn pause(&self) -> anyhow::Result<()> {
        Ok(self.addr.send(crate::Msg::Pause).await?)
    }

    /// Resume the connector harness
    /// # Errors
    /// - If the connector could not be resumed
    pub async fn resume(&self) -> anyhow::Result<()> {
        Ok(self.addr.send(crate::Msg::Resume).await?)
    }

    /// Stop the connector harness and returns lingering events
    /// # Errors
    /// - If the connector could not be stopped
    pub async fn stop(mut self) -> anyhow::Result<(Vec<Event>, Vec<Event>)> {
        let (tx, mut rx) = bounded(qsize());
        debug!("Stopping harness...");
        self.addr.stop(tx).await?;
        debug!("Waiting for stop result...");
        let cr = rx
            .recv()
            .await
            .ok_or(GenericImplementationError::ChannelEmpty)?;
        debug!("Stop result received.");
        cr.res?;
        //self.handle.cancel().await;
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

    /// Get a status report of the connector
    /// # Errors
    /// - If the status could not be retrieved
    pub async fn status(&self) -> anyhow::Result<StatusReport> {
        let (report_tx, report_rx) = oneshot::channel();
        self.addr.send(crate::Msg::Report(report_tx)).await?;
        Ok(report_rx.await?)
    }

    /// Wait for the connector to be connected.
    ///
    /// # Errors
    ///
    /// If communication with the connector fails or we time out without reaching connected state.
    pub async fn wait_for_connected(&self) -> anyhow::Result<()> {
        while self.status().await?.connectivity() != &Connectivity::Connected {
            // TODO create my own future here that succeeds on poll when status is connected
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Ensure ticks happening so we can trigger tick based events while waiting
            self.signal_tick_to_sink().await?;
        }
        Ok(())
    }

    /// everytime we boot up and start a sink connector
    /// the runtime will emit a Cb Open and a Cb `SinkStart` message. We consume those here to clear out the pipe.
    ///
    /// # Errors
    /// If we receive different bootup contraflow messages
    pub async fn consume_initial_sink_contraflow(&mut self) -> anyhow::Result<()> {
        for cf in [
            self.get_pipe(IN)?.get_contraflow().await?,
            self.get_pipe(IN)?.get_contraflow().await?,
        ] {
            if !matches!(cf.cb, CbAction::SinkStart(_) | CbAction::Restore) {
                bail!("Expected SinkStart or Open Contraflow message, got: {cf:?}");
            }
        }

        Ok(())
    }

    /// Wait for the connector to reach the given `state`.
    ///
    /// # Errors
    ///
    /// If communication with the connector fails or we time out without reaching the desired state
    pub async fn wait_for_state(&self, state: State) -> anyhow::Result<()> {
        while self.status().await?.status() != &state {
            // TODO create my own future here that succeeds on poll when status is connected
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Get the pipeline connected to a port
    /// # Errors
    /// If there is no pipeline connected to the port
    pub fn get_pipe<T>(&mut self, port: T) -> anyhow::Result<&mut TestPipeline>
    where
        T: Into<Port<'static>>,
    {
        let port = port.into();
        self.pipes
            .get_mut(&port)
            .ok_or_else(|| anyhow::anyhow!("No pipeline connected to port {port}"))
    }

    /// get the out pipeline - if any
    /// # Errors
    /// If there is no pipeline for the out port
    pub fn out(&mut self) -> anyhow::Result<&mut TestPipeline> {
        self.get_pipe(OUT)
    }

    /// get the err pipeline - if any
    /// # Errors
    /// If there is no pipeline for the err port
    pub fn err(&mut self) -> anyhow::Result<&mut TestPipeline> {
        self.get_pipe(ERR)
    }

    /// checks all pipes for events or signals to allow inspecting communications andf errors
    /// # Errors
    /// If an error occures while the event was received
    pub fn get_event(&mut self) -> anyhow::Result<Option<(Port<'static>, Event)>> {
        for (port, p) in &mut self.pipes {
            if let Some(event) = p.try_get_event()? {
                debug!("Got event from {port}: {event:?}");
                return Ok(Some((port.clone(), event)));
            }
        }
        Ok(None)
    }

    /// Send an event to the connector to a specific port
    /// # Errors
    /// If the event could not be sent
    pub async fn send_to_sink_port(&self, event: Event, port: Port<'static>) -> anyhow::Result<()> {
        Ok(self
            .addr
            .send_sink(sink::Msg::Event { event, port })
            .await?)
    }

    /// Send an event to the connector to the `IN` port
    /// # Errors
    /// If the event could not be sent
    pub async fn send_to_sink(&self, event: Event) -> anyhow::Result<()> {
        Ok(self
            .addr
            .send_sink(sink::Msg::Event {
                event,
                port: Port::In,
            })
            .await?)
    }

    /// sends a message to the source
    /// # Errors
    /// If the message could not be sent
    pub fn send_to_source(&self, msg: source::Msg) -> anyhow::Result<()> {
        Ok(self.addr.send_source(msg)?)
    }

    /// Send a signal to the connector
    ///
    /// # Errors
    /// If the signal could not be sent
    pub async fn signal_tick_to_sink(&self) -> anyhow::Result<()> {
        Ok(self
            .addr
            .send_sink(sink::Msg::Signal {
                signal: Event::signal_tick(),
            })
            .await?)
    }

    /// Send a contraflow event to the connector
    /// # Errors
    /// If the contraflow event could not be sent
    pub fn send_contraflow(&self, cb: CbAction, id: EventId) -> anyhow::Result<()> {
        Ok(self.addr.send_source(source::Msg::Cb(cb, id))?)
    }
}

/// A test pipeline
#[derive(Debug)]
pub struct TestPipeline {
    rx: Receiver<Box<dataplane::Msg>>,
    rx_cf: UnboundedReceiver<contraflow::Msg>,
    addr: pipeline::Addr,
}

impl TestPipeline {
    pub(crate) async fn stop(&self) -> anyhow::Result<()> {
        Ok(self.addr.send_mgmt(controlplane::Msg::Stop).await?)
    }
    pub(crate) fn new(alias: String) -> Self {
        let flow_id = alias::Flow::new("test");
        let qsize = qsize();
        let (tx, rx) = bounded(qsize);
        let (tx_cf, rx_cf) = unbounded();
        let (tx_mgmt, mut rx_mgmt) = bounded(qsize);
        let pipeline_id = alias::Pipeline::new(flow_id, alias);
        let addr = pipeline::Addr::new(tx, tx_cf, tx_mgmt, pipeline_id);

        task::spawn(async move {
            while let Some(msg) = rx_mgmt.recv().await {
                match msg {
                    controlplane::Msg::Stop => {
                        break;
                    }
                    controlplane::Msg::ConnectInput { tx, .. } => {
                        if let Err(e) = tx.send(Ok(())).await {
                            error!("Oh no error in test: {e}");
                        }
                    }
                    other => {
                        debug!("Ignoring message: {:?}", other);
                    }
                }
            }
        });

        Self { rx, rx_cf, addr }
    }

    /// get all available contraflow events
    pub fn get_contraflow_events(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Ok(contraflow::Msg::Insight(event)) = self.rx_cf.try_recv() {
            events.push(event);
        }
        events
    }

    /// wait for a contraflow
    /// # Errors
    /// If no contraflow is received within the timeout (20s) or the channel is closed
    pub async fn get_contraflow(&mut self) -> anyhow::Result<Event> {
        match timeout(Duration::from_secs(20), self.rx_cf.recv())
            .await?
            .ok_or(GenericImplementationError::ChannelEmpty)?
        {
            contraflow::Msg::Insight(event) => Ok(event),
        }
    }

    /// get all currently available events from the pipeline
    pub fn get_events(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Ok(msg) = self.rx.try_recv() {
            match *msg {
                dataplane::Msg::Event { event, .. } => {
                    events.push(event.clone());
                }
                dataplane::Msg::Signal(signal) => {
                    debug!("Received signal: {:?}", signal.kind);
                }
            }
        }
        events
    }

    /// try to get a single or event event from the pipeline
    /// # Errors
    /// If the channel is closed
    pub fn try_get_event(&mut self) -> anyhow::Result<Option<Event>> {
        // TODI: this is busy polling we could do this better at one point
        match self.rx.try_recv() {
            Ok(msg) => match *msg {
                dataplane::Msg::Event { event, .. } => Ok(Some(event)),
                dataplane::Msg::Signal(signal) => {
                    debug!("Received signal: {:?}", signal.kind);
                    Ok(Some(signal))
                }
            },
            Err(TryRecvError::Empty) => match self.rx_cf.try_recv() {
                Ok(contraflow::Msg::Insight(event)) => Ok(Some(event)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => {
                    Err(GenericImplementationError::ChannelEmpty.into())
                }
            },
            Err(TryRecvError::Disconnected) => Err(GenericImplementationError::ChannelEmpty.into()),
        }
    }

    /// get a single event from the pipeline
    /// wait for an event to arrive
    /// # Errors
    /// If no event is received within the timeout (120s) or the channel is closed
    pub async fn get_event(&mut self) -> anyhow::Result<Event> {
        const TIMEOUT: Duration = Duration::from_secs(120);
        let start = Instant::now();
        loop {
            match timeout(TIMEOUT, self.rx.recv()).await {
                Ok(Some(msg)) => {
                    match *msg {
                        dataplane::Msg::Event { event, .. } => break Ok(event),
                        // filter out signals
                        dataplane::Msg::Signal(signal) => {
                            debug!("Received signal: {:?}", signal.kind);
                        }
                    }
                }
                Ok(None) => {
                    return Err(GenericImplementationError::ChannelEmpty.into());
                }
                Err(_) => {
                    return Err(GenericImplementationError::Timeout(TIMEOUT).into());
                }
            }
            if start.elapsed() > TIMEOUT {
                return Err(GenericImplementationError::Timeout(TIMEOUT).into());
            }
        }
    }

    /// expect no event for a given duration
    /// # Errors
    /// If an event is received within the duration
    pub async fn expect_no_event_for(&mut self, duration: Duration) -> anyhow::Result<()> {
        let start = Instant::now();
        loop {
            match timeout(duration, self.rx.recv()).await {
                Err(_timeout_error) => {
                    return Ok(());
                }
                Ok(Some(msg)) => match *msg {
                    dataplane::Msg::Signal(_signal) => (),
                    dataplane::Msg::Event { event, .. } => {
                        return Err(anyhow::anyhow!(
                            "Expected no event for {duration:?}, got: {event:?}"
                        ));
                    }
                },
                Ok(None) => {
                    return Err(GenericImplementationError::ChannelEmpty.into());
                }
            }
            if start.elapsed() > duration {
                return Ok(());
            }
        }
    }
}
