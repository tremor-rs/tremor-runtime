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
use crate::onramp::prelude::*;
use crate::pipeline;
use crate::repository::ServantId;
use crate::url::TremorURL;
use async_std::sync::{self, channel};
use async_std::task::{self, JoinHandle};
use crossbeam_channel::Sender as CbSender;
use serde_yaml::Value;
use std::fmt;
use std::time::Duration;
use tremor_pipeline::EventOriginUri;
use tremor_script::LineValue;

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
// mod rest;
mod ws;

pub(crate) type Sender = sync::Sender<ManagerMsg>;

pub(crate) trait Impl {
    fn from_config(id: &TremorURL, config: &Option<Value>) -> Result<Box<dyn Onramp>>;
}

#[derive(Clone, Debug)]
pub enum Msg {
    Connect(Vec<(TremorURL, pipeline::Addr)>),
    Disconnect { id: TremorURL, tx: CbSender<bool> },
    Cb(CBAction, Ids),
}

pub type Addr = sync::Sender<Msg>;

#[async_trait::async_trait]
pub(crate) trait Onramp: Send {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<Addr>;
    fn default_codec(&self) -> &str;
}

pub(crate) enum SourceState {
    Connected,
    Disconnected,
}
pub(crate) enum SourceReply {
    Data {
        origin_uri: EventOriginUri,
        data: Vec<u8>,
        stream: usize,
    },
    Structured {
        origin_uri: EventOriginUri,
        data: LineValue,
    },
    StartStream(usize),
    EndStream(usize),
    StateChange(SourceState),
    Empty(u64),
}
#[async_trait::async_trait]
pub(crate) trait Source {
    async fn read(&mut self, id: u64) -> Result<SourceReply>;
    async fn init(&mut self) -> Result<SourceState>;
    fn id(&self) -> &TremorURL;
    fn trigger_breaker(&mut self) {}
    fn restore_breaker(&mut self) {}
    fn ack(&mut self, id: u64) {
        let _ = id;
    }
    fn fail(&mut self, id: u64) {
        let _ = id;
    }
}

pub(crate) struct SourceManager<T>
where
    T: Source,
{
    source_id: TremorURL,
    source: T,
    rx: Receiver<onramp::Msg>,
    tx: sync::Sender<onramp::Msg>,
    pp_template: Vec<String>,
    preprocessors: Vec<Option<Preprocessors>>,
    codec: Box<dyn Codec>,
    metrics_reporter: RampReporter,
    triggered: bool,
    pipelines: Vec<(TremorURL, pipeline::Addr)>,
    id: u64,
    /// Unique Id for the source
    uid: u64,
}

impl<T> SourceManager<T>
where
    T: Source + Send + 'static + std::fmt::Debug,
{
    fn handle_pp(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        data: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut data = vec![data];
        let mut data1 = Vec::new();
        if let Some(pps) = self.preprocessors.get_mut(stream).and_then(Option::as_mut) {
            for pp in pps {
                data1.clear();
                for (i, d) in data.iter().enumerate() {
                    match pp.process(ingest_ns, d) {
                        Ok(mut r) => data1.append(&mut r),
                        Err(e) => {
                            error!("Preprocessor[{}] error {}", i, e);
                            return Err(e);
                        }
                    }
                }
                std::mem::swap(&mut data, &mut data1);
            }
        }
        Ok(data)
    }
    // We are borrowing a dyn box as we don't want to pass ownership.
    fn send_event(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        origin_uri: &tremor_pipeline::EventOriginUri,
        data: Vec<u8>,
    ) {
        let original_id = self.id;
        let mut error = false;
        // FIXME record 1st id for pp data
        if let Ok(data) = self.handle_pp(stream, ingest_ns, data) {
            for d in data {
                match self.codec.decode(d, *ingest_ns) {
                    Ok(Some(data)) => {
                        error |= self.transmit_event(data, *ingest_ns, origin_uri.clone());
                    }
                    Ok(None) => (),
                    Err(e) => {
                        self.metrics_reporter.increment_error();
                        error!("[Codec] {}", e);
                    }
                }
            }
        } else {
            // record preprocessor failures too
            self.metrics_reporter.increment_error();
        };
        // We ONLY fail on transmit errors as preprocessor errors might be
        // problematic
        if error {
            self.source.fail(original_id);
        }
    }
    fn handle_pipelines_msg(&mut self, msg: onramp::Msg) -> Result<PipeHandlerResult> {
        if self.pipelines.is_empty() {
            match msg {
                onramp::Msg::Connect(ps) => {
                    for p in &ps {
                        if p.0 == *METRICS_PIPELINE {
                            self.metrics_reporter.set_metrics_pipeline(p.clone());
                        } else {
                            p.1.send(pipeline::Msg::ConnectOnramp(
                                self.source_id.clone(),
                                self.tx.clone(),
                            ))?;
                            self.pipelines.push(p.clone());
                        }
                    }
                    Ok(PipeHandlerResult::Retry)
                }
                onramp::Msg::Disconnect { tx, .. } => {
                    tx.send(true)?;
                    Ok(PipeHandlerResult::Terminate)
                }
                onramp::Msg::Cb(cb, ids) => Ok(PipeHandlerResult::Cb(cb, ids)),
            }
        } else {
            match msg {
                onramp::Msg::Connect(mut ps) => {
                    for p in &ps {
                        p.1.send(pipeline::Msg::ConnectOnramp(
                            self.source_id.clone(),
                            self.tx.clone(),
                        ))?;
                    }
                    self.pipelines.append(&mut ps);
                    Ok(PipeHandlerResult::Normal)
                }
                onramp::Msg::Disconnect { id, tx } => {
                    for (pid, p) in &self.pipelines {
                        if pid == &id {
                            p.send(pipeline::Msg::DisconnectInput(id.clone()))?;
                        }
                    }
                    self.pipelines.retain(|(pipeline, _)| pipeline != &id);
                    if self.pipelines.is_empty() {
                        tx.send(true)?;
                        Ok(PipeHandlerResult::Terminate)
                    } else {
                        tx.send(false)?;
                        Ok(PipeHandlerResult::Normal)
                    }
                }
                onramp::Msg::Cb(cb, ids) => Ok(PipeHandlerResult::Cb(cb, ids)),
            }
        }
    }

    async fn handle_pipelines2(&mut self) -> Result<PipeHandlerResult> {
        if self.pipelines.is_empty() || self.triggered {
            let msg = self.rx.recv().await?;
            self.handle_pipelines_msg(msg)
        } else if self.rx.is_empty() {
            Ok(PipeHandlerResult::Normal)
        } else {
            let msg = self.rx.recv().await?;
            self.handle_pipelines_msg(msg)
        }
    }

    pub(crate) fn transmit_event(
        &mut self,
        data: LineValue,
        ingest_ns: u64,
        origin_uri: EventOriginUri,
    ) -> bool {
        // We only try to send here since we can't guarantee
        // that nothing else has send (and overfilled) the pipelines
        // inbox.
        // We try to avoid this situation by checking but given
        // we can't coordinate w/ other onramps we got to
        // ensure that we are ready to discard messages and prioritize
        // progress.
        //
        // Notably in a Guaranteed delivery scenario those discarded
        let event = Event {
            id: Ids::new(self.uid, self.id),
            data,
            ingest_ns,
            // TODO make origin_uri non-optional here too?
            origin_uri: Some(origin_uri),
            ..Event::default()
        };
        let mut error = false;
        self.id += 1;
        if let Some(((input, addr), pipelines)) = self.pipelines.split_last() {
            self.metrics_reporter.periodic_flush(ingest_ns);
            self.metrics_reporter.increment_out();

            for (input, addr) in pipelines {
                if let Some(input) = input.instance_port() {
                    if let Err(e) = addr.try_send(pipeline::Msg::Event {
                        input: input.to_string().into(),
                        event: event.clone(),
                    }) {
                        error!("[Onramp] failed to send to pipeline: {}", e);
                        error = true;
                    }
                }
            }
            if let Some(input) = input.instance_port() {
                if let Err(e) = addr.try_send(pipeline::Msg::Event {
                    input: input.to_string().into(),
                    event,
                }) {
                    error!("[Onramp] failed to send to pipeline: {}", e);
                    error = true;
                }
            }
        }
        error
    }
    async fn new(
        uid: u64,
        mut source: T,
        preprocessors: &[String],
        codec: &str,
        metrics_reporter: RampReporter,
    ) -> Result<(Self, sync::Sender<onramp::Msg>)> {
        let (tx, rx) = channel(1);
        let codec = codec::lookup(&codec)?;
        let pp_template = preprocessors.to_vec();
        let preprocessors = vec![Some(make_preprocessors(&pp_template)?)];
        source.init().await?;
        Ok((
            Self {
                source_id: source.id().clone(),
                pp_template,
                source,
                rx,
                tx: tx.clone(),
                preprocessors,
                codec,
                metrics_reporter,
                triggered: false,
                id: 0,
                pipelines: Vec::new(),
                uid,
            },
            tx,
        ))
    }

    async fn start(
        uid: u64,
        source: T,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let name = source.id().short_id("src");
        let (manager, tx) =
            SourceManager::new(uid, source, preprocessors, codec, metrics_reporter).await?;
        thread::Builder::new()
            .name(name)
            .spawn(move || task::block_on(manager.run()))?;
        Ok(tx)
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            match self.handle_pipelines2().await? {
                PipeHandlerResult::Retry => continue,
                PipeHandlerResult::Terminate => return Ok(()),
                PipeHandlerResult::Normal => (),
                PipeHandlerResult::Cb(CBAction::Fail, ids) => {
                    if let Some(id) = ids.get(self.uid) {
                        self.source.fail(id);
                    }
                }
                PipeHandlerResult::Cb(CBAction::Ack, ids) => {
                    if let Some(id) = ids.get(self.uid) {
                        self.source.ack(id);
                    }
                }
                PipeHandlerResult::Cb(CBAction::Trigger, _ids) => {
                    // FIXME eprintln!("triggered for: {:?}", self.source);
                    self.source.trigger_breaker();
                    self.triggered = true
                }
                PipeHandlerResult::Cb(CBAction::Restore, _ids) => {
                    // FIXME eprintln!("restored for: {:?}", self.source);
                    self.source.restore_breaker();
                    self.triggered = false
                }
            }

            if !self.triggered
                && !self.pipelines.is_empty()
                && self.pipelines.iter().all(|(_, p)| p.ready())
            {
                match self.source.read(self.id).await? {
                    SourceReply::StartStream(id) => {
                        while self.preprocessors.len() <= id {
                            self.preprocessors.push(None)
                        }

                        self.preprocessors
                            .push(Some(make_preprocessors(&self.pp_template)?));
                    }
                    SourceReply::EndStream(id) => {
                        if let Some(v) = self.preprocessors.get_mut(id) {
                            *v = None
                        }

                        while let Some(None) = self.preprocessors.last() {
                            self.preprocessors.pop();
                        }
                    }
                    SourceReply::Structured { origin_uri, data } => {
                        let ingest_ns = nanotime();

                        self.transmit_event(data, ingest_ns, origin_uri);
                    }
                    SourceReply::Data {
                        mut origin_uri,
                        data,
                        stream,
                    } => {
                        origin_uri.maybe_set_uid(self.uid);
                        let mut ingest_ns = nanotime();
                        self.send_event(stream, &mut ingest_ns, &origin_uri, data);
                    }
                    SourceReply::StateChange(SourceState::Disconnected) => return Ok(()),
                    SourceReply::StateChange(SourceState::Connected) => (),
                    SourceReply::Empty(sleep_ms) => {
                        task::sleep(Duration::from_millis(sleep_ms)).await
                    }
                }
            }
        }
    }
}

// just a lookup
#[cfg_attr(tarpaulin, skip)]
pub(crate) fn lookup(
    name: &str,
    id: &TremorURL,
    config: &Option<Value>,
) -> Result<Box<dyn Onramp>> {
    match name {
        "blaster" => blaster::Blaster::from_config(id, config),
        "file" => file::File::from_config(id, config),
        #[cfg(feature = "gcp")]
        "gsub" => gsub::GSub::from_config(id, config),
        "kafka" => kafka::Kafka::from_config(id, config),
        "postgres" => postgres::Postgres::from_config(id, config),
        "metronome" => metronome::Metronome::from_config(id, config),
        "crononome" => crononome::Crononome::from_config(id, config),
        "udp" => udp::Udp::from_config(id, config),
        "tcp" => tcp::Tcp::from_config(id, config),
        // "rest" => rest::Rest::from_config(config),
        "ws" => ws::Ws::from_config(id, config),
        _ => Err(format!("[onramp:{}] Onramp type {} not known", id, name).into()),
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
pub(crate) enum ManagerMsg {
    Create(async_std::sync::Sender<Result<Addr>>, Box<Create>),
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
            let mut onramp_uid: u64 = 0;
            info!("Onramp manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping onramps...");
                        break;
                    }
                    Ok(ManagerMsg::Create(r, c)) => {
                        let Create {
                            codec,
                            mut stream,
                            preprocessors,
                            metrics_reporter,
                            id,
                        } = *c;
                        onramp_uid += 1;
                        match stream
                            .start(onramp_uid, &codec, &preprocessors, metrics_reporter)
                            .await
                        {
                            Ok(addr) => {
                                info!("Onramp {} started.", id);
                                r.send(Ok(addr)).await
                            }
                            Err(e) => error!("Creating an onramp failed: {}", e),
                        }
                    }
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
            Ok(_x) => true,
            _otherwise => false,
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
