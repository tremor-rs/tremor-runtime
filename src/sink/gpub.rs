// Copyright 2020-2021, The Tremor Team
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

//! # File Offramp
//!
//! Writes events to a file, one event per line
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

#![cfg(not(tarpaulin_include))]

use crate::connectors::gcp::{pubsub, pubsub_auth};
use crate::connectors::qos::{self, QoSFacilities, SinkQoS};
use crate::sink::prelude::*;
use futures::executor::block_on;
use googapis::google::pubsub::v1::publisher_client::PublisherClient;
use halfbrown::HashMap;
use tonic::transport::Channel;
use tremor_pipeline::{EventIdGenerator, OpMeta};
use tremor_value::Value;

pub struct GoogleCloudPubSub {
    #[allow(dead_code)]
    config: Config,
    remote: Option<PublisherClient<Channel>>,
    is_down: bool,
    qos_facility: Box<dyn SinkQoS>,
    reply_channel: Option<Sender<sink::Reply>>,
    is_linked: bool,
    postprocessors: Postprocessors,
    codec: Box<dyn Codec>,
    event_id_gen: EventIdGenerator,
}

#[derive(Deserialize)]
pub struct Config {}

enum PubSubCommand {
    SendMessage(String, String, Value<'static>),
    Unknown,
}

impl std::fmt::Display for PubSubCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                PubSubCommand::SendMessage(_, _, _) => "send_message",
                PubSubCommand::Unknown => "Unknown",
            }
        )
    }
}

impl ConfigImpl for Config {}

impl offramp::Impl for GoogleCloudPubSub {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let remote = Some(block_on(pubsub_auth::setup_publisher_client())?);
            let hostport = "pubsub.googleapis.com:443";
            Ok(SinkManager::new_box(Self {
                config,
                remote,
                is_down: false,
                qos_facility: Box::new(QoSFacilities::recoverable(hostport.to_string())),
                reply_channel: None,
                is_linked: false,
                postprocessors: vec![],
                codec: Box::new(crate::codec::null::Null {}),
                event_id_gen: EventIdGenerator::new(0), // Fake ID overwritten in init
            }))
        } else {
            Err("Offramp Google Cloud Pubsub (pub) requires a config".into())
        }
    }
}

macro_rules! parse_arg {
    ($field_name: expr, $o: expr) => {
        if let Some(Value::String(snot)) = $o.get($field_name) {
            snot.to_string()
        } else {
            return Err(format!("Invalid Command, expected `{}` field", $field_name).into());
        }
    };
}

fn parse_command(value: &Value) -> Result<PubSubCommand> {
    if let Value::Object(o) = value {
        let cmd_name: &str = &parse_arg!("command", o);
        let command = match cmd_name {
            "send_message" => PubSubCommand::SendMessage(
                parse_arg!("project", o),
                parse_arg!("topic", o),
                if let Some(data) = o.get("data") {
                    data.clone().into_static()
                } else {
                    return Err("Invalid Command, expected `data` field".into());
                },
            ),
            _ => PubSubCommand::Unknown,
        };
        return Ok(command);
    }

    Err("Invalid Command".into())
}

#[async_trait::async_trait]
impl Sink for GoogleCloudPubSub {
    async fn terminate(&mut self) {}

    #[allow(clippy::too_many_lines)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        if self.remote.is_none() {
            self.remote = Some(pubsub_auth::setup_publisher_client().await?);
            // TODO - Qos checks
        };
        if let Some(remote) = &mut self.remote {
            let maybe_correlation = event.correlation_meta();
            let mut response = Vec::new();
            for value in event.value_iter() {
                let command = parse_command(value)?;
                match command {
                    PubSubCommand::SendMessage(project_id, topic_name, data) => {
                        let mut msg: Vec<u8> = vec![];
                        let encoded = codec.encode(&data)?;
                        let mut processed =
                            postprocess(&mut self.postprocessors, event.ingest_ns, encoded)?;
                        for processed_elem in &mut processed {
                            msg.append(processed_elem);
                        }
                        response.push(make_command_response(
                            "send_message",
                            &pubsub::send_message(remote, &project_id, &topic_name, &msg).await?,
                        ));
                    }

                    PubSubCommand::Unknown => {
                        warn!(
                            "Unknown Google Cloud PubSub command: `{}` attempted",
                            command.to_string()
                        );
                        return Err(format!(
                            "Unknown Google Cloud PubSub command: `{}` attempted",
                            command.to_string()
                        )
                        .into());
                    }
                };
            }

            if self.is_linked {
                if let Some(reply_channel) = &self.reply_channel {
                    let mut meta = Object::with_capacity(1);
                    if let Some(correlation) = maybe_correlation {
                        meta.insert_nocheck("correlation".into(), correlation);
                    }

                    reply_channel
                        .send(sink::Reply::Response(
                            OUT,
                            Event {
                                id: self.event_id_gen.next_id(),
                                data: (response, meta).into(),
                                ingest_ns: nanotime(),
                                origin_uri: Some(EventOriginUri {
                                    uid: 0,
                                    scheme: "gRPC".into(),
                                    host: "".into(),
                                    port: None,
                                    path: vec![],
                                }),
                                kind: None,
                                is_batch: false,
                                cb: CbAction::None,
                                op_meta: OpMeta::default(),
                                transactional: false,
                            },
                        ))
                        .await?;
                }
            }
        }

        self.is_down = false;
        return Ok(Some(vec![qos::ack(&mut event)]));
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        _sink_url: &TremorUrl,
        codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.event_id_gen = EventIdGenerator::new(sink_uid);
        self.postprocessors = make_postprocessors(processors.post)?;
        self.reply_channel = Some(reply_channel);
        self.codec = codec.boxed_clone();
        self.is_linked = is_linked;
        Ok(())
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.is_down && self.qos_facility.probe(signal.ingest_ns) {
            self.is_down = false;
            // This means the port is connectable
            info!("Google Cloud PubSub -  sink remote endpoint - recovered and contactable");
            self.is_down = false;
            // Clone needed to make it mutable, lint is wrong
            #[allow(clippy::redundant_clone)]
            let mut signal = signal.clone();
            return Ok(Some(vec![qos::open(&mut signal)]));
        }

        Ok(None)
    }

    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
}

fn make_command_response(cmd: &str, message_id: &str) -> Value<'static> {
    literal!({
        "command": cmd,
        "message-id": message_id
    })
    .into_static()
}
