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

use crate::connectors::gcp::{auth, storage};
use crate::connectors::qos::{self, QoSFacilities, SinkQoS};
use crate::sink::prelude::*;
use futures::executor::block_on;
use halfbrown::HashMap;
use http::HeaderMap;
use reqwest::Client;
use simd_json::json;
use tremor_pipeline::{EventId, OpMeta};
use tremor_value::Value;

pub struct GoogleCloudStorage {
    config: Config,
    remote: Option<Client>,
    is_down: bool,
    qos_facility: Box<dyn SinkQoS>,
    reply_channel: Option<Sender<sink::Reply>>,
    is_linked: bool,
}

#[derive(Deserialize)]
pub struct Config {
    pem: String,
}

enum StorageCommand {
    Create(String, String),
    Add(String, String, String),
    Remove(String, String),
    ListBuckets(String),
    Fetch(String, String),
    Download(String, String),
    Delete(String),
    ListObjects(String),
    Unknown,
}

impl std::fmt::Display for StorageCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                StorageCommand::Create(_, _) => "create_bucket",
                StorageCommand::Add(_, _, _) => "add_object",
                StorageCommand::Remove(_, _) => "remove_object",
                StorageCommand::ListBuckets(_) => "list_buckets",
                StorageCommand::Fetch(_, _) => "get_object",
                StorageCommand::Download(_, _) => "download_object",
                StorageCommand::Delete(_) => "delete_bucket",
                StorageCommand::ListObjects(_) => "list_objects",
                StorageCommand::Unknown => "Unknown",
            }
        )
    }
}

impl ConfigImpl for Config {}

impl offramp::Impl for GoogleCloudStorage {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let headers = HeaderMap::new();
            let remote = Some(block_on(auth::json_api_client(&config.pem, &headers))?);
            let hostport = "storage.googleapis.com:443";
            Ok(SinkManager::new_box(Self {
                config,
                remote,
                is_down: false,
                qos_facility: Box::new(QoSFacilities::recoverable(hostport.to_string())),
                reply_channel: None,
                is_linked: false,
            }))
        } else {
            Err("Offramp Google Cloud Storage requires a config".into())
        }
    }
}

fn parse_command(value: &Value<'_>) -> Result<StorageCommand> {
    if let Value::Object(o) = value {
        let cmd_name: &str = if let Some(Value::String(cmd_name)) = o.get("command") {
            cmd_name
        } else {
            return Err("Invalid Command, expected `command` field".into());
        };

        let command = match cmd_name {
            "fetch" => StorageCommand::Fetch(
                if let Some(Value::String(bucket_name)) = o.get("bucket") {
                    bucket_name.to_string()
                } else {
                    return Err("Invalid Command, expected `bucket` field".into());
                },
                if let Some(Value::String(object_name)) = o.get("object") {
                    object_name.to_string()
                } else {
                    return Err("Invalid Command, expected `object` field".into());
                },
            ),
            "list_buckets" => StorageCommand::ListBuckets(
                if let Some(Value::String(project_id)) = o.get("project_id") {
                    project_id.to_string()
                } else {
                    return Err("Invalid Command, expected `project_id` field".into());
                },
            ),
            "list_objects" => StorageCommand::ListObjects(
                if let Some(Value::String(bucket_name)) = o.get("bucket") {
                    bucket_name.to_string()
                } else {
                    return Err("Invalid Command, expected `bucket` field".into());
                },
            ),
            "add_object" => StorageCommand::Add(
                if let Some(Value::String(bucket_name)) = o.get("bucket") {
                    bucket_name.to_string()
                } else {
                    return Err("Invalid Command, expected `bucket` field".into());
                },
                if let Some(Value::String(object_name)) = o.get("object") {
                    object_name.to_string()
                } else {
                    return Err("Invalid Command, expected `object` field".into());
                },
                if let Some(Value::String(file_path)) = o.get("file_path") {
                    file_path.to_string()
                } else {
                    return Err("Invalid Command, expected `file_path` field".into());
                },
            ),
            "remove_object" => StorageCommand::Remove(
                if let Some(Value::String(bucket_name)) = o.get("bucket") {
                    bucket_name.to_string()
                } else {
                    return Err("Invalid Command, expected `bucket` field".into());
                },
                if let Some(Value::String(object_name)) = o.get("object") {
                    object_name.to_string()
                } else {
                    return Err("Invalid Command, expected `object` field".into());
                },
            ),
            "create_bucket" => StorageCommand::Create(
                if let Some(Value::String(project_id)) = o.get("project_id") {
                    project_id.to_string()
                } else {
                    return Err("Invalid Command, expected `project_id` field".into());
                },
                if let Some(Value::String(bucket_name)) = o.get("bucket") {
                    bucket_name.to_string()
                } else {
                    return Err("Invalid Command, expected `bucket` field".into());
                },
            ),
            "delete_bucket" => {
                StorageCommand::Delete(if let Some(Value::String(bucket_name)) = o.get("bucket") {
                    bucket_name.to_string()
                } else {
                    return Err("Invalid Command, expected `bucket` field".into());
                })
            }
            "download_object" => StorageCommand::Download(
                if let Some(Value::String(bucket_name)) = o.get("bucket") {
                    bucket_name.to_string()
                } else {
                    return Err("Invalid Command, expected `bucket` field".into());
                },
                if let Some(Value::String(object_name)) = o.get("object") {
                    object_name.to_string()
                } else {
                    return Err("Invalid Command, expected `object` field".into());
                },
            ),
            _ => StorageCommand::Unknown,
        };
        return Ok(command);
    }

    Err("Invalid Command".into())
}

#[async_trait::async_trait]
impl Sink for GoogleCloudStorage {
    async fn terminate(&mut self) {}

    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        let remote = if let Some(remote) = &self.remote {
            remote
        } else {
            self.remote = Some(auth::json_api_client(&self.config.pem, &HeaderMap::new()).await?);
            let remote = self.remote.as_ref().ok_or("Client error!")?;
            remote
            // TODO - Qos checks
        };
        for value in event.value_iter() {
            let command = parse_command(value)?;
            let command_str = command.to_string();
            let response = match command {
                StorageCommand::Fetch(bucket_name, object_name) => {
                    storage::get_object(&remote, &bucket_name, &object_name).await?
                }
                StorageCommand::ListBuckets(project_id) => {
                    storage::list_buckets(&remote, &project_id).await?
                }
                StorageCommand::ListObjects(bucket_name) => {
                    storage::list_objects(&remote, &bucket_name).await?
                }
                StorageCommand::Add(bucket_name, object, file_path) => {
                    storage::add_object(&remote, &bucket_name, &object, &file_path).await?
                }
                StorageCommand::Remove(bucket_name, object) => {
                    storage::delete_object(&remote, &bucket_name, &object).await?
                }
                StorageCommand::Create(project_id, bucket_name) => {
                    storage::create_bucket(&remote, &project_id, &bucket_name).await?
                }
                StorageCommand::Delete(bucket_name) => {
                    storage::delete_bucket(&remote, &bucket_name).await?
                }
                StorageCommand::Download(bucket_name, object_name) => {
                    storage::download_object(&remote, &bucket_name, &object_name).await?
                }
                StorageCommand::Unknown => {
                    warn!(
                        "Unknown Google Cloud Storage command: `{}` attempted",
                        command.to_string()
                    );

                    return Err(format!(
                        "Unknown Google Cloud Storage command: `{}` attempted",
                        command.to_string()
                    )
                    .into());
                }
            };
            let response: Value = json!({
                "cmd": command_str,
                "data": response
            })
            .into();
            if self.is_linked {
                if let Some(reply_channel) = &self.reply_channel {
                    reply_channel
                        .send(sink::Reply::Response(
                            OUT,
                            Event {
                                id: EventId::new(1, 2, 3),
                                data: LineValue::new(vec![], |_| ValueAndMeta::from(response)),
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
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.reply_channel = Some(reply_channel);
        self.is_linked = is_linked;
        Ok(())
    }
    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.is_down && self.qos_facility.probe(signal.ingest_ns) {
            self.is_down = false;
            // This means the port is connectable
            info!("Google Cloud Storage -  sink remote endpoint - recovered and contactable");
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
