// Copyright 2024, The Tremor Team
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

// #![cfg_attr(coverage, no_coverage)] // NOTE We let this hang wet - no Azure env for CI / coverage

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use log::error;
use tremor_config::NameWithConfig;

use crate::monitor::ingest::writer::RequestBuilder;
use crate::rest::extract_request_meta;
use crate::rest::extract_response_meta;
use crate::rest::RequestId;

use super::Config;
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;
use tremor_common::time::nanotime;
use tremor_connectors::{
    sink::{concurrency_cap::ConcurrencyCap, prelude::*},
    source::SourceReply,
};
use tremor_script::prelude::MutableObject;
use tremor_script::prelude::{literal, ValueObjectAccess};
use tremor_script::EventOriginUri;

pub(crate) struct AmiSink {
    request_counter: u64,
    client: Option<Arc<dyn azure_core::HttpClient>>,
    config: Config,
    response_tx: Sender<SourceReply>,
    reply_tx: ReplySender,
    concurrency_cap: ConcurrencyCap,
    origin_uri: EventOriginUri,
    // we should only send responses down the channel when we know there is a source consuming them
    // otherwise the channel would fill up and we'd be stuck
    // TODO: find/implement a channel that just throws away the oldest message when it is full, like a ring-buffer
    source_is_connected: Arc<AtomicBool>,
    serializer: EventSerializer,
}

impl AmiSink {
    pub fn new(
        response_tx: Sender<SourceReply>,
        reply_tx: ReplySender,
        config: Config,
        source_is_connected: Arc<AtomicBool>,
    ) -> anyhow::Result<Self> {
        let concurrency_cap = ConcurrencyCap::new(config.concurrency, reply_tx.clone());

        Ok(Self {
            request_counter: 1, // always start by 1, 0 is DEFAULT_STREAM_ID and this might interfere with custom codecs
            response_tx,
            reply_tx,
            client: None,
            config,
            origin_uri: EventOriginUri {
                scheme: String::from("azure_monitor_ingest_writer"),
                host: String::from("dummy"), // will be replaced in `on_event`
                port: None,
                path: vec![],
            },
            concurrency_cap,
            source_is_connected,
            serializer: json_serializer()?,
        })
    }
}

fn json_serializer() -> anyhow::Result<EventSerializer> {
    EventSerializer::new(
        Some(NameWithConfig {
            name: "json".to_string(),
            config: None,
        }),
        CodecReq::Required,
        vec![],
        &ConnectorType::from("azure_monitor_ingest_writer"),
        &tremor_common::alias::Connector::new(
            "azure_monitor_ingest_writer", // TODO This doesn't adapt well to structured where codecs/processors are needed
            "azure_monitor_ingest_writer",
        ),
    )
}

#[async_trait::async_trait]
impl Sink for AmiSink {
    async fn connect(&mut self, _ctx: &SinkContext, _attempt: &Attempt) -> anyhow::Result<bool> {
        let azure_client = azure_core::new_http_client();
        self.client = Some(azure_client);

        Ok(true)
    }

    #[allow(clippy::too_many_lines)]
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        start: u64,
    ) -> anyhow::Result<SinkReply> {
        let guard = self.concurrency_cap.inc_for(&event);

        if let Some(client) = self.client.clone() {
            let task_ctx = ctx.clone();

            let response_tx = self
                .source_is_connected
                .load(Ordering::Acquire)
                .then(|| self.response_tx.clone());
            let reply_tx = self.reply_tx.clone();

            let contraflow_data = if event.transactional {
                Some(ContraflowData::from(&event))
            } else {
                None
            };

            let mut origin_uri = self.origin_uri.clone();

            // assign a unique request id to this event
            let request_id = RequestId::new(self.request_counter);
            self.request_counter = self.request_counter.wrapping_add(1).max(1);

            let mut builder = None;
            let mut correlation_meta = None;

            for (value, meta) in event.value_meta_iter() {
                match &mut builder {
                    None => {
                        // take the metadata from the first element of the batch
                        correlation_meta = meta.get("correlation").map(Value::clone_static);
                        let http_meta = ctx.extract_meta(meta);
                        let mut once_per_request =
                            RequestBuilder::new(request_id, http_meta, &mut self.config).await?;
                        once_per_request
                            .append(value, meta, start, &mut self.serializer)
                            .await?;
                        builder = Some(once_per_request);
                    }
                    Some(builder) => {
                        builder
                            .append(value, meta, start, &mut self.serializer)
                            .await?;
                    }
                }
            }

            let t = self
                .config
                .timeout
                .map_or_else(|| Duration::from_secs(60), Duration::from_nanos);

            let request = if let Some(builder) = &mut builder {
                builder.finalize(&mut self.serializer)?;
                builder.take_request()
            } else {
                error!("{ctx} Cannot convert event to azure request error.");
                return Ok(SinkReply::FAIL);
            };

            let req_meta = extract_request_meta(&request)?;

            // spawn
            // spawn the sending task
            tokio::task::spawn(async move {
                // extract request meta for the response metadata from the finally prepared request
                // the actual sent request might differ from the metadata used to create this request
                let url = request.url();
                if let Some(host) = url.host() {
                    origin_uri.host = host.to_string();
                }
                origin_uri.port = url.port();
                origin_uri.path = url.path().split('/').map(ToString::to_string).collect();
                let response = client.execute_request(&request);

                match timeout(t, response).await {
                    Ok(Ok(response)) => {
                        let (data, response_meta) = extract_response_meta(response).await?;

                        if let Some(response_tx) = response_tx {
                            let mut meta = task_ctx.meta(literal!({
                                "request": std::convert::Into::<Value>::into(req_meta),
                                "request_id": request_id.get(),
                                // NOTE Azure always returns 204 here, even for malformed requests
                                // so we don't inspect or track the response at this time. Hopefully
                                // a future ingest API for data collection endpoints will be a little
                                // less cromulent in the future...
                                //
                                // "response": std::convert::Into::<Value>::into(response_meta),
                                "content-length": response_meta.content_length,
                            }));

                            if let Some(corr_meta) = correlation_meta {
                                // NOTE correlation as yet untested - this may change before release
                                // pending experimentation
                                meta.try_insert("correlation", corr_meta);
                            }

                            let reply = SourceReply::Data {
                                origin_uri,
                                data,
                                meta: Some(meta),
                                stream: None, // a response (as well as a request) is a discrete unit and not part of a stream
                                port: None,
                                codec_overwrite: None,
                            };
                            task_ctx.swallow_err(
                                response_tx.send(reply).await,
                                "Error sending response to source",
                            );
                        }
                        if let Some(contraflow_data) = contraflow_data {
                            task_ctx.swallow_err(
                                reply_tx
                                    .send(AsyncSinkReply::Ack(contraflow_data, nanotime() - start)),
                                "Error sending ack contraflow",
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        error!("{task_ctx} Error sending HTTP request: {e}");
                        if let Some(contraflow_data) = contraflow_data {
                            task_ctx.swallow_err(
                                reply_tx.send(AsyncSinkReply::Fail(contraflow_data)),
                                "Error sending fail contraflow",
                            );
                        }
                        // We force a reconnect as otherwise the HTTP library can become stale and never progress when sending failed.
                        task_ctx.notifier().connection_lost().await?;
                    }
                    Err(e) => {
                        error!("{task_ctx} Error sending HTTP request: {e}");
                        if let Some(contraflow_data) = contraflow_data {
                            task_ctx.swallow_err(
                                reply_tx.send(AsyncSinkReply::Fail(contraflow_data)),
                                "Error sending fail contraflow",
                            );
                        }
                        // task_ctx.notifier().connection_lost().await?;
                    }
                }
                drop(guard);
                anyhow::Ok(())
            });
        } else {
            error!("{ctx} No azure http client available.");
            return Ok(SinkReply::FAIL);
        }

        Ok(SinkReply::NONE)
    }

    fn asynchronous(&self) -> bool {
        true
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_json_serializer() -> anyhow::Result<()> {
        let mut serializer = json_serializer()?;
        let got = serializer
            .serialize(&literal!({ "foo": "bar" }), &literal!(null), 0)
            .await?;
        assert_eq!(r#"{"foo":"bar"}"#, std::str::from_utf8(&got[0])?);
        Ok(())
    }
}
