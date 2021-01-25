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

//! # Elastic Search Offramp
//!
//! The Elastic Search Offramp uses batch writes to send data to elastic search
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Input Metadata Variables
//!   * `index` - index to write to (required)
//!   * `doc-type` - document type for the event (required)
//!   * `pipeline` - pipeline to use
//!
//! ## Outputs
//!
//! The 1st additional output is used to send divert messages that can not be
//! enqueued due to overload

#![cfg(not(tarpaulin_include))]

use crate::postprocessor::Postprocessors;
use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use async_std::task::JoinHandle;
use elastic::{
    client::responses::bulk::{ErrorItem, OkItem},
    prelude::*,
};
use halfbrown::HashMap;
use simd_json::json;
use std::str;
use std::time::Instant;
use tremor_pipeline::{EventId, EventIdGenerator, OpMeta};
use tremor_script::prelude::*;
use tremor_script::Object;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub nodes: Vec<String>,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "concurrency")]
    pub concurrency: usize,
}
fn concurrency() -> usize {
    4
}
impl ConfigImpl for Config {}

pub struct Elastic {
    sink_url: TremorURL,
    client: SyncClient,
    queue: AsyncSink<u64>,
    postprocessors: Postprocessors,
    insight_tx: Sender<sink::Reply>,
    is_linked: bool,
    response_sender: Sender<(LineValue, Cow<'static, str>)>,
    response_task_handle: Option<JoinHandle<Result<()>>>,
    origin_uri: EventOriginUri,
}

impl offramp::Impl for Elastic {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let client = SyncClientBuilder::new()
                .static_nodes(config.nodes.into_iter())
                .build()?;

            let queue = AsyncSink::new(config.concurrency);
            let (tx, _rx) = bounded(1); // dummy value
            let (res_tx, _res_rx) = bounded(1); // dummy value

            Ok(SinkManager::new_box(Self {
                sink_url: TremorURL::from_offramp_id("elastic")?, // just a dummy value, gonna be overwritten on init
                postprocessors: vec![],
                client,
                queue,
                insight_tx: tx,
                is_linked: false,
                response_sender: res_tx,
                response_task_handle: None,
                origin_uri: EventOriginUri::default(),
            }))
        } else {
            Err("Elastic offramp requires a configuration.".into())
        }
    }
}

fn build_source(id: &EventId, origin_uri: Option<&EventOriginUri>) -> Value<'static> {
    let mut source = Object::with_capacity(2);
    source.insert("event_id".into(), Value::from(id.to_string()));
    source.insert(
        "origin".into(),
        origin_uri.map_or_else(Value::null, |uri| Value::from(uri.to_string())),
    );
    Value::from(source)
}

/**
Format:

Value:
{
    "source": {
        "`event_id`": ...,
        "origin": "..."
    },
    "payload": ...,
    "success": false,
    "error": {
        <es error struct>
    }
}

Meta:
{
    "elastic": {
        "id": ...,
        "type": ...,
        "index": ...
    }
}
*/
fn build_bulk_error_data(
    item: &ErrorItem<String, String, String>,
    id: &EventId,
    payload: Value<'static>,
    origin_uri: Option<&EventOriginUri>,
) -> Result<LineValue> {
    let mut meta = Object::with_capacity(1);
    let mut es_meta = Object::with_capacity(3);
    es_meta.insert("id".into(), Value::from(item.id().to_string()));
    es_meta.insert("index".into(), Value::from(item.index().to_string()));
    es_meta.insert("doc_type".into(), Value::from(item.ty().to_string()));
    meta.insert("elastic".into(), Value::from(es_meta));

    let mut value = Object::with_capacity(4);
    let source = build_source(id, origin_uri);
    value.insert("source".into(), source);
    value.insert("payload".into(), payload);
    // TODO: how to convert serde Value to our Value?
    value.insert("error".into(), tremor_value::to_value(item.err())?);
    value.insert("success".into(), Value::from(false));
    Ok((value, meta).into())
}

/// Format:
/// {
///    "source": {
///        "`event_id`": ...,
///        "origin": ...
///    },
///    "payload": ...,
///    "success": true
/// }
///
/// Meta:
/// {
///    "elastic": {
///        "id": ...,
///        "type": ...,
///        "index": ...,
///        "version": ...
///    }
/// }
fn build_bulk_success_data(
    item: &OkItem<String, String, String>,
    id: &EventId,
    payload: Value<'static>,
    origin_uri: Option<&EventOriginUri>,
) -> Result<LineValue> {
    let mut meta = Object::with_capacity(1);
    let mut es_meta = Object::with_capacity(4);
    es_meta.insert("id".into(), Value::from(item.id().to_string()));
    es_meta.insert("index".into(), Value::from(item.index().to_string()));
    es_meta.insert("doc_type".into(), Value::from(item.ty().to_string()));
    es_meta.insert(
        "_version".into(),
        item.version().map_or_else(Value::null, Value::from),
    );
    meta.insert("elastic".into(), Value::from(es_meta));

    let mut value = Object::with_capacity(3);
    let source = build_source(id, origin_uri);
    value.insert("source".into(), source);
    value.insert("payload".into(), payload);
    value.insert("success".into(), Value::from(true));
    Ok((value, meta).into())
}

/// Build event payload for elasticsearch _bulk request
fn build_event_payload(event: &Event) -> Result<Vec<u8>> {
    // We estimate a single message is 512 byte on everage, might be off but it's
    // a guess
    let vec_size = 512 * event.len();
    let mut payload = Vec::with_capacity(vec_size);
    for (value, meta) in event.value_meta_iter() {
        let index = meta
            .get("index")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::from("'index' not set for elastic offramp!"))?;
        let mut index_meta = json!({
            "_index": index,
        });
        // _type is deprecated in ES 7.10, thus it is no longer required
        if let Some(doc_type) = meta.get("doc_type").and_then(Value::as_str) {
            index_meta.insert("_type", doc_type)?;
        }
        if let Some(doc_id) = meta.get("doc_id").and_then(Value::as_str) {
            index_meta.insert("_id", doc_id)?;
        };
        if let Some(pipeline) = meta.get("pipeline").and_then(Value::as_str) {
            index_meta.insert("pipeline", pipeline)?;
        };
        let value_meta = json!({ "index": index_meta });
        value_meta.write(&mut payload)?;
        payload.push(b'\n');
        value.write(&mut payload)?;
        payload.push(b'\n');
    }

    Ok(payload)
}

impl Elastic {
    #[allow(clippy::clippy::too_many_lines)]
    async fn enqueue_send_future(&mut self, mut event: Event) -> Result<()> {
        let (tx, rx) = bounded(1);
        let insight_tx = self.insight_tx.clone();
        let response_tx = self.response_sender.clone();
        let is_linked = self.is_linked;

        // extract values, we don't care about later, but we need to keep the event around
        let mut id = EventId::default();
        std::mem::swap(&mut id, &mut event.id);
        let mut op_meta = OpMeta::default();
        std::mem::swap(&mut op_meta, &mut event.op_meta);
        let insight_id = id.clone();
        let response_origin_uri = if is_linked {
            event.origin_uri.clone()
        } else {
            None
        };

        let mut responses = Vec::with_capacity(if is_linked { 8 } else { 0 });
        // build payload and request
        let payload = match build_event_payload(&event) {
            Ok(payload) => payload,
            Err(e) => {
                // send error response about event not being able to be serialized into ES payload
                let mut data = Value::object_with_capacity(2);
                data.insert("success", Value::from(false))?;
                data.insert("error", Value::from("Invalid ES Payload"))?;
                let source = build_source(&event.id, event.origin_uri.as_ref());
                data.insert("source", source)?;
                responses.push(((data, Value::null()).into(), ERR)); // send response
                return Err(e); // this will bubble up the error to the calling on_event, sending a CB fail back
            }
        };
        let req = self.client.request(BulkRequest::new(payload));

        // go async
        task::spawn_blocking(move || {
            let ress = &mut responses;

            // The truncation we do is sensible since we're only looking at a short timeframe
            #[allow(clippy::cast_possible_truncation)]
            let r: Result<u64> = (move || {
                let start = Instant::now();
                // send event to elastic, blockingly
                let bulk_res = req.send()?.into_response::<BulkResponse>()?;

                if is_linked {
                    // send out response events for each item
                    // success events via OUT port
                    // error   events via ERR port
                    for (item, value) in bulk_res.iter().zip(event.value_iter()) {
                        let item_res = match item {
                            Ok(ok_item) => (
                                OUT,
                                build_bulk_success_data(
                                    ok_item,
                                    &id,
                                    value.clone_static(), // uaarrghhh
                                    event.origin_uri.as_ref(),
                                ),
                            ),
                            Err(err_item) => (
                                ERR,
                                build_bulk_error_data(
                                    err_item,
                                    &id,
                                    value.clone_static(), // uaarrrghhh
                                    event.origin_uri.as_ref(),
                                ),
                            ),
                        };
                        if let (port, Ok(item_body)) = item_res {
                            ress.push((item_body, port));
                        } else {
                            error!(
                                "Error extracting bulk item response for document id {}",
                                item.map_or_else(ErrorItem::id, OkItem::id)
                            );
                        }
                    }
                }
                Ok(start.elapsed().as_millis() as u64)
            })();

            let mut m = Value::object_with_capacity(1);
            let cb = match &r {
                Ok(time) => {
                    if m.insert("time", *time).is_err() {
                        // ALLOW: this is OK
                        unreachable!()
                    };
                    CBAction::Ack
                }
                Err(e) => {
                    // request failed
                    // TODO update error metric here?
                    println!("ES request failed: {:?}", e);
                    if is_linked {
                        // send error event via ERR port
                        let mut error_data = Object::with_capacity(1);
                        let mut source = Object::with_capacity(2);
                        source.insert("event_id".into(), Value::from(insight_id.to_string()));
                        source.insert(
                            "origin".into(),
                            response_origin_uri
                                .map_or_else(Value::null, |uri| Value::from(uri.to_string())),
                        );
                        error_data.insert("success".into(), Value::from(false));
                        error_data.insert("source".into(), Value::from(source));
                        error_data.insert("error".into(), Value::from(e.to_string()));
                        responses.push(((error_data, Value::null()).into(), ERR));
                    }
                    CBAction::Fail
                }
            };
            let insight = Event {
                id: insight_id,
                data: (Value::null(), m).into(),
                ingest_ns: nanotime(),
                op_meta,
                cb,
                ..Event::default()
            };

            task::block_on(async move {
                // send response events
                for response in responses {
                    if let Err(e) = response_tx.send(response).await {
                        error!("Failed to send bulk item response: {}", e);
                    }
                }

                // send insight
                if let Err(e) = insight_tx.send(sink::Reply::Insight(insight)).await {
                    error!("Failed to send insight: {}", e);
                }

                // mark this task as done in order to free a slot
                if let Err(e) = tx.send(r).await {
                    error!("Failed to send AsyncSink done message: {}", e)
                }
            });
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }

    async fn maybe_enque(&mut self, event: Event) -> Result<()> {
        // TODO: write overflow messages to overflow port
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                let mut m = Object::with_capacity(1);
                let error_msg = "Dropped data due to es overload";
                m.insert("error".into(), error_msg.into());

                let insight = Event {
                    id: event.id,
                    data: (Value::null(), m).into(),
                    ingest_ns: nanotime(),
                    cb: CBAction::Fail,
                    ..Event::default()
                };

                if self
                    .insight_tx
                    .send(sink::Reply::Insight(insight))
                    .await
                    .is_err()
                {
                    error!("[Sink::{}] Failed to send insight", &self.sink_url)
                };

                if self.is_linked {
                    // send error message on overflow to ERR port
                    let mut data = Object::with_capacity(2);
                    let mut meta = Object::with_capacity(1);
                    data.insert("success".into(), Value::from(false));
                    data.insert("error".into(), Value::from(error_msg));
                    meta.insert("error".into(), Value::from(error_msg));

                    if let Err(e) = self.response_sender.send(((data, meta).into(), ERR)).await {
                        error!(
                            "[Sink::{}] Failed to send error response on overflow: {}.",
                            self.sink_url, e
                        );
                    }
                }

                error!("[Sink::{}] {}", &self.sink_url, error_msg);
                Err(error_msg.into())
            }
            _ => {
                if self.enqueue_send_future(event).await.is_err() {
                    error!(
                        "[Sink::{}] Failed to enqueue send request to elastic",
                        &self.sink_url
                    );
                    Err("Failed to enqueue send request to elastic".into())
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// task responsible to receive raw response data, build response event and send it off.
/// keeps ownership of `EventIdGenerator` and more
async fn response_task(
    mut id_gen: EventIdGenerator,
    origin_uri: EventOriginUri,
    rx: Receiver<(LineValue, Cow<'static, str>)>,
    insight_tx: Sender<sink::Reply>,
) -> Result<()> {
    loop {
        match rx.recv().await {
            Ok((data, port)) => {
                let response_event = Event {
                    id: id_gen.next_id(),
                    data,
                    ingest_ns: nanotime(),
                    origin_uri: Some(origin_uri.clone()),
                    ..Event::default()
                };
                eprintln!("Sending response {:?} via {}", &response_event, port);
                if let Err(e) = insight_tx
                    .send(sink::Reply::Response(port, response_event))
                    .await
                {
                    eprintln!("Error sending insight {}", e);
                    return Err(e.into());
                }
            }
            Err(e) => {
                eprintln!("Error receiving response data {}", e);
            }
        }
    }
}

#[async_trait::async_trait]
impl Sink for Elastic {
    // We enforce json here!
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        self.maybe_enque(event).await?;
        Ok(None) // insights are sent via reply_channel directly
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorURL,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        // try to connect to check provided config and extract the cluster name
        let cluster_name = self.client.ping().send()?.cluster_name().to_string();
        info!(
            "[Sink::{}] Connected to ES cluster {}.",
            &sink_url, &cluster_name
        );

        self.sink_url = sink_url.clone();
        self.postprocessors = make_postprocessors(processors.post)?;
        self.insight_tx = reply_channel;
        self.is_linked = is_linked;
        if is_linked {
            let event_id_gen = EventIdGenerator::new(sink_uid);
            self.origin_uri = EventOriginUri {
                uid: sink_uid, // placeholder,
                scheme: "elastic".to_string(),
                host: cluster_name,
                port: None,
                path: vec![],
            };
            // channels that events go through that end up here are bounded, so we should not grow out of memory
            let (tx, rx) = async_channel::unbounded();
            self.response_sender = tx;
            self.response_task_handle = Some(task::spawn(response_task(
                event_id_gen,
                self.origin_uri.clone(),
                rx,
                self.insight_tx.clone(),
            )));
        }
        Ok(())
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None) // insights are sent via reply_channel directly
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    async fn terminate(&mut self) {
        let mut swap: Option<JoinHandle<Result<()>>> = None;
        std::mem::swap(&mut swap, &mut self.response_task_handle);
        if let Some(handle) = swap {
            info!("[Sink::{}] Terminating response task...", self.sink_url);
            handle.cancel().await;
            info!("[Sink::{}] Done.", self.sink_url);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn build_event_payload_test() -> Result<()> {
        let mut numbers = Value::array_with_capacity(3);
        for x in 1..=3 {
            numbers.push(Value::from(x))?;
        }
        numbers.push(Value::from("snot"))?;

        let data = json!({
            "numbers": numbers,
        });
        let meta = json!({
            "index": "my_index",
            "doc_type": "my_type",
            "doc_id": "my_id",
            "pipeline": "my_pipeline"
        });

        let event = Event {
            data: (data.clone(), meta).into(),
            ..Event::default()
        };
        let payload = build_event_payload(&event)?;

        let mut expected = Vec::new();
        let es_meta = json!({
            "index": {
                "_index": "my_index",
                "_type": "my_type",
                "_id": "my_id",
                "pipeline": "my_pipeline"
            }
        });
        es_meta.write(&mut expected)?;
        expected.push(b'\n');
        data.write(&mut expected)?; // this clone is here so both are the same structure internally
        expected.push(b'\n');

        assert_eq!(
            String::from_utf8_lossy(&expected),
            String::from_utf8_lossy(&payload)
        );
        Ok(())
    }

    #[test]
    fn build_event_payload_missing_index() -> Result<()> {
        let meta = json!({
            "doc_type": "my_type",
            "doc_id": "my_id"
        });

        let event = Event {
            data: (Value::object(), meta).into(),
            ..Event::default()
        };

        let p = build_event_payload(&event);
        assert!(p.is_err(), "Didnt fail with missing index.");
        Ok(())
    }
}
