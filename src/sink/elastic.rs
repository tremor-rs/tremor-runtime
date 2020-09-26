// Copyright 2020, The Tremor Team
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
//! ## Input Variables
//!   * `index` - index to write to (required)
//!   * `doc-type` - document type for the event (required)
//!   * `pipeline` - pipeline to use
//!
//! ## Outputs
//!
//! The 1st additional output is used to send divert messages that can not be
//! enqueued due to overload

use crate::postprocessor::Postprocessors;
use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use elastic::prelude::*;
use simd_json::borrowed::Object;
use simd_json::json;
use std::str;
use std::time::Instant;
use tremor_pipeline::OpMeta;
use tremor_script::prelude::*;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// list of endpoint urls
    pub endpoints: Vec<String>,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "dflt::d_4")]
    pub concurrency: usize,
}

impl ConfigImpl for Config {}

pub struct Elastic {
    client: SyncClient,
    queue: AsyncSink<u64>,
    postprocessors: Postprocessors,
    tx: Sender<SinkReply>,
    rx: Receiver<SinkReply>,
}

impl offramp::Impl for Elastic {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let client = SyncClientBuilder::new()
                .static_nodes(config.endpoints.into_iter())
                .build()?;

            let queue = AsyncSink::new(config.concurrency);
            let (tx, rx) = bounded(crate::QSIZE);

            Ok(SinkManager::new_box(Self {
                postprocessors: vec![],
                client,
                queue,
                rx,
                tx,
            }))
        } else {
            Err("Elastic offramp requires a configuration.".into())
        }
    }
}

impl Elastic {
    async fn drain_insights(&mut self) -> ResultVec {
        let mut v = Vec::with_capacity(self.tx.len() + 1);
        while let Ok(e) = self.rx.try_recv() {
            v.push(e)
        }
        Ok(Some(v))
    }

    async fn enqueue_send_future(
        &mut self,
        id: Ids,
        op_meta: OpMeta,
        payload: Vec<u8>,
    ) -> Result<()> {
        let (tx, rx) = bounded(1);
        let insight_tx = self.tx.clone();

        let req = self.client.request(BulkRequest::new(payload));

        task::spawn_blocking(move || {
            let r = (move || {
                let start = Instant::now();
                for item in req.send()?.into_response::<BulkErrorsResponse>()? {
                    // TODO update error metric here?
                    error!("Elastic Search item error: {:?}", item);
                }
                let d = start.elapsed();
                let d = duration_to_millis(d);
                Ok(d)
            })();

            let mut m = Value::object_with_capacity(2);
            let cb;
            if let Ok(t) = r {
                if m.insert("time", t).is_err() {
                    // ALLOW: this is OK
                    unreachable!()
                };
                cb = CBAction::Ack;
            } else {
                // TODO update error metric here?
                error!("Elastic search error: {:?}", r);
                cb = CBAction::Fail;
            };
            let insight = Event {
                data: (Value::null(), m).into(),
                ingest_ns: nanotime(),
                id,
                op_meta,
                cb,
                ..Event::default()
            };
            task::block_on(async {
                if insight_tx.send(SinkReply::Insight(insight)).await.is_err() {
                    error!("Failed to send insight")
                };

                // TODO: Handle contraflow for notification
                if let Err(e) = tx.send(r).await {
                    error!("Failed to send reply: {}", e)
                }
            });
        });
        self.queue.enqueue(rx)?;
        Ok(())
    }

    async fn maybe_enque(&mut self, id: Ids, op_meta: OpMeta, payload: Vec<u8>) -> Result<()> {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                let mut m = Object::new();
                m.insert("error".into(), "Dropped data due to es overload".into());

                let insight = Event {
                    data: (Value::null(), m).into(),
                    ingest_ns: nanotime(),
                    ..Event::default()
                };

                if self.tx.send(SinkReply::Insight(insight)).await.is_err() {
                    error!("Failed to send insight")
                };

                error!("Dropped data due to es overload");
                Err("Dropped data due to es overload".into())
            }
            _ => {
                if self
                    .enqueue_send_future(id, op_meta, payload)
                    .await
                    .is_err()
                {
                    // TODO: handle reply to the pipeline
                    error!("Failed to enqueue send request to elastic");
                    Err("Failed to enqueue send request to elastic".into())
                } else {
                    Ok(())
                }
            }
        }
    }
}
#[async_trait::async_trait]
impl Sink for Elastic {
    // We enforce json here!
    #[allow(clippy::used_underscore_binding)]
    async fn on_event(&mut self, _input: &str, _codec: &dyn Codec, event: Event) -> ResultVec {
        // We estimate a single message is 512 byte on everage, might be off but it's
        // a guess
        let mut payload = Vec::with_capacity(4096);
        let mut output = None;
        let op_meta = event.op_meta.clone();

        for (value, meta) in event.value_meta_iter() {
            if output.is_none() {
                output = meta.get("backpressure-output").map(Value::clone_static);
            }
            let index = meta
                .get("index")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::from("'index' not set for elastic offramp!"))?;
            let doc_type = meta
                .get("doc_type")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::from("'doc-type' not set for elastic offramp!"))?;
            match meta.get("pipeline").and_then(Value::as_str) {
                None => json!({
                "index":
                {
                    "_index": index,
                    "_type": doc_type
                }})
                .write(&mut payload)?,

                Some(pipeline) => json!({
                "index":
                {
                    "_index": index,
                    "_type": doc_type,
                    "pipeline": pipeline
                }})
                .write(&mut payload)?,
            };
            payload.push(b'\n');
            value.write(&mut payload)?;
            payload.push(b'\n');
        }
        self.maybe_enque(event.id, op_meta, payload).await?;
        self.drain_insights().await
    }

    async fn init(
        &mut self,
        postprocessors: &[String],
        _is_linked: bool,
        _reply_channel: Sender<SinkReply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(postprocessors)?;
        Ok(())
    }

    #[allow(clippy::used_underscore_binding)]
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        self.drain_insights().await
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
}
