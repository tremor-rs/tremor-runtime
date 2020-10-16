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
use crate::pipeline;
use crate::sink::prelude::*;
use crate::url::TremorURL;
use async_channel::Sender;
use halfbrown::HashMap;
use std::borrow::Cow;

pub(crate) mod blackhole;
pub(crate) mod debug;
pub(crate) mod elastic;
pub(crate) mod exit;
pub(crate) mod file;
pub(crate) mod kafka;
pub(crate) mod newrelic;
pub(crate) mod postgres;
pub(crate) mod prelude;
pub(crate) mod rest;
pub(crate) mod stderr;
pub(crate) mod stdout;
pub(crate) mod tcp;
pub(crate) mod udp;
pub(crate) mod ws;

#[derive(Debug)]
pub enum Reply {
    Insight(Event),
    // out port and the event
    // TODO better name for this?
    Response(Cow<'static, str>, Event),
}

/// Result for a sink function that may provide insights or response.
///
/// It can return None or Some(vec![]) if no insights/response were generated.
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
pub(crate) type ResultVec = Result<Option<Vec<Reply>>>;

#[async_trait::async_trait]
pub(crate) trait Sink {
    async fn on_event(
        &mut self,
        input: &str,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec;
    async fn on_signal(&mut self, signal: Event) -> ResultVec;

    /// This function should be implemented to be idempotent
    ///
    /// The passed reply_channel is for fast-tracking sink-replies going back to the connected pipelines.
    /// It is an additional way to returning them in a ResultVec via on_event, on_signal.
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorURL,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<Reply>,
    ) -> Result<()>;

    /// Callback for graceful shutdown (default behaviour: do nothing)
    async fn terminate(&mut self) {}

    /// Is the sink active and ready to process events
    fn is_active(&self) -> bool;

    /// Is the sink automatically acknowleding events or engaged in some form of delivery
    /// guarantee
    fn auto_ack(&self) -> bool;

    fn default_codec(&self) -> &str;
}

pub(crate) struct SinkManager<T>
where
    T: Sink,
{
    sink: T,
    pipelines: HashMap<TremorURL, pipeline::Addr>,
    // for linked offramps
    dest_pipelines: HashMap<Cow<'static, str>, Vec<(TremorURL, pipeline::Addr)>>,
}

impl<T> SinkManager<T>
where
    T: Sink + Send,
{
    fn new(sink: T) -> Self {
        Self {
            sink,
            pipelines: HashMap::new(),
            dest_pipelines: HashMap::new(),
        }
    }

    fn new_box(sink: T) -> Box<Self> {
        Box::new(Self::new(sink))
    }
}

#[async_trait::async_trait]
impl<T> Offramp for SinkManager<T>
where
    T: Sink + Send,
{
    async fn terminate(&mut self) {
        self.sink.terminate().await
    }
    #[allow(clippy::too_many_arguments)]
    async fn start(
        &mut self,
        offramp_uid: u64,
        offramp_url: &TremorURL,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<Reply>,
    ) -> Result<()> {
        self.sink
            .init(
                offramp_uid, // we treat offramp_uid and sink_uid as the same thing
                offramp_url,
                codec,
                codec_map,
                processors,
                is_linked,
                reply_channel,
            )
            .await
    }

    async fn on_event(
        &mut self,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        input: &str,
        event: Event,
    ) -> Result<()> {
        if let Some(mut replies) = self.sink.on_event(input, codec, codec_map, event).await? {
            for reply in replies.drain(..) {
                match reply {
                    Reply::Insight(e) => handle_insight(e, self.pipelines.values()).await?,
                    Reply::Response(port, event) => {
                        if let Some(pipelines) = self.dest_pipelines.get_mut(&port) {
                            handle_response(event, pipelines.iter()).await?
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn default_codec(&self) -> &str {
        self.sink.default_codec()
    }

    fn add_pipeline(&mut self, id: TremorURL, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }

    fn add_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorURL, addr: pipeline::Addr) {
        let p = (id, addr);
        if let Some(port_ps) = self.dest_pipelines.get_mut(&port) {
            port_ps.push(p);
        } else {
            self.dest_pipelines.insert(port, vec![p]);
        }
    }

    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty() && self.dest_pipelines.is_empty()
    }
    fn remove_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorURL) -> bool {
        if let Some(port_ps) = self.dest_pipelines.get_mut(&port) {
            port_ps.retain(|(url, _)| url != &id)
        }
        self.pipelines.is_empty() && self.dest_pipelines.is_empty()
    }

    async fn on_signal(&mut self, signal: Event) -> Option<Event> {
        let replies = self.sink.on_signal(signal).await.ok()??;
        for reply in replies {
            match reply {
                Reply::Insight(e) => {
                    if let Err(e) = handle_insight(e, self.pipelines.values()).await {
                        error!("Error handling insight in sink: {}", e)
                    }
                }
                Reply::Response(port, event) => {
                    if let Some(pipelines) = self.dest_pipelines.get_mut(&port) {
                        if let Err(e) = handle_response(event, pipelines.iter()).await {
                            error!("Error handling response in sink: {}", e)
                        }
                    }
                }
            }
        }
        None
    }

    fn is_active(&self) -> bool {
        self.sink.is_active()
    }

    fn auto_ack(&self) -> bool {
        self.sink.auto_ack()
    }
}

/// we explicitly do not fail upon send errors, just log errors
pub(crate) async fn handle_insight<'iter, T>(insight: Event, mut pipelines: T) -> Result<()>
where
    T: Iterator<Item = &'iter pipeline::Addr>,
{
    if let Some(first) = pipelines.next() {
        for p in pipelines {
            if let Err(e) = p.send_insight(insight.clone()).await {
                // TODO: is this wanted to not raise the error here?
                error!("Error: {}", e)
            };
        }
        if let Err(e) = first.send_insight(insight).await {
            error!("Error: {}", e)
        };
    }
    Ok(())
}

/// handle response back from sink e.g. in linked transport case
///
/// we explicitly do not fail upon send errors, just log errors
pub(crate) async fn handle_response<'iter, T>(response: Event, mut pipelines: T) -> Result<()>
where
    T: Iterator<Item = &'iter (TremorURL, pipeline::Addr)>,
{
    if let Some((first_id, first_addr)) = pipelines.next() {
        for (id, addr) in pipelines {
            // TODO alt way here?
            // pre-save this already in dest_pipelines?
            let port = id.instance_port_required()?.to_owned();
            if let Err(e) = addr
                .send(pipeline::Msg::Event {
                    event: response.clone(),
                    input: port.into(),
                })
                .await
            {
                error!("Error: {}", e)
            };
        }
        let first_port = first_id.instance_port_required()?.to_owned();

        if let Err(e) = first_addr
            .send(pipeline::Msg::Event {
                event: response,
                input: first_port.into(),
            })
            .await
        {
            error!("Error: {}", e)
        };
    }
    Ok(())
}
