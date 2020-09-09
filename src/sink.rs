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
use hashbrown::HashMap;

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

/// Result for a sink function that may provide insights.
///
/// It can return None or Some(vec![]) if no insights were generated.
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
pub(crate) type ResultVec = Result<Option<Vec<Event>>>;

#[async_trait::async_trait]
pub(crate) trait Sink {
    async fn on_event(&mut self, input: &str, codec: &dyn Codec, event: Event) -> ResultVec;
    async fn on_signal(&mut self, signal: Event) -> ResultVec;

    /// This function should be implemented to be idempotent
    async fn init(&mut self, postprocessors: &[String]) -> Result<()>;

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
}

impl<T> SinkManager<T>
where
    T: Sink + Send,
{
    fn new(sink: T) -> Self {
        Self {
            sink,
            pipelines: HashMap::new(),
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
    #[allow(clippy::used_underscore_binding)]
    async fn start(&mut self, _codec: &dyn Codec, postprocessors: &[String]) -> Result<()> {
        self.sink.init(postprocessors).await
    }

    async fn on_event(&mut self, codec: &dyn Codec, input: &str, event: Event) -> Result<()> {
        if let Some(mut insights) = self.sink.on_event(input, codec, event).await? {
            for insight in insights.drain(..) {
                let mut i = self.pipelines.values_mut();
                if let Some(first) = i.next() {
                    for p in i {
                        if let Err(e) = p.send_insight(insight.clone()).await {
                            error!("Error: {}", e)
                        };
                    }
                    if let Err(e) = first.send_insight(insight).await {
                        error!("Error: {}", e)
                    };
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

    fn remove_pipeline(&mut self, id: TremorURL) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty()
    }

    async fn on_signal(&mut self, signal: Event) -> Option<Event> {
        let insights = self.sink.on_signal(signal).await.ok()??;
        for insight in insights {
            let mut i = self.pipelines.values_mut();
            if let Some(first) = i.next() {
                for p in i {
                    if let Err(e) = p.send_insight(insight.clone()).await {
                        error!("Error: {}", e)
                    };
                }
                if let Err(e) = first.send_insight(insight).await {
                    error!("Error: {}", e)
                };
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
