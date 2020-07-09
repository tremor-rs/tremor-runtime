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
use crate::offramp::prelude::*;
use hashbrown::HashMap;

pub(crate) mod blackhole;
pub(crate) mod rest;
pub(crate) mod stderr;
pub(crate) mod stdout;

#[async_trait::async_trait]
pub(crate) trait Sink {
    /// FIXME we can't make this async right now
    /// https://github.com/rust-lang/rust/issues/63033
    async fn on_event(
        &mut self,
        input: &str,
        codec: &dyn Codec,
        event: Event,
    ) -> Result<Vec<Event>>;
    async fn init(&mut self, postprocessors: &[String]) -> Result<()>;

    async fn on_signal(&mut self, signal: Event) -> Result<Vec<Event>>;

    fn is_active(&self) -> bool;
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
    // async fn start() {}

    // pub(crate) async fn run(self) -> Result<()> {
    //     Ok(())
    // }
}

impl<T> Offramp for SinkManager<T>
where
    T: Sink + Send,
{
    fn start(&mut self, _codec: &dyn Codec, postprocessors: &[String]) -> Result<()> {
        task::block_on(self.sink.init(postprocessors))
    }

    fn on_event(&mut self, codec: &dyn Codec, input: &str, event: Event) -> Result<()> {
        for insight in task::block_on(self.sink.on_event(input, codec, event))? {
            for p in self.pipelines.values_mut() {
                if let Err(e) = p.send_insight(insight.clone()) {
                    error!("Error: {}", e)
                };
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

    fn on_signal(&mut self, signal: Event) -> Option<Event> {
        if let Ok(insights) = task::block_on(self.sink.on_signal(signal)) {
            for insight in insights {
                for p in self.pipelines.values_mut() {
                    if let Err(e) = p.send_insight(insight.clone()) {
                        error!("Error: {}", e)
                    };
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

    fn ready(&mut self) -> bool {
        self.pipelines.values_mut().all(pipeline::Addr::drain_ready)
    }
}
