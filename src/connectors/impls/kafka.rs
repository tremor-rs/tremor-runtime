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
pub(crate) mod consumer;
pub(crate) mod producer;

use core::future::Future;
use futures::future;
use rdkafka::util::AsyncRuntime;
use std::time::{Duration, Instant};

pub struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        // This needs to be smol::spawn we can't use async_std::task::spawn
        smol::spawn(task).detach();
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        // This needs to be smol::Timer we can't use async_io::Timer
        futures::FutureExt::map(smol::Timer::after(duration), |_| ())
    }
}
