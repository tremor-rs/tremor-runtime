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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::mpsc::Receiver;
use tremor_connectors::{
    errors::GenericImplementationError,
    source::{Source, SourceContext, SourceReply},
};

pub(crate) struct AmiSource {
    pub(crate) source_is_connected: Arc<AtomicBool>,
    pub(crate) rx: Receiver<SourceReply>,
}

#[async_trait::async_trait()]
impl Source for AmiSource {
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        anyhow::Ok(
            self.rx
                .recv()
                .await
                .ok_or(GenericImplementationError::ChannelEmpty)?,
        )
    }

    fn is_transactional(&self) -> bool {
        false
    }

    /// there is no asynchronous task driving this and is being stopped by the quiescence process
    fn asynchronous(&self) -> bool {
        false
    }

    async fn on_cb_restore(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        // we will only know if we are connected to some pipelines if we receive a CBAction::Restore contraflow event
        // we will not send responses to out/err if we are not connected and this is determined by this variable
        self.source_is_connected.store(true, Ordering::Release);
        Ok(())
    }
}
