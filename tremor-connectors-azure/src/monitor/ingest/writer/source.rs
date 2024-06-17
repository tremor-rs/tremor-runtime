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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;
    use tremor_common::alias;
    use tremor_common::alias::Flow;
    use tremor_common::ids::Id;
    use tremor_common::ids::SourceId;
    use tremor_connectors::utils::quiescence::QuiescenceBeacon;
    use tremor_connectors::utils::reconnect::ConnectionLostNotifier;
    use tremor_connectors::ConnectorType;
    use tremor_script::EventOriginUri;
    use tremor_system::qsize;

    #[tokio::test]
    async fn test_ami_source() -> anyhow::Result<()> {
        let (event_tx, rx) = channel(qsize());
        let mut source = AmiSource {
            source_is_connected: Arc::new(AtomicBool::new(false)),
            rx,
        };

        assert!(!source.is_transactional());
        assert!(!source.asynchronous());

        let alias =
            alias::Connector::new(Into::<Flow>::into("snot"), Into::<String>::into("badger"));

        let (lost_tx, _lost_rx) = channel(qsize());
        let lost_found = ConnectionLostNotifier::new(&alias, lost_tx);
        let mut pull_id = 0;
        let ctx: SourceContext = SourceContext {
            uid: SourceId::new(1),
            alias,
            connector_type: ConnectorType::default(),
            quiescence_beacon: QuiescenceBeacon::default(),
            notifier: lost_found,
        };

        // Inject a reply so pull does not block on us
        event_tx
            .send(SourceReply::Data {
                origin_uri: EventOriginUri::default(),
                data: vec![],
                meta: None,
                stream: Some(1),
                port: None,
                codec_overwrite: None,
            })
            .await?;
        let _reply = source.pull_data(&mut pull_id, &ctx).await?;

        source.on_cb_restore(&ctx).await?;
        assert!(source.source_is_connected.load(Ordering::Acquire));

        Ok(())
    }
}
