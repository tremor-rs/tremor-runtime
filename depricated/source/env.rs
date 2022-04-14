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
// #![cfg_attr(coverage, no_coverage)]

use crate::source::prelude::*;

#[derive(Clone)]
pub(crate) struct Env {
    onramp_id: TremorUrl,
    origin_uri: EventOriginUri,
    sent: bool,
}

impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ENV")
    }
}
pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, _config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        let origin_uri = EventOriginUri {
            scheme: "tremor-env".to_string(),
            host: hostname(),
            port: None,
            path: vec![],
        };

        Ok(Box::new(Env {
            sent: false,
            origin_uri,
            onramp_id: id.clone(),
        }))
    }
}

#[async_trait::async_trait()]
impl Source for Env {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        if self.sent {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        } else {
            let vars = std::env::vars();
            let env: Value = vars.collect();
            let data = literal!({ "env": env });
            self.sent = true;
            Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                data: data.into(),
            })
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Env {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = self.clone();
        SourceManager::start(source, config).await
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
