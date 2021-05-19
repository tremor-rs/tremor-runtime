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

// this is not enabled
#![cfg(not(tarpaulin_include))]

use std::fmt;

use crate::errors::*;
use crate::op::prelude::*;
//use rust_bert::pipelines::common::ModelType;
use rust_bert::pipelines::summarization::{SummarizationConfig, SummarizationModel};
use tremor_script::prelude::*;

#[derive(Deserialize)]
struct Config {
    #[serde(default = "Default::default")]
    file: String, // just a stupid placeholder
}

impl ConfigImpl for Config {}

struct Summerization {
    model: SummarizationModel,
}

impl fmt::Debug for Summerization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "Summerization")
    }
}

op!(SummerizationFactory(_uid, node) {
    if let Some(config_map) = &node.config {
        let config = Config::new(config_map)?;
        debug!("{}", config.file);
        let s_config =SummarizationConfig::default();
        if let Ok(model) = SummarizationModel::new(s_config) {
            Ok(Box::new(Summerization {
                model
            }))
        } else {
            Err(ErrorKind::BadOpConfig("Could not instantiate this BERT summarization operator.".to_string()).into())
        }

    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())
    }
});

#[allow(unused_mut)]
impl Operator for Summerization {
    fn handles_contraflow(&self) -> bool {
        false
    }

    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(&mut self, _uid: u64, _signal: &mut Event) -> Result<EventAndInsights> {
        Ok(EventAndInsights::default())
    }

    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        event.data.rent_mut(|ValueAndMeta { v, m }| {
            if let Some(s) = v.as_str() {
                let mut summary = self.model.summarize(&[s]);
                if let Some(s) = summary.pop() {
                    m.insert("summary", s)?;
                }
            }
        });
        Ok(EventAndInsights::from(event))
    }
}
