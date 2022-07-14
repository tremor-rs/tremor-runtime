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
// #![cfg_attr(coverage, no_coverage)]

use std::fmt;

use crate::op::prelude::*;
//use rust_bert::pipelines::common::ModelType;
use rust_bert::pipelines::summarization::{SummarizationConfig, SummarizationModel};
use std::sync::Mutex;
use tremor_script::prelude::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "Default::default")]
    file: String, // just a stupid placeholder
}

impl tremor_config::Impl for Config {}

struct Summerization {
    model: Mutex<SummarizationModel>,
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
        let s_config = SummarizationConfig::default();
        if let Ok(model) = SummarizationModel::new(s_config) {
            Ok(Box::new(Summerization {
                model: Mutex::new(model)
            }))
        } else {
            Err(ErrorKind::BadOpConfig("Could not instantiate this BERT summarization operator.".to_string()).into())
        }

    } else {
        Err(ErrorKind::MissingOpConfig(node.id.clone()).into())
    }
});

impl Operator for Summerization {
    fn on_event(
        &mut self,
        _node_id: u64,
        _uid: OperatorUId,
        _port: &Port<'static>,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        event.data.rent_mut(|data| -> Result<()> {
            let (v, m) = data.parts_mut();
            if let Some(s) = v.as_str() {
                let mut summary = self.model.lock()?.summarize(&[s]);
                if let Some(s) = summary.pop() {
                    m.try_insert("summary", s);
                }
            }
            Ok(())
        })?;
        Ok(EventAndInsights::from(event))
    }
}
