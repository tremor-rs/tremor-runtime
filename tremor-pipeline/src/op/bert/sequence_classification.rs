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

use crate::errors::*;
use crate::op::prelude::*;
//use rust_bert::pipelines::common::ModelType;
use rust_bert::pipelines::sequence_classification::{
    SequenceClassificationConfig, SequenceClassificationModel,
};
use tremor_script::prelude::*;

#[derive(Deserialize)]
struct Config {
    file: String, // just a stupid placeholder
}

impl ConfigImpl for Config {}

#[derive(Debug)]
struct SequenceClassification {
    model: SequenceClassificationModel,
}

op!(SequenceClassificationFactory(node) {
    if let Some(config_map) = &node.config {
        let config = Config::new(config_map)?;
        debug!("{}", config.file);
        let sc_config =SequenceClassificationConfig::default();
        if let Ok(model) = SequenceClassificationModel::new(sc_config) {
            Ok(Box::new(SequenceClassification {
                model
            }))
        } else {
            Err(ErrorKind::BadOpConfig("Could not instantiate this BERT sequence classification operator.".to_string()).into())
        }

    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())
    }
});

#[allow(unused_mut)]
impl Operator for SequenceClassification {
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
        event: Event,
    ) -> Result<EventAndInsights> {
        let (data, meta) = event.data.parts();
        if let Some(s) = data.as_str() {
            let labels = self.model.predict(&[s]);
            let mut label_meta = Value::object_with_capacity(labels.len());
            for label in labels {
                label_meta.insert(label.text, label.score)?;
            }
            // mutating the event
            meta.insert("classification", label_meta)?;
        }
        Ok(EventAndInsights::from(vec![(OUT, event)]))
    }
}
