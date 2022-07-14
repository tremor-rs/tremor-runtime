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

use crate::op::prelude::*;
use rust_bert::resources::{LocalResource, RemoteResource};
use rust_bert::{
    pipelines::sequence_classification::{
        SequenceClassificationConfig, SequenceClassificationModel,
    },
    resources::ResourceProvider,
};
use std::path::PathBuf;
use std::sync::Mutex;
use tremor_script::prelude::*;
use url::Url;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "dflt_config")]
    config_file: String,
    #[serde(default = "dflt_model")]
    model_file: String,
    #[serde(default = "dflt_vocabulary")]
    vocabulary_file: String,
}

fn dflt_config() -> String {
    "https://cdn.huggingface.co/distilbert-base-uncased-finetuned-sst-2-english-config.json"
        .to_string()
}

fn dflt_model() -> String {
    "https://cdn.huggingface.co/distilbert-base-uncased-finetuned-sst-2-english-rust_model.ot"
        .to_string()
}

fn dflt_vocabulary() -> String {
    "https://cdn.huggingface.co/distilbert-base-uncased-finetuned-sst-2-english-vocab.txt"
        .to_string()
}
type Resource = Box<dyn ResourceProvider + Send>;

impl tremor_config::Impl for Config {}

struct SequenceClassification {
    model: Mutex<SequenceClassificationModel>,
}

impl std::fmt::Debug for SequenceClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SequenceClassification")
            .field("model", &"...")
            .finish()
    }
}

fn get_resource(resource: &str) -> Result<Resource> {
    if resource.starts_with('/') {
        // local
        Ok(Box::new(LocalResource {
            local_path: PathBuf::from(resource),
        }))
    } else {
        let remote_url = Url::parse(resource)?;
        let err: Error = ErrorKind::BadOpConfig("Invalid URL".to_string()).into();
        let name = remote_url
            .path_segments()
            .and_then(std::iter::Iterator::last)
            .and_then(|l| l.split('.').next())
            .ok_or(err)?;
        Ok(Box::new(RemoteResource::from_pretrained((name, resource))))
    }
}

op!(SequenceClassificationFactory(_uid, node) {

    let config_map =  node.config.clone().unwrap_or_else(Value::object);
    let config = Config::new(&config_map)?;
    let sc_config = SequenceClassificationConfig {
        config_resource: get_resource(config.config_file.as_str())?,
        model_resource: get_resource(config.model_file.as_str())?,
        vocab_resource: get_resource(config.vocabulary_file.as_str())?,
        ..Default::default()
    };

    if let Ok(model) = SequenceClassificationModel::new(sc_config) {
        Ok(Box::new(SequenceClassification {
            model: Mutex::new(model)
        }))
    } else {
        Err(ErrorKind::BadOpConfig("Could not instantiate this BERT sequence classification operator.".to_string()).into())
    }
});

impl Operator for SequenceClassification {
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
                let labels = self.model.lock()?.predict([s]);
                let mut label_meta = Value::object_with_capacity(labels.len());
                for label in labels {
                    label_meta.try_insert(label.text, label.score);
                }
                // mutating the event
                m.try_insert("classification", label_meta);
            }
            Ok(())
        })?;
        Ok(EventAndInsights::from(event))
    }
}
