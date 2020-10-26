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
use rust_bert::resources::{LocalResource, RemoteResource, Resource};
use std::path::PathBuf;
use tremor_script::prelude::*;
use url::Url;

#[derive(Deserialize, Default)]
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

impl ConfigImpl for Config {}

#[derive(Debug)]
struct SequenceClassification {
    model: SequenceClassificationModel,
}

fn get_resource(resource: &str) -> Result<Resource> {
    if resource.starts_with("/") {
        // local
        Ok(Resource::Local(LocalResource {
            local_path: PathBuf::from(resource),
        }))
    } else {
        let remote_url = Url::parse(resource)?;
        let err: Error = ErrorKind::BadOpConfig("Invalid URL".to_string()).into();
        let name = remote_url
            .path_segments()
            .and_then(|x| x.last())
            .and_then(|l| l.split(".").next())
            .ok_or(err)?;
        Ok(Resource::Remote(RemoteResource::from_pretrained((
            name, resource,
        ))))
    }
}

op!(SequenceClassificationFactory(node) {
    let mapping = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    let config_map = if let Some(map) = &node.config {
        map
    } else {
        &mapping
    };
    let config = Config::new(&config_map)?;
    let mut sc_config = SequenceClassificationConfig::default();
    sc_config.config_resource = get_resource(config.config_file.as_str())?; //rust_bert::resources::Resource::Remote(RemoteResource::)
    sc_config.model_resource = get_resource(config.model_file.as_str())?;
    sc_config.vocab_resource = get_resource(config.vocabulary_file.as_str())?;

    if let Ok(model) = SequenceClassificationModel::new(sc_config) {
        Ok(Box::new(SequenceClassification {
            model
        }))
    } else {
        Err(ErrorKind::BadOpConfig("Could not instantiate this BERT sequence classification operator.".to_string()).into())
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
        Ok(EventAndInsights::from(event))
    }
}
