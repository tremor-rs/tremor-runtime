// Copyright 2018, Wayfair GmbH
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

//!
//! # Count events passing through
//!
//! The `count` operation allows counting event flow at a specific point in the pipeline.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! The metric is prefixed with `tremor` as a system, so a metric of `events` will be published
//! as `tremor_events`. The label `instance` is added to every counter with the `instance`
//! specified as a command line argument. In addition the label `event` is added and either set to:
//!
//! * `sent` - for counting passing down the pipeline
//! * `return_ok` - for counting successful returns. (of `count_results` is set to true)
//! * `return_error` - for counting error returns. (of `count_results` is set to true)

use crate::dflt;
use crate::errors::*;
use crate::pipeline::prelude::*;
use prometheus::IntCounterVec; // w/ instance
use serde_yaml;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// the metric name
    pub metric: String,
    /// description of the metric
    pub desc: String,
    /// labels to add to the metric
    pub labels: HashMap<String, String>,
    /// if returns on this operation should be counted to indicate
    /// success or failure of the downstream pipeline. (default: false)
    #[serde(default = "dflt::d_false")]
    pub count_results: bool,
}

pub struct Op {
    conf: Config,
    counter: IntCounterVec,
}

impl fmt::Debug for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.conf)
    }
}

impl Op {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let conf: Config = serde_yaml::from_value(opts.clone())?;

        let mut labels = conf.labels.clone();
        // We know this only gets set at the very start of our program
        // by the time this code gets executed it willnot change any more
        labels.insert("instance".into(), instance!());
        // Counter for messages going through this.
        let opts = opts!(conf.metric.clone(), conf.desc.clone());
        let counter =
            register_int_counter_vec!(opts.namespace("tremor").const_labels(labels), &["event"])?;

        Ok(Op { conf, counter })
    }
}

impl Opable for Op {
    fn on_event(&mut self, event: EventData) -> EventResult {
        self.counter.with_label_values(&["send"]).inc();
        next!(event)
    }

    fn on_result(&mut self, result: EventReturn) -> EventReturn {
        if self.conf.count_results {
            if result.is_ok() {
                self.counter.with_label_values(&["return_ok"]).inc();
            } else {
                self.counter.with_label_values(&["return_error"]).inc();
            };
        }
        result
    }

    opable_types!(ValueType::Same, ValueType::Same);
}
