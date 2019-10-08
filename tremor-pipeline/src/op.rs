// Copyright 2018-2019, Wayfair GmbH
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

pub mod debug;
pub mod generic;
pub mod grouper;
pub mod identity;
pub mod runtime;

use super::{Event, NodeConfig};
use crate::errors::*;
use halfbrown::HashMap;
use regex::Regex;
use serde_yaml::Value;
use simd_json::OwnedValue;

pub trait Operator: std::fmt::Debug + Send {
    fn on_event(&mut self, port: &str, event: Event) -> Result<Vec<(String, Event)>>;

    fn handles_signal(&self) -> bool {
        false
    }
    // A lot of operators won't need to handle signals so we default to
    // passing the signal through
    fn on_signal(&mut self, _signal: &mut Event) -> Result<Vec<(String, Event)>> {
        Ok(vec![])
    }

    fn handles_contraflow(&self) -> bool {
        false
    }
    // A lot of operators won't need to handle insights so we default to
    // passing the isnight through
    fn on_contraflow(&mut self, _insight: &mut Event) {}

    // Returns metrics for this operator
    fn metrics(&self, _tags: HashMap<String, String>, _timestamp: u64) -> Result<Vec<OwnedValue>> {
        Ok(Vec::new())
    }
}

pub trait InitializableOperator {
    fn from_node(&self, node: &NodeConfig) -> Result<Box<dyn Operator>>;
}

// duplicated trait from main tremor-runtime utils (used for onramp/offramp configs there)
// TODO remove from here once the runtime utils is part of its own crate
pub trait ConfigImpl {
    fn new(config: &Value) -> Result<Self>
    where
        for<'de> Self: serde::de::Deserialize<'de>,
    {
        // simpler ways, but does not give us the kind of error info we want
        //let validated_config: Config = serde_yaml::from_value(c.clone())?;
        //let validated_config: Config = serde_yaml::from_str(&serde_yaml::to_string(c)?)?;

        // serialize the YAML config and deserialize it again, so that we get extra info on
        // YAML errors here (eg: name of the config key where the errror occured). can just
        // use serde_yaml::from_value() here, but the error message there is limited.
        match serde_yaml::from_str(&serde_yaml::to_string(config)?) {
            Ok(c) => Ok(c),
            Err(e) => {
                // remove the potentially misleading "at line..." info, since it does not
                // correspond to the numbers in the file now
                let re = Regex::new(r" at line \d+ column \d+$")?;
                let e_cleaned = re.replace(&e.to_string(), "").into_owned();
                Err(e_cleaned.into())
            }
        }
    }
}
