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

#[cfg(feature = "bert")]
pub mod bert;
pub mod debug;
pub mod generic;
pub mod grouper;
pub mod identity;
pub mod prelude;
pub mod qos;
pub mod trickle;

use self::prelude::OUT;
use super::{Event, NodeConfig};
use crate::errors::Result;
use beef::Cow;
use halfbrown::HashMap;
use regex::Regex;
use tremor_script::Value;

lazy_static::lazy_static! {
    static ref LINE_REGEXP: Regex = {
        #[allow(clippy::unwrap_used)]
         // ALLOW: we tested this
        Regex::new(r" at line \d+ column \d+$").unwrap()
    };
}

/// Response type for operator callbacks returning both events and insights
#[derive(Default, Clone, PartialEq, Debug)]
pub struct EventAndInsights {
    /// Events being returned
    /// tuples of (port, event)
    pub events: Vec<(Cow<'static, str>, Event)>,
    /// Insights being returned
    pub insights: Vec<Event>,
}

impl From<Vec<(beef::Cow<'static, str>, Event)>> for EventAndInsights {
    fn from(events: Vec<(beef::Cow<'static, str>, Event)>) -> Self {
        Self {
            events,
            ..Self::default()
        }
    }
}

impl From<Event> for EventAndInsights {
    fn from(event: Event) -> Self {
        Self::from(vec![(OUT, event)])
    }
}

#[cfg(test)]
impl EventAndInsights {
    fn len(&self) -> usize {
        self.events.len()
    }
}

/// The operator trait, this reflects the functionality of an operator in the
/// pipeline graph
pub trait Operator: std::fmt::Debug + Send {
    /// Called on every Event. The event and input port are passed in,
    /// a vector of events is passed out.
    fn on_event(
        &mut self,
        uid: u64,
        port: &str,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights>;

    /// Defines if the operatoir shold be called on the singalflow, defaults
    /// to `false`. If set to `true`, `on_signal` should also be implemented.
    #[cfg(not(tarpaulin_include))]
    fn handles_signal(&self) -> bool {
        false
    }
    /// Handle singal events, defaults to returning an empty vector.
    fn on_signal(&mut self, _uid: u64, _signal: &mut Event) -> Result<EventAndInsights> {
        // Make the trait signature nicer
        Ok(EventAndInsights::default())
    }

    /// Defines if the operatoir shold be called on the contraflow, defaults
    /// to `false`. If set to `true`, `on_contraflow` should also be implemented.
    #[cfg(not(tarpaulin_include))]
    fn handles_contraflow(&self) -> bool {
        false
    }

    /// Handles contraflow events - defaults to a noop
    fn on_contraflow(&mut self, _uid: u64, _insight: &mut Event) {
        // Make the trait signature nicer
    }

    /// Returns metrics for this operator, defaults to no extra metrics.
    fn metrics(
        &self,
        _tags: HashMap<Cow<'static, str>, Value<'static>>,
        _timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        // Make the trait signature nicer
        Ok(Vec::new())
    }

    /// An operator is skippable and doesn't need to be executed
    #[cfg(not(tarpaulin_include))]
    fn skippable(&self) -> bool {
        false
    }
}

/// Initialisable trait that can be turned from a `NodeConfig`
pub trait InitializableOperator {
    /// Takes a `NodeConfig` and intialises the operator.
    fn from_node(&self, uid: u64, node: &NodeConfig) -> Result<Box<dyn Operator>>;
}

/// Trait for detecting errors in config and the key names are included in errors
pub trait ConfigImpl {
    /// deserialises the yaml into a struct and returns nice errors
    /// this doesn't need to be overwritten in most cases.
    fn new(config: &serde_yaml::Value) -> Result<Self>
    where
        for<'de> Self: serde::de::Deserialize<'de>,
    {
        // simpler ways, but does not give us the kind of error info we want
        //let validated_config: Config = serde_yaml::from_value(c.clone())?;
        //let validated_config: Config = serde_yaml::from_str(&serde_yaml::to_string(c)?)?;

        // serialize the YAML config and deserialize it again, so that we get extra info on
        // YAML errors here (eg: name of the config key where the errror occured). can just
        // use serde_yaml::from_value() here, but the error message there is limited.
        serde_yaml::from_str(&serde_yaml::to_string(config)?).map_err(|e| {
            // remove the potentially misleading "at line..." info, since it does not
            // correspond to the numbers in the file now
            LINE_REGEXP.replace(&e.to_string(), "").to_string().into()
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn error() {
        #[derive(serde::Deserialize)]
        struct C {
            _s: String,
        };
        impl ConfigImpl for C {};

        let y: serde_yaml::Value = serde_yaml::from_str("5").unwrap();

        let e = C::new(&y).err().unwrap().to_string();

        assert_eq!(e, "invalid type: integer `5`, expected struct C")
    }
}
