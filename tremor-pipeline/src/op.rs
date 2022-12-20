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
use tremor_common::{ids::OperatorId, ports::Port};
use tremor_value::Value;

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
    pub events: Vec<(Port<'static>, Event)>,
    /// Insights being returned
    pub insights: Vec<Event>,
}

impl From<Vec<(Port<'static>, Event)>> for EventAndInsights {
    fn from(events: Vec<(Port<'static>, Event)>) -> Self {
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
pub trait Operator: std::fmt::Debug + Send + Sync {
    /// Called on every Event. The event and input port are passed in,
    /// a vector of events is passed out.
    ///
    /// # Errors
    /// if the event can not be processed
    fn on_event(
        &mut self,
        uid: OperatorId,
        port: &Port<'static>,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights>;

    /// Defines if the operatoir shold be called on the singalflow, defaults
    /// to `false`. If set to `true`, `on_signal` should also be implemented.

    fn handles_signal(&self) -> bool {
        false
    }
    /// Handle singal events, defaults to returning an empty vector.
    /// Gets an immutable reference to the pipeline state
    ///
    /// # Errors
    /// if the singal can not be processed
    fn on_signal(
        &mut self,
        _uid: OperatorId,
        _state: &mut Value<'static>,
        _signal: &mut Event,
    ) -> Result<EventAndInsights> {
        // Make the trait signature nicer
        Ok(EventAndInsights::default())
    }

    /// Defines if the operatoir shold be called on the contraflow, defaults
    /// to `false`. If set to `true`, `on_contraflow` should also be implemented.

    fn handles_contraflow(&self) -> bool {
        false
    }

    /// Handles contraflow events - defaults to a noop
    ///
    /// # Errors
    /// if the insight can not be processed
    fn on_contraflow(&mut self, _uid: OperatorId, _insight: &mut Event) {
        // Make the trait signature nicer
    }

    /// Returns metrics for this operator, defaults to no extra metrics.
    ///
    /// # Errors
    /// if metrics can not be generated
    fn metrics(
        &self,
        _tags: &HashMap<Cow<'static, str>, Value<'static>>,
        _timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        // Make the trait signature nicer
        Ok(Vec::new())
    }

    /// An operator is skippable and doesn't need to be executed

    fn skippable(&self) -> bool {
        false
    }

    /// Initial state for an operator
    fn initial_state(&self) -> Value<'static> {
        Value::const_null()
    }
}

/// Initialisable trait that can be turned from a `NodeConfig`
pub trait InitializableOperator {
    /// Takes a `NodeConfig` and intialises the operator.
    ///
    /// # Errors
    //// if no operator con be instanciated from the provided NodeConfig
    fn node_to_operator(&self, uid: OperatorId, node: &NodeConfig) -> Result<Box<dyn Operator>>;
}

/// Trait for detecting errors in config and the key names are included in errors
pub trait ConfigImpl {
    /// deserialises the config into a struct and returns nice errors
    /// this doesn't need to be overwritten in most cases.
    ///
    /// # Errors
    /// if the Configuration is invalid
    fn new(config: &tremor_value::Value) -> Result<Self>
    where
        Self: serde::de::Deserialize<'static>,
    {
        Ok(tremor_value::structurize(config.clone_static())?)
    }
}
