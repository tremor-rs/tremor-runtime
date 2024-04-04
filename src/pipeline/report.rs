// Copyright 2022-2024, The Tremor Team
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

use tremor_common::ports::Port;
use tremor_system::instance::State;

use super::{DeployEndpoint, InputTarget, OutputTarget};

#[derive(Debug, Clone)]
pub(crate) struct StatusReport {
    pub(crate) state: State,
    pub(crate) inputs: Vec<InputReport>,
    pub(crate) outputs: halfbrown::HashMap<String, Vec<OutputReport>>,
}

impl StatusReport {
    pub fn state(&self) -> State {
        self.state
    }

    pub fn inputs(&self) -> &[InputReport] {
        &self.inputs
    }

    pub fn outputs(&self) -> &halfbrown::HashMap<String, Vec<OutputReport>> {
        &self.outputs
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum InputReport {
    Pipeline { alias: String, port: Port<'static> },
    Source { alias: String, port: Port<'static> },
}

impl InputReport {
    pub(crate) fn pipeline(alias: &str, port: Port<'static>) -> Self {
        Self::Pipeline {
            alias: alias.to_string(),
            port,
        }
    }
    pub(crate) fn source(alias: &str, port: Port<'static>) -> Self {
        Self::Source {
            alias: alias.to_string(),
            port,
        }
    }

    pub(crate) fn new(endpoint: &DeployEndpoint, target: &InputTarget) -> Self {
        match target {
            InputTarget::Pipeline(_addr) => {
                InputReport::pipeline(endpoint.alias(), endpoint.port().clone())
            }
            InputTarget::Source(_addr) => {
                InputReport::source(endpoint.alias(), endpoint.port().clone())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OutputReport {
    Pipeline { alias: String, port: Port<'static> },
    Sink { alias: String, port: Port<'static> },
}

impl OutputReport {
    pub(crate) fn pipeline(alias: &str, port: Port<'static>) -> Self {
        Self::Pipeline {
            alias: alias.to_string(),
            port,
        }
    }
    pub(crate) fn sink(alias: &str, port: Port<'static>) -> Self {
        Self::Sink {
            alias: alias.to_string(),
            port,
        }
    }
}
impl From<&(DeployEndpoint, OutputTarget)> for OutputReport {
    fn from(target: &(DeployEndpoint, OutputTarget)) -> Self {
        match target {
            (endpoint, OutputTarget::Pipeline(_)) => {
                OutputReport::pipeline(endpoint.alias(), endpoint.port().clone())
            }
            (endpoint, OutputTarget::Sink(_)) => {
                OutputReport::sink(endpoint.alias(), endpoint.port().clone())
            }
        }
    }
}
