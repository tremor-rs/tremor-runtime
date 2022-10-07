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

//
// Artefact instance lifecycle support and specializations for the
// different artefact types
//

use std::fmt::{Display, Formatter};

/// Possible lifecycle states of an instance
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// initializing - first state after coming to life
    Initializing,
    /// Running and consuming/producing/handling events
    Running,
    /// Paused, not consuming/producing/handling events
    Paused,
    /// Drained - flushing out all the pending events
    Draining,
    /// Stopped, final state
    Stopped,
    /// failed to start
    Failed,
}

impl State {
    /// checks if the state is stopped
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        *self == State::Stopped
    }
}

impl Display for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Initializing => "initialized",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        })
    }
}

/// Representing the state an instance should be in
/// subset of `State` as those are the only states instances can be transitioned to
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub enum IntendedState {
    #[default]
    Running,
    Paused,
    Stopped,
}

impl Display for IntendedState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Running => "running",
                Self::Paused => "paused",
                Self::Stopped => "stopped",
            }
        )
    }
}

impl From<&IntendedState> for State {
    fn from(intended: &IntendedState) -> Self {
        match intended {
            IntendedState::Paused => State::Paused,
            IntendedState::Running => State::Running,
            IntendedState::Stopped => State::Stopped,
        }
    }
}
