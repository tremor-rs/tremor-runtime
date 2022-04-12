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

use crate::lifecycle::ActivatorLifecycleFsm;
use crate::repository::{
    BindingArtefact, ConnectorArtefact, OfframpArtefact, OnrampArtefact, PipelineArtefact,
};
use crate::url::TremorUrl;

/// A servant ID
pub type Id = TremorUrl;
/// A pipeline servant
pub type Pipeline = ActivatorLifecycleFsm<PipelineArtefact>;
/// An onramp servant
pub type Onramp = ActivatorLifecycleFsm<OnrampArtefact>;
/// An offramp servant
pub type Offramp = ActivatorLifecycleFsm<OfframpArtefact>;
/// A binding servant
pub type Binding = ActivatorLifecycleFsm<BindingArtefact>;
/// A connector servant
pub type Connector = ActivatorLifecycleFsm<ConnectorArtefact>;
