// Copyright 2024, The Tremor Team
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

pub use crate::{
    config::Connector as ConnectorConfig,
    errors::GenericImplementationError,
    sink::{
        AsyncSinkReply, ContraflowData, EventSerializer, ReplySender, Sink, SinkAck, SinkContext,
        SinkManagerBuilder, SinkReply,
    },
    CodecReq, Connector, ConnectorBuilder, ConnectorType, Context,
};
pub use serde::Deserialize;
pub use tremor_common::alias;
pub use tremor_config::Impl;
pub use tremor_script::EventPayload;
pub use tremor_system::{
    connector::{sink::Addr as SinkAddr, Attempt},
    controlplane::CbAction,
    event::{Event, EventId},
    killswitch::KillSwitch,
    pipeline::OpMeta,
};
pub use tremor_value::Value;
