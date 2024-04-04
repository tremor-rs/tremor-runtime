// Copyright 2021, The Tremor Team
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

pub(crate) use crate::{
    channel::{bounded, Receiver, Sender},
    errors::{Error, GenericImplementationError},
    metrics::make_metrics_payload,
    qsize,
    sink::{
        channel_sink::{ChannelSink, ChannelSinkRuntime},
        AsyncSinkReply, ContraflowData, EventSerializer, ReplySender, Sink, SinkAck, SinkContext,
        SinkManagerBuilder, SinkReply, SinkRuntime, StreamWriter,
    },
    source::{
        ChannelSource, ChannelSourceRuntime, Source, SourceContext, SourceManagerBuilder,
        SourceReply, StreamReader,
    },
    spawn_task,
    utils::hostname,
    CodecReq, Connector, ConnectorBuilder, ConnectorContext, ConnectorType, Context, StreamDone,
    StreamIdGen, ACCEPT_TIMEOUT,
};
pub(crate) use std::sync::atomic::Ordering;
pub use tremor_common::alias;
pub(crate) use tremor_common::{
    ports::{Port, ERR, IN, OUT},
    url::{Defaults, HttpsDefaults, Url},
};
pub(crate) use tremor_config::Impl;
pub use tremor_config::NameWithConfig;
pub(crate) use tremor_script::prelude::*;
pub(crate) use tremor_system::connector::sink::Addr as SinkAddr;
pub(crate) use tremor_system::connector::source::Addr as SourceAddr;
pub(crate) use tremor_system::connector::Attempt;
pub(crate) use tremor_system::controlplane::CbAction;
pub(crate) use tremor_system::event::{Event, EventId, DEFAULT_STREAM_ID};
pub(crate) use tremor_system::killswitch::{KillSwitch, ShutdownMode};
pub(crate) use tremor_system::pipeline::OpMeta;
/// default buf size used for reading from files and streams (sockets etc)
///
/// equals default chunk size for `BufReader`
pub(crate) const DEFAULT_BUF_SIZE: usize = 8 * 1024;
/// default buf size used for reading from files and streams (sockets etc)
pub(crate) fn default_buf_size() -> usize {
    DEFAULT_BUF_SIZE
}

/// Default TCP backlog size
///
/// Value taken from the Rust std library
const DEFAULT_BACKLOG: i32 = 128;

/// Default TCP backlog size
pub(crate) fn default_backlog() -> i32 {
    DEFAULT_BACKLOG
}

/// Encapsulates connector configuration
pub(crate) use crate::config::Connector as ConnectorConfig;

pub(crate) use tremor_common::{default_false, default_true};
