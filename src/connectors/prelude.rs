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

pub(crate) use crate::connectors::sink::{
    AsyncSinkReply, ChannelSink, ChannelSinkRuntime, ContraflowData, EventSerializer,
    SingleStreamSink, SingleStreamSinkRuntime, Sink, SinkAck, SinkAddr, SinkContext,
    SinkManagerBuilder, SinkMeta, SinkReply, SinkRuntime, StreamWriter,
};

pub(crate) use crate::connectors::source::{
    ChannelSource, ChannelSourceRuntime, Source, SourceAddr, SourceContext, SourceManagerBuilder,
    SourceReply, StreamReader,
};
pub(crate) use crate::connectors::utils::{
    reconnect::Attempt,
    url::{Defaults, Url},
};
pub(crate) use crate::connectors::{
    metrics::make_metrics_payload, spawn_task, Alias, CodecReq, Connector, ConnectorBuilder,
    ConnectorContext, ConnectorType, Context, StreamDone, StreamIdGen, ACCEPT_TIMEOUT,
};
pub(crate) use crate::errors::{err_connector_def, Error, Kind as ErrorKind, Result};
pub(crate) use crate::system::KillSwitch;
pub(crate) use crate::utils::hostname;
pub(crate) use crate::{Event, QSIZE};
pub(crate) use std::sync::atomic::Ordering;
pub(crate) use tremor_common::ports::{ERR, IN, OUT};
pub use tremor_pipeline::{
    CbAction, ConfigImpl, EventIdGenerator, EventOriginUri, DEFAULT_STREAM_ID,
};

pub(crate) use tremor_script::prelude::*;
/// default buf size used for reading from files and streams (sockets etc)
///
/// equals default chunk size for `BufReader`
pub(crate) const DEFAULT_BUF_SIZE: usize = 8 * 1024;
/// default buf size used for reading from files and streams (sockets etc)
pub(crate) fn default_buf_size() -> usize {
    DEFAULT_BUF_SIZE
}

/// Encapsulates connector configuration
pub(crate) use crate::connectors::ConnectorConfig;

pub(crate) fn default_true() -> bool {
    true
}
pub(crate) fn default_false() -> bool {
    false
}
