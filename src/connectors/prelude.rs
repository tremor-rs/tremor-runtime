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
    connectors::{
        metrics::make_metrics_payload,
        sink::{
            channel_sink::{ChannelSink, ChannelSinkRuntime},
            AsyncSinkReply, ContraflowData, EventSerializer, ReplySender, Sink, SinkAck, SinkAddr,
            SinkContext, SinkManagerBuilder, SinkReply, SinkRuntime, StreamWriter,
        },
        source::{
            ChannelSource, ChannelSourceRuntime, Source, SourceAddr, SourceContext,
            SourceManagerBuilder, SourceReply, StreamReader,
        },
        spawn_task,
        utils::reconnect::Attempt,
        Alias, CodecReq, Connector, ConnectorBuilder, ConnectorContext, ConnectorType, Context,
        StreamDone, StreamIdGen, ACCEPT_TIMEOUT,
    },
    errors::{err_connector_def, Error, Kind as ErrorKind, Result},
    qsize,
    system::KillSwitch,
    utils::hostname,
    Event,
};

pub(crate) use std::sync::atomic::Ordering;
pub(crate) use tremor_common::{
    ports::{Port, ERR, IN, OUT},
    url::{Defaults, HttpsDefaults, Url},
};
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

/// Default TCP backlog size
///
/// Value taken from the Rust std library
const DEFAULT_BACKLOG: i32 = 128;

/// Default TCP backlog size
pub(crate) fn default_backlog() -> i32 {
    DEFAULT_BACKLOG
}

/// Encapsulates connector configuration
pub(crate) use crate::connectors::ConnectorConfig;

pub(crate) fn default_true() -> bool {
    true
}
pub(crate) fn default_false() -> bool {
    false
}
