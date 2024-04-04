// Copyright 2020-2024, The Tremor Team
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

//NOTE: error_chain

use std::time::Duration;

use tokio::task::JoinError;
use tremor_common::{alias, ports::Port};

/// The error type for connectors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid port
    #[error("{0} Invalid port '{1}'")]
    InvalidPort(alias::Connector, Port<'static>),
    /// Unsupported codec
    #[error("{0} is a structured connector and can't be configured with a codec")]
    UnsupportedCodec(alias::Connector),
    /// Missing codec
    #[error("{0} is missing a codec")]
    MissingCodec(alias::Connector),
    /// Controlplane reply error
    #[error("{0} Controlplane reply error")]
    ControlplaneReply(alias::Connector),
    /// Connection failed
    #[error("{0} Connection failed")]
    ConnectionFailed(alias::Connector),
    /// Invalid configuration
    #[error("{0} Invalid configuration: {1}")]
    InvalidConfiguration(alias::Connector, &'static str),
    /// Invalid definition
    #[error("[{0}] Invalid definition: {1}")]
    InvalidDefinition(alias::Connector, String),
    /// Conector was already created
    #[error("Conector was already created")]
    AlreadyCreated(alias::Connector),
    /// Missing configuration
    #[error("{0} Missing Configuration")]
    MissingConfiguration(alias::Connector),
    /// BuildCfg is unimplemented
    #[error("{0} build_cfg is unimplemented")]
    BuildCfg(alias::Connector),
    /// Value error
    #[error("{0} Value error: {1}")]
    ValueError(alias::Connector, tremor_value::Error),
    /// Connector implementation error
    #[error("{0} Connector implementation error: {1}")]
    ImplError(alias::Connector, anyhow::Error),
    /// Drainage send error
    #[error("{0}  Drainage send error")]
    DrainageSend(alias::Connector),
    /// Reconnect join error
    #[error("{0} reconnect join error: {1}")]
    ReconnectJoin(alias::Connector, JoinError),
    /// Sink backplane error
    #[error("{0} Creating source failed: {1}")]
    CreateSource(alias::Connector, anyhow::Error),
    /// Sink backplane error
    #[error("{0} Creating sink failed: {1}")]
    CreateSink(alias::Connector, anyhow::Error),
    /// Channel empty
    #[error("{0} Channel empty")]
    ChannelEmpty(alias::Connector),
    /// Connector backplane error
    #[error(transparent)]
    ConnectorBackplaneError(#[from] tremor_system::connector::Error),
    /// Sink backplane error
    #[error("{0} Sink backplane error: {1}")]
    SinkBackplane(alias::Connector, tremor_system::connector::sink::Error),
    /// Source backplane error
    #[error("{0} Source backplane error: {1}")]
    SourceBackplane(alias::Connector, tremor_system::connector::source::Error),
    /// Connection lost notification failed
    #[error("{0} Connection lost notification failed")]
    ConnectionLostNotifier(alias::Connector),
}

impl Error {
    /// the alias of the connector
    #[must_use]
    pub fn alias(&self) -> &alias::Connector {
        match self {
            Self::InvalidPort(alias, _)
            | Self::UnsupportedCodec(alias)
            | Self::MissingCodec(alias)
            | Self::ConnectionFailed(alias)
            | Self::InvalidConfiguration(alias, _)
            | Self::InvalidDefinition(alias, _)
            | Self::AlreadyCreated(alias)
            | Self::MissingConfiguration(alias)
            | Self::BuildCfg(alias)
            | Self::ValueError(alias, _)
            | Self::ImplError(alias, _)
            | Self::DrainageSend(alias)
            | Self::SinkBackplane(alias, _)
            | Self::SourceBackplane(alias, _)
            | Self::ConnectionLostNotifier(alias)
            | Self::ChannelEmpty(alias)
            | Self::CreateSink(alias, _)
            | Self::CreateSource(alias, _)
            | Self::ControlplaneReply(alias)
            | Self::ReconnectJoin(alias, _) => alias,
            Self::ConnectorBackplaneError(e) => e.alias(),
        }
    }
}

/// Generic errors for connector implementations
#[derive(Debug, thiserror::Error)]
pub enum GenericImplementationError {
    /// Connector not available
    #[error("{0} client not available")]
    ClientNotAvailable(&'static str),
    /// Connector not available
    #[error("producer not available, already connected")]
    AlreadyConnected,
    /// Connector not available
    #[error("Timeout reached: {0:?}")]
    Timeout(Duration),
    /// Connector not available
    #[error(" Channel empty")]
    ChannelEmpty,
}

/// Utility function to create an invalid definition error
pub fn error_connector_def<E: ToString + ?Sized>(c: &alias::Connector, e: &E) -> Error {
    Error::InvalidDefinition(c.clone(), e.to_string())
}

pub(crate) fn error_impl_def<E: Into<anyhow::Error> + std::error::Error + Send + Sync>(
    c: &alias::Connector,
    e: E,
) -> Error {
    Error::ImplError(c.clone(), e.into())
}
