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

//NOTE: error_chain
#![allow(deprecated, missing_docs, clippy::large_enum_variant)]

use error_chain::error_chain;
use tokio::sync::broadcast;
use value_trait::prelude::*;

pub type Kind = ErrorKind;

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl From<Box<dyn std::error::Error + Sync + Send>> for Error {
    fn from(e: Box<dyn std::error::Error + Sync + Send>) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl<T> From<crate::channel::SendError<T>> for Error {
    fn from(e: crate::channel::SendError<T>) -> Self {
        Self::from(format!("{e}"))
    }
}

impl From<crate::channel::TryRecvError> for Error {
    fn from(e: crate::channel::TryRecvError) -> Self {
        Self::from(format!("{e}"))
    }
}

impl<T> From<broadcast::error::SendError<T>> for Error
where
    T: std::fmt::Debug,
{
    fn from(e: broadcast::error::SendError<T>) -> Self {
        Self::from(format!("{e:?}"))
    }
}
impl From<broadcast::error::TryRecvError> for Error {
    fn from(e: broadcast::error::TryRecvError) -> Self {
        Self::from(format!("{e:?}"))
    }
}
impl From<broadcast::error::RecvError> for Error {
    fn from(e: broadcast::error::RecvError) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {e:?}"))
    }
}

impl From<TryTypeError> for Error {
    fn from(e: TryTypeError) -> Self {
        ErrorKind::TypeError(e.expected, e.got).into()
    }
}

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Self) -> bool {
        // This might be Ok since we try to compare Result in tests
        false
    }
}

// TODO: This is a workaround for the fact that `error_chain` does not support send and sync
unsafe impl Send for Error {}
unsafe impl Sync for Error {}

error_chain! {
    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
        Pipeline(tremor_pipeline::errors::Error, tremor_pipeline::errors::ErrorKind);
        Codec(tremor_codec::errors::Error, tremor_codec::errors::ErrorKind);
    }
    foreign_links {
        Archive(tremor_archive::Error);
        AddrParseError(std::net::AddrParseError);
        AnyhowError(anyhow::Error);
        Base64Error(tremor_common::base64::DecodeError);
        ChannelReceiveError(std::sync::mpsc::RecvError);
        Common(tremor_common::Error);
        Config(tremor_config::Error);
        EnvVarError(std::env::VarError);
        FromUtf8Error(std::string::FromUtf8Error);
        Io(std::io::Error);
        JoinError(tokio::task::JoinError);
        JsonAccessError(value_trait::AccessError);
        JsonError(simd_json::Error);
        OneShotRecv(tokio::sync::oneshot::error::RecvError);
        ParseFloatError(std::num::ParseFloatError);
        ParseIntError(std::num::ParseIntError);
        Timeout(tokio::time::error::Elapsed);
        TryFromIntError(std::num::TryFromIntError);
        Utf8Error(std::str::Utf8Error);
        ValueError(tremor_value::Error);
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        ConnectorError(tremor_connectors::errors::Error);
        ConnectorImplGenericError(tremor_connectors::errors::GenericImplementationError);
        SystemConnectorError(tremor_system::connector::Error);
        SystemDataplaneError(tremor_system::dataplane::Error);
        SystemKillswitchError(tremor_system::killswitch::Error);
    }

    errors {
        TypeError(expected: ValueType, found: ValueType) {
            description("Type error")
                display("Type error: Expected {}, found {}", expected, found)
        }


        UnknownOp(n: String, o: String) {
            description("Unknown operator")
                display("Unknown operator: {}::{}", n, o)
        }
        UnknownConnectorType(t: String) {
            description("Unknown connector type")
                display("Unknown connector type {}", t)
        }

        // TODO: Old errors, verify if needed
        BadOpConfig(e: String) {
            description("Operator config has a bad syntax")
                display("Operator config has a bad syntax: {}", e)
        }

        UnknownNamespace(n: String) {
            description("Unknown namespace")
                display("Unknown namespace: {}", n)
        }

        BadUtF8InString {
            description("Bad UTF8 in input string")
                display("Bad UTF8 in input string")

        }
        InvalidCompression {
            description("Data can't be decompressed")
                display("The data did not contain a known magic header to identify a supported compression")
        }
        KvError(s: String) {
            description("KV error")
                display("{}", s)
        }
        TLSError(s: String) {
            description("TLS error")
                display("{}", s)
        }
        InvalidTremorUrl(msg: String, invalid_url: String) {
            description("Invalid Tremor URL")
                display("Invalid Tremor URL {}: {}", invalid_url, msg)
        }

        MissingConfiguration(s: String) {
            description("Missing Configuration")
                display("Missing Configuration for {}", s)
        }
        InvalidConfiguration(configured_thing: String, msg: String) {
            description("Invalid Configuration")
                display("Invalid Configuration for {}: {}", configured_thing, msg)
        }


        DeployFlowError(flow: String, err: String) {
            description("Error deploying Flow")
                display("Error deploying Flow {}: {}", flow, err)
        }
        DuplicateFlow(flow: String) {
            description("Duplicate Flow")
                display("Flow with id \"{}\" is already deployed.", flow)
        }
        FlowNotFound(alias: String) {
            description("Deployment not found")
                display("Deployment \"{}\" not found", alias)
        }
        ConnectorNotFound(flow_id: String, alias: String) {
            description("Connector not found")
                display("Connector \"{}\" not found in Flow \"{}\"", alias, flow_id)
        }

        PipelineSendError(s: String) {
            description("Pipeline send error")
                display("Pipeline send error: {}", s)
        }
        ChannelEmpty {
            description("Channel empty")
                display("Channel empty")
        }
        AlreadyCreated {
            description("Connector already created")
                display("Connector already created")
        }
    }
}

pub(crate) fn empty_error() -> Error {
    ErrorKind::ChannelEmpty.into()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_type_error() {
        let r = Error::from(TryTypeError {
            expected: ValueType::Object,
            got: ValueType::String,
        })
        .0;
        matches!(
            r,
            ErrorKind::TypeError(ValueType::Object, ValueType::String)
        );
    }
}
