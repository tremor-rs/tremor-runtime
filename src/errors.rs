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
use hdrhistogram::{self, serialization as hdr_s};
use tokio::sync::broadcast;
use tremor_common::ports::Port;
use value_trait::prelude::*;

pub type Kind = ErrorKind;

impl From<sled::transaction::TransactionError<()>> for Error {
    fn from(e: sled::transaction::TransactionError<()>) -> Self {
        Self::from(format!("Sled Transaction Error: {e:?}"))
    }
}

impl From<hdr_s::DeserializeError> for Error {
    fn from(e: hdr_s::DeserializeError) -> Self {
        Self::from(format!("{e:?}"))
    }
}

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

impl From<hdrhistogram::errors::CreationError> for Error {
    fn from(e: hdrhistogram::errors::CreationError) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl From<hdrhistogram::RecordError> for Error {
    fn from(e: hdrhistogram::RecordError) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl From<hdrhistogram::serialization::V2SerializeError> for Error {
    fn from(e: hdrhistogram::serialization::V2SerializeError) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::from(format!("{e}"))
    }
}

impl From<glob::PatternError> for Error {
    fn from(e: glob::PatternError) -> Self {
        Self::from(format!("{e}"))
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

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(e: async_channel::SendError<T>) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl<T> From<async_std::channel::SendError<T>> for Error {
    fn from(e: async_std::channel::SendError<T>) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl<T> From<async_std::channel::TrySendError<T>> for Error {
    fn from(e: async_std::channel::TrySendError<T>) -> Self {
        Self::from(format!("{e:?}"))
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

impl<T: std::fmt::Debug> From<aws_sdk_s3::error::SdkError<T>> for Error {
    fn from(e: aws_sdk_s3::error::SdkError<T>) -> Self {
        Self::from(ErrorKind::S3Error(format!("{e:?}")))
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

error_chain! {
    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
        Pipeline(tremor_pipeline::errors::Error, tremor_pipeline::errors::ErrorKind);
        Codec(tremor_codec::errors::Error, tremor_codec::errors::ErrorKind);
        Interceptor(tremor_interceptor::errors::Error, tremor_interceptor::errors::ErrorKind);
    }
    foreign_links {
        AddrParseError(std::net::AddrParseError);
        AnyhowError(anyhow::Error);
        AsyncChannelRecvError(async_std::channel::RecvError);
        AsyncChannelTryRecvError(async_std::channel::TryRecvError);
        Base64Error(tremor_common::base64::DecodeError);
        ChannelReceiveError(std::sync::mpsc::RecvError);
        Clickhouse(clickhouse_rs::errors::Error);
        Common(tremor_common::Error);
        Config(tremor_config::Error);
        CronError(cron::error::Error);
        DnsError(trust_dns_resolver::error::ResolveError);
        ElasticError(elasticsearch::Error);
        ElasticTransportBuildError(elasticsearch::http::transport::BuildError);
        EnvVarError(std::env::VarError);
        FromUtf8Error(std::string::FromUtf8Error);
        GoogleAuthError(gouth::Error);
        GrokError(grok::Error);
        HeaderToStringError(http::header::ToStrError);
        Hex(hex::FromHexError);
        Http(http::Error);
        HttpHeaderError(http::header::InvalidHeaderValue);
        Hyper(hyper::Error);
        InvalidHeaderName(reqwest::header::InvalidHeaderName);
        InvalidMetadataValue(tonic::metadata::errors::InvalidMetadataValue);
        InvalidMethod(http::method::InvalidMethod);
        InvalidStatusCode(http::status::InvalidStatusCode);
        InvalidTLSClientName(rustls::client::InvalidDnsNameError);
        Io(std::io::Error);
        JoinError(tokio::task::JoinError);
        JsonAccessError(value_trait::AccessError);
        JsonError(simd_json::Error);
        KafkaError(rdkafka::error::KafkaError);
        SerdeJsonError(serde_json::Error);
        MimeParsingError(mime::FromStrError);
        ModeParseError(file_mode::ModeParseError);
        OneShotRecv(tokio::sync::oneshot::error::RecvError);
        ParseFloatError(std::num::ParseFloatError);
        ParseIntError(std::num::ParseIntError);
        RaftAPIError(crate::raft::api::client::Error);
        RegexError(regex::Error);
        ReqwestError(reqwest::Error);
        RustlsError(rustls::Error);
        S3ByteStream(aws_sdk_s3::primitives::ByteStreamError);
        S3Endpoint(aws_smithy_http::endpoint::error::InvalidEndpointError);
        Serenity(serenity::Error);
        Sled(sled::Error);
        Timeout(tokio::time::error::Elapsed);
        TonicStatusError(tonic::Status);
        TonicTransportError(tonic::transport::Error);
        TryFromIntError(std::num::TryFromIntError);
        UriParserError(http::uri::InvalidUri);
        UrlParserError(url::ParseError);
        Utf8Error(std::str::Utf8Error);
        ValueError(tremor_value::Error);
        WalInfailable(qwal::Error<std::convert::Infallible>);
        WalJson(qwal::Error<simd_json::Error>);
        Ws(tokio_tungstenite::tungstenite::Error);
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        CheckIsLeaderError(openraft::error::RaftError<crate::raft::NodeId, openraft::error::CheckIsLeaderError<crate::raft::NodeId, crate::raft::node::Addr>>);
        ClientWriteError(openraft::error::RaftError<crate::raft::NodeId, openraft::error::ClientWriteError<crate::raft::NodeId, crate::raft::node::Addr>>);
    }

    errors {
        RaftNotRunning {
            description("Raft is not running")
                display("Raft is not running")
        }
        TypeError(expected: ValueType, found: ValueType) {
            description("Type error")
                display("Type error: Expected {}, found {}", expected, found)
        }

        S3Error(n: String) {
            description("S3 Error")
            display("S3Error: {}", n)
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
        InvalidConnectorDefinition(connector_id: String, msg: String) {
            description("Invalid Connector Definition")
                display("Invalid Definition for connector \"{}\": {}", connector_id, msg)
        }
        InvalidConnect(target: String, port: Port<'static>) {
            description("Invalid Connect attempt")
                display("Invalid Connect to {} via port {}", target, port)
        }
        InvalidDisconnect(target: String, entity: String, port: Port<'static>) {
            description("Invalid Disonnect attempt")
                display("Invalid Disconnect of {} from {} via port {}", entity, target, port)
        }
        InvalidMetricsData {
            description("Invalid Metrics data")
                display("Invalid Metrics data")
        }
        NoSocket {
            description("No socket available")
                display("No socket available. Probably not connected yet.")
        }
        FlowFailed(flow: String) {
            description("Flow entered failed state")
            display("Flow {flow} entered failed state")
        }
        DeployFlowError(flow: String, err: String) {
            description("Error deploying Flow")
                display("Error deploying Flow {}: {}", flow, err)
        }
        DuplicateFlow(flow: String) {
            description("Duplicate Flow")
                display("Flow with id \"{}\" is already deployed.", flow)
        }
        ProducerNotAvailable(alias: String) {
            description("Producer not available")
                display("Kafka Producer not available for Connector {}", alias)
        }
        FlowNotFound(alias: String) {
            description("Deployment not found")
                display("Deployment \"{}\" not found", alias)
        }
        ConnectorNotFound(flow_id: String, alias: String) {
            description("Connector not found")
                display("Connector \"{}\" not found in Flow \"{}\"", alias, flow_id)
        }

        GbqSinkFailed(msg: &'static str) {
            description("GBQ Sink failed")
                display("GBQ Sink failed: {}", msg)
        }
        GbqSchemaNotProvided(table: String) {
            description("GBQ Schema not provided")
                display("GBQ Schema not provided for table {}", table)
        }
        ClientNotAvailable(name: &'static str, msg: &'static str) {
            description("Client not available")
                display("{} client not available: {}", name, msg)
        }
        PubSubError(msg: &'static str) {
            description("PubSub error")
                display("PubSub error: {}", msg)
        }
        BigQueryTypeMismatch(expected: &'static str, actual:value_trait::ValueType) {
            description("Type in the message does not match BigQuery type")
                display("Type in the message does not match BigQuery type. Expected: {}, actual: {:?}", expected, actual)
        }


        NoClickHouseClientAvailable {
            description("The ClickHouse adapter has no client available")
            display("The ClickHouse adapter has no client available")
        }

        ExpectedObjectEvent(found_type: ValueType) {
            description("Expected object event")
                display("Expected an object event, found a \"{found_type:?}\"")
        }

        MissingEventColumn(column: String) {
            description("Missing event column")
                display("Column \"{column}\" is missing")
        }

        UnexpectedEventFormat(column_name: String, expected_type: String, found_type: ValueType) {
            description("Unexpected event format")
                display("Field \"{column_name}\" is of type \'{found_type:?}\" while it should have type \"{expected_type}\"")
        }

        MalformedIpAddr {
            description("Malformed IP address")
                display("Malformed IP address")
        }

        MalformedUuid {
            description("Malformed UUID")
                display("Malformed UUID")
        }

        GclSinkFailed(msg: &'static str) {
            description("Google Cloud Logging Sink failed")
                display("Google Cloud Logging Sink failed: {}", msg)
        }

        GclTypeMismatch(expected: &'static str, actual:value_trait::ValueType) {
            description("Type in the message does not match Google Cloud Logging API type")
            display("Type in the message does not match Google Cloud Logging API type. Expected: {}, actual: {:?}", expected, actual)
        }
        GoogleCloudStorageError(msg: String) {
            description("Google cloud storage error")
                display("Google cloud storage error: {}", msg)
        }
        ObjectStorageError(msg: String) {
            description("Object storage error")
                display("{}", msg)
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
pub(crate) fn already_created_error() -> Error {
    ErrorKind::AlreadyCreated.into()
}
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn pipe_send_e<T>(e: crate::channel::SendError<T>) -> Error {
    ErrorKind::PipelineSendError(e.to_string()).into()
}
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn connector_send_err<T>(e: crate::channel::SendError<T>) -> Error {
    format!("could not send to connector: {e}").into()
}

pub(crate) fn err_connector_def<C: ToString + ?Sized, E: ToString + ?Sized>(c: &C, e: &E) -> Error {
    ErrorKind::InvalidConnectorDefinition(c.to_string(), e.to_string()).into()
}

pub(crate) fn err_gcs(msg: impl Into<String>) -> Error {
    ErrorKind::GoogleCloudStorageError(msg.into()).into()
}

pub(crate) fn err_object_storage(msg: impl Into<String>) -> Error {
    ErrorKind::ObjectStorageError(msg.into()).into()
}

#[cfg(test)]
mod test {
    use super::*;
    use matches::assert_matches;

    #[test]
    fn test_err_conector_def() {
        let r = err_connector_def("snot", "badger").0;
        assert_matches!(r, ErrorKind::InvalidConnectorDefinition(_, _));
    }

    #[test]
    fn test_type_error() {
        let r = Error::from(TryTypeError {
            expected: ValueType::Object,
            got: ValueType::String,
        })
        .0;
        assert_matches!(
            r,
            ErrorKind::TypeError(ValueType::Object, ValueType::String)
        );
    }
    #[test]
    fn aws_error() {
        use aws_sdk_s3::error::SdkError;
        #[derive(Debug)]
        struct DemoError();
        impl std::fmt::Display for DemoError {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "DemoError")
            }
        }
        impl std::error::Error for DemoError {}
        let e: SdkError<()> = SdkError::timeout_error(Box::new(DemoError()));
        let r = Error::from(e);
        assert_matches!(r.0, ErrorKind::S3Error(_));
    }

    #[test]
    fn send_error() {
        let e = async_channel::SendError(0u8);
        let error = Error::from(e);
        assert_eq!(error.to_string(), "SendError(..)".to_string(),);
    }
}
