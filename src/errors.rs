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
#![allow(deprecated)]
#![allow(missing_docs)]
#![allow(clippy::large_enum_variant)]

use beef::Cow;
use error_chain::error_chain;
use hdrhistogram::{self, serialization as hdr_s};
use value_trait::prelude::*;

use tremor_influx as influx;

pub type Kind = ErrorKind;

impl From<sled::transaction::TransactionError<()>> for Error {
    fn from(e: sled::transaction::TransactionError<()>) -> Self {
        Self::from(format!("Sled Transaction Error: {:?}", e))
    }
}

impl From<hdr_s::DeserializeError> for Error {
    fn from(e: hdr_s::DeserializeError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<Box<dyn std::error::Error + Sync + Send>> for Error {
    fn from(e: Box<dyn std::error::Error + Sync + Send>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<hdrhistogram::errors::CreationError> for Error {
    fn from(e: hdrhistogram::errors::CreationError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<hdrhistogram::RecordError> for Error {
    fn from(e: hdrhistogram::RecordError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<hdrhistogram::serialization::V2SerializeError> for Error {
    fn from(e: hdrhistogram::serialization::V2SerializeError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::from(format!("{}", e))
    }
}

impl From<glob::PatternError> for Error {
    fn from(e: glob::PatternError) -> Self {
        Self::from(format!("{}", e))
    }
}

impl<T> From<async_std::channel::SendError<T>> for Error {
    fn from(e: async_std::channel::SendError<T>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl<T> From<async_std::channel::TrySendError<T>> for Error {
    fn from(e: async_std::channel::TrySendError<T>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl<T> From<async_broadcast::TrySendError<T>> for Error {
    fn from(e: async_broadcast::TrySendError<T>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<async_broadcast::TryRecvError> for Error {
    fn from(e: async_broadcast::TryRecvError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<async_broadcast::RecvError> for Error {
    fn from(e: async_broadcast::RecvError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {:?}", e))
    }
}

impl<T: std::fmt::Debug> From<aws_sdk_s3::types::SdkError<T>> for Error {
    fn from(e: aws_sdk_s3::types::SdkError<T>) -> Self {
        Self::from(ErrorKind::S3Error(format!("{e:?}")))
    }
}

impl From<aws_smithy_http::byte_stream::Error> for Error {
    fn from(e: aws_smithy_http::byte_stream::Error) -> Self {
        Self::from(ErrorKind::S3Error(format!("{e}")))
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
    }
    foreign_links {
        AddrParseError(std::net::AddrParseError);
        AnyhowError(anyhow::Error);
        AsyncChannelRecvError(async_std::channel::RecvError);
        AsyncChannelTryRecvError(async_std::channel::TryRecvError);
        Base64Error(base64::DecodeError);
        ChannelReceiveError(std::sync::mpsc::RecvError);
        Clickhouse(clickhouse_rs::errors::Error);
        Common(tremor_common::Error);
        CronError(cron::error::Error);
        CsvError(csv::Error);
        DateTimeParseError(chrono::ParseError);
        DnsError(async_std_resolver::ResolveError);
        ElasticError(elasticsearch::Error);
        ElasticTransportBuildError(elasticsearch::http::transport::BuildError);
        FromUtf8Error(std::string::FromUtf8Error);
        GoogleAuthError(gouth::Error);
        GrokError(grok::Error);
        Hex(hex::FromHexError);
        HttpHeaderError(http::header::InvalidHeaderValue);
        InfluxEncoderError(influx::EncoderError);
        Io(std::io::Error);
        JsonAccessError(value_trait::AccessError);
        JsonError(simd_json::Error);
        KafkaError(rdkafka::error::KafkaError);
        ModeParseError(file_mode::ModeParseError);
        MsgPackDecoderError(rmp_serde::decode::Error);
        MsgPackEncoderError(rmp_serde::encode::Error);
        ParseIntError(std::num::ParseIntError);
        ParseFloatError(std::num::ParseFloatError);
        //Postgres(postgres::Error);
        RegexError(regex::Error);
        ReqwestError(reqwest::Error);
        InvalidHeaderName(reqwest::header::InvalidHeaderName);
        RustlsError(rustls::TLSError);
        Sled(sled::Error);
        SnappyError(snap::Error);
        Timeout(async_std::future::TimeoutError);
        TonicStatusError(tonic::Status);
        TonicTransportError(tonic::transport::Error);
        TryFromIntError(std::num::TryFromIntError);
        ValueError(tremor_value::Error);
        UrlParserError(url::ParseError);
        UriParserError(http::uri::InvalidUri);
        Utf8Error(std::str::Utf8Error);
        WsError(async_tungstenite::tungstenite::Error);
        EnvVarError(std::env::VarError);
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        WalJson(qwal::Error<simd_json::Error>);
        WalInfailable(qwal::Error<std::convert::Infallible>);
        Uuid(uuid::Error);
        Serenity(serenity::Error);
        InvalidMetadataValue(tonic::metadata::errors::InvalidMetadataValue);
    }

    errors {
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

        CodecNotFound(name: String) {
            description("Codec not found")
                display("Codec \"{}\" not found.", name)
        }

        NotCSVSerializableValue(value: String) {
            description("The value cannot be serialized to CSV. Expected an array.")
            display("The value {} cannot be serialized to CSV. Expected an array.", value)
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

        InvalidGelfHeader(len: usize, initial: Option<[u8; 2]>) {
            description("Invalid GELF header")
                display("Invalid GELF header len: {}, prefix: {:?}", len, initial)
        }

        InvalidStatsD {
            description("Invalid statsd metric")
                display("Invalid statsd metric")
        }
        InvalidInfluxData(s: String, e: influx::DecoderError) {
            description("Invalid Influx Line Protocol data")
                display("Invalid Influx Line Protocol data: {}\n{}", e, s)
        }
        InvalidBInfluxData(s: String) {
            description("Invalid BInflux Line Protocol data")
                display("Invalid BInflux Line Protocol data: {}", s)
        }
        InvalidSyslogData(s: &'static str) {
            description("Invalid Syslog Protocol data")
                display("Invalid Syslog Protocol data: {}", s)
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
        InvalidConnect(target: String, port: Cow<'static, str>) {
            description("Invalid Connect attempt")
                display("Invalid Connect to {} via port {}", target, port)
        }
        InvalidDisconnect(target: String, entity: String, port: Cow<'static, str>) {
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
        InvalidInputData(msg: &'static str) {
            description("Invalid Input data")
                display("Invalid Input data: {}", msg)
        }
        GbqSinkFailed(msg: &'static str) {
            description("GBQ Sink failed")
                display("GBQ Sink failed: {}", msg)
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
        GoogleCloudStorageError(msg: &'static str) {
            description("Google cloud storage error")
                display("Google cloud storage error: {}", msg)
        }
        PipelineSendError(s: String) {
            description("Pipeline send error")
                display("Pipeline send error: {}", s)
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn pipe_send_e<T>(e: async_std::channel::SendError<T>) -> Error {
    ErrorKind::PipelineSendError(e.to_string()).into()
}
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn connector_send_err<T>(e: async_std::channel::SendError<T>) -> Error {
    format!("could not send to connector: {e}").into()
}

pub(crate) fn err_connector_def<C: ToString + ?Sized, E: ToString + ?Sized>(c: &C, e: &E) -> Error {
    ErrorKind::InvalidConnectorDefinition(c.to_string(), e.to_string()).into()
}

#[cfg(test)]
mod test {
    use super::*;
    use matches::assert_matches;

    #[test]
    fn test_err_conector_def() {
        let r = err_connector_def("snot", "badger").0;
        assert_matches!(r, ErrorKind::InvalidConnectorDefinition(_, _))
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
        )
    }
}
