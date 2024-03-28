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
#![allow(deprecated, missing_docs, clippy::large_enum_variant)]

use error_chain::error_chain;
use hdrhistogram::{self, serialization as hdr_s};
use tokio::sync::broadcast;
use tremor_common::ports::Port;

pub type Kind = ErrorKind;

impl From<hdr_s::DeserializeError> for Error {
    fn from(e: hdr_s::DeserializeError) -> Self {
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

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {e:?}"))
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
impl From<tokio::sync::mpsc::error::TryRecvError> for Error {
    fn from(e: tokio::sync::mpsc::error::TryRecvError) -> Self {
        Self::from(format!("{e}"))
    }
}

impl From<broadcast::error::RecvError> for Error {
    fn from(e: broadcast::error::RecvError) -> Self {
        Self::from(format!("{e:?}"))
    }
}

impl<T: std::fmt::Debug> From<aws_sdk_s3::error::SdkError<T>> for Error {
    fn from(e: aws_sdk_s3::error::SdkError<T>) -> Self {
        Self::from(ErrorKind::S3Error(format!("{e:?}")))
    }
}

error_chain! {

    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
        // Pipeline(tremor_pipeline::errors::Error, tremor_pipeline::errors::ErrorKind);
        Codec(tremor_codec::errors::Error, tremor_codec::errors::ErrorKind);
        Interceptor(tremor_interceptor::errors::Error, tremor_interceptor::errors::ErrorKind);
    }
    foreign_links {
        UrlParserError(url::ParseError);
        Base64Error(tremor_common::base64::DecodeError);
        ValueError(tremor_value::Error);
        Config(tremor_config::Error);
        Common(tremor_common::Error);
        Io(std::io::Error);
        JsonAccessError(value_trait::AccessError);
        JsonError(simd_json::Error);
        FromUtf8Error(std::string::FromUtf8Error);
        ParseIntError(std::num::ParseIntError);
        Timeout(tokio::time::error::Elapsed);
        TryFromIntError(std::num::TryFromIntError);
        OneShotRecv(tokio::sync::oneshot::error::RecvError);
        JoinError(tokio::task::JoinError);
        AddrParseError(std::net::AddrParseError);
        AsyncChannelRecvError(async_std::channel::RecvError);
        Utf8Error(std::str::Utf8Error);
        AnyhowError(anyhow::Error);

        DnsError(trust_dns_resolver::error::ResolveError) #[cfg(feature = "dns")];

        ElasticError(elasticsearch::Error) #[cfg(feature = "elasticsearch")];
        ElasticTransportBuildError(elasticsearch::http::transport::BuildError) #[cfg(feature = "elasticsearch")];

        HeaderToStringError(http::header::ToStrError) #[cfg(feature = "http")];
        Http(http::Error) #[cfg(feature = "http")];
        HttpHeaderError(http::header::InvalidHeaderValue) #[cfg(feature = "http")];
        Hyper(hyper::Error) #[cfg(feature = "http")];
        InvalidHeaderName(reqwest::header::InvalidHeaderName) #[cfg(feature = "http")];
        InvalidMethod(http::method::InvalidMethod) #[cfg(feature = "http")];
        InvalidStatusCode(http::status::InvalidStatusCode) #[cfg(feature = "http")];
        ReqwestError(reqwest::Error) #[cfg(feature = "http")];
        UriParserError(http::uri::InvalidUri) #[cfg(feature = "http")];
        MimeParsingError(mime::FromStrError) #[cfg(feature = "http")];
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"] #[cfg(feature = "http")];

        InvalidTLSClientName(rustls::client::InvalidDnsNameError) #[cfg(feature = "tls")];
        RustlsError(rustls::Error) #[cfg(feature = "tls")];


        GoogleAuthError(gouth::Error) #[cfg(feature = "gcp")];
        InvalidMetadataValue(tonic::metadata::errors::InvalidMetadataValue) #[cfg(feature = "gcp")];

        S3ByteStream(aws_sdk_s3::primitives::ByteStreamError) #[cfg(feature = "aws")];
        S3Endpoint(aws_smithy_http::endpoint::error::InvalidEndpointError) #[cfg(feature = "aws")];

        KafkaError(rdkafka::error::KafkaError) #[cfg(feature = "kafka")];

        Serenity(serenity::Error) #[cfg(feature = "discord")];

        ModeParseError(file_mode::ModeParseError) #[cfg(feature = "file")];

        Sled(sled::Error) #[cfg(feature = "kv")];

        WalInfailable(qwal::Error<std::convert::Infallible>) #[cfg(feature = "wal")];
        WalJson(qwal::Error<simd_json::Error>) #[cfg(feature = "wal")];

        TonicStatusError(tonic::Status) #[cfg(feature = "otel")];
        TonicTransportError(tonic::transport::Error) #[cfg(feature = "otel")];
        Hex(hex::FromHexError) #[cfg(feature = "otel")];

        //
        Ws(tokio_tungstenite::tungstenite::Error) #[cfg(feature = "websockets")];


        Clickhouse(clickhouse_rs::errors::Error) #[cfg(feature = "clickhouse")];

        CronError(cron::error::Error) #[cfg(feature = "crononome")];


    }

    errors {
        InvalidConnect(target: String, port: Port<'static>) {
            description("Invalid Connect attempt")
                display("Invalid Connect to {} via port {}", target, port)
        }
        InvalidDisconnect(target: String, entity: String, port: Port<'static>) {
            description("Invalid Disonnect attempt")
                display("Invalid Disconnect of {} from {} via port {}", entity, target, port)
        }
        InvalidConfiguration(configured_thing: String, msg: String) {
            description("Invalid Configuration")
                display("Invalid Configuration for {}: {}", configured_thing, msg)
        }
        NoClickHouseClientAvailable {
            description("The ClickHouse adapter has no client available")
            display("The ClickHouse adapter has no client available")
        }
        ExpectedObjectEvent(found_type: value_trait::ValueType) {
            description("Expected object event")
                display("Expected an object event, found a \"{found_type:?}\"")
        }
        MalformedIpAddr {
            description("Malformed IP address")
                display("Malformed IP address")
        }

        BigQueryTypeMismatch(expected: &'static str, actual:value_trait::ValueType) {
            description("Type in the message does not match BigQuery type")
                display("Type in the message does not match BigQuery type. Expected: {}, actual: {:?}", expected, actual)
        }

        TLSError(s: String) {
            description("TLS error")
                display("{}", s)
        }


        MalformedUuid {
            description("Malformed UUID")
                display("Malformed UUID")
        }
        UnexpectedEventFormat(column_name: String, expected_type: String, found_type: value_trait::ValueType) {
            description("Unexpected event format")
                display("Field \"{column_name}\" is of type \'{found_type:?}\" while it should have type \"{expected_type}\"")
        }
        InvalidMetricsData {
            description("Invalid Metrics data")
                display("Invalid Metrics data")
        }
        MissingConfiguration(s: String) {
            description("Missing Configuration")
                display("Missing Configuration for {}", s)
        }

        ProducerNotAvailable(alias: String) {
            description("Producer not available")
                display("Kafka Producer not available for Connector {}", alias)
        }

        NoSocket {
            description("No socket available")
                display("No socket available. Probably not connected yet.")
        }

        InvalidConnectorDefinition(connector_id: String, msg: String) {
            description("Invalid Connector Definition")
                display("Invalid Definition for connector \"{}\": {}", connector_id, msg)
        }
        GoogleCloudStorageError(msg: String) {
            description("Google cloud storage error")
                display("Google cloud storage error: {}", msg)
        }
        ChannelEmpty {
            description("Channel empty")
                display("Channel empty")
        }
        AlreadyCreated {
            description("Connector already created")
                display("Connector already created")
        }
        ClientNotAvailable(name: &'static str, msg: &'static str) {
            description("Client not available")
                display("{} client not available: {}", name, msg)
        }

        GbqSinkFailed(msg: &'static str) {
            description("GBQ Sink failed")
                display("GBQ Sink failed: {}", msg)
        }
        GbqSchemaNotProvided(table: String) {
            description("GBQ Schema not provided")
                display("GBQ Schema not provided for table {}", table)
        }
        PubSubError(msg: &'static str) {
            description("PubSub error")
                display("PubSub error: {}", msg)
        }

        MissingEventColumn(column: String) {
            description("Missing event column")
                display("Column \"{column}\" is missing")
        }

        GclSinkFailed(msg: &'static str) {
            description("Google Cloud Logging Sink failed")
                display("Google Cloud Logging Sink failed: {}", msg)
        }

        GclTypeMismatch(expected: &'static str, actual:value_trait::ValueType) {
            description("Type in the message does not match Google Cloud Logging API type")
            display("Type in the message does not match Google Cloud Logging API type. Expected: {}, actual: {:?}", expected, actual)
        }
        ObjectStorageError(msg: String) {
            description("Object storage error")
                display("{}", msg)
        }


        PipelineSendError(s: String) {
            description("Pipeline send error")
                display("Pipeline send error: {}", s)
        }

        S3Error(n: String) {
            description("S3 Error")
            display("S3Error: {}", n)
        }


    }
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
pub(crate) fn empty_error() -> Error {
    ErrorKind::ChannelEmpty.into()
}
pub(crate) fn already_created_error() -> Error {
    ErrorKind::AlreadyCreated.into()
}
pub(crate) fn err_object_storage(msg: impl Into<String>) -> Error {
    ErrorKind::ObjectStorageError(msg.into()).into()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_err_conector_def() {
        let r = err_connector_def("snot", "badger").0;
        matches!(r, ErrorKind::InvalidConnectorDefinition(_, _));
    }
}
