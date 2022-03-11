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

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {:?}", e))
    }
}

impl<T: std::fmt::Debug> From<aws_sdk_s3::types::SdkError<T>> for Error {
    fn from(e: aws_sdk_s3::types::SdkError<T>) -> Self {
        Self::from(ErrorKind::S3Error(format!("{:?}", e)))
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
        Postgres(postgres::Error);
        RegexError(regex::Error);
        ReqwestError(reqwest::Error);
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
    }

    errors {
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
        InvalidLines {
            description("Invalid Lines")
                display("Invalid Lines")
        }
    }
}
