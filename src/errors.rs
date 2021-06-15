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

use crate::async_sink;
use beef::Cow;
use error_chain::error_chain;
use hdrhistogram::{self, serialization as hdr_s};

use tremor_influx as influx;

pub type Kind = ErrorKind;

// impl Clone for Error {
//     fn clone(&self) -> Self {
//         ErrorKind::ClonedError(format!("{}", self)).into()
//     }
// }

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

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(e: async_channel::SendError<T>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl<T> From<async_channel::TrySendError<T>> for Error {
    fn from(e: async_channel::TrySendError<T>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<tremor_script::errors::CompilerError> for Error {
    fn from(e: tremor_script::errors::CompilerError) -> Self {
        e.error().into()
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {:?}", e))
    }
}

impl<F> From<rental::RentalError<F, Box<Vec<u8>>>> for Error {
    fn from(_e: rental::RentalError<F, Box<Vec<u8>>>) -> Self {
        Self::from("Rental Error".to_string())
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
        ValueError(tremor_value::Error);
        Base64Error(base64::DecodeError);
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        JsonError(simd_json::Error);
        Io(std::io::Error);
        SinkDequeueError(async_sink::SinkDequeueError);
        SinkEnqueueError(async_sink::SinkEnqueueError);
        FromUtf8Error(std::string::FromUtf8Error);
        Utf8Error(std::str::Utf8Error);
        ElasticError(elastic::Error);
        KafkaError(rdkafka::error::KafkaError);
        ParseIntError(std::num::ParseIntError);
        TryFromIntError(std::num::TryFromIntError);
        UrlParserError(url::ParseError);
        ParseFloatError(std::num::ParseFloatError);
        AnyhowError(anyhow::Error);
        ChannelReceiveError(std::sync::mpsc::RecvError);
        MsgPackDecoderError(rmp_serde::decode::Error);
        MsgPackEncoderError(rmp_serde::encode::Error);
        GrokError(grok::Error);
        DateTimeParseError(chrono::ParseError);
        SnappyError(snap::Error);
        AddrParseError(std::net::AddrParseError);
        RegexError(regex::Error);
        WsError(async_tungstenite::tungstenite::Error);
        InfluxEncoderError(influx::EncoderError);
        AsyncChannelRecvError(async_channel::RecvError);
        AsyncChannelTryRecvError(async_channel::TryRecvError);
        JsonAccessError(value_trait::AccessError);
        CronError(cron::error::Error);
        Postgres(postgres::Error);
        Common(tremor_common::Error);
        Sled(sled::Error);
        DnsError(async_std_resolver::ResolveError);
        GoogleAuthError(gouth::Error);
        ReqwestError(reqwest::Error);
        HttpHeaderError(http::header::InvalidHeaderValue);
        Timeout(async_std::future::TimeoutError);
        TonicTransportError(tonic::transport::Error);
        TonicStatusError(tonic::Status);
        RustlsError(rustls::TLSError);
        Hex(hex::FromHexError);
        CsvError(csv::Error);
        ModeParseError(file_mode::ModeParseError);
    }

    errors {
        UnknownOp(n: String, o: String) {
            description("Unknown operator")
                display("Unknown operator: {}::{}", n, o)
        }
        UnknownOnrampType(t: String) {
            description("Unknown onramp type")
                display("Unknown onramp type {}", t)
        }
        UnknownOfframpType(t: String) {
            description("Unknown offramp type")
                display("Unknown offramp type {}", t)
        }
        UnknownConnector(n: String) {
            description("Unknown connector")
                display("Unknown connector {}", n)
        }
        UnknownConnectorType(t: String) {
            description("Unknown connector type")
                display("Unknown connector type {}", t)
        }
        ArtefactNotFound(id: String) {
            description("The artefact was not found")
                display("The artefact was not found: {}", id)
        }
        InstanceNotFound(id: String) {
            description("The artefact instance not found")
                display("The artefact instance {} not found", id)
        }
        CodecNotFound(name: String) {
            description("Codec not found")
                display("The codec was not found: {}", name)
        }
        PublishFailedAlreadyExists(key: String) {
            description("The published artefact already exists")
                display("The published {} already exists.", key)
        }

        UnpublishFailedDoesNotExist(key: String) {
            description("The unpublished artefact does not exist")
                display("The unpublished {} does not exist.", key)
        }
        UnpublishFailedNonZeroInstances(key: String) {
            description("The artefact has non-zero instances and cannot be unpublished")
                display("Cannot unpublish artefact {} which has non-zero instances.", key)
        }
        UnpublishFailedSystemArtefact(key: String) {
            description("The artefact is a system artefact and cannot be unpublished")
                display("Cannot unpublish system artefact {}.", key)
        }

        BindFailedAlreadyExists(key: String) {
            description("The binding already exists")
                display("The binding with the id {} already exists.", key)
        }

        BindFailedKeyNotExists(key: String) {
            description("Failed to bind non existand artefact")
                display("Failed to bind non existand {}.", key)
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

        }
        InvalidCompression {
            description("Data can't be decompressed")
                display("The data did not contain a known magic header to identify a supported compression")
        }
        NoSocket {
            description("No socket available")
                display("No socket available")
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
        InvalidInstanceUrl(url: String) {
            description("Invalid artefact instance URL")
                 display("Invalid artefact instance URL: {}", url)
        }
        MissingConfiguration(s: String) {
            description("Missing Configuration")
                display("Missing Configuration for {}", s)
        }
        InvalidConnect(target: String, port: Cow<'static, str>) {
            description("Invalid Connect attempt")
                display("Invalid Connect to {} via port {}", target, port)
        }
        InvalidDisconnect(target: String, entity: String, port: Cow<'static, str>) {
            description("Invalid Disonnect attempt")
                display("Invalid Disconnect of {} from {} via port {}", entity, target, port)
        }
        MaxRetriesExceeded(max_retries: u64) {
            description("Max reconnect retries exceeded")
                display("Max reconnect retries ({}) exceeded", max_retries)
        }

        InvalidMetricsData {
            description("Invalid Metrics data")
                display("Invalid Metrics data")
        }
    }
}
