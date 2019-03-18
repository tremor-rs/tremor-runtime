// Copyright 2018-2019, Wayfair GmbH
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

#![allow(deprecated)]
use crate::async_sink;
//use crate::pipeline::types::ValueType;
use actix;
use elastic;
use hdrhistogram::{self, serialization as hdr_s};
use log4rs;
use prometheus;
use rdkafka;
use rmp_serde;
use serde_json;
use serde_yaml;
use std;
use tokio_sync::mpsc::error as tokio_sync_error;
use tremor_pipeline;
use url;
//use tokio::sync::mpsc::bounded;

impl Clone for Error {
    fn clone(&self) -> Self {
        ErrorKind::ClonedError(format!("{}", self)).into()
    }
}

impl From<actix::MailboxError> for Error {
    fn from(m: actix::MailboxError) -> Error {
        ErrorKind::MailboxError(m).into()
    }
}
impl From<tokio_sync_error::SendError> for Error {
    fn from(e: tokio_sync_error::SendError) -> Error {
        Error::from(format!("{}", e))
    }
}

impl From<hdr_s::DeserializeError> for Error {
    fn from(e: hdr_s::DeserializeError) -> Error {
        Error::from(format!("{:?}", e))
    }
}

impl From<hdrhistogram::errors::CreationError> for Error {
    fn from(e: hdrhistogram::errors::CreationError) -> Error {
        Error::from(format!("{:?}", e))
    }
}

impl<T> From<crossbeam_channel::SendError<T>> for Error {
    fn from(e: crossbeam_channel::SendError<T>) -> Error {
        Error::from(format!("{:?}", e))
    }
}
impl From<crossbeam_channel::RecvError> for Error {
    fn from(e: crossbeam_channel::RecvError) -> Error {
        Error::from(format!("{:?}", e))
    }
}
/*
impl From<std::option::NoneError> for Error {
    fn from(e: std::option::NoneError) -> Error {
        Error::from(format!("Expected Some but got None: {:?}", e))
    }
}
*/
//impl actix_web::error::ResponseError for Error {}

error_chain! {
    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
        Pipeline(tremor_pipeline::errors::Error, tremor_pipeline::errors::ErrorKind);
    }
    foreign_links {
        YAMLError(serde_yaml::Error) #[doc = "Error during yalm parsing"];
        JSONError(serde_json::Error);
        Io(std::io::Error) #[cfg(unix)];
        Prometheus(prometheus::Error);
        SinkDequeueError(async_sink::SinkDequeueError);
        SinkEnqueueError(async_sink::SinkEnqueueError);
        HTTPClientError(reqwest::Error);
        FromUTF8Error(std::string::FromUtf8Error);
        UTF8Error(std::str::Utf8Error);
        ElasticError(elastic::Error);
        KafkaError(rdkafka::error::KafkaError);
        ParseIntError(std::num::ParseIntError);
        UrlParserError(url::ParseError);
        ParseFloatError(std::num::ParseFloatError);
        LoggerError(log4rs::Error);
        ChannelReceiveError(std::sync::mpsc::RecvError);
        MsgPackDecoderError(rmp_serde::decode::Error);
        MsgPackEncoderError(rmp_serde::encode::Error);
    }

    errors {
        MailboxError(e: actix::MailboxError) {
            description("Actix mailbox error")
                display("Actix mailbox error: {:?}", e)

        }
        UnknownOp(n: String, o: String) {
            description("Unknown operator")
                display("Unknown operator: {}::{}", n, o)
        }
        ArtifactNotFound(id: String) {
            description("The artifact was not found")
                display("The artifact was not found: {}", id)
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

        BindFailedAlreadyExists(key: String) {
            description("The binding already exists")
                display("The binding with the id {} already exists.", key)
        }

        BindFailedKeyNotExists(key: String) {
            description("Failed to bind non existand artefact")
                display("Failed to bind non existand {}.", key)
        }

        // TODO: Old errors, verify if needed
        ClonedError(t: String) {
            description("This is a cloned error we need to get rod of this")
                display("Cloned error: {}", t)
        }
        MissingSteps(t: String) {
            description("missing steps")
                display("missing steps in: '{}'", t)
        }

        BadOpConfig(e: String) {
            description("Operator config has a bad syntax")
                display("Operator config has a bad syntax: {}", e)
        }

        // TypeError(l: String, expected: ValueType, got: ValueType) {
        //     description("Type error")
        //         display("expected type '{}' but found type '{}' in {}", expected, got, l)
        // }

        UnknownNamespace(n: String) {
            description("Unknown namespace")
                display("Unknown namespace: {}", n)
        }


        UnknownSubPipeline(p: String) {
            description("Reference to unknown sub-pipeline")
                display("The sub-pipelines '{}' is not defined", p)
        }

        UnknownPipeline(p: String) {
            description("Reference to unknown pipeline")
                display("The pipelines '{}' is not defined", p)
        }

        PipelineStartError(p: String) {
            description("Failed to start pipeline")
                display("Failed to start pipeline '{}'", p)
        }

        OnrampError(i: u64) {
            description("Error in onramp")
                display("Error in onramp '{}'", i)
        }

        OnrampMissingPipeline(o: String) {
            description("Onramp is missing a pipeline")
                display("The onramp '{}' is missing a pipeline", o)
        }
        InvalidInfluxData(s: String) {
            description("Invalid Influx Line Protocol data")
                display("Invalid Influx Line Protocol data: {}", s)
        }
        BadOutputid(i: usize) {
            description("Bad output pipeline id.")
                display("Bad output pipeline id {}", i - 1)
        }
    }
}
