// Copyright 2018-2020, Wayfair GmbH
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
#![allow(clippy::large_enum_variant)]
#![allow(missing_docs)]

use error_chain::error_chain;
impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {:?}", e))
    }
}
impl From<regex::Error> for Error {
    fn from(e: regex::Error) -> Self {
        Self::from(format!("Regex Error: {:?}", e))
    }
}

impl From<sled::TransactionError<()>> for Error {
    fn from(e: sled::TransactionError<()>) -> Self {
        Self::from(format!("Sled Transaction Error: {:?}", e))
    }
}

error_chain! {
    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
    }
    foreign_links {
        YAMLError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        JSONError(simd_json::Error);
        Io(std::io::Error);
        FromUTF8Error(std::string::FromUtf8Error);
        UTF8Error(std::str::Utf8Error);
        ParseIntError(std::num::ParseIntError);
        ParseFloatError(std::num::ParseFloatError);
        Sled(sled::Error);
    }

    errors {
        /*
         * Query langauge pipeline conversion errors
         */
        PipelineError(g: String) {
            description("Error detected in pipeline conversion")
                display("Error detected in trickle: {}", g)
        }

        CyclicGraphError(g: String) {
            description("Cycle detected in graph")
                display("Cycle detected in graph: {}", g)
        }
        MissingOpConfig(e: String) {
            description("Operator config is missing")
                display("Operator config for {} is missing", e)
        }
        ExtraOpConfig(e: String) {
            description("Operator has extra config")
                display("Operator {} has a config but can't be configured", e)
        }

        UnknownOp(n: String, o: String) {
            description("Unknown operator")
                display("Unknown operator: {}::{}", n, o)
        }

        MissingSteps(t: String) {
            description("missing steps")
                display("missing steps in: '{}'", t)
        }

        BadOpConfig(e: String) {
            description("Operator config has a bad syntax")
                display("Operator config has a bad syntax: {}", e)
        }

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
        InvalidInfluxData(s: String, e: tremor_influx::DecoderError) {
            description("Invalid Influx Line Protocol data")
                display("Invalid Influx Line Protocol data: {}\n{}", e, s)
        }
        InvalidJsonData(s: String) {
            description("Invalid JSON")
                display("Invalid JSON data: {}", s)
        }

        BadOutputid(i: usize) {
            description("Bad output pipeline id.")
                display("Bad output pipeline id {}", i - 1)
        }
    }
}

/// Creates a missing config field error
pub fn missing_config(f: &str) -> Error {
    ErrorKind::MissingOpConfig(format!("missing field {}", f)).into()
}
