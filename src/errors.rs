// Copyright 2018, Wayfair GmbH
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

use error;
use pipeline::types::ValueType;
use prometheus;
use serde_yaml;
use std;

error_chain!{

    foreign_links {
        YAML(serde_yaml::Error) #[doc = "Error during yalm parsing"];
        Io(std::io::Error) #[cfg(unix)];
        Prometheus(prometheus::Error);
        Standard(error::TSError);
    }

    errors {
        MissingSteps(t: String) {
            description("missing steps")
                display("missing steps in: '{}'", t)
        }

        BadOpConfig(e: String) {
            description("Step config has a bad syntax")
                display("Step config has a bad syntax: {}", e)
        }

        TypeError(l: String, expected: ValueType, got: ValueType) {
            description("Type error")
                display("expected type '{}' but found type '{}' in {}", expected, got, l)
        }

        UnknownNamespace(n: String) {
            description("Unknown namespace")
                display("Unknown namespace: {}", n)
        }

        UnknownOp(n: String, o: String) {
            description("Unknown op")
                display("Unknown op: {}::{}", n, o)
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
    }
}
