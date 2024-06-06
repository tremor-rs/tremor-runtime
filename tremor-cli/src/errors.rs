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

use crate::util::SourceKind;
use error_chain::error_chain;

impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::from(format!("{e}"))
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {e:?}"))
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::from("Send Error")
    }
}

error_chain! {
    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
        Pipeline(tremor_pipeline::errors::Error, tremor_pipeline::errors::ErrorKind);
        Runtime(tremor_runtime::errors::Error, tremor_runtime::errors::ErrorKind);
        Codec(tremor_codec::errors::Error, tremor_codec::errors::ErrorKind);
    }
    foreign_links {
        Archive(tremor_archive::Error);
        Value(tremor_value::Error);
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        JsonError(simd_json::Error) #[doc = "Error during json parsing"];
        Io(std::io::Error) #[doc = "Error during std::io"];
        Fmt(std::fmt::Error) #[doc = "Error during std::fmt"];
        Timeout(tokio::time::error::Elapsed) #[doc = "Error waiting for futures to complete"];
        Globwalk(globwalk::GlobError) #[doc = "Glob walker error"];
        SendError(std::sync::mpsc::SendError<String>);
        AnyhowError(anyhow::Error);
        Url(url::ParseError) #[doc = "Error while parsing a url"];
        Common(tremor_common::Error);
        ParseIntError(std::num::ParseIntError);
        JoinError(tokio::task::JoinError);
        PreprocessorError(tremor_interceptor::preprocessor::Error);
        PostprocessorError(tremor_interceptor::postprocessor::Error);

    }
    errors {
        TestFailures(stats: crate::test::stats::Stats) {
            description("Some tests failed")
                display("{} out of {} tests failed.\nFailed tests: {}",
                  stats.fail, stats.fail + stats.skip + stats.pass, stats.print_failed_test_names())
        }
        FileLoadError(file: String, error: tremor_runtime::errors::Error) {
            description("Failed to load config file")
                display("An error occurred while loading the file `{}`: {}", file, error)
        }
        UnsupportedFileType(file: String, file_type: SourceKind, expected: &'static str) {
            description("Unsupported file type")
                display("The file `{}` has the type `{}`, but tremor expected: {}", file, file_type, expected)
        }
    }
}
