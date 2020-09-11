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

use error_chain::error_chain;

impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::from(format!("{}", e))
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {:?}", e))
    }
}

impl From<tremor_script::errors::CompilerError> for Error {
    fn from(e: tremor_script::errors::CompilerError) -> Self {
        e.error().into()
    }
}

error_chain! {
    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
        Pipeline(tremor_pipeline::errors::Error, tremor_pipeline::errors::ErrorKind);
        Runtime(tremor_runtime::errors::Error, tremor_runtime::errors::ErrorKind);
    }
    foreign_links {
        YAMLError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        JSONError(simd_json::Error) #[doc = "Error during json parsing"];
        Io(std::io::Error) #[doc = "Error during std::io"];
        SendError(std::sync::mpsc::SendError<String>);
        LoggingError(log4rs::Error);
        TestKindError(crate::test::UnknownKind);
    }
}
