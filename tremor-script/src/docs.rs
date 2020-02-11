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

// Structs here are currently for use in tremor-language-server (where they are
// dependencies for the build script as well as the package -- hence they are
// bundled here for common use). These can be eventually utilized from tremor-script,
// as part of structured documentation and automatic doc generation.

use std::fmt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionSignatureDoc {
    pub full_name: String,
    pub args: Vec<String>,
    pub result: String,
}

impl fmt::Display for FunctionSignatureDoc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({}) -> {}",
            self.full_name,
            self.args.join(", "),
            self.result
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionDoc {
    pub signature: FunctionSignatureDoc,
    pub description: String,
    pub summary: Option<String>,
    pub examples: Option<String>,
}

impl fmt::Display for FunctionDoc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}\n\n{}", self.signature, self.description)
    }
}
