// Copyright 2018-2019, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a cstd::result::Result::Err(*right_val)::Result::Err(*right_val)License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[derive(Debug, Default, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize)]
pub struct EventContext {
    pub at: u64,
}

impl EventContext {
    pub fn ingest_ns(&self) -> u64 {
        self.at
    }

    pub fn from_ingest_ns(ingest_ns: u64) -> Self {
        Self { at: ingest_ns }
    }
}
