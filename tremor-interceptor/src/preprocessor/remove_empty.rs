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

//! Removes empty messages (aka zero len). This one is best used in a chain after a splitting preprocessor, like [`separate`](./separate.md)

use super::prelude::*;
use crate::errors::Result;

#[derive(Default, Debug, Clone)]
pub(crate) struct RemoveEmpty {}

impl Preprocessor for RemoveEmpty {
    fn name(&self) -> &str {
        "remove-empty"
    }
    fn process(
        &mut self,
        _ingest_ns: &mut u64,
        data: &[u8],
        meta: Value<'static>,
    ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
        if data.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![(data.to_vec(), meta)])
        }
    }
}
