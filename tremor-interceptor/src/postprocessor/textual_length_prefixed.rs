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

//! Prefixes the data with the length of the event data in bytes as an unsigned 64 bit big-endian integer.

use super::Postprocessor;
use std::io::Write;

#[derive(Clone, Default)]
pub(crate) struct TextualLengthPrefixed {}
impl Postprocessor for TextualLengthPrefixed {
    fn name(&self) -> &str {
        "textual-length-prefixed"
    }

    fn process(
        &mut self,
        _ingres_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let size = data.len();
        let mut digits: Vec<u8> = size.to_string().into_bytes();
        let mut res = Vec::with_capacity(digits.len() + 1 + size);
        res.append(&mut digits);
        res.push(32);
        res.write_all(data)?;
        Ok(vec![res])
    }
}
