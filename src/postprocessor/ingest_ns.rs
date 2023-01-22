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

//! Prepends the event ingest timestamp as an unsigned 64 bit big-endian integer before the evetn payload.

use super::Postprocessor;
use crate::Result;
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;

#[derive(Default)]
pub(crate) struct IngestNs {}
impl Postprocessor for IngestNs {
    fn name(&self) -> &str {
        "attach-ingress-ts"
    }

    fn process(&mut self, ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(ingres_ns)?;
        res.write_all(data)?;

        Ok(vec![res])
    }
}
