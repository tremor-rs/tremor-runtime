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

use super::Postprocessor;
use crate::errors::*;

#[derive(Clone, Default)]
pub struct GELF {
    id: u64,
}

fn encode_gelf(id: u64, data: &[u8]) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(data.len() + 12);
    // Serialize header
    buf.append(&mut vec![0x1e, 0x0f]);
    buf.append(&mut id.to_be_bytes().to_vec());
    buf.push(0);
    buf.push(1);
    buf.append(&mut data.to_vec());

    Ok(buf)
}

impl Postprocessor for GELF {
    fn process(&mut self, _ingest_ns: u64, _egest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let msg = encode_gelf(self.id, data)?;
        self.id += 1;
        Ok(vec![msg])
    }
}
