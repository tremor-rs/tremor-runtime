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

//! Extracts the ingest timestamp from the first 8 bytes of the message and removes it from the message.

use super::prelude::*;
use byteorder::{BigEndian, ReadBytesExt};
use value_trait::Mutable;

#[derive(Clone, Default, Debug)]
pub(crate) struct SchemaRegistryPrefix {}
impl Preprocessor for SchemaRegistryPrefix {
    fn name(&self) -> &str {
        "schema-registry"
    }

    fn process(
        &mut self,
        _ingest_ns: &mut u64,
        data: &[u8],
        mut meta: Value<'static>,
    ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
        use std::io::Cursor;
        if let Some(d) = data.get(8..) {
            let magic = Cursor::new(data).read_u32::<BigEndian>()?;
            if magic != 0 {
                return Err(format!(
                    "Invalid magic bytes (0x00000000) for kafka wire format: {magic}"
                )
                .into());
            }
            let schema = Cursor::new(data).read_u32::<BigEndian>()?;
            meta.insert("schema_id", schema)?;
            Ok(vec![(d.to_vec(), meta)])
        } else {
            Err("Kafka schema registry Preprocessor: < 8 byte".into())
        }
    }
}
