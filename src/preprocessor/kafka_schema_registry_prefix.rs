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
            let mut c = Cursor::new(data);
            let magic = c.read_u32::<BigEndian>()?;
            if magic != 0 {
                return Err(format!(
                    "Invalid magic bytes (0x00000000) for kafka wire format: {magic}"
                )
                .into());
            }
            let schema = c.read_u32::<BigEndian>()?;
            meta.insert("schema_id", schema)?;
            Ok(vec![(d.to_vec(), meta)])
        } else {
            Err("Kafka schema registry Preprocessor: < 8 byte".into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use value_trait::ValueAccess;

    /// Tests if the preprocessor errors on data that's less then 8 bytes
    #[test]
    fn test_preprocessor_less_then_8_bytes() {
        let mut pp = SchemaRegistryPrefix::default();
        let mut ingest_ns = 0;
        let data = vec![0, 0, 0, 0, 0, 0, 0];
        let meta = Value::object();
        let res = pp.process(&mut ingest_ns, &data, meta);
        assert!(res.is_err());
    }

    /// Tests if `schema_id` is added to the meta data properly
    #[test]
    fn test_preprocessor_schema_id() -> Result<()> {
        let mut pp = SchemaRegistryPrefix::default();
        let mut ingest_ns = 0;
        let data = vec![0, 0, 0, 0, 0, 0, 0, 1, 42];
        let meta = Value::object();
        let mut res = pp.process(&mut ingest_ns, &data, meta)?;
        let (rest, meta) = res.pop().expect("no result");
        assert_eq!(meta.get_u8("schema_id"), Some(1));
        assert_eq!(rest, vec![42]);
        Ok(())
    }

    /// Tests if the preprocessor errors on invalid magic bytes
    #[test]
    fn test_preprocessor_invalid_magic_bytes() {
        let mut pp = SchemaRegistryPrefix::default();
        let mut ingest_ns = 0;
        let data = vec![0, 0, 0, 1, 0, 0, 0, 1];
        let meta = Value::object();
        let res = pp.process(&mut ingest_ns, &data, meta);
        assert!(res.is_err());
    }
}
