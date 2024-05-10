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

/// Extracts the ingest timestamp from the first 8 bytes of the message and removes it from the message.
#[derive(Clone, Default, Debug)]
pub(crate) struct ExtractIngestTs {}

/// `ExtractIngestTs` error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Message too small
    #[error("Extract Ingest Ts Preprocessor: < 8 byte")]
    MessageTooSmall,
}
impl Preprocessor for ExtractIngestTs {
    fn name(&self) -> &str {
        "ingest-ts"
    }

    fn process(
        &mut self,
        ingest_ns: &mut u64,
        data: &[u8],
        meta: Value<'static>,
    ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
        use std::io::Cursor;
        if let Some(d) = data.get(8..) {
            *ingest_ns = Cursor::new(data).read_u64::<BigEndian>()?;
            Ok(vec![(d.to_vec(), meta)])
        } else {
            Err(Error::MessageTooSmall.into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn name() {
        let pre = ExtractIngestTs {};
        assert_eq!(pre.name(), "ingest-ts");
    }

    #[test]
    fn test_extract_ingest_ts() -> anyhow::Result<()> {
        let mut ingest_ns = 0_u64;
        let mut pre = ExtractIngestTs {};
        let data = &[0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 42u8, b'8', b'9'][..];
        let meta = Value::null();
        let res = pre.process(&mut ingest_ns, data, meta)?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, b"89");
        assert_eq!(ingest_ns, 42);
        Ok(())
    }

    #[test]
    fn test_extract_ingest_ts_too_small() -> anyhow::Result<()> {
        let mut ingest_ns = 0_u64;
        let mut pre = ExtractIngestTs {};
        let data = &[0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8][..];

        let meta = Value::null();
        let res = pre.process(&mut ingest_ns, data, meta);
        assert!(res.is_err());
        Ok(())
    }
}
