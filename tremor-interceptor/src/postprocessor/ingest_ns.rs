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
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;

#[derive(Default)]
pub(crate) struct IngestNs {
    last_ns: u64,
}
impl Postprocessor for IngestNs {
    fn is_streaming(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        "attach-ingress-ts"
    }

    fn process(
        &mut self,
        ingres_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        self.last_ns = ingres_ns;
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(ingres_ns)?;
        res.write_all(data)?;

        Ok(vec![res])
    }

    fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        if let Some(data) = data {
            self.process(self.last_ns, 0, data)
        } else {
            Ok(vec![])
        }
    }
}
#[cfg(test)]
mod test {
    use tremor_value::prelude::*;

    use super::*;
    use crate::preprocessor::{ingest_ns::ExtractIngestTs, Preprocessor};

    #[test]
    fn name() {
        let post = IngestNs::default();
        assert_eq!(post.name(), "attach-ingress-ts");
    }
    #[test]
    fn ingest_ts() -> anyhow::Result<()> {
        let mut pre_p = ExtractIngestTs {};
        let mut post_p = IngestNs { last_ns: 0 };

        let data = vec![1_u8, 2, 3];

        let encoded = post_p.process(42, 23, &data)?.pop().expect("no data");

        let mut in_ns = 0u64;
        let decoded = pre_p
            .process(&mut in_ns, &encoded, Value::object())?
            .pop()
            .expect("no data")
            .0;

        assert!(pre_p.finish(None, None)?.is_empty());

        assert_eq!(data, decoded);
        assert_eq!(in_ns, 42);

        // data too short
        assert!(pre_p.process(&mut in_ns, &[0_u8], Value::object()).is_err());
        Ok(())
    }
}
