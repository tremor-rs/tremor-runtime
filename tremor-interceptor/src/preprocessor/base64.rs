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

//! Decodes base64 encoded data to the raw bytes.
use super::prelude::*;
use tremor_common::base64;

#[derive(Clone, Default, Debug)]
pub(crate) struct Base64 {}
impl Preprocessor for Base64 {
    fn name(&self) -> &str {
        "base64"
    }

    fn process(
        &mut self,
        _ingest_ns: &mut u64,
        data: &[u8],
        meta: Value<'static>,
    ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
        Ok(vec![(base64::decode(data)?, meta)])
    }
}

#[cfg(test)]

mod test {
    use crate::postprocessor::{self, Postprocessor};

    #[test]
    fn name() {
        let post = postprocessor::base64::Base64 {};
        assert_eq!(post.name(), "base64");
    }

    use super::*;
    #[test]
    fn test_base64() -> anyhow::Result<()> {
        let int = "snot badger".as_bytes();
        let enc = "c25vdCBiYWRnZXI=".as_bytes();

        let mut pre = Base64::default();
        let mut post = postprocessor::base64::Base64::default();

        // Fake ingest_ns and egress_ns
        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, int);
        let ext = &r?[0];
        let ext = ext.as_slice();
        // Assert actual encoded form is as expected
        assert_eq!(&enc, &ext);

        let r = pre.process(&mut ingest_ns, ext, Value::object());
        let out = &r?[0].0;
        let out = out.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(&int, &out);

        // assert empty finish, no leftovers
        assert!(pre.finish(None, None)?.is_empty());
        Ok(())
    }
}
