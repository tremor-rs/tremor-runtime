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

#[cfg(test)]
mod test {
    use tremor_runtime::errors::*;
    use tremor_runtime::postprocessor::Postprocessor;
    use tremor_runtime::preprocessor::Preprocessor;

    macro_rules! assert_simple_symmetric {
        ($internal:expr, $which:ident, $magic:expr) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = tremor_runtime::preprocessor::$which::default();
            let mut outbound = tremor_runtime::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0u64;
            let egress_ns = 1u64;

            let r = outbound.process(ingest_ns, egress_ns, $internal);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected ( magic code only )
            assert_eq!($magic, decode_magic(&ext));

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$internal, &out);
        };
    }

    macro_rules! assert_symmetric {
        ($inbound:expr, $outbound:expr, $which:ident) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = tremor_runtime::preprocessor::$which::default();
            let mut outbound = tremor_runtime::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0u64;
            let egress_ns = 1u64;

            let enc = $outbound.clone();
            let r = outbound.process(ingest_ns, egress_ns, $inbound);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected
            assert_eq!(&enc, &ext);

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$inbound, &out);
        };
    }

    macro_rules! assert_not_symmetric {
        ($inbound:expr, $internal:expr, $outbound:expr, $which:ident) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = tremor_runtime::preprocessor::$which::default();
            let mut outbound = tremor_runtime::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0u64;
            let egress_ns = 1u64;

            let enc = $internal.clone();
            let r = outbound.process(ingest_ns, egress_ns, $inbound);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected
            assert_eq!(&enc, &ext);

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$outbound, &out);
        };
    }

    fn decode_magic(data: &[u8]) -> &'static str {
        dbg!(&data.get(0..6));
        match data.get(0..6) {
            Some(&[0x1f, 0x8b, _, _, _, _]) => "gzip",
            Some(&[0x78, _, _, _, _, _]) => "zlib",
            Some(&[0xfd, b'7', b'z', _, _, _]) => "xz2",
            Some(b"sNaPpY") => "snap",
            Some(&[0xff, 0x6, 0x0, 0x0, _, _]) => "snap",
            Some(&[0x04, 0x22, 0x4d, 0x18, _, _]) => "lz4",
            _ => "fail/unknown",
        }
    }

    #[test]
    fn test_lines() -> Result<()> {
        let int = "snot\nbadger".as_bytes();
        let enc = "snot\nbadger\n".as_bytes(); // First event ( event per line )
        let out = "snot".as_bytes();
        assert_not_symmetric!(int, enc, out, Lines);
        Ok(())
    }

    #[test]
    fn test_base64() -> Result<()> {
        let int = "snot badger".as_bytes();
        let enc = "c25vdCBiYWRnZXI=".as_bytes();
        assert_symmetric!(int, enc, Base64);
        Ok(())
    }

    #[test]
    fn test_gzip() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Gzip, "gzip");
        Ok(())
    }

    #[test]
    fn test_zlib() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Zlib, "zlib");
        Ok(())
    }

    #[test]
    fn test_snappy() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Snappy, "snap");
        Ok(())
    }

    #[test]
    fn test_xz2() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Xz2, "xz2");
        Ok(())
    }

    #[test]
    fn test_lz4() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Lz4, "lz4");
        Ok(())
    }
}
