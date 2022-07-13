// Copyright 2022, The Tremor Team
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

use rand::Rng;
use tremor_script::{tremor_fn, Registry, Value};

pub(crate) mod writer;

// The span ID within the trace associated with the log entry.
//  For Trace spans, this is the same format that the Trace API v2
//  uses: a 16-character hexadecimal encoding of an 8-byte array,
// such as 000000000000004a
fn random_span_id_value(ingest_ns_seed: u64) -> Value<'static> {
    let mut rng = tremor_common::rand::make_prng(ingest_ns_seed);
    let span_id: String = (0..8)
        .map(|_| rng.gen::<u8>())
        .map(|b| format!("{:02x}", b))
        .collect();
    Value::from(span_id)
}

fn random_trace_id_value(ingest_ns_seed: u64) -> Value<'static> {
    let mut rng = tremor_common::rand::make_prng(ingest_ns_seed);
    let span_id: String = (0..16)
        .map(|_| rng.gen::<u8>())
        .map(|b| format!("{:02x}", b))
        .collect();
    Value::from(span_id)
}

/// Extend function registry with `GCP Cloud Logging` support
pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (google_logging|gen_span_id_string(ctx) {
            Ok(random_span_id_value(ctx.ingest_ns()))
        }))
        .insert(tremor_fn! (google_logging|gen_trace_id_string(ctx) {
            Ok(random_trace_id_value(ctx.ingest_ns()))
        }));
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;
    use tremor_script::{registry, EventContext};

    #[test_case("gen_span_id_string" ; "google_logging::gen_span_id_string")]
    #[test_case("gen_trace_id_string" ; "google_logging::gen_trace_id_string")]
    fn test_tremor_fns(fun_name: &str) {
        let mut ok_count = 0;
        let mut reg = registry::registry();
        load(&mut reg);

        let f = reg.find("google_logging", fun_name);

        if let Ok(f) = f {
            let context = EventContext::default();
            let r = f.invoke(&context, &[]);
            assert!(r.is_ok());
            ok_count += 1;
        }

        assert_eq!(1, ok_count);
    }
}
