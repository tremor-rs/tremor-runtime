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

pub(crate) mod common;
pub(crate) mod id;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod resource;
pub(crate) mod trace;

use tremor_script::tremor_fn;
use tremor_script::Registry;

/// Extend function registry with `CNCF OpenTelemetry` support
pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (cncf::otel|gen_span_id_string(_context) {
            Ok(id::random_span_id_value().into_static())
        }))
        .insert(tremor_fn! (cncf::otel|gen_span_id_array(_context) {
            Ok(id::random_span_id_array().into_static())
        }))
        .insert(tremor_fn! (cncf::otel|gen_span_id_bytes(_context) {
            Ok(Value::Bytes(id::random_span_id_bytes().into()).into_static())
        }))
        .insert(tremor_fn! (cncf::otel|gen_trace_id_string(_context) {
            Ok(id::random_trace_id_value().into_static())
        }))
        .insert(tremor_fn! (cncf::otel|gen_trace_id_array(_context) {
            Ok(id::random_trace_id_array().into_static())
        }))
        .insert(tremor_fn! (cncf::otel|gen_trace_id_bytes(_context) {
            Ok(Value::Bytes(id::random_trace_id_bytes().into()).into_static())
        }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;
    use tremor_script::{registry, EventContext};

    #[test_case("gen_span_id_string" ; "cncf::otel::gen_span_id_string")]
    #[test_case("gen_span_id_array" ; "cncf::otel::gen_span_id_array")]
    #[test_case("gen_span_id_bytes" ; "cncf::otel::gen_span_id_bytes")]
    #[test_case("gen_trace_id_string" ; "cncf::otel::gen_trace_id_string")]
    #[test_case("gen_trace_id_array" ; "cncf::otel::gen_trace_id_array")]
    #[test_case("gen_trace_id_bytes" ; "cncf::otel::gen_trace_id_bytes")]
    fn test_tremor_fns(fun_name: &str) {
        let mut reg = registry::registry();
        load(&mut reg);

        let f = reg.find("cncf::otel", fun_name);

        if let Ok(f) = f {
            let context = EventContext::default();
            let r = f.invoke(&context, &[]);
            assert!(r.is_ok());
        } else {
            assert!(false, "unknown function error for cncf::otel extension");
        }
    }
}
