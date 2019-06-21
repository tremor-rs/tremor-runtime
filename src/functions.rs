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

use crate::errors::*;
use simd_json::OwnedValue;
use tremor_pipeline::FN_REGISTRY;
use tremor_script::grok;
use tremor_script::tremor_fn;

pub fn load() -> Result<()> {
    FN_REGISTRY
        .lock()?
        //        .or_ok(Error::from("Could not lock function registry"))?
        .insert(tremor_fn!(system::instance(_context) {
            Ok(Value::from(instance!()))
        }))
        .insert(
            tremor_fn!(logstash::grok(_context, _pattern: String, _text: String) {
                let recognizer = grok::resolve(_pattern.to_string());
                match recognizer.matches(_text.to_string().as_bytes().to_vec()) {
                    Ok(j) => {
                        let v: OwnedValue = j;
                        Ok(v)
                    },
                    _ => Err(to_runtime_error(format!("{} for pattern: `{}` and text `{}`", "logstash::grok failure", _pattern, _text))),
                }
            }),
        );
    Ok(())

    // TODO: ingest_ns requires us to go away from a global registry.
}
