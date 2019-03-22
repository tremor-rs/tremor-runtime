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
use tremor_pipeline::FN_REGISTRY;
use tremor_script::tremor_fn;
pub fn load() -> Result<()> {
    use tremor_script::errors::*;
    FN_REGISTRY
        .lock()?
        //        .or_ok(Error::from("Could not lock function registry"))?
        .insert(tremor_fn!(system::instance(_context) {
            Ok(serde_json::Value::String(instance!()))
        }));
    Ok(())

    // TODO: ingest_ns requires us to go away from a global registry.
}
