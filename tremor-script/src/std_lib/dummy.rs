/// Dummy functions that get replaced wiuth real ones as part
/// of the runtime system.
/// This is used to allow runing tremor-script on full
/// scripts outside of the runtime.
// Copyright 2018-2020, Wayfair GmbH
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
use crate::registry::Registry;
// use crate::tremor_fn;

pub fn load(_registry: &mut Registry) {
    //    registry.insert(tremor_fn! (system::ingest_ns(_context) {
    //        Ok(Value::from(0))
    //    }));
}
