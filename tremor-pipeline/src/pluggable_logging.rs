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

use crate::errors::Result;
use tremor_script::registry::Registry;
use tremor_script::tremor_fn;

/// Install's common functions into a registry
///
/// # Errors
///  * if we can't install extensions
pub fn load(reg: &mut Registry) -> Result<()> {
    reg.insert(tremor_fn!(logging|info(_context) {
        // TODO FIXME Add logging logic
        Ok(Value::from("snot"))
    }));

    Ok(())
}
