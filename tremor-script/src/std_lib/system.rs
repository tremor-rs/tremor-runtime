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

use crate::registry::Registry;
use crate::tremor_fn;
use tremor_common::time::nanotime;

pub fn load(registry: &mut Registry) {
    registry.insert(tremor_fn!(system::nanotime(_context) {
      Ok(Value::from(nanotime()))
    }));
}

#[cfg(test)]
mod test {
    use crate::prelude::*;
    use crate::registry::fun;
    use tremor_common::time::nanotime;
    #[test]
    fn system_nanotime() {
        let f = fun("system", "nanotime");
        let r = f(&[]);
        if let Some(x) = r.ok().as_u64() {
            let status = x <= nanotime();
            assert!(status);
        } else {
            unreachable!("test failed")
        }
    }
}
