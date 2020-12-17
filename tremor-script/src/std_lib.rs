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

#[cfg(test)]
macro_rules! assert_val {
    ($e:expr, $r:expr) => {
        assert_eq!($e, Ok(Value::from($r)))
    };
}

mod array;
mod base64;
mod binary;
mod chash;
mod datetime;
mod dummy;
mod float;
mod integer;
mod json;
mod math;
mod origin;
mod random;
mod range;
mod re;
mod record;
mod stats;
mod string;
mod system;
mod test;
mod r#type;
mod url;
mod win;

use crate::registry::{Aggr as AggrRegistry, Registry};

pub fn load(registry: &mut Registry) {
    array::load(registry);
    base64::load(registry);
    binary::load(registry);
    chash::load(registry);
    datetime::load(registry);
    dummy::load(registry);
    float::load(registry);
    integer::load(registry);
    json::load(registry);
    math::load(registry);
    origin::load(registry);
    random::load(registry);
    range::load(registry);
    re::load(registry);
    record::load(registry);
    string::load(registry);
    system::load(registry);
    test::load(registry);
    r#type::load(registry);
    url::load(registry);
}

pub fn load_aggr(registry: &mut AggrRegistry) {
    stats::load_aggr(registry);
    win::load_aggr(registry);
}
