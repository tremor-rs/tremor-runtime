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

mod array;
mod datetime;
mod integer;
mod json;
mod math;
mod re;
mod record;
mod string;
mod r#type;
use crate::registry::{Context, Registry};

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    array::load(registry);
    integer::load(registry);
    json::load(registry);
    record::load(registry);
    re::load(registry);
    string::load(registry);
    math::load(registry);
    r#type::load(registry);
    datetime::load(registry);
}
