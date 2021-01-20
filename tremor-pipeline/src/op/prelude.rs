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

pub use super::*;
pub use crate::errors::*;
pub use crate::{CBAction, Event, OpMeta, Operator};
pub use beef::Cow;
pub use halfbrown::{hashmap, HashMap};
pub use serde_yaml;
pub use simd_json::OwnedValue;
pub use value_trait::Value as ValueTrait;

pub const OUT: Cow<'static, str> = Cow::const_str("out");
pub const IN: Cow<'static, str> = Cow::const_str("in");
pub const ERR: Cow<'static, str> = Cow::const_str("err");
pub const METRICS: Cow<'static, str> = Cow::const_str("metrics");
