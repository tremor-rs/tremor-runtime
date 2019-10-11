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

use super::Helper;
use crate::errors::*;
use halfbrown::HashMap;

pub trait Upable<'script> {
    type Target;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target>;
}

impl<'script, U: Upable<'script>> Upable<'script> for Vec<U> {
    type Target = Vec<U::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        self.into_iter().map(|v| v.up(helper)).collect()
    }
}

impl<'script, U: Upable<'script>> Upable<'script> for Option<U> {
    type Target = Option<U::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        self.map(|v| v.up(helper)).transpose()
    }
}

#[allow(clippy::implicit_hasher)]
impl<'script, K, U: Upable<'script>> Upable<'script> for HashMap<K, U>
where
    K: std::cmp::Eq + std::hash::Hash,
{
    type Target = HashMap<K, U::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        self.into_iter()
            .map(|(k, v)| Ok((k, v.up(helper)?)))
            .collect()
    }
}
