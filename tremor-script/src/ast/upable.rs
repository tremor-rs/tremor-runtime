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

use super::Helper;
use crate::errors::Result;

/// Updatable structure
pub trait Upable<'script> {
    /// Target to update to
    type Target;
    /// Update function
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target>;
}

impl<'script, U: Upable<'script>> Upable<'script> for Vec<U> {
    type Target = Vec<U::Target>;
    fn up<'registry>(mut self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if let Some(last) = self.pop() {
            let was_leaf = helper.possible_leaf;
            helper.possible_leaf = false;
            let r: Result<Self::Target> = self.into_iter().map(|v| v.up(helper)).collect();
            let mut r = r?;
            helper.possible_leaf = was_leaf;
            r.push(last.up(helper)?);
            Ok(r)
        } else {
            Ok(vec![])
        }
    }
}

impl<'script, U: Upable<'script>> Upable<'script> for Option<U> {
    type Target = Option<U::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        self.map(|v| v.up(helper)).transpose()
    }
}
