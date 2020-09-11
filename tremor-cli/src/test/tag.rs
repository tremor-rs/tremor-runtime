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

use crate::errors::Result;
use crate::util::slurp_string;
use std::collections::HashSet;
use std::iter::FromIterator;

#[derive(Serialize, Debug)]
pub(crate) struct TagFilter {
    includes: HashSet<String>,
    excludes: HashSet<String>,
}

pub(crate) type Tags = Vec<String>;

pub(crate) fn maybe_slurp_tags(path: &str) -> Result<Tags> {
    let tags_data = slurp_string(path);
    match tags_data {
        Ok(tags_data) => match serde_json::from_str(&tags_data) {
            Ok(s) => Ok(s),
            _not_well_formed => Ok(vec![]),
        },
        _not_found => Ok(vec![]),
    }
}

impl TagFilter {
    pub(crate) fn new(excludes: Vec<String>, includes: Vec<String>) -> Self {
        Self {
            includes: HashSet::from_iter(includes.into_iter()),
            excludes: HashSet::from_iter(excludes.into_iter()),
        }
    }

    pub(crate) fn matches(&self, tags: &[String]) -> (Vec<String>, bool) {
        let tags: HashSet<String> = HashSet::from_iter(tags.iter().cloned());
        let includes: HashSet<String> = HashSet::from_iter(self.includes.iter().cloned());
        let excludes: HashSet<String> = HashSet::from_iter(self.excludes.iter().cloned());
        let accepted: Vec<&String> = tags.intersection(&includes).collect();
        let redacted: Vec<&String> = tags.intersection(&excludes).collect();

        if self.is_empty() {
            // if there are no inclusions/exclusions we match
            // regardless of current tags
            //
            (vec!["<all>".into()], true)
        } else {
            // If the current tags matched at least one include
            // and there were no excluded tags matched, then
            // we have a match
            (
                accepted.iter().map(|x| (*x).to_string()).collect(),
                !accepted.is_empty() && redacted.is_empty(),
            )
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.includes.len() == 0 && self.excludes.len() == 0
    }

    // pub(crate) fn join(&self, tags: Option<Vec<String>>) -> TagFilter {
    //     let mut includes: Vec<String> = Vec::from_iter(self.includes.iter().cloned());
    //     let excludes: Vec<String> = Vec::from_iter(self.excludes.iter().cloned());
    //     if let Some(mut tags) = tags {
    //         includes.append(&mut tags);
    //     }
    //     TagFilter::new(includes, excludes)
    // }
}
