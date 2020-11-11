// Copyright 2020, The Tremor Team
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
use crate::util;
use crate::util::slurp_string;
use std::iter::FromIterator;
use std::{collections::HashSet, path::Path};

#[derive(Serialize, Debug)]
pub(crate) struct TagFilter {
    pub(crate) includes: HashSet<String>,
    pub(crate) excludes: HashSet<String>,
}

pub(crate) type Tags = Vec<String>;

pub(crate) fn maybe_slurp_tags(path: &str) -> Result<Tags> {
    let tags_data = slurp_string(path);
    match tags_data {
        Ok(tags_data) => match serde_json::from_str(&tags_data) {
            Ok(s) => Ok(s),
            Err(_not_well_formed) => Ok(vec![]),
        },
        Err(_not_found) => Ok(vec![]),
    }
}

impl TagFilter {
    pub(crate) fn new(excludes: Vec<String>, includes: Vec<String>) -> Self {
        Self {
            includes: HashSet::from_iter(includes.into_iter()),
            excludes: HashSet::from_iter(excludes.into_iter()),
        }
    }

    pub(crate) fn includes(&self) -> Vec<String> {
        self.includes
            .iter()
            .map(std::string::ToString::to_string)
            .collect()
    }

    pub(crate) fn excludes(&self) -> Vec<String> {
        self.excludes
            .iter()
            .map(std::string::ToString::to_string)
            .collect()
    }

    // We allow this since the logic below is more readable when allowing for if not else
    #[allow(clippy::if_not_else)]
    pub(crate) fn matches(
        &self,
        system_allow: &[&str],
        allowing: &[String],
        denying: &[String],
    ) -> (Vec<String>, bool) {
        // Tags we want to allow based on the system
        let system_allow: HashSet<&str> = HashSet::from_iter(system_allow.iter().copied());
        // Tags we want to allow
        let allowing: HashSet<&str> = HashSet::from_iter(allowing.iter().map(String::as_str));
        // Tags we want to deny
        let denying: HashSet<&str> = HashSet::from_iter(denying.iter().map(String::as_str));
        // Tags in the current
        let includes: HashSet<&str> = HashSet::from_iter(self.includes.iter().map(String::as_str));
        // Tags that passed the system req
        let sys_accepted: Vec<_> = includes.intersection(&system_allow).collect();
        // The tags that were accepted
        let accepted: Vec<_> = includes.intersection(&allowing).collect();
        // The tags that were rejected
        let redacted: Vec<_> = includes.intersection(&denying).collect();

        if sys_accepted.is_empty() && !system_allow.is_empty() {
            // If this was excluded by the system we don't want to run it
            (vec!["<system>".into()], false)
        } else if allowing.is_empty() && denying.is_empty() {
            // if there are no inclusions/exclusions we match
            // regardless of current tags
            (vec!["<all>".into()], true)
        } else {
            // This test is specifically included
            let is_included = !accepted.is_empty();
            // This test is specifically excluded
            let is_excluded = !redacted.is_empty();

            let accept = if is_included {
                // If we included a tag specifically we always accept it
                true
            } else if is_excluded {
                // If we didn't specifically include it but it was excluded we don't accept it
                false
            // Starting here we have neither included nor excluded the tag specifically
            // so the behavior got to depend on what was defined
            } else if allowing.is_empty() && denying.is_empty() {
                // If we have neither allowed or denied any tags we pass
                // run w/o params
                true
            } else if !allowing.is_empty() {
                // if we are allowing any tags but we want to default to reject
                // run w/ -i <tag>
                false
            } else if !denying.is_empty() {
                // if we are denying some tags but allow some we want to default to accept
                // run w/ -e <tag>
                true
            } else {
                // this never can be reached;
                error!("this condition of tags should never be reached!");
                true
            };

            // If the current tags matched at least one include
            // and there were no excluded tags matched, then
            // we have a match
            (accepted.iter().map(|x| (**x).to_string()).collect(), accept)
        }
    }

    pub(crate) fn join(&self, tags: Option<Vec<String>>) -> TagFilter {
        let mut includes: Vec<String> = Vec::from_iter(self.includes.iter().cloned());
        let excludes: Vec<String> = Vec::from_iter(self.excludes.iter().cloned());
        if let Some(mut tags) = tags {
            includes.append(&mut tags);
        }
        TagFilter::new(excludes, includes)
    }
}

pub(crate) fn resolve(base: &Path, other: &Path) -> Result<TagFilter> {
    if let Ok(rel) = util::relative_path(base, other) {
        let mut base = base.to_string_lossy().to_string();
        let tags_file = format!("{}/tags.json", &base);
        let mut tags = TagFilter::new(vec![], vec![]);
        tags = tags.join(Some(maybe_slurp_tags(&tags_file)?));
        for dirname in rel.split('/') {
            base = format!("{}/{}", base, dirname);
            let tags_file = format!("{}/tags.json", &base);
            tags = tags.join(Some(maybe_slurp_tags(&tags_file)?));
        }
        Ok(tags)
    } else {
        Err(format!(
            "Unexpected error resolving tags for test: {}",
            other.to_string_lossy()
        )
        .into())
    }
}
