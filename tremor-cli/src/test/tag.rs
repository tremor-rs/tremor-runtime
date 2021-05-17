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
use crate::util::slurp_string;
use std::{collections::HashSet, path::Path};

#[derive(Serialize, Debug, PartialEq)]
pub(crate) struct TagFilter {
    pub(crate) includes: HashSet<String>,
    pub(crate) excludes: HashSet<String>,
}

pub(crate) type Tags = Vec<String>;

pub(crate) fn maybe_slurp_tags(path: &Path) -> Tags {
    let tags_data = slurp_string(path);
    match tags_data {
        Ok(tags_data) => serde_json::from_str(&tags_data).unwrap_or_default(),
        Err(_not_found) => vec![],
    }
}

impl TagFilter {
    pub(crate) fn new(includes: Tags, excludes: Tags) -> Self {
        Self {
            includes: includes.into_iter().collect(),
            excludes: excludes.into_iter().collect(),
        }
    }

    pub(crate) fn includes(&self) -> Tags {
        self.includes
            .iter()
            .map(std::string::ToString::to_string)
            .collect()
    }

    pub(crate) fn excludes(&self) -> Tags {
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
        let system_allow: HashSet<&str> = system_allow.iter().copied().collect();
        // Tags we want to allow
        let allowing: HashSet<&str> = allowing.iter().map(String::as_str).collect();
        // Tags we want to deny
        let denying: HashSet<&str> = denying.iter().map(String::as_str).collect();
        // Tags in the current
        let includes: HashSet<&str> = self.includes.iter().map(String::as_str).collect();
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
        let mut includes: Tags = self.includes.iter().cloned().collect();
        let excludes: Tags = self.excludes.iter().cloned().collect();
        if let Some(mut tags) = tags {
            includes.append(&mut tags);
        }
        TagFilter::new(includes, excludes)
    }
}

// The intention is to find all tag files in the directories between base and other
// basically if base is /a/b/c and other is /a/b/c/d/e/f we want to look for:
// - /a/b/c/tags.json
// - /a/b/c/d/tags.json
// - /a/b/c/d/e/tags.json
// - /a/b/c/d/e/f/tags.json
pub(crate) fn resolve<P>(base: P, other: P) -> Result<TagFilter>
where
    P: AsRef<Path>,
{
    let mut base = base.as_ref().canonicalize()?;
    let other = other.as_ref().canonicalize()?;
    if let Ok(rel) = other.strip_prefix(&base) {
        let tags_file = base.join("tags.json");
        let mut tags = TagFilter::new(vec![], vec![]);
        tags = tags.join(Some(maybe_slurp_tags(&tags_file)));
        for dirname in rel.components() {
            base = base.join(dirname.as_os_str());
            let tags_file = base.join("tags.json");
            tags = tags.join(Some(maybe_slurp_tags(&tags_file)));
        }

        Ok(tags)
    } else {
        Err(format!(
            "Unexpected error resolving tags for test: {}",
            other.display()
        )
        .into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_resolve() {
        let tag_filter = TagFilter::new(
            vec!["b".to_string(), "c".to_string(), "d".to_string()],
            vec![],
        );
        assert_eq!(
            resolve(
                "tests/fixtures/resolve_tags/a/b",
                "tests/fixtures/resolve_tags/a/b/c/d"
            )
            .unwrap(),
            tag_filter
        );
    }
}
