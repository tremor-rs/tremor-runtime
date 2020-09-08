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

use crate::errors::{self, Result};
use crate::report;
use crate::status;
use crate::test::stats;
use crate::util::slurp_string;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn file_contains(path_str: &str, what: &Vec<String>) -> bool {
    let f = File::open(path_str);

    let mut asserts = HashMap::new();
    for test in what {
        let test = test.trim();
        asserts.insert(test, 0);
    }

    if let Ok(f) = f {
        let lines = BufReader::new(&f);
        for line in lines.lines() {
            for test in what {
                let test = test.trim();
                if let Ok(ref line) = line {
                    if line.contains(test) {
                        asserts.insert(
                            test,
                            match asserts.get(&test) {
                                Some(n) => *n + 1,
                                None => 1,
                            },
                        );
                    }
                }
            }
        }

        for (_, v) in asserts {
            if v == 0 {
                return false;
            }
        }
        true
    } else {
        false
    }
}

#[derive(Debug)]
pub(crate) enum Source {
    Stdout,
    Stderr,
    File(String),
}

impl<'de> Deserialize<'de> for Source {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let source = match s.to_lowercase().as_str() {
            "stdout" => Source::Stdout,
            "stderr" => Source::Stderr,
            other => Source::File(other.to_string()),
        };
        Ok(source)
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct AssertSpec {
    pub(crate) status: i32,
    pub(crate) name: String,
    pub(crate) asserts: Asserts,
}
#[derive(Deserialize, Debug)]
pub(crate) struct FileBasedAssert {
    pub(crate) source: Source,
    pub(crate) contains: Option<Vec<String>>,
}

pub(crate) type Asserts = Vec<FileBasedAssert>;

pub(crate) fn load_assert(path_str: &str) -> Result<AssertSpec> {
    let data = slurp_string(path_str)?;
    match serde_yaml::from_str(&data) {
        Ok(s) => Ok(s),
        Err(_) => Err(errors::Error::from(format!(
            "Unable to load `assert.yaml` from path: {}",
            path_str
        ))),
    }
}

pub(crate) fn process(
    stdout_path: &str,
    stderr_path: &str,
    status: Option<i32>,
    spec: &AssertSpec,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut s = stats::Stats::new();

    if let Some(code) = status {
        let success = code == spec.status;
        status::assert(
            "Assert 0",
            &format!("Status {}", &spec.name,),
            success,
            &spec.status.to_string(),
            &code.to_string(),
        )
        .ok();
        elements.push(report::TestElement {
            description: format!("Process expected to exit with status code {}", spec.status),
            info: Some(code.to_string()),
            hidden: false,
            keyword: report::KeywordKind::Predicate,
            result: report::ResultKind {
                status: if success {
                    s.pass();
                    report::StatusKind::Passed
                } else {
                    s.fail();
                    report::StatusKind::Failed
                },
                duration: 0,
            },
        });
    };

    let (assert_stats, mut filebased_assert_elements) =
        process_filebased_asserts(stdout_path, stderr_path, &spec.asserts)?;
    s.merge(&assert_stats);
    elements.append(&mut filebased_assert_elements);

    Ok((s, elements))
}

pub(crate) fn process_filebased_asserts(
    stdout_path: &str,
    stderr_path: &str,
    asserts: &Vec<FileBasedAssert>,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut counter = 0;
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();
    for assert in asserts {
        match assert {
            FileBasedAssert {
                source,
                contains: Some(contains),
                ..
            } => {
                let file = match source {
                    Source::File(file) => file.to_string(),
                    Source::Stdout => stdout_path.into(),
                    Source::Stderr => stderr_path.into(),
                };
                // Overall pass/fail
                let condition = file_contains(&file, contains);
                // By line reporting
                for c in contains {
                    counter += 1;
                    let condition = file_contains(&file, &vec![c.to_string()]);
                    status::assert_has(
                        &format!("Assert {}", counter),
                        &format!("Contains `{}` in `{}`", &c.trim(), &file),
                        condition,
                    )
                    .ok();
                }

                elements.push(report::TestElement {
                    description: format!("File `{}` contains", file),
                    info: Some(contains.clone().join("\n")),
                    hidden: false,
                    keyword: report::KeywordKind::Predicate,
                    result: report::ResultKind {
                        status: if condition {
                            stats.pass();
                            report::StatusKind::Passed
                        } else {
                            stats.fail();
                            report::StatusKind::Failed
                        },
                        duration: 0,
                    },
                });
            }
            FileBasedAssert { contains: None, .. } => {
                stats.skip();
                // skip
            }
        }
    }

    Ok((stats, elements))
}
