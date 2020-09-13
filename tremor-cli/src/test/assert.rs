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
use crate::test::stats;
use crate::util::slurp_string;
use crate::{open_file, report, status};
use errors::Error;
use serde::{Deserialize, Deserializer};
use std::io::prelude::*;
use std::io::BufReader;
use std::{collections::HashMap, path::Path};

fn file_contains(path_str: &str, what: &[String], base: Option<&String>) -> Result<bool> {
    let f = open_file(path_str, base)?;

    let mut asserts = HashMap::new();
    for test in what {
        let test = test.trim();
        asserts.insert(test, 0);
    }

    let lines = BufReader::new(&f);
    for line in lines.lines() {
        let line = line?;
        for test in what {
            let test = test.trim();
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

    for (_, v) in asserts {
        if v == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn file_equals(path_str: &str, other: &str, base: Option<&String>) -> Result<bool> {
    let mut got_f = open_file(path_str, base)?;
    let mut expected_f = open_file(other, base)?;

    let mut got = Vec::new();
    let mut expected = Vec::new();
    got_f.read_to_end(&mut got)?;
    expected_f.read_to_end(&mut expected)?;

    Ok(got == expected)
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
    pub(crate) base: Option<String>,
}
#[derive(Deserialize, Debug)]
pub(crate) struct FileBasedAssert {
    pub(crate) source: Source,
    pub(crate) contains: Option<Vec<String>>,
    pub(crate) equals_file: Option<String>,
}

pub(crate) type Asserts = Vec<FileBasedAssert>;

pub(crate) fn load_assert(path_str: &str) -> Result<AssertSpec> {
    let data = slurp_string(path_str)?;
    match serde_yaml::from_str::<AssertSpec>(&data) {
        Ok(mut s) => {
            let base = Path::new(path_str);
            s.base = base.parent().and_then(Path::to_str).map(String::from);
            for a in &s.asserts {
                if let Some(f) = &a.equals_file {
                    if !Path::new(f).is_file() {
                        if let Some(base) = &s.base {
                            let mut b = Path::new(base).to_path_buf();
                            b.push(f);
                            if !b.is_file() {
                                return Err(Error::from(format!(
                                    "equals_file  `{}` not found in `assert.yaml`",
                                    f
                                )));
                            }
                        } else {
                            return Err(Error::from(format!(
                                "equals_file  `{}` not found in `assert.yaml`",
                                f
                            )));
                        }
                    }
                }
            }
            Ok(s)
        }

        Err(e) => Err(Error::from(format!(
            "Unable to load `assert.yaml` from path `{}`: {}",
            path_str, e
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
        )?;
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
    } else {
        let success = false;
        status::assert(
            "Assert 0",
            &format!("Status {}", &spec.name,),
            success,
            &spec.status.to_string(),
            "signal",
        )?;
        s.fail();
        elements.push(report::TestElement {
            description: format!("Process expected to exit with status code {}", spec.status),
            info: Some("terminated by signal".into()),
            hidden: false,
            keyword: report::KeywordKind::Predicate,
            result: report::ResultKind {
                status: report::StatusKind::Failed,

                duration: 0,
            },
        });
    };

    let (assert_stats, mut filebased_assert_elements) =
        process_filebased_asserts(stdout_path, stderr_path, &spec.asserts, &spec.base)?;
    s.merge(&assert_stats);
    elements.append(&mut filebased_assert_elements);

    Ok((s, elements))
}

pub(crate) fn process_filebased_asserts(
    stdout_path: &str,
    stderr_path: &str,
    asserts: &[FileBasedAssert],
    base: &Option<String>,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut counter = 0;
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();
    for assert in asserts {
        match assert {
            FileBasedAssert {
                contains: None,
                equals_file: None,
                ..
            } => {
                stats.skip();
                // skip
            }
            FileBasedAssert {
                source,
                contains,
                equals_file,
                ..
            } => {
                let file = match source {
                    Source::File(file) => file.to_string(),
                    Source::Stdout => stdout_path.into(),
                    Source::Stderr => stderr_path.into(),
                };
                // Overall pass/fail
                if let Some(contains) = contains {
                    let mut total_condition = true;
                    // By line reporting
                    for c in contains {
                        counter += 1;
                        let condition = file_contains(&file, &[c.to_string()], base.as_ref())?;
                        total_condition &= condition;
                        status::assert_has(
                            &format!("Assert {}", counter),
                            &format!("Contains `{}` in `{}`", &c.trim(), &file),
                            condition,
                        )?;
                    }
                    elements.push(report::TestElement {
                        description: format!("File `{}` contains", file),
                        info: Some(contains.clone().join("\n")),
                        hidden: false,
                        keyword: report::KeywordKind::Predicate,
                        result: report::ResultKind {
                            status: if total_condition {
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
                if let Some(equals_file) = equals_file {
                    // By line reporting
                    counter += 1;
                    let condition = file_equals(&file, &equals_file, base.as_ref())?;
                    status::assert_has(
                        &format!("Assert {}", counter),
                        &format!("Fille `{}` equals `{}`", &file, equals_file),
                        condition,
                    )?;

                    elements.push(report::TestElement {
                        description: format!("File `{}` equals", file),
                        info: Some(equals_file.clone()),
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
            }
        }
    }

    Ok((stats, elements))
}
