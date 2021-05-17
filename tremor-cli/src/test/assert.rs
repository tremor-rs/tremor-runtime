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

use crate::errors::{self, Result};
use crate::test::stats;
use crate::util::slurp_string;
use crate::{open_file, report, status};
use difference::Changeset;
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

fn file_equals(path_str: &str, other: &str, base: Option<&String>) -> Result<Changeset> {
    let mut got_f = open_file(path_str, base)?;
    let mut expected_f = open_file(other, base)?;

    let mut got = String::new();
    let mut expected = String::new();
    got_f.read_to_string(&mut got)?;
    expected_f.read_to_string(&mut expected)?;

    Ok(Changeset::new(&expected, &got, "\n"))
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

pub(crate) fn load_assert(path: &Path) -> Result<AssertSpec> {
    let data = slurp_string(path)?;
    match serde_yaml::from_str::<AssertSpec>(&data) {
        Ok(mut s) => {
            let base = path;
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
            path.to_string_lossy(),
            e
        ))),
    }
}

pub(crate) fn process(
    stdout_path: &Path,
    stderr_path: &Path,
    status: Option<i32>,
    spec: &AssertSpec,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut s = stats::Stats::new();
    s.assert(); // status code
    if let Some(code) = status {
        let success = code == spec.status;
        status::assert_has(
            "   ",
            "Assert 0",
            &format!("Status {}", &spec.name,),
            Some(&spec.status.to_string()),
            success,
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
        status::assert_has(
            "    ",
            &format!("Assert Status {}", &spec.name,),
            &spec.status.to_string(),
            Some(&"signal".to_string()),
            success,
        )?;
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
        process_filebased_asserts("   ", stdout_path, stderr_path, &spec.asserts, &spec.base)?;
    s.merge(&assert_stats);
    elements.append(&mut filebased_assert_elements);

    Ok((s, elements))
}

pub(crate) fn process_filebased_asserts(
    prefix: &str,
    stdout_path: &Path,
    stderr_path: &Path,
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
                    Source::Stdout => stdout_path.to_string_lossy().to_string(),
                    Source::Stderr => stderr_path.to_string_lossy().to_string(),
                };
                // Overall pass/fail
                if let Some(contains) = contains {
                    let mut total_condition = true;
                    // By line reporting
                    for c in contains {
                        stats.assert();
                        counter += 1;
                        let condition = file_contains(&file, &[c.to_string()], base.as_ref())?;
                        stats.report(condition);
                        total_condition &= condition;
                        status::assert_has(
                            prefix,
                            &format!("Assert {}", counter),
                            &format!("  Contains `{}` in `{}`", &c.trim(), &file),
                            None,
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
                                report::StatusKind::Passed
                            } else {
                                report::StatusKind::Failed
                            },
                            duration: 0,
                        },
                    });
                }
                if let Some(equals_file) = equals_file {
                    // By line reporting
                    counter += 1;
                    stats.assert();
                    let changeset = file_equals(&file, &equals_file, base.as_ref())?;
                    let info = Some(changeset.to_string());
                    let condition = changeset.distance == 0;

                    status::assert_has(
                        prefix,
                        &format!("Assert {}", counter),
                        &format!("File `{}` equals `{}`", &file, equals_file),
                        info.as_ref(),
                        condition,
                    )?;

                    elements.push(report::TestElement {
                        description: format!("File `{}` equals", file),
                        info,
                        hidden: false,
                        keyword: report::KeywordKind::Predicate,
                        result: report::ResultKind {
                            status: stats.report(condition),
                            duration: 0,
                        },
                    });
                }
            }
        }
    }

    Ok((stats, elements))
}
