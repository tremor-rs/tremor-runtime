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

use crate::errors::{Error, Result};
use crate::job;
use crate::status;
use crate::test::after;
use crate::test::assert;
use crate::test::before;
use crate::test::report;
use crate::test::stats;
use crate::test::tag::{TagFilter, Tags};
use crate::test::Meta;
use crate::util::{nanotime, slurp_string};
use globwalk::{FileType, GlobWalkerBuilder};
use std::collections::HashMap;
use std::path::Path;

#[derive(Deserialize, Debug)]
pub(crate) struct CommandRun {
    pub(crate) name: String,
    pub(crate) tags: Option<Tags>,
    pub(crate) suites: Vec<CommandSuite>,
}

#[derive(Deserialize, Debug)]
pub(crate) struct CommandSuite {
    pub(crate) name: String,
    pub(crate) cases: Vec<CommandTest>,
}

#[derive(Deserialize, Debug)]
pub(crate) struct CommandTest {
    pub(crate) name: String,
    pub(crate) command: String,
    pub(crate) tags: Option<Tags>,
    pub(crate) status: i32,
    pub(crate) expects: assert::Asserts,
}

pub(crate) fn suite_command(
    root: &Path,
    _meta: &Meta,
    by_tag: &TagFilter,
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    let api_suites = GlobWalkerBuilder::new(root, "**/command.yml")
        .case_insensitive(true)
        .file_type(FileType::FILE)
        .build()
        .map_err(|e| {
            Error::from(format!(
                "Unable to walk test path (`{}`) for command-driven tests: {:?}",
                root.to_str().unwrap_or_default(),
                e
            ))
        })?;

    let mut evidence = HashMap::new();
    let mut stats = stats::Stats::new();

    let mut suites: HashMap<String, report::TestSuite> = HashMap::new();
    let mut counter = 0;
    let mut api_stats = stats::Stats::new();
    let report_start = nanotime();
    let api_suites = api_suites.filter_map(std::result::Result::ok);
    for suite in api_suites {
        if let Some(root) = suite.path().parent() {
            let base = root.to_string_lossy().to_string();

            // Set cwd to test root
            let cwd = std::env::current_dir()?;
            std::env::set_current_dir(Path::new(&base))?;

            let mut before = before::BeforeController::new(&base);
            let before_process = before.spawn()?;
            std::thread::spawn(move || {
                if let Err(e) = before.capture(before_process) {
                    eprint!("Can't capture results from 'before' process: {}", e)
                };
            });

            let suite_start = nanotime();
            let command_str = slurp_string(&suite.path().to_string_lossy())?;
            let suite = serde_yaml::from_str::<CommandRun>(&command_str)?;

            match &suite.tags {
                Some(tags) => {
                    if let (_, false) = by_tag.matches(&tags) {
                        status::skip(&suite.name)?;
                        continue; // SKIP
                    } else {
                        status::tags(&by_tag, Some(&tags))?;
                    }
                }
                None => (),
            }

            for suite in suite.suites {
                for case in suite.cases {
                    status::h1("Command Test", &case.name)?;

                    let args = shell_words::split(&case.command).unwrap_or_default();
                    // FIXME wintel
                    let mut fg_process =
                        job::TargetProcess::new_with_stderr("/usr/bin/env", &args)?;
                    let exit_status = fg_process.wait_with_output();

                    let fg_out_file = format!("{}/fg.{}.out.log", base.clone(), counter);
                    let fg_err_file = format!("{}/fg.{}.err.log", base.clone(), counter);
                    let start = nanotime();
                    fg_process.tail(&fg_out_file, &fg_err_file)?;
                    let elapsed = nanotime() - start;

                    counter += 1;

                    let (case_stats, elements) = process_testcase(
                        &fg_out_file,
                        &fg_err_file,
                        exit_status?.code(),
                        elapsed,
                        &case,
                    )?;

                    stats.merge(&case_stats);
                    let suite = report::TestSuite {
                        name: case.name.trim().into(),
                        description: "Command-driven test".to_string(),
                        elements,
                        evidence: None,
                        stats: case_stats,
                        duration: nanotime() - suite_start,
                    };
                    suites.insert(case.name, suite);
                }
                api_stats.merge(&stats);
                status::stats(&api_stats)?;
            }

            before::update_evidence(&base, &mut evidence)?;

            let mut after = after::AfterController::new(&base);
            after.spawn()?;
            after::update_evidence(&base, &mut evidence)?;

            // Reset cwd
            std::env::set_current_dir(Path::new(&cwd))?;
        } else {
            return Err("Could not get parent of base path in command driven test walker".into());
        }
    }

    status::rollups("\nCommand", &api_stats)?;

    let elapsed = nanotime() - report_start;
    status::duration(elapsed)?;
    Ok((
        stats.clone(),
        vec![report::TestReport {
            description: "Command-based test suite".into(),
            elements: suites,
            stats,
            duration: elapsed,
        }],
    ))
}

fn process_testcase(
    stdout_path: &str,
    stderr_path: &str,
    process_status: Option<i32>,
    duration: u64,
    spec: &CommandTest,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stat_s = stats::Stats::new();
    if let Some(code) = process_status {
        let success = code == spec.status;
        status::assert(
            "Assert 0",
            &format!("Status {}", &spec.name.trim()),
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
                    stat_s.pass();
                    report::StatusKind::Passed
                } else {
                    stat_s.fail();
                    report::StatusKind::Failed
                },
                duration,
            },
        });
    };

    let (stat_s, mut filebased_assert_elements) =
        assert::process_filebased_asserts(stdout_path, stderr_path, &spec.expects, &None)?;
    elements.append(&mut filebased_assert_elements);

    Ok((stat_s, elements))
}
