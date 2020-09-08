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
        .unwrap()
        .into_iter()
        .filter_map(std::result::Result::ok);
    let mut evidence = HashMap::new();
    let mut stats = stats::Stats::new();
    let api_test_root = root.to_string_lossy();
    let mut before = before::BeforeController::new(&api_test_root);
    let before_process = before.spawn()?;

    std::thread::spawn(move || {
        before.capture(before_process).ok();
    });

    let mut suites: HashMap<String, report::TestSuite> = HashMap::new();
    let mut counter = 0;
    let mut api_stats = stats::Stats::new();
    let report_start = nanotime();
    for suite in api_suites {
        let suite_start = nanotime();
        let suite: CommandRun =
            serde_yaml::from_str(&slurp_string(&suite.path().to_string_lossy()).unwrap()).unwrap();

        match &suite.tags {
            Some(tags) => {
                if let (_, false) = by_tag.matches(&tags) {
                    status::skip(&suite.name).ok();
                    continue; // SKIP
                } else {
                    status::tags(&by_tag, &Some(&tags)).ok();
                }
            }
            None => (),
        }

        for suite in suite.suites {
            for case in suite.cases {
                status::h1("Command Test", &case.name).ok();

                // FIXME wintel
                let mut fg_process = job::TargetProcess::new_with_stderr(
                    "/usr/bin/env",
                    &shell_words::split(&case.command).unwrap(),
                );
                let exit_status = fg_process.wait_with_output();

                let fg_out_file = format!("{}/fg.{}.out.log", api_test_root.clone(), counter);
                let fg_err_file = format!("{}/fg.{}.err.log", api_test_root.clone(), counter);
                let start = nanotime();
                fg_process.tail(&fg_out_file, &fg_err_file).ok();
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
            status::stats(&api_stats).ok();
        }
    }
    status::rollups("\nCommand", &api_stats).ok();

    before::update_evidence(&api_test_root, &mut evidence)?;

    let mut after = after::AfterController::new(&api_test_root);
    after.spawn().ok();
    after::update_evidence(&api_test_root, &mut evidence).ok();

    let elapsed = nanotime() - report_start;
    status::duration(elapsed).ok();
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
        )
        .ok();
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
        assert::process_filebased_asserts(stdout_path, stderr_path, &spec.expects)?;
    elements.append(&mut filebased_assert_elements);

    Ok((stat_s, elements))
}
