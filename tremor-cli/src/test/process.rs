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

use super::super::status;
use super::after;
use super::assert;
use super::before;
use super::job;
use super::stats;
use super::tag;
use crate::report;
use crate::util::slurp_string;
use crate::{
    errors::{Error, Result},
    report::TestReport,
};
use globwalk::GlobWalkerBuilder;
use std::collections::HashMap;
use std::path::Path;
use std::process::ExitStatus;
use tremor_common::{file::canonicalize, time::nanotime};

/// run the process
///
/// `tests_root_dir`: the base path in which we discovered this test.
/// `test_dir`: directory of the current test
#[allow(clippy::too_many_lines)]
pub(crate) fn run_process(
    kind: &str,
    _tests_root_dir: &Path,
    test_dir: &Path,
    _by_tag: &tag::TagFilter,
) -> Result<TestReport> {
    let mut evidence = HashMap::new();

    let mut artefacts = GlobWalkerBuilder::from_patterns(
        canonicalize(test_dir)?,
        &["*.{yaml,tremor,trickle}", "!assert.yaml", "!logger.yaml"],
    )
    .case_insensitive(true)
    .sort_by(|a, b| a.file_name().cmp(b.file_name()))
    .max_depth(1)
    .build()
    .map_err(|e| Error::from(format!("Unable to walk path for artefacts capture: {}", e)))?
    .filter_map(|x| x.ok().map(|x| x.path().to_string_lossy().to_string()))
    .peekable();

    // fail fast if no artefacts are provided
    if artefacts.peek().is_none() {
        warn!(
            "No tremor artefacts found in '{}'.",
            test_dir.to_string_lossy()
        );
    }
    let args: Vec<String> = vec!["server", "run", "-n"]
        .iter()
        .map(|x| (*x).to_string())
        .chain(artefacts)
        .collect();

    let process_start = nanotime();

    let mut before = before::BeforeController::new(test_dir);
    let before_process = before.spawn()?;

    let test_dir_buf = test_dir.to_path_buf();

    // enable info level logging
    let mut env = HashMap::with_capacity(1);
    env.insert(
        String::from("RUST_LOG"),
        std::env::var("RUST_LOG").unwrap_or(String::from("info")),
    );

    let mut process = job::TargetProcess::new_in_dir(job::which("tremor")?, &args, &env, test_dir)?;
    let process_status = process.wait_with_output()?;

    let fg_out_file = test_dir_buf.join("fg.out.log");
    let fg_err_file = test_dir_buf.join("fg.err.log");
    let fg = std::thread::spawn(move || -> Result<ExitStatus> {
        let fg_out_file = test_dir_buf.join("fg.out.log");
        let fg_err_file = test_dir_buf.join("fg.err.log");
        process.tail(&fg_out_file, &fg_err_file)?;
        process.wait_with_output()
    });

    std::thread::spawn(move || {
        if let Err(e) = before.capture(before_process) {
            eprintln!("Failed to capture input from before thread: {}", e);
        };
    });

    match fg.join() {
        Ok(_) => (),
        Err(_) => return Err("Failed to join test foreground thread/process error".into()),
    };

    before::update_evidence(test_dir, &mut evidence)?;

    // As our primary process is finished, check for after hooks
    let mut after = after::AfterController::new(test_dir);
    after.spawn()?;
    after::update_evidence(test_dir, &mut evidence)?;
    // Assertions
    let assert_path = test_dir.join("assert.yaml");
    let report = if (&assert_path).is_file() {
        let assert_yaml = assert::load_assert(&assert_path)?;
        Some(assert::process(
            &fg_out_file,
            &fg_err_file,
            process_status.code(),
            &assert_yaml,
        )?)
    } else {
        None
    };
    evidence.insert("test: stdout".to_string(), slurp_string(&fg_out_file)?);
    evidence.insert("test: stderr".to_string(), slurp_string(&fg_err_file)?);

    let mut stats = stats::Stats::new();
    if let Some((report_stats, report)) = report {
        // There were assertions which is typical of integration tests
        // but benchmarks may also use the assertion facility
        //
        let mut elements = HashMap::new();
        stats.merge(&report_stats);
        let elapsed = nanotime() - process_start;
        elements.insert(
            "integration".to_string(),
            report::TestSuite {
                description: format!("{} test suite", kind),
                name: "name".into(),
                elements: report,
                evidence: Some(evidence),
                stats: report_stats,
                duration: elapsed,
            },
        );
        Ok(report::TestReport {
            description: "Tremor Test Report".into(),
            elements,
            stats,
            duration: elapsed,
        })
    } else {
        // There were no assertions which is typical of benchmarks
        //
        status::text("      ", &slurp_string(&fg_out_file)?)?;
        let mut report = HashMap::new();
        let elapsed = nanotime() - process_start;
        if process_status.success() {
            stats.pass();
        } else {
            // TODO It would be nicer if there was something like a status::err() that would print
            // the errors in a nicer way
            println!("ERROR: \n{}", &slurp_string(&fg_err_file)?);
            stats.fail(
                test_dir
                    .file_name()
                    .ok_or("unable to find the benchmark name")?
                    .to_str()
                    .unwrap_or_default(),
            );
        };
        report.insert(
            "bench".to_string(),
            report::TestSuite {
                name: test_dir
                    .file_name()
                    .ok_or("unable to find the benchmark name")?
                    .to_string_lossy()
                    .into(),
                description: format!("{} test suite", kind),
                elements: vec![],
                evidence: Some(evidence),
                stats: stats.clone(),
                duration: elapsed,
            },
        );
        Ok(report::TestReport {
            description: "Tremor Test Report".into(),
            elements: report,
            stats,
            duration: elapsed,
        })
    }
}
