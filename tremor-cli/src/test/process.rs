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

use super::after;
use super::assert;
use super::before;
use super::job;
use super::stats;
use super::tag;
use crate::errors::Result;
use crate::report;
use crate::util::*;
use globwalk::GlobWalkerBuilder;
use std::collections::HashMap;
use std::path::Path;

pub(crate) fn run_process(
    kind: &str,
    bench_root: &Path,
    _by_tag: &tag::TagFilter,
) -> Result<report::TestReport> {
    let mut evidence = HashMap::new();

    let artefacts = GlobWalkerBuilder::from_patterns(
        bench_root.canonicalize()?,
        &["*.{yaml,tremor,trickle}", "!assert.yaml"],
    )
    .case_insensitive(true)
    .max_depth(1)
    .build()
    .unwrap()
    .into_iter()
    .filter_map(std::result::Result::ok)
    .map(|x| x.path().to_string_lossy().to_string());

    let mut artefacts = artefacts.collect::<Vec<String>>();
    let mut args: Vec<String> = vec!["server", "run", "-n", "-f"]
        .iter()
        .map(|x| x.to_string())
        .collect();
    args.append(&mut artefacts);

    let process_start = nanotime();
    let bench_root = bench_root.to_string_lossy();
    // let tags_str = &format!("{}/tags.json", bench_root);
    // let tags = tag::maybe_slurp_tags(tags_str);

    let mut before = before::BeforeController::new(&bench_root);
    let before_process = before.spawn()?;

    // FIXME consider using current exe
    let bench_rootx = bench_root.to_string();
    let cmd = "/Code/oss/wayfair-tremor/tremor-runtime/target/release/tremor";
    let mut process = job::TargetProcess::new_with_stderr(cmd, &args);
    let status = process.wait_with_output();

    let fg_out_file = format!("{}/fg.out.log", bench_root);
    let fg_err_file = format!("{}/fg.err.log", bench_root);
    let fg = std::thread::spawn(move || -> Result<std::process::ExitStatus> {
        let fg_out_file = format!("{}/fg.out.log", bench_rootx.clone());
        let fg_err_file = format!("{}/fg.err.log", bench_rootx.clone());
        process.tail(&fg_out_file, &fg_err_file).unwrap();
        Ok(process.wait_with_output()?)
    });

    std::thread::spawn(move || {
        before.capture(before_process).ok();
    });

    match fg.join() {
        Ok(_) => (),
        Err(_) => return Err("Failed to join test foreground thread/process error".into()),
    };

    before::update_evidence(&bench_root, &mut evidence)?;

    // As our primary process is finished, check for after hooks
    let mut after = after::AfterController::new(&bench_root);
    after.spawn().ok();
    after::update_evidence(&bench_root, &mut evidence).ok();

    // Assertions
    let assert_str = &format!("{}/assert.yaml", bench_root);
    let assert_yaml = assert::load_assert(assert_str);
    let report = match assert_yaml {
        Ok(assert_yaml) => Some(assert::process(&fg_out_file, &fg_err_file, status?.code(), &assert_yaml)?),
        _not_found => None,
    };

    evidence.insert("test: stdout".to_string(), slurp_string(&fg_out_file)?);
    evidence.insert("test: stderr".to_string(), slurp_string(&fg_err_file)?);

    let mut stats = stats::Stats::new();
    match report {
        Some((report_stats, report)) => {
            // There were assertions which is typical of integration tests
            // but benchmarks may also use the assertion facility
            //
            let mut elements = HashMap::new();
            stats.merge(&report_stats);
            let elapsed = nanotime() - process_start;
            elements.insert(
                "integration".to_string(),
                report::TestSuite {
                    description: format!("{} test suite", kind).into(),
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
        }
        None => {
            // There were no assertions which is typical of benchmarks
            //
            let mut report = HashMap::new();
            let elapsed = nanotime() - process_start;
            report.insert(
                "bench".to_string(),
                report::TestSuite {
                    name: "name".into(),
                    description: format!("{} test suite", kind).into(),
                    elements: vec![],
                    evidence: Some(evidence),
                    stats: stats::Stats::new(),
                    duration: elapsed,
                },
            );
            Ok(report::TestReport {
                description: "Tremor Test Report".into(),
                elements: report,
                stats: stats::Stats::new(),
                duration: elapsed,
            })
        }
    }
}
