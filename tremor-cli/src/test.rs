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

use crate::errors::{Error, ErrorKind, Result};
use crate::job;
use crate::report;
use crate::status;
use crate::test;
use crate::util::{basename, slurp_string};
use clap::ArgMatches;
use globwalk::{FileType, GlobWalkerBuilder};
use kind::TestKind;
pub(crate) use kind::UnknownKind;
use metadata::Meta;
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::Write;
use std::path::Path;
use tag::TagFilter;
use tremor_common::file;
use tremor_common::time::nanotime;

mod after;
mod assert;
mod before;
mod command;
mod kind;
mod metadata;
mod process;
pub mod stats;
pub mod tag;
mod unit;

fn suite_bench(
    base: &Path,
    root: &Path,
    meta: &Meta,
    by_tag: (&[&str], &[String], &[String]),
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(benches) = GlobWalkerBuilder::new(root, &meta.includes)
        .case_insensitive(true)
        .file_type(FileType::DIR)
        .build()
    {
        let benches = benches.filter_map(std::result::Result::ok);

        let mut suite = vec![];
        let mut stats = stats::Stats::new();

        status::h0("Framework", "Finding benchmark test scenarios")?;

        for bench in benches {
            let root = bench.path();
            let bench_root = root.to_string_lossy();
            let tags = tag::resolve(base, root)?;

            let (matched, is_match) = tags.matches(&by_tag.0, &by_tag.1, &by_tag.1);
            if is_match {
                status::h1("Benchmark", &format!("Running {}", &basename(&bench_root)))?;
                status::tags(&tags, Some(&matched), Some(&by_tag.1))?;
                let test_report = process::run_process("bench", base, root, &tags)?;
                status::duration(test_report.duration, "  ")?;
                if test_report.stats.is_pass() {
                    stats.pass();
                } else {
                    stats.fail();
                }
                suite.push(test_report);
            } else {
                stats.skip();
                status::h1(
                    "  Benchmark",
                    &format!("Skipping {}", &basename(&bench_root)),
                )?;
                status::tags(&tags, Some(&matched), Some(&by_tag.1))?;
            }
        }

        Ok((stats, suite))
    } else {
        Err("Unable to walk test path for benchmarks".into())
    }
}

fn suite_integration(
    base: &Path,
    root: &Path,
    meta: &Meta,
    sys_filter: &[&str],
    include: &[String],
    exclude: &[String],
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(tests) = GlobWalkerBuilder::new(root, &meta.includes)
        .case_insensitive(true)
        .file_type(FileType::DIR)
        .build()
    {
        let tests = tests.filter_map(std::result::Result::ok);

        let mut suite = vec![];
        let mut stats = stats::Stats::new();

        status::h0("Framework", "Finding integration test scenarios")?;

        for test in tests {
            let root = test.path();
            let bench_root = root.to_string_lossy();
            let tags = tag::resolve(base, root)?;

            let (matched, is_match) = tags.matches(sys_filter, include, exclude);
            if is_match {
                status::h1(
                    "Integration",
                    &format!("Running {}", &basename(&bench_root)),
                )?;
                // Set cwd to test root
                let cwd = std::env::current_dir()?;
                std::env::set_current_dir(Path::new(&root))?;
                status::tags(&tags, Some(&matched), Some(exclude))?;

                // Run integration tests
                let test_report = process::run_process("integration", base, root, &tags)?;

                // Restore cwd
                file::set_current_dir(&cwd)?;

                if test_report.stats.is_pass() {
                    stats.pass();
                } else {
                    stats.fail();
                }
                stats.assert += &test_report.stats.assert;

                status::stats(&test_report.stats, "  ")?;
                status::duration(test_report.duration, "    ")?;
                suite.push(test_report);
            } else {
                stats.skip();
                status::h1(
                    "Integration",
                    &format!("Skipping {}", &basename(&bench_root)),
                )?;
                status::tags(&tags, Some(&matched), Some(exclude))?;
            }
        }

        status::rollups("\n  Integration", &stats)?;

        Ok((stats, suite))
    } else {
        Err("Unable to walk test path for integration tests".into())
    }
}

fn suite_unit(
    base: &Path,
    root: &Path,
    _meta: &Meta,
    sys_filter: &[&str],
    includes: &[String],
    excludes: &[String],
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(suites) = GlobWalkerBuilder::new(root, "all.tremor")
        .case_insensitive(true)
        .file_type(FileType::FILE)
        .build()
    {
        let suites = suites.filter_map(std::result::Result::ok);
        let mut reports = vec![];
        let mut stats = stats::Stats::new();

        status::h0("Framework", "Finding unit test scenarios")?;

        for suite in suites {
            status::h0("  Unit Test Scenario", &suite.path().to_string_lossy())?;
            let scenario_tags = tag::resolve(base, root)?;
            let (matched, _is_match) = scenario_tags.matches(
                sys_filter,
                &scenario_tags.includes(),
                &scenario_tags.excludes(),
            );
            status::tags(&scenario_tags, Some(&matched), Some(&excludes))?;

            let report = unit::run_suite(
                &suite.path(),
                &scenario_tags,
                sys_filter,
                includes,
                excludes,
            )?;
            stats.merge(&report.stats);
            status::stats(&report.stats, "  ")?;
            status::duration(report.duration, "    ")?;
            reports.push(report);
        }

        status::rollups("  Unit", &stats)?;

        Ok((stats, reports))
    } else {
        Err("Unable to walk test path for unit tests".into())
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    let kind: test::TestKind = matches.value_of("MODE").unwrap_or_default().try_into()?;
    let path = matches.value_of("PATH").unwrap_or_default();
    let report = matches.value_of("REPORT").unwrap_or_default();
    let quiet = matches.is_present("QUIET");
    let mut includes: Vec<String> = if matches.is_present("INCLUDES") {
        if let Some(matches) = matches.values_of("INCLUDES") {
            matches.map(std::string::ToString::to_string).collect()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    let excludes: Vec<String> = if matches.is_present("EXCLUDES") {
        if let Some(matches) = matches.values_of("EXCLUDES") {
            matches.map(std::string::ToString::to_string).collect()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    let found = GlobWalkerBuilder::new(tremor_common::file::canonicalize(&path)?, "meta.json")
        .case_insensitive(true)
        .build()
        .map_err(|e| Error::from(format!("failed to walk directory `{}`: {}", path, e)))?;

    let mut reports = HashMap::new();
    let mut bench_stats = stats::Stats::new();
    let mut unit_stats = stats::Stats::new();
    let mut cmd_stats = stats::Stats::new();
    let mut integration_stats = stats::Stats::new();
    let mut elapsed = 0;

    let cwd = std::env::current_dir()?;
    let base = format!("{}/{}", &cwd.to_string_lossy(), path);
    let base = Path::new(&base);
    let found = found.filter_map(std::result::Result::ok);
    let start = nanotime();
    for meta in found {
        if let Some(root) = meta.path().parent() {
            let mut meta_str = slurp_string(&meta.path().to_string_lossy())?;
            let meta: Meta = simd_json::from_str(meta_str.as_mut_str())?;

            if meta.kind == TestKind::All {
                includes.push("all".into());
            }

            if meta.kind == TestKind::Bench && (kind == TestKind::All || kind == TestKind::Bench) {
                let tag_filter = (&["bench"][..], includes.as_slice(), excludes.as_slice());
                let (stats, test_reports) = suite_bench(base, root, &meta, tag_filter)?;
                reports.insert("bench".to_string(), test_reports);
                bench_stats.merge(&stats);
                status::hr();
            }

            if meta.kind == TestKind::Integration
                && (kind == TestKind::All || kind == TestKind::Integration)
            {
                let (stats, test_reports) =
                    suite_integration(base, root, &meta, &["integration"], &includes, &excludes)?;
                reports.insert("integration".to_string(), test_reports);
                integration_stats.merge(&stats);
                status::hr();
            }

            if meta.kind == TestKind::Command
                && (kind == TestKind::All || kind == TestKind::Command)
            {
                let (stats, test_reports) = command::suite_command(
                    base,
                    root,
                    &meta,
                    quiet,
                    &["command"],
                    &includes,
                    &excludes,
                )?;
                reports.insert("command".to_string(), test_reports);
                cmd_stats.merge(&stats);
                status::hr();
            }

            if meta.kind == TestKind::Unit && (kind == TestKind::All || kind == TestKind::Unit) {
                let (stats, test_reports) =
                    suite_unit(base, root, &meta, &["unit"], &includes, &excludes)?;
                reports.insert("unit".to_string(), test_reports);
                unit_stats.merge(&stats);
                status::hr();
            }
        }

        elapsed = nanotime() - start;
    }

    status::hr();
    status::hr();
    status::rollups("All Benchmark", &bench_stats)?;
    status::rollups("All Integration", &integration_stats)?;
    status::rollups("All Command", &cmd_stats)?;
    status::rollups("All Unit", &unit_stats)?;
    let mut all_stats = stats::Stats::new();
    all_stats.merge(&bench_stats);
    all_stats.merge(&integration_stats);
    all_stats.merge(&cmd_stats);
    all_stats.merge(&unit_stats);
    status::rollups("Total", &all_stats)?;
    let mut stats_map = HashMap::new();
    stats_map.insert("all".to_string(), all_stats.clone());
    stats_map.insert("bench".to_string(), bench_stats);
    stats_map.insert("integration".to_string(), integration_stats);
    stats_map.insert("command".to_string(), cmd_stats);
    stats_map.insert("unit".to_string(), unit_stats);
    status::total_duration(elapsed)?;

    let test_run = report::TestRun {
        metadata: report::metadata(),
        includes,
        excludes,
        reports,
        stats: stats_map,
    };
    let mut file = file::create(report)?;

    if let Ok(result) = serde_json::to_string(&test_run) {
        file.write_all(&result.as_bytes())
            .map_err(|e| Error::from(format!("Failed to write report to `{}`: {}", report, e)))?;
    }

    if all_stats.fail > 0 {
        Err(ErrorKind::TestFailures(all_stats).into())
    } else {
        Ok(())
    }
}
