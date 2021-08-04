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
use crate::test::command::suite_command;
use crate::util::{basename, slurp_string};
use clap::ArgMatches;
use globwalk::{FileType, GlobWalkerBuilder};
use kind::Kind;
pub(crate) use kind::Unknown;
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
    config: &TestConfig,
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(benches) = GlobWalkerBuilder::new(root, &config.meta.includes)
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

            let (matched, is_match) = config.matches(&tags);
            if is_match {
                status::h1("Benchmark", &format!("Running {}", &basename(&bench_root)))?;
                let cwd = std::env::current_dir()?;
                std::env::set_current_dir(Path::new(&root))?;
                status::tags(&tags, Some(&matched), Some(&config.excludes))?;
                let test_report = process::run_process("bench", base, root, &tags)?;

                // Restore cwd
                file::set_current_dir(&cwd)?;

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
                status::tags(&tags, Some(&matched), Some(&config.excludes))?;
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
    config: &TestConfig,
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(tests) = GlobWalkerBuilder::new(root, &config.meta.includes)
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

            let (matched, is_match) = config.matches(&tags);
            if is_match {
                status::h1(
                    "Integration",
                    &format!("Running {}", &basename(&bench_root)),
                )?;
                // Set cwd to test root
                let cwd = std::env::current_dir()?;
                std::env::set_current_dir(&root)?;
                status::tags(&tags, Some(&matched), Some(&config.excludes))?;

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
                status::tags(&tags, Some(&matched), Some(&config.excludes))?;
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
    conf: &TestConfig,
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    let suites = GlobWalkerBuilder::new(root, "all.tremor")
        .case_insensitive(true)
        .file_type(FileType::FILE)
        .build()
        .map_err(|e| format!("Unable to walk test path for unit tests: {}", e))?;

    let suites = suites.filter_map(std::result::Result::ok);
    let mut reports = vec![];
    let mut stats = stats::Stats::new();

    status::h0("Framework", "Finding unit test scenarios")?;

    for suite in suites {
        status::h0("  Unit Test Scenario", &suite.path().to_string_lossy())?;
        let scenario_tags = tag::resolve(base, root)?;
        status::tags(&scenario_tags, Some(&conf.includes), Some(&conf.excludes))?;
        let report = unit::run_suite(suite.path(), &scenario_tags, conf)?;
        stats.merge(&report.stats);
        status::stats(&report.stats, "  ")?;
        status::duration(report.duration, "    ")?;
        reports.push(report);
    }

    status::rollups("  Unit", &stats)?;

    Ok((stats, reports))
}

pub(crate) struct TestConfig {
    pub(crate) quiet: bool,
    pub(crate) verbose: bool,
    pub(crate) sys_filter: &'static [&'static str],
    pub(crate) includes: Vec<String>,
    pub(crate) excludes: Vec<String>,
    pub(crate) meta: Meta,
}
impl TestConfig {
    fn matches(&self, filter: &TagFilter) -> (Vec<String>, bool) {
        filter.matches(self.sys_filter, &self.includes, &self.excludes)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    let kind: test::Kind = matches.value_of("MODE").unwrap_or_default().try_into()?;
    let path = matches.value_of("PATH").unwrap_or_default();
    let report = matches.value_of("REPORT");
    let quiet = matches.is_present("QUIET");
    let verbose = matches.is_present("verbose");
    let includes: Vec<String> = if matches.is_present("INCLUDES") {
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
    let mut config = TestConfig {
        quiet,
        verbose,
        includes,
        excludes,
        sys_filter: &[],
        meta: Meta::default(),
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
    let base = cwd.join(path);
    let found = found.filter_map(std::result::Result::ok);
    let start = nanotime();
    for meta in found {
        if let Some(root) = meta.path().parent() {
            let mut meta_str = slurp_string(&meta.path())?;
            let meta: Meta = simd_json::from_str(meta_str.as_mut_str())?;
            config.meta = meta;

            if config.meta.kind == Kind::All {
                config.includes.push("all".into());
            }

            if !(kind == Kind::All || kind == config.meta.kind) {
                continue;
            }

            let test_reports = match config.meta.kind {
                Kind::Bench => {
                    let (s, t) = suite_bench(&base, root, &config)?;
                    bench_stats.merge(&s);
                    t
                }
                Kind::Integration => {
                    let (s, t) = suite_integration(&base, root, &config)?;
                    integration_stats.merge(&s);
                    t
                }
                Kind::Command => {
                    let (s, t) = suite_command(&base, root, &config)?;
                    cmd_stats.merge(&s);
                    t
                }
                Kind::Unit => {
                    let (s, t) = suite_unit(&base, root, &config)?;
                    unit_stats.merge(&s);
                    t
                }
                Kind::All | Kind::Unknown(_) => continue,
            };
            reports.insert(config.meta.kind.to_string(), test_reports);
            status::hr();
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
        includes: config.includes,
        excludes: config.excludes,
        reports,
        stats: stats_map,
    };
    if let Some(report) = report {
        let mut file = file::create(report)?;
        let result = simd_json::to_string(&test_run)?;
        file.write_all(result.as_bytes())
            .map_err(|e| Error::from(format!("Failed to write report to `{}`: {}", report, e)))?;
    }

    if all_stats.fail > 0 {
        Err(ErrorKind::TestFailures(all_stats).into())
    } else {
        Ok(())
    }
}
