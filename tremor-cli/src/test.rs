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
use crate::report::{self, TestReport};
use crate::status;
use crate::test::command::suite_command;
use crate::test::stats::Stats;
use crate::util::{basename, slurp_string};
use clap::ArgMatches;
use globwalk::{DirEntry, FileType, GlobWalkerBuilder};
use kind::Kind;
pub(crate) use kind::Unknown;
use metadata::Meta;
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::Write;
use std::path::{Path, PathBuf};
use tag::TagFilter;
use tremor_common::file::{self, canonicalize};
use tremor_common::time::nanotime;

mod after;
mod assert;
mod before;
mod command;
pub(crate) mod kind;
mod metadata;
mod process;
pub mod stats;
pub mod tag;
mod unit;

fn suite_bench(root: &Path, config: &TestConfig) -> Result<(Stats, Vec<report::TestReport>)> {
    if let Ok(benches) = GlobWalkerBuilder::new(root, &config.meta.includes)
        .case_insensitive(true)
        .file_type(FileType::DIR)
        .build()
    {
        let benches = benches.filter_map(std::result::Result::ok);

        let mut suite = vec![];
        let mut test_stats = Stats::new();

        status::h0("Framework", "Finding benchmark test scenarios")?;

        for bench in benches {
            let (s, t) = run_bench(bench.path(), config)?;
            test_stats.merge(&s);
            if let Some(report) = t {
                suite.push(report);
            }
        }

        Ok((test_stats, suite))
    } else {
        Err("Unable to walk test path for benchmarks".into())
    }
}

fn run_bench(root: &Path, config: &TestConfig) -> Result<(Stats, Option<report::TestReport>)> {
    let tags = tag::resolve(config.base_directory.as_path(), root)?;
    let mut test_stats = Stats::new();

    let (matched, is_match) = config.matches(&tags);
    if is_match {
        status::h1("Benchmark", &format!("Running {}", &root.display()))?;
        let cwd = std::env::current_dir()?;
        std::env::set_current_dir(&root)?;
        status::tags(&tags, Some(&matched), Some(&config.excludes))?;
        let test_report = process::run_process(
            "bench",
            config.base_directory.as_path(),
            &cwd.join(root),
            &tags,
        )?;

        // Restore cwd
        file::set_current_dir(&cwd)?;

        status::duration(test_report.duration, "  ")?;
        if test_report.stats.is_pass() {
            test_stats.pass();
        } else {
            test_stats.fail(&config.base_directory.display().to_string());
        }
        Ok((test_stats, Some(test_report)))
    } else {
        test_stats.skip();
        status::h1(
            "  Benchmark",
            &format!("Skipping {}", &config.base_directory.display()),
        )?;
        status::tags(&tags, Some(&matched), Some(&config.excludes))?;
        Ok((test_stats, None))
    }
}

fn suite_integration(root: &Path, config: &TestConfig) -> Result<(Stats, Vec<report::TestReport>)> {
    if let Ok(tests) = GlobWalkerBuilder::new(root, &config.meta.includes)
        .case_insensitive(true)
        .file_type(FileType::DIR)
        .build()
    {
        let tests = tests.filter_map(std::result::Result::ok);

        let mut suite = vec![];
        let mut test_stats = Stats::new();

        status::h0("Framework", "Finding integration test scenarios")?;

        for test in tests {
            let (s, t) = run_integration(test.path(), config)?;

            test_stats = s;
            if let Some(report) = t {
                suite.push(report);
            }
        }

        status::rollups("\n  Integration", &test_stats)?;

        Ok((test_stats, suite))
    } else {
        Err("Unable to walk test path for integration tests".into())
    }
}

fn run_integration(
    root: &Path,
    config: &TestConfig,
) -> Result<(Stats, Option<report::TestReport>)> {
    let base = config.base_directory.as_path();
    let bench_root = root.to_string_lossy();
    let tags = tag::resolve(base, root)?;
    let mut test_stats = Stats::new();

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

        let test_report = process::run_process("integration", base, root, &tags)?;

        // Restore cwd
        file::set_current_dir(&cwd)?;

        if test_report.stats.is_pass() {
            test_stats.pass();
        } else {
            test_stats.fail(&bench_root);
        }
        test_stats.assert += &test_report.stats.assert;

        status::stats(&test_report.stats, "  ")?;
        status::duration(test_report.duration, "    ")?;
        Ok((test_stats, Some(test_report)))
    } else {
        test_stats.skip();
        status::h1(
            "Integration",
            &format!("Skipping {}", &basename(&bench_root)),
        )?;
        status::tags(&tags, Some(&matched), Some(&config.excludes))?;
        Ok((test_stats, None))
    }
}

fn suite_unit(root: &Path, conf: &TestConfig) -> Result<(Stats, Vec<report::TestReport>)> {
    let base = conf.base_directory.as_path();
    let suites = GlobWalkerBuilder::new(root, "all.tremor")
        .case_insensitive(true)
        .file_type(FileType::FILE)
        .build()
        .map_err(|e| format!("Unable to walk test path for unit tests: {}", e))?;

    let suites = suites.filter_map(std::result::Result::ok);
    let mut test_reports = vec![];
    let mut test_stats = Stats::new();

    status::h0("Framework", "Finding unit test scenarios")?;

    for suite in suites {
        status::h0("  Unit Test Scenario", &suite.path().to_string_lossy())?;
        let scenario_tags = tag::resolve(base, root)?;
        status::tags(&scenario_tags, Some(&conf.includes), Some(&conf.excludes))?;
        let report = unit::run_suite(suite.path(), &scenario_tags, conf)?;
        test_stats.merge(&report.stats);
        status::stats(&report.stats, "  ")?;
        status::duration(report.duration, "    ")?;
        test_reports.push(report);
    }

    status::rollups("  Unit", &test_stats)?;

    Ok((test_stats, test_reports))
}

#[derive(Debug, Clone)]
pub(crate) struct TestConfig {
    pub(crate) quiet: bool,
    pub(crate) verbose: bool,
    pub(crate) sys_filter: &'static [&'static str],
    pub(crate) includes: Vec<String>,
    pub(crate) excludes: Vec<String>,
    pub(crate) meta: Meta,
    pub(crate) report: Option<String>,
    pub(crate) base_directory: PathBuf,
}
impl TestConfig {
    fn matches(&self, filter: &TagFilter) -> (Vec<String>, bool) {
        filter.matches(self.sys_filter, &self.includes, &self.excludes)
    }

    fn from_matches(args: &ArgMatches) -> Result<TestConfig> {
        let base_directory = canonicalize(args.value_of("PATH").unwrap_or_default())?;
        let report = args
            .value_of("REPORT")
            .map(std::string::ToString::to_string);
        let quiet = args.is_present("QUIET");
        let verbose = args.is_present("verbose");
        let includes = match args.values_of("INCLUDES") {
            Some(matches) => matches.map(std::string::ToString::to_string).collect(),
            None => vec![],
        };

        let excludes = match args.values_of("EXCLUDES") {
            Some(matches) => matches.map(std::string::ToString::to_string).collect(),
            None => vec![],
        };

        Ok(TestConfig {
            quiet,
            verbose,
            includes,
            excludes,
            report,
            sys_filter: &[],
            meta: Meta::default(),
            base_directory,
        })
    }
}

fn run_single_test(config: &TestConfig) -> Result<(Stats, Vec<TestReport>)> {
    // Our starting point in running a single test is also the directory we're
    // trying to run. No further tests hide somewhere down the directory
    // hierarchy.
    let path = config.base_directory.as_path();

    match config.meta.kind {
        Kind::Bench => {
            let (stats, test_report) = run_bench(path, config)?;
            Ok((stats, vec![test_report].into_iter().flatten().collect()))
        }
        Kind::Integration => {
            let (stats, test_report) = run_integration(path, config)?;
            Ok((stats, vec![test_report].into_iter().flatten().collect()))
        }
        // Command tests are their own beast, one singular folder might
        // well result in many tests run
        Kind::Command => command::suite_command(path, config),
        Kind::Unit => suite_unit(path, config),
        Kind::All => {
            Err(Error::from("No tests run: Can't run this test without being told what type it is. Rerun with `bench`, `integration`, `command` or `unit`."))
        }
        Kind::Unknown(ref x) => {
            Err(Error::from(format!(
                "No tests run: Unknown kind of test: {}",
                x
            )))
        }
    }
}

fn run_suite(
    config: &TestConfig,
    meta_path: &DirEntry,
) -> Result<(Stats, Vec<report::TestReport>, Kind)> {
    let mut config = config.clone();
    let root = match meta_path.path().parent() {
        Some(root) => root,
        None => return Err(Error::from("")),
    };
    let mut meta_str = slurp_string(&meta_path.path())?;
    let local_meta: Meta = simd_json::from_str(meta_str.as_mut_str())?;

    if config.meta.kind == Kind::All || local_meta.kind == config.meta.kind {
        if config.meta.kind == Kind::All {
            config.includes.push("all".into());
        }

        // Set config meta to the local file for the duration of the test run.
        let old_meta = config.meta;
        config.meta = local_meta.clone();

        // This dispatches on the type specified in the local `meta.json`,
        // therefore we can't be sure we haven't already ruled out not-matching
        // suite types .
        let (stats, reports) = match config.meta.kind {
            Kind::Bench => suite_bench(root, &config),
            Kind::Integration => suite_integration(root, &config),
            Kind::Command => suite_command(root, &config),
            Kind::Unit => suite_unit(root, &config),
            Kind::All => {
                status::h1(
                    &meta_path.path().to_string_lossy(),
                    "Skipping suite due to invalid local kind: All",
                )?;
                Ok((Stats::new(), Vec::new()))
            }
            Kind::Unknown(x) => {
                status::h1(
                    &meta_path.path().to_string_lossy(),
                    &format!("Skipping suite due to invalid kind: {}", x),
                )?;
                Ok((Stats::new(), Vec::new()))
            }
        }?;

        config.meta = old_meta;
        Ok((stats, reports, local_meta.kind))
    } else {
        // Skip test if not of the kind that is being run (bench,
        // integration, unit, command)
        Ok((Stats::new(), Vec::new(), local_meta.kind))
    }
}

pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    env_logger::init();
    let mut config = TestConfig::from_matches(matches)?;
    let found = GlobWalkerBuilder::new(&config.base_directory, "meta.json")
        .case_insensitive(true)
        .build()
        .map_err(|e| {
            Error::from(format!(
                "failed to walk directory `{}`: {}",
                &config.base_directory.display(),
                e
            ))
        })?;

    let mut test_stats: HashMap<Kind, Stats> = HashMap::new();
    test_stats.insert(Kind::Bench, Stats::new());
    test_stats.insert(Kind::Unit, Stats::new());
    test_stats.insert(Kind::Command, Stats::new());
    test_stats.insert(Kind::Integration, Stats::new());

    let mut test_reports = HashMap::new();

    let found: Vec<_> = found.filter_map(std::result::Result::ok).collect();
    let start = nanotime();

    if found.len() > 1 {
        for meta in found {
            let (stats, reports, kind_run) = run_suite(&config, &meta)?;
            test_stats
                .get_mut(&kind_run)
                .ok_or_else(|| Error::from(format!("No stats for test kind {:?}", &kind_run)))?
                .merge(&stats);

            test_reports.insert(meta.path().to_string_lossy().to_string(), reports);

            status::hr();
        }
    } else {
        // No meta.json was found, therefore we might have the path to a
        // specific folder. Let's apply some heuristics to see if we have
        // something runnable.
        let files = GlobWalkerBuilder::from_patterns(
            &config.base_directory,
            &["*.{yaml,tremor,trickle}", "!assert.yaml", "!logger.yaml"],
        )
        .case_insensitive(true)
        .max_depth(1)
        .build()?
        .filter_map(std::result::Result::ok);

        if files.count() >= 1 {
            // Use CLI kind instead of default kind, since we don't have a
            // meta.json to override the default with.
            config.meta.kind = matches.value_of("MODE").unwrap_or_default().try_into()?;
            let (stats, report) = run_single_test(&config)?;
            test_stats
                .get_mut(&config.meta.kind)
                .ok_or_else(|| {
                    Error::from(format!("No stats for test kind {:?}", &config.meta.kind))
                })?
                .merge(&stats);
            test_reports.insert(config.base_directory.to_string_lossy().to_string(), report);
        } else {
            return Err(Error::from(
                "Specified folder does not contain a runnable test",
            ));
        }
    };
    let elapsed = nanotime() - start;

    status::hr();
    status::hr();
    status::rollups(
        "All Benchmark",
        test_stats.get(&Kind::Bench).unwrap_or(&Stats::new()),
    )?;
    status::rollups(
        "All Integration",
        test_stats.get(&Kind::Integration).unwrap_or(&Stats::new()),
    )?;
    status::rollups(
        "All Command",
        test_stats.get(&Kind::Command).unwrap_or(&Stats::new()),
    )?;
    status::rollups(
        "All Unit",
        test_stats.get(&Kind::Unit).unwrap_or(&Stats::new()),
    )?;

    let mut all_stats = Stats::new();
    for s in test_stats.values() {
        all_stats.merge(s);
    }

    status::rollups("Total", &all_stats)?;

    test_stats.insert(Kind::All, all_stats.clone());

    status::total_duration(elapsed)?;

    if let Some(report) = config.report {
        let test_run = report::TestRun {
            metadata: report::metadata(),
            includes: config.includes,
            excludes: config.excludes,
            reports: test_reports,
            stats: test_stats,
        };
        let mut file = file::create(&report)?;
        let result = simd_json::to_string(&test_run)?;
        file.write_all(result.as_bytes())
            .map_err(|e| Error::from(format!("Failed to write report to `{}`: {}", report, e)))?;
    }

    if all_stats.fail > 0 {
        Err(ErrorKind::TestFailures(all_stats.clone()).into())
    } else {
        Ok(())
    }
}
