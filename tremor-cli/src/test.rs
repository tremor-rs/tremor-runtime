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
use crate::report;
use crate::status;
use crate::test;
use crate::util::*;
use clap::ArgMatches;
use globwalk::{FileType, GlobWalkerBuilder};
use std::collections::HashMap;
use std::path::Path;

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

use kind::TestKind;
use metadata::Meta;
use std::{fs::File, io::Write};
use tag::TagFilter;

fn suite_bench(
    root: &Path,
    meta: &Meta,
    by_tag: &TagFilter,
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(benches) = GlobWalkerBuilder::new(root, &meta.includes)
        .case_insensitive(true)
        .file_type(FileType::DIR)
        .build()
    {
        let benches = benches.into_iter().filter_map(std::result::Result::ok);

        let mut suite = vec![];
        let mut stats = stats::Stats::new();
        for bench in benches {
            let root = bench.path();
            let bench_root = root.to_string_lossy();
            let tags_str = &format!("{}/tags.json", bench_root);
            let tags = tag::maybe_slurp_tags(tags_str)?;

            if let (_matched, true) = by_tag.matches(&tags) {
                status::h1("Benchmark", &format!("Running {}", &basename(&bench_root)))?;
                let test_report = process::run_process("bench", root, by_tag)?;
                status::duration(test_report.duration).ok();
                status::tags(&by_tag, &Some(&tags)).ok();
                suite.push(test_report);
                stats.pass(); // FIXME invent a better way of capturing benchmark status
            } else {
                stats.skip();
                status::h1("Benchmark", &format!("Skipping {}", &basename(&bench_root)))?;
            }
        }

        Ok((stats, suite))
    } else {
        Err("Unable to walk test path for benchmarks".into())
    }
}

fn suite_integration(
    root: &Path,
    meta: &Meta,
    by_tag: &TagFilter,
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(tests) = GlobWalkerBuilder::new(root, &meta.includes)
        .case_insensitive(true)
        .file_type(FileType::DIR)
        .build()
    {
        let tests = tests.into_iter().filter_map(std::result::Result::ok);

        let mut suite = vec![];
        let mut stats = stats::Stats::new();
        for test in tests {
            let root = test.path();
            let bench_root = root.to_string_lossy();
            let tags_str = &format!("{}/tags.json", bench_root);
            let tags = tag::maybe_slurp_tags(tags_str)?;

            if let (_matched, true) = by_tag.matches(&tags) {
                status::h1(
                    "Integration",
                    &format!("Running {}", &basename(&bench_root)),
                )?;
                let test_report = process::run_process("integration", root, by_tag)?;
                stats.merge(&test_report.stats);
                status::stats(&test_report.stats).ok();
                status::duration(test_report.duration).ok();
                status::tags(&by_tag, &Some(&tags)).ok();
                suite.push(test_report);
            } else {
                stats.skip();
                status::h1(
                    "Integration",
                    &format!("Skipping {}", &basename(&bench_root)),
                )?;
            }
        }

        status::rollups("\nIntegration", &stats).ok();

        Ok((stats, suite))
    } else {
        Err("Unable to walk test path for integration tests".into())
    }
}

fn suite_unit(
    root: &Path,
    _meta: &Meta,
    by_tag: &TagFilter,
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    if let Ok(suites) = GlobWalkerBuilder::new(root, "all.tremor")
        .case_insensitive(true)
        .file_type(FileType::FILE)
        .build()
    {
        let suites = suites.into_iter().filter_map(std::result::Result::ok);
        let mut reports = vec![];
        let mut stats = stats::Stats::new();
        for suite in suites {
            let report = unit::run_suite(&suite.path(), by_tag)?;
            stats.merge(&report.stats);
            status::stats(&report.stats).ok();
            status::duration(report.duration).ok();
            reports.push(report);
        }

        status::rollups("\nUnit", &stats).ok();

        Ok((stats, reports))
    } else {
        Err("Unable to walk test path for unit tests".into())
    }
}

pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    let kind: test::TestKind = matches.value_of("MODE").unwrap_or_default().into();
    let path = matches.value_of("PATH").unwrap_or_default();
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

    let filter_by_tags = TagFilter::new(excludes.clone(), includes.clone());

    let found = GlobWalkerBuilder::new(Path::new(&path).canonicalize()?, "meta.json")
        .case_insensitive(true)
        .build()
        .ok();

    let mut reports = HashMap::new();
    let mut bench_stats = stats::Stats::new();
    let mut unit_stats = stats::Stats::new();
    let mut cmd_stats = stats::Stats::new();
    let mut integration_stats = stats::Stats::new();
    let mut elapsed = 0;

    if let Some(found) = found {
        let found = found.into_iter().filter_map(std::result::Result::ok);
        let start = nanotime();
        for meta in found {
            let root = meta.path().parent();
            if let Some(root) = root {
                if let Ok(meta_str) = slurp_string(&meta.path().to_string_lossy()) {
                    let meta: Option<Meta> = serde_json::from_str(&meta_str).ok();
                    if let Some(meta) = meta {
                        if meta.kind == TestKind::Bench
                            && (kind == TestKind::All || kind == TestKind::Bench)
                        {
                            let (stats, test_reports) = suite_bench(root, &meta, &filter_by_tags)?;
                            reports.insert("bench".to_string(), test_reports);
                            bench_stats.merge(&stats);
                            status::hr().ok();
                        }

                        if meta.kind == TestKind::Integration
                            && (kind == TestKind::All || kind == TestKind::Integration)
                        {
                            let (stats, test_reports) =
                                suite_integration(root, &meta, &filter_by_tags)?;
                            reports.insert("integration".to_string(), test_reports);
                            integration_stats.merge(&stats);
                            status::hr().ok();
                        }

                        if meta.kind == TestKind::Command
                            && (kind == TestKind::All || kind == TestKind::Command)
                        {
                            let (stats, test_reports) =
                                command::suite_command(root, &meta, &filter_by_tags)?;
                            reports.insert("api".to_string(), test_reports);
                            cmd_stats.merge(&stats);
                            status::hr().ok();
                        }

                        if meta.kind == TestKind::Unit
                            && (kind == TestKind::All || kind == TestKind::Unit)
                        {
                            let (stats, test_reports) = suite_unit(root, &meta, &filter_by_tags)?;
                            reports.insert("unit".to_string(), test_reports);
                            unit_stats.merge(&stats);
                            status::hr().ok();
                        }
                    }
                }
            }
        }
        elapsed = nanotime() - start;
    }

    status::hr().ok();
    status::hr().ok();
    status::rollups("All Benchmark Stats", &bench_stats).ok();
    status::rollups("All Integration Stats", &integration_stats).ok();
    status::rollups("All Command Stats", &cmd_stats).ok();
    status::rollups("All Unit Stats", &unit_stats).ok();
    let mut all_stats = stats::Stats::new();
    all_stats.merge(&bench_stats);
    all_stats.merge(&integration_stats);
    all_stats.merge(&cmd_stats);
    all_stats.merge(&unit_stats);
    status::rollups("Total Stats", &all_stats).ok();
    let mut stats_map = HashMap::new();
    stats_map.insert("all".to_string(), all_stats);
    stats_map.insert("bench".to_string(), bench_stats);
    stats_map.insert("integration".to_string(), integration_stats);
    stats_map.insert("command".to_string(), cmd_stats);
    stats_map.insert("unit".to_string(), unit_stats);
    status::total_duration(elapsed).ok();

    let test_run = report::TestRun {
        metadata: report::metadata(),
        includes,
        excludes,
        reports,
        stats: stats_map,
    };
    let file = File::create("report.txt").ok();
    if let Some(mut file) = file {
        if let Ok(result) = serde_json::to_string(&test_run) {
            file.write_all(&result.as_bytes()).ok();
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_filters() {
        // TODO Investigate a richer include/exclude model
        // TODO Current implementation is simplest that could possibly work
        let tf: TagFilter = TagFilter::new(vec![], vec![]);
        assert!(tf.matches(&vec!["foo".to_string()]).1);
        assert!(tf.matches(&vec![]).1);
        assert!(
            tf.matches(&vec![
                "foo".to_string(),
                "bar".to_string(),
                "baz".to_string()
            ])
            .1
        );
        let tf = TagFilter::new(vec!["no".to_string()], vec!["yes".to_string()]);
        assert!(tf.matches(&vec!["yes".to_string()]).1);
        assert!(!tf.matches(&vec!["no".to_string()]).1);
        assert!(!tf.matches(&vec!["snot".to_string()]).1);
    }
}
