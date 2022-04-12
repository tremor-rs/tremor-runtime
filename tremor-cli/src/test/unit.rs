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

use crate::env;
use crate::errors::{Error, Result};
use crate::report;
use crate::test;
use crate::test::stats;
use crate::test::status;
use report::TestSuite;
use std::io::Read;
use std::{collections::HashMap, path::Path};
use test::tag;
use tremor_common::time::nanotime;
use tremor_script::ast::base_expr::BaseExpr;
use tremor_script::ast::{Expr, ImutExpr, ImutExprInt, Invoke, List, Record};
use tremor_script::ctx::EventContext;
use tremor_script::highlighter::{Dumb as DumbHighlighter, Highlighter, Term as TermHighlighter};
use tremor_script::interpreter::{AggrType, Env, ExecOpts, LocalStack};
use tremor_script::prelude::*;
use tremor_script::Value;

use super::TestConfig;
const EXEC_OPTS: ExecOpts = ExecOpts {
    result_needed: true,
    aggr: AggrType::Tick,
};

fn eval_suite_entrypoint(
    env: &Env,
    local: &LocalStack,
    script: &str,
    suite_spec: &Record<'_>,
    tags: &tag::TagFilter,
    config: &TestConfig,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();

    let spec = suite_spec
        .get_field_expr("tests")
        .and_then(ImutExprInt::as_list)
        .ok_or("Missing suite tests")?;

    if let Ok((s, mut e)) = eval_suite_tests(env, local, script, spec, tags, config) {
        elements.append(&mut e);
        stats.merge(&s);
    } else {
        stats.fail(
            suite_spec
                .get_literal("name")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
    }

    Ok((stats, elements))
}

fn eval(expr: &ImutExprInt, env: &Env, local: &LocalStack) -> Result<Value<'static>> {
    let state = Value::object();
    let meta = Value::object();
    let event = Value::object();
    Ok(expr
        .run(EXEC_OPTS, env, &event, &state, &meta, local)?
        .into_owned()
        .into_static())
}

fn eval_suite_tests(
    env: &Env,
    local: &LocalStack,
    script: &str,
    suite_spec: &List,
    suite_tags: &test::TagFilter,
    config: &TestConfig,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();

    let ll = suite_spec.exprs.len();
    for (idx, item) in suite_spec.exprs.iter().enumerate() {
        if let ImutExpr(ImutExprInt::Invoke1(Invoke {
            module, fun, args, ..
        })) = item
        {
            if module != &["test"] || fun != "test" {
                continue;
            }
            let spec = args
                .first()
                .and_then(ImutExpr::as_record)
                .ok_or_else(|| Error::from("Invalid test specification"))?;

            let mut found_tags = Vec::new();

            if let Some(tags) = spec.get_field_expr("tags") {
                let tag_value = eval(tags, env, local)?;
                if let Some(tags) = tag_value.as_array() {
                    let inner_tags = tags.iter().map(ToString::to_string);
                    found_tags.extend(inner_tags);
                }
            } else if let Some(tags) = spec.get_literal("tags").and_then(Value::as_array) {
                let inner_tags = tags.iter().map(ToString::to_string);
                found_tags.extend(inner_tags);
            }

            let case_tags = suite_tags.join(Some(found_tags));
            let test_name = spec
                .get_literal("name")
                .map(ToString::to_string)
                .unwrap_or_default();
            if let (matched, false) = config.matches(&case_tags) {
                if !config.verbose {
                    status::h1("    Test ( Skipping )", &test_name)?;
                    status::tagsx(
                        "        ",
                        &case_tags,
                        Some(&matched),
                        Some(&config.excludes),
                    )?;
                }
                stats.skip();
            } else if let Some(item) = spec.get_field_expr("test") {
                let start = nanotime();
                let value = eval(item, env, local)?;
                let elapsed = nanotime() - start;

                // Non colorized test source extent for json report capture
                let extent = item.extent(env.meta);
                let mut hh = DumbHighlighter::new();
                tremor_script::Script::highlight_script_with_range(script, extent, &mut hh)?;

                let mut info = hh.to_string();
                let success = if let Some(success) = value.as_bool() {
                    success
                } else if let Some([expected, got]) = value.as_array().map(Vec::as_slice) {
                    info.push_str(&format!("{} != {}", expected, got));
                    false
                } else {
                    false
                };

                let prefix = if success { "(+)" } else { "(-)" };
                let report = stats.report(success, &test_name);

                let hidden = config.quiet && success;
                if !hidden {
                    status::h1("    Test", &test_name)?;
                    status::tagsx(
                        "        ",
                        &case_tags,
                        Some(&config.includes),
                        Some(&config.excludes),
                    )?;
                    // Interactive console report
                    status::executing_unit_testcase(idx, ll, success)?;

                    let mut hh: TermHighlighter = TermHighlighter::default();
                    tremor_script::Script::highlight_script_with_range_indent(
                        "       ", script, extent, &mut hh,
                    )?;
                    if let Some([expected, got]) = value.as_array().map(Vec::as_slice) {
                        println!("             | {} != {}", expected, got);
                    }
                    hh.finalize()?;
                    println!();
                }
                // Test record
                elements.push(report::TestElement {
                    description: format!("{} Executing test {} of {}", prefix, idx + 1, ll),
                    keyword: report::KeywordKind::Test,
                    result: report::ResultKind {
                        status: report,
                        duration: elapsed,
                    },
                    info: Some(info),
                    hidden,
                });
                stats.assert();
            }
        }
    }

    Ok((stats, elements))
}

#[allow(clippy::too_many_lines)]
pub(crate) fn run_suite(
    path: &Path,
    scenario_tags: &tag::TagFilter,
    config: &TestConfig,
) -> Result<report::TestReport> {
    println!();

    let mut suites: HashMap<String, TestSuite> = HashMap::new();
    let script = path.to_string_lossy().to_string();

    let mut raw = String::new();
    let mut input = crate::open_file(&path, None)?;
    input.read_to_string(&mut raw)?;

    let env = env::setup()?;
    let report_start = nanotime();
    let mut stats = stats::Stats::new();
    match tremor_script::Script::parse(&env.module_path, &script, raw.clone(), &env.fun) {
        Ok(runnable) => {
            let local = LocalStack::default();

            let mut h = TermHighlighter::default();
            runnable.format_warnings_with(&mut h)?;

            let script = runnable.script.suffix();

            let context = &EventContext::new(nanotime(), None);
            let env = Env {
                context,
                consts: script.consts.run(),
                aggrs: &script.aggregates,
                meta: &script.node_meta,
                recursion_limit: tremor_script::recursion_limit(),
            };

            for expr in script.exprs.iter().filter_map(Expr::as_invoke) {
                let mut stats = stats::Stats::new();
                let mut elements = Vec::new();

                let Invoke {
                    module, fun, args, ..
                } = expr;

                let m = module.join("").to_string();
                if m == "test" && fun == "suite" {
                    // A Test suite
                    let spec = args
                        .first()
                        .and_then(ImutExpr::as_record)
                        .ok_or_else(|| Error::from("Invalid test specification"))?;

                    let mut found_tags = Vec::new();
                    if let Some(tags) = spec.get_field_expr("tags") {
                        let tag_value = eval(tags, &env, &local)?;
                        if let Some(tags) = tag_value.as_array() {
                            let inner_tags = tags.iter().map(|x| (*x).to_string());
                            found_tags.extend(inner_tags);
                        }
                    } else if let Some(tags) = spec.get_literal("tags").and_then(Value::as_array) {
                        let inner_tags = tags.iter().map(|x| (*x).to_string());
                        found_tags.extend(inner_tags);
                    }

                    let suite_tags = scenario_tags.join(Some(found_tags));
                    let suite_name = spec
                        .get_literal("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();

                    // TODO revisit tags in unit tests
                    if let (_matched, true) = config.matches(&suite_tags) {
                        status::h1("  Suite", suite_name)?;
                        status::tagsx(
                            "      ",
                            &suite_tags,
                            Some(&config.includes),
                            Some(&config.excludes),
                        )?;
                        let (test_stats, mut test_reports) =
                            eval_suite_entrypoint(&env, &local, &raw, spec, &suite_tags, config)?;

                        stats.merge(&test_stats);
                        elements.append(&mut test_reports);
                    }
                    suites.insert(
                        suite_name.to_string(),
                        TestSuite {
                            name: suite_name.to_string(),
                            description: suite_name.to_string(),
                            elements,
                            evidence: None,
                            stats,
                            duration: 0,
                        },
                    );
                }
            }
        }
        Err(e) => {
            stats.fail(&script);
            let mut h = TermHighlighter::default();
            if let Err(e) = tremor_script::Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
        }
    }

    for v in suites.values() {
        stats.merge(&v.stats);
    }

    Ok(report::TestReport {
        description: "unit test suites".into(),
        elements: suites,
        stats,
        duration: nanotime() - report_start,
    })
}
