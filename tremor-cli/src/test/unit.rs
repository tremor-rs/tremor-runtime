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
use crate::report;
use crate::test;
use crate::test::stats;
use crate::test::status;
use halfbrown::hashmap;
use report::TestSuite;
use simd_json::prelude::*;
use simd_json::{borrowed::Value, StaticNode};
use std::io::Read;
use std::{collections::HashMap, path::Path};
use test::tag;
use tremor_common::time::nanotime;
use tremor_script::ast::base_expr::BaseExpr;
use tremor_script::ast::{Expr, ImutExpr, ImutExprInt, Invoke, List, Literal, NodeMetas, Record};
use tremor_script::ctx::{EventContext, EventOriginUri};
use tremor_script::highlighter::{Dumb as DumbHighlighter, Highlighter, Term as TermHighlighter};
use tremor_script::interpreter::{AggrType, Env, ExecOpts, LocalStack};
use tremor_script::path::load as load_module_path;
use tremor_script::{registry, Registry};
const EXEC_OPTS: ExecOpts = ExecOpts {
    result_needed: true,
    aggr: AggrType::Tick,
};

#[allow(clippy::too_many_arguments)]
fn eval_suite_entrypoint(
    env: &Env,
    local: &LocalStack,
    script: &str,
    meta: &NodeMetas,
    suite_spec: &Record<'_>,
    suite_result: &[Value<'_>],
    tags: &tag::TagFilter,
    by_tag: (&[String], &[String]),
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();

    let o = suite_result
        .get(0)
        .and_then(ValueTrait::as_object)
        .ok_or_else(|| Error::from("bad suite results"))?;
    let suite_name = o.get("name");
    if let Some(_suite_name) = suite_name {
        let suite_spec_index = suite_spec.fields.iter().position(|f| {
            if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                value == "tests"
            } else {
                false
            }
        });
        let suite_name_index = suite_spec.fields.iter().position(|f| {
            if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                value == "name"
            } else {
                false
            }
        });

        if let Some(suite_spec_index) = suite_spec_index {
            if let Some(suite_name_index) = suite_name_index {
                let name = &suite_spec.fields[suite_name_index].value;
                if let ImutExprInt::Literal(Literal { .. }) = name {
                    if let ImutExprInt::List(l) = &suite_spec.fields[suite_spec_index].value {
                        if let Some(suite) = o.get("suite") {
                            if let Value::Object(suite) = suite {
                                if let Some(tests) = suite.get("tests") {
                                    let (s, mut e) = eval_suite_tests(
                                        &env, &local, script, meta, l, tests, &tags, by_tag,
                                    )?;
                                    elements.append(&mut e);
                                    stats.merge(&s);
                                }
                            }
                        }
                    }
                }
            }
        } // TODO error/warning handling when no tests found
    }

    Ok((stats, elements))
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
fn eval_suite_tests(
    env: &Env,
    local: &LocalStack,
    script: &str,
    node_metas: &NodeMetas,
    suite_spec: &List,
    suite_result: &Value,
    suite_tags: &test::TagFilter,
    by_tag: (&[String], &[String]),
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();
    if let Value::Array(a) = suite_result {
        let al = a.len();
        let ll = suite_spec.exprs.len();
        for i in 0..ll {
            let item = &suite_spec.exprs[i];
            if let ImutExpr(ImutExprInt::Invoke1(Invoke {
                module, fun, args, ..
            })) = item
            {
                let m = module.join("").to_string();
                if m == "test" && fun == "test" {
                    if let ImutExpr(ImutExprInt::Record(Record { fields, .. })) = &args[0] {
                        let test_spec_index = fields.iter().position(|f| {
                            if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                                value == "test"
                            } else {
                                false
                            }
                        });

                        let tag_spec_index = fields.iter().position(|f| {
                            if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                                value == "tags"
                            } else {
                                false
                            }
                        });

                        let mut found_tags = Vec::new();
                        match tag_spec_index {
                            None => (),
                            Some(tag_spec_index) => {
                                let tags = &fields[tag_spec_index].value;
                                let tag_state = Value::Object(Box::new(hashmap! {}));
                                let meta = Value::Object(Box::new(hashmap! {}));
                                let event = Value::Object(Box::new(hashmap! {}));
                                let tag_value =
                                    tags.run(EXEC_OPTS, &env, &event, &tag_state, &meta, &local)?;
                                if let Value::Array(tags) = tag_value.as_ref() {
                                    let mut inner_tags = tags
                                        .iter()
                                        .map(|x| (*x).to_string())
                                        .collect::<Vec<String>>();
                                    found_tags.append(&mut inner_tags);
                                }
                            }
                        };

                        let case_tags = suite_tags.join(Some(found_tags));

                        if let Some(test_spec_index) = test_spec_index {
                            let item = &fields[test_spec_index].value;
                            let test_state = Value::Object(Box::new(hashmap! {}));
                            let event = Value::Object(Box::new(hashmap! {}));
                            let meta = Value::Object(Box::new(hashmap! {}));

                            let start = nanotime();
                            let value =
                                item.run(EXEC_OPTS, &env, &event, &test_state, &meta, &local)?;
                            let elapsed = nanotime() - start;

                            if let Value::Static(StaticNode::Bool(status)) = value.into_owned() {
                                // Non colorized test source extent for json report capture
                                let extent = suite_spec.exprs[i].extent(node_metas);
                                let mut hh = DumbHighlighter::new();
                                tremor_script::Script::highlight_script_with_range(
                                    script, extent, &mut hh,
                                )?;

                                if let (matched, false) = case_tags.matches(&by_tag.0, &by_tag.1) {
                                    status::h1("    Test ( Skipping )", "")?;
                                    status::tagsx(
                                        "        ",
                                        &case_tags,
                                        Some(&matched),
                                        Some(&by_tag.1),
                                    )?;
                                    stats.skip();
                                    continue;
                                } else {
                                    status::h1("    Test", "")?;
                                    status::tagsx(
                                        "        ",
                                        &case_tags,
                                        Some(&by_tag.0),
                                        Some(&by_tag.1),
                                    )?;
                                }

                                let prefix = if status { "(+)" } else { "(-)" };
                                // Test record
                                elements.push(report::TestElement {
                                    description: format!(
                                        "{} Executing test {} of {}",
                                        prefix,
                                        i + 1,
                                        ll
                                    ),
                                    keyword: report::KeywordKind::Test,
                                    result: report::ResultKind {
                                        status: if status {
                                            stats.pass();
                                            report::StatusKind::Passed
                                        } else {
                                            stats.fail();
                                            report::StatusKind::Failed
                                        },
                                        duration: elapsed,
                                    },
                                    info: Some(hh.to_string()),
                                    hidden: false,
                                });
                                drop(hh);
                                stats.assert();

                                // Interactive console report
                                status::executing_unit_testcase(i, ll, status)?;
                                let mut hh: TermHighlighter = TermHighlighter::default();
                                tremor_script::Script::highlight_script_with_range_indent(
                                    "       ", script, extent, &mut hh,
                                )?;
                                hh.finalize()?;
                                drop(hh);
                                println!();
                            }
                            continue;
                        }
                    };
                }
            }

            if let Value::Static(StaticNode::Bool(status)) = &suite_result[i] {
                // Predicate tests do not have tags support

                // Non colorized test source extent for json report capture
                let extent = suite_spec.exprs[i].extent(node_metas);
                let mut hh = DumbHighlighter::new();
                tremor_script::Script::highlight_script_with_range(script, extent, &mut hh)?;
                let prefix = if *status { "(+)" } else { "(-)" };

                let case_tags = suite_tags;

                let (matched, is_match) = case_tags.matches(&by_tag.0, &by_tag.1);
                if is_match {
                    status::h1("    Test", "")?;
                    status::tagsx("        ", &case_tags, Some(&matched), Some(&by_tag.1))?;
                } else {
                    status::h1("    Test ( Skipping )", "")?;
                    status::tagsx("        ", &case_tags, Some(&matched), Some(&by_tag.1))?;
                    continue;
                }

                // Test record
                elements.push(report::TestElement {
                    description: format!("    {} Executing test {} of {}", prefix, i + 1, al),
                    keyword: report::KeywordKind::Predicate,
                    result: report::ResultKind {
                        status: if *status {
                            stats.pass();
                            report::StatusKind::Passed
                        } else {
                            stats.fail();
                            report::StatusKind::Failed
                        },
                        duration: 0, // Compile time evaluation
                    },
                    info: Some(hh.to_string()),
                    hidden: false,
                });
                drop(hh);

                stats.assert();

                // Interactive console report
                status::executing_unit_testcase(i, ll, *status)?;
                let mut h = TermHighlighter::default();
                tremor_script::Script::highlight_script_with_range_indent(
                    "      ", script, extent, &mut h,
                )?;
                h.finalize()?;
                drop(h);
            }
        }
    }

    Ok((stats, elements))
}

#[allow(clippy::too_many_lines)]
pub(crate) fn run_suite(
    path: &Path,
    scenario_tags: &tag::TagFilter,
    by_tag: (&[String], &[String]),
) -> Result<report::TestReport> {
    println!();

    let mut suites: HashMap<String, TestSuite> = HashMap::new();
    let script = path.to_string_lossy().to_string();

    let mut raw = String::new();
    let mut input = crate::open_file(&path, None)?;
    input.read_to_string(&mut raw)?;

    let module_path = load_module_path();
    let reg: Registry = registry::registry();

    let report_start = nanotime();
    match tremor_script::Script::parse(&module_path, &script, raw.clone(), &reg) {
        Ok(runnable) => {
            let local = LocalStack::default();

            let mut h = TermHighlighter::default();
            runnable.format_warnings_with(&mut h)?;

            let script = runnable.script.suffix();

            let context = &EventContext::new(nanotime(), Some(EventOriginUri::default()));
            let env = Env {
                context,
                consts: &script.consts,
                aggrs: &script.aggregates,
                meta: &script.node_meta,
                recursion_limit: tremor_script::recursion_limit(),
            };

            let mut stat_s = stats::Stats::new();
            for expr in &script.exprs {
                let state = Value::Object(Box::new(hashmap! {}));
                let event = Value::Object(Box::new(hashmap! {}));
                let meta = Value::Object(Box::new(hashmap! {}));
                let mut elements = Vec::new();
                let mut suite_name = "".to_string();

                if let Expr::Imut(ImutExprInt::Invoke1(Invoke {
                    module, fun, args, ..
                })) = expr
                {
                    let m = module.join("").to_string();
                    if m == "test" && fun == "suite" {
                        // A Test suite
                        let mut specs: Vec<Value> = vec![];
                        for arg in args {
                            let value = arg.run(EXEC_OPTS, &env, &event, &state, &meta, &local)?;
                            specs.push(value.into_owned());
                        }
                        if let ImutExpr(ImutExprInt::Record(Record { fields, .. })) = &args[0] {
                            let suite_spec_index = fields.iter().position(|f| {
                                if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                                    value == "suite"
                                } else {
                                    false
                                }
                            });
                            if let Some(suite_spec_index) = suite_spec_index {
                                let item = &fields[suite_spec_index].value;
                                let tag_spec_index = fields.iter().position(|f| {
                                    if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                                        value == "tags"
                                    } else {
                                        false
                                    }
                                });
                                let name_spec_index = fields.iter().position(|f| {
                                    if let ImutExprInt::Literal(Literal { value, .. }) = &f.name {
                                        suite_name.push_str(&value.to_string());
                                        value == "name"
                                    } else {
                                        suite_name = "Unknown Suite".to_string();
                                        false
                                    }
                                });

                                if let Some(name_spec_index) = name_spec_index {
                                    if let Some(_tag_spec_index) = tag_spec_index {
                                        if let ImutExprInt::Literal(Literal { value, .. }) =
                                            &fields[name_spec_index].value
                                        {
                                            suite_name = value.to_string()
                                        };
                                    }
                                }

                                let mut found_tags = Vec::new();
                                match tag_spec_index {
                                    None => (),
                                    Some(tag_spec_index) => {
                                        let tags = &fields[tag_spec_index].value;
                                        let tag_state = Value::Object(Box::new(hashmap! {}));
                                        let tag_value = tags.run(
                                            EXEC_OPTS, &env, &event, &tag_state, &meta, &local,
                                        )?;
                                        if let Value::Array(tags) = tag_value.as_ref() {
                                            let mut inner_tags = tags
                                                .iter()
                                                .map(|x| (*x).to_string())
                                                .collect::<Vec<String>>();
                                            found_tags.append(&mut inner_tags);
                                        }
                                    }
                                };

                                let suite_tags = scenario_tags.join(Some(found_tags));
                                if let ImutExprInt::Record(r) = item {
                                    // TODO revisit tags in unit tests
                                    if let (_matched, true) =
                                        suite_tags.matches(&by_tag.0, &by_tag.1)
                                    {
                                        status::h1("  Suite", &suite_name.to_string())?;
                                        status::tagsx(
                                            "      ",
                                            &suite_tags,
                                            Some(&by_tag.0),
                                            Some(&by_tag.1),
                                        )?;

                                        let (test_stats, mut test_reports) = eval_suite_entrypoint(
                                            &env,
                                            &local,
                                            &runnable.source,
                                            &script.node_meta,
                                            r,
                                            &specs,
                                            &suite_tags,
                                            by_tag,
                                        )?;
                                        stat_s.merge(&test_stats);
                                        elements.append(&mut test_reports);
                                    }
                                } else {
                                    stat_s.skip();

                                    status::h1("  Suite", &suite_name.to_string())?;
                                    status::tagsx(
                                        "      ",
                                        &suite_tags,
                                        Some(&by_tag.0),
                                        Some(&by_tag.1),
                                    )?;
                                }
                            }

                            suites.insert(
                                suite_name.clone(),
                                TestSuite {
                                    name: suite_name.to_string(),
                                    description: suite_name,
                                    elements,
                                    evidence: None,
                                    stats: stat_s.clone(),
                                    duration: 0,
                                },
                            );
                        };
                    }
                };
            }
        }
        Err(e) => {
            let mut h = TermHighlighter::default();
            if let Err(e) = tremor_script::Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
        }
    }

    let mut stat_x = stats::Stats::new();
    for v in suites.values() {
        stat_x.merge(&v.stats)
    }

    Ok(report::TestReport {
        description: "unit test suites".into(),
        elements: suites,
        stats: stat_x,
        duration: nanotime() - report_start,
    })
}
