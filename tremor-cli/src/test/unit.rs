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

use crate::errors::Result;
use halfbrown::hashmap;
use report::TestSuite;
use simd_json::{
    borrowed::{Value, Value::Array},
    StaticNode,
};
use std::io::Read;
use std::{collections::HashMap, path::Path};
use tremor_script::ast::base_expr::BaseExpr;
use tremor_script::ast::{Expr, ImutExpr, ImutExprInt, Invoke, List, Literal, NodeMetas, Record};
use tremor_script::ctx::{EventContext, EventOriginUri};
use tremor_script::highlighter::{Dumb as DumbHighlighter, Highlighter, Term as TermHighlighter};
use tremor_script::interpreter::{AggrType, Env, ExecOpts, LocalStack};
use tremor_script::path::load as load_module_path;
use tremor_script::{registry, Registry};

use crate::report;
use crate::test;
use crate::test::stats;
use crate::test::status;
use crate::util::nanotime;

const EXEC_OPTS: ExecOpts = ExecOpts {
    result_needed: true,
    aggr: AggrType::Tick,
};

fn eval_suite_entrypoint(
    env: &Env,
    local: &LocalStack,
    script: &str,
    meta: &NodeMetas,
    suite_spec: &Record<'_>,
    suite_result: &[Value<'_>],
    by_tag: &test::TagFilter,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();
    // TODO FIXME handle zero-args case
    if let Value::Object(o) = &suite_result[0] {
        let suite_name = o.get("name");
        if let Some(suite_name) = suite_name {
            status::h1("Suite", &suite_name.to_string())?;
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
                                            &env, &local, script, meta, l, tests, by_tag,
                                        )?;
                                        elements.append(&mut e);
                                        stats.merge(&s);
                                    }
                                }
                            }
                        }
                    }
                }
            } // FIXME error/warning handling when no tests found
        }
    };

    Ok((stats, elements))
}

#[allow(clippy::too_many_lines)]
fn eval_suite_tests(
    env: &Env,
    local: &LocalStack,
    script: &str,
    node_metas: &NodeMetas,
    suite_spec: &List,
    suite_result: &Value,
    _by_tag: &test::TagFilter,
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

                        if let Some(test_spec_index) = test_spec_index {
                            let item = &fields[test_spec_index].value;
                            let test_state = Value::Object(Box::new(hashmap! {}));
                            let event = Value::Object(Box::new(hashmap! {}));
                            let meta = Value::Object(Box::new(hashmap! {}));

                            // FIXME revisit tag filtering inside unit tests

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

                                // Interactive console report
                                status::executing_unit_testcase(i, ll, status)?;
                                let mut h = TermHighlighter::new();
                                tremor_script::Script::highlight_script_with_range(
                                    script, extent, &mut h,
                                )?;
                                h.finalize()?;
                                drop(h);
                            }
                            continue;
                        }
                    };
                }
            }

            if let Value::Static(StaticNode::Bool(status)) = &suite_result[i] {
                // Non colorized test source extent for json report capture
                let extent = suite_spec.exprs[i].extent(node_metas);
                let mut hh = DumbHighlighter::new();
                tremor_script::Script::highlight_script_with_range(script, extent, &mut hh)?;
                let prefix = if *status { "(+)" } else { "(-)" };

                // Test record
                elements.push(report::TestElement {
                    description: format!("{} Executing test {} of {}", prefix, i + 1, al),
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

                // Interactive console report
                status::executing_unit_testcase(i, ll, *status)?;
                let mut h = TermHighlighter::new();
                tremor_script::Script::highlight_script_with_range(script, extent, &mut h)?;
                h.finalize()?;
                drop(h);
            }
        }
    }

    Ok((stats, elements))
}

#[allow(clippy::too_many_lines)]
pub(crate) fn run_suite(path: &Path, by_tag: &test::TagFilter) -> Result<report::TestReport> {
    println!();
    println!();
    println!();

    let mut suites: HashMap<String, TestSuite> = HashMap::new();

    let script = path.to_string_lossy().to_string();

    let mut raw = String::new();
    let mut input = crate::open_file(&script, None)?;
    input.read_to_string(&mut raw)?;

    let module_path = load_module_path();
    let reg: Registry = registry::registry();

    let report_start = nanotime();
    match tremor_script::Script::parse(&module_path, &script, raw.clone(), &reg) {
        Ok(runnable) => {
            let local = LocalStack::default();

            let mut h = TermHighlighter::new();
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

            //                let suite_start = nanotime();
            for expr in &script.exprs {
                let mut suite_name = String::from("Unknown Suite");
                let state = Value::Object(Box::new(hashmap! {}));
                let event = Value::Object(Box::new(hashmap! {}));
                let meta = Value::Object(Box::new(hashmap! {}));
                let mut elements = Vec::new();

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
                            let mut stat_s = stats::Stats::new();

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
                                        value == "name"
                                    } else {
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
                                match tag_spec_index {
                                    None => {
                                        if let ImutExprInt::Record(r) = item {
                                            let (test_stats, mut test_reports) =
                                                eval_suite_entrypoint(
                                                    &env,
                                                    &local,
                                                    &runnable.source,
                                                    &script.node_meta,
                                                    r,
                                                    &specs,
                                                    by_tag,
                                                )?;
                                            elements.append(&mut test_reports);
                                            stat_s.merge(&test_stats);
                                        }
                                    }
                                    Some(tag_spec_index) => {
                                        let tags = &fields[tag_spec_index].value;
                                        if let ImutExprInt::Record(r) = item {
                                            if let ImutExprInt::Literal(Literal {
                                                value: Array(arr),
                                                ..
                                            }) = tags
                                            {
                                                let arr = arr
                                                    .iter()
                                                    .map(std::string::ToString::to_string)
                                                    .collect::<Vec<String>>();
                                                // FIXME revisit tags in unit tests
                                                if let (_matched, true) = by_tag.matches(&arr) {
                                                    let (test_stats, mut test_reports) =
                                                        eval_suite_entrypoint(
                                                            &env,
                                                            &local,
                                                            &runnable.source,
                                                            &script.node_meta,
                                                            r,
                                                            &specs,
                                                            by_tag,
                                                        )?;
                                                    stat_s.merge(&test_stats);
                                                    elements.append(&mut test_reports);
                                                }
                                            }
                                        }
                                    }
                                }
                                // }
                            }
                            suites.insert(
                                suite_name.clone(),
                                TestSuite {
                                    name: suite_name,
                                    description: "A suite".into(),
                                    elements: vec![],
                                    evidence: None,
                                    stats: stat_s,
                                    duration: 0,
                                },
                            );
                        };
                    }
                };
            }
        }
        Err(e) => {
            let mut h = TermHighlighter::new();
            if let Err(e) = tremor_script::Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
        }
    }

    let mut stat_s = stats::Stats::new();
    for v in suites.values() {
        stat_s.merge(&v.stats)
    }

    Ok(report::TestReport {
        description: "unit test suites".into(),
        elements: suites,
        stats: stat_s,
        duration: nanotime() - report_start,
    })
}
