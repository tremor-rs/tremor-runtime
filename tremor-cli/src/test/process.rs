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
use super::stats;
use super::tag;
use super::target_process;
use crate::report;
use crate::util::slurp_string;
use crate::{
    errors::{Error, Result},
    report::TestReport,
};
use async_std::prelude::*;
use async_std::sync::Arc;
use globwalk::GlobWalkerBuilder;
use signal_hook::consts::signal::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::Signals;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tremor_common::{file::canonicalize, time::nanotime};

fn test_env(tests_root_dir: &Path, test_dir: &Path) -> Result<HashMap<String, String>> {
    let mut env = HashMap::with_capacity(1);
    env.insert(
        String::from("RUST_LOG"),
        std::env::var("RUST_LOG").unwrap_or_else(|_| String::from("info")),
    );
    let tremor_path = std::env::var("TREMOR_PATH").unwrap_or_default();
    let test_lib = format!(
        "{}/lib:{}/lib",
        tests_root_dir.to_string_lossy(),
        test_dir.to_string_lossy()
    );
    let tremor_path = if tremor_path.is_empty() {
        test_lib
    } else {
        format!("{}:{}", tremor_path, test_lib)
    };
    if let Ok(env_content) = slurp_string("env.yaml") {
        // ALLOW: We don't use the string anymore after this
        let env_value = serde_yaml::from_str::<HashMap<String, String>>(env_content.as_str())?;
        for (k, v) in env_value {
            env.insert(k, v);
        }
    }
    env.insert(String::from("TREMOR_PATH"), tremor_path);
    Ok(env)
}

async fn handle_signals(
    signals: Signals,
    test_dir: PathBuf,
    env: HashMap<String, String>,
    run_after: Arc<AtomicBool>,
) -> Result<()> {
    let mut signals = signals.fuse();

    while let Some(signal) = signals.next().await {
        if run_after.load(Ordering::Acquire) {
            let mut after = after::AfterController::new(&test_dir, &env);
            after.spawn().await?;
        }
        signal_hook::low_level::emulate_default_handler(signal)?;
    }
    Ok(())
}

/// run the process
///
/// `tests_root_dir`: the base path in which we discovered this test.
/// `test_dir`: directory of the current test
#[allow(clippy::too_many_lines)]
pub(crate) async fn run_process(
    kind: &str,
    tests_root_dir: &Path,
    test_dir: &Path,
    _by_tag: &tag::TagFilter,
) -> Result<TestReport> {
    let mut evidence = HashMap::new();

    let mut artefacts = GlobWalkerBuilder::from_patterns(canonicalize(test_dir)?, &["*.troy"])
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
    let args: Vec<String> = vec!["server", "run", "-n", "--debug-connectors"]
        .iter()
        .map(|x| (*x).to_string())
        .chain(artefacts)
        .collect();

    let process_start = nanotime();

    // enable info level logging
    let env = test_env(tests_root_dir, test_dir)?;

    let mut before = before::BeforeController::new(test_dir, &env);
    let mut after = after::AfterController::new(test_dir, &env);
    if let Err(e) = before.spawn().await {
        after.spawn().await?;
        return Err(e);
    }

    // register signal handler - to ensure we run `after` even if we do a Ctrl-C
    let run_after = Arc::new(AtomicBool::new(true));

    let signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
    let signal_handle = signals.handle();
    let signal_handler_task = async_std::task::spawn(handle_signals(
        signals,
        test_dir.to_path_buf(),
        env.clone(),
        run_after.clone(),
    ));

    let binary = target_process::which("tremor")?;
    let mut process = target_process::TargetProcess::new_in_dir(binary, &args, &env, test_dir)?;
    info!("Starting {} ...", &process);
    let fg_out_file = test_dir.join("fg.out.log");
    let fg_err_file = test_dir.join("fg.err.log");
    let stdin_file = test_dir.join("stdin");
    if stdin_file.is_file() {
        info!("Writing `stdin` contents to Tremor's stdin.");
        let stdin_f = tremor_common::asy::file::open(&stdin_file).await?;
        process.write_to_stdin(stdin_f).await?;
    }
    let fg = process.tail(&fg_out_file, &fg_err_file).await?;
    info!("Process exited with {}", fg);

    before::update_evidence(test_dir, &mut evidence)?;

    // As our primary process is finished, check for after hooks
    after.spawn().await?;
    after::update_evidence(test_dir, &mut evidence)?;
    run_after.store(false, Ordering::Release);

    // Assertions
    let assert_path = test_dir.join("assert.yaml");
    let report = if (&assert_path).is_file() {
        let assert_yaml = assert::load_assert(&assert_path)?;
        Some(assert::process(
            &fg_out_file,
            &fg_err_file,
            fg.code(),
            &assert_yaml,
        )?)
    } else {
        None
    };
    evidence.insert("test: stdout".to_string(), slurp_string(&fg_out_file)?);
    evidence.insert("test: stderr".to_string(), slurp_string(&fg_err_file)?);

    signal_handle.close();
    signal_handler_task.cancel().await;

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
        if fg.success() {
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
