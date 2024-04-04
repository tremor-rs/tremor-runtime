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

use crate::test::stats;
use std::collections::HashMap;

/// A test run is a collection of test reports that
/// have executed in the context of a test run
///
#[derive(Serialize, Debug, Clone)]
pub(crate) struct TestRun {
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) includes: Vec<String>,
    pub(crate) excludes: Vec<String>,
    pub(crate) reports: HashMap<String, Vec<TestReport>>,
    pub(crate) stats: HashMap<String, stats::Stats>,
}

/// A test report is the collection of test suites that
/// have executed in the context of a test run
///
#[derive(Serialize, Debug, Clone)]
pub(crate) struct TestReport {
    pub(crate) description: String,
    pub(crate) elements: HashMap<String, TestSuite>,
    pub(crate) stats: stats::Stats,
    pub(crate) duration: u64,
}

/// A test suite is a logical grouping of test elements
///
#[derive(Serialize, Debug, Clone)]
pub(crate) struct TestSuite {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) elements: Vec<TestElement>,
    /// Evidence - a set of file ( text for now) artefacts generated during
    ///  execution of a test suite
    pub(crate) evidence: Option<HashMap<String, String>>,
    pub(crate) stats: stats::Stats,
    pub(crate) duration: u64,
}

/// A test element is an atomic unit of test assertion. These
/// are asserts in unit and integration tests and benchmarks
///
#[derive(Serialize, Debug, Clone)]
pub(crate) struct TestElement {
    pub(crate) description: String,
    pub(crate) keyword: KeywordKind,
    pub(crate) result: ResultKind,
    pub(crate) info: Option<String>,
    pub(crate) hidden: bool,
}

#[derive(Serialize, Debug, Clone)]
pub(crate) struct ResultKind {
    pub(crate) status: StatusKind,
    pub(crate) duration: u64,
}

#[derive(Serialize, Debug, Clone)]
pub(crate) enum StatusKind {
    Passed,
    Failed,
}

#[derive(Serialize, Debug, Clone)]
pub(crate) enum KeywordKind {
    Test,
    Predicate,
}

#[allow(clippy::manual_string_new)]
/// for `env!` macro
pub(crate) fn metadata() -> HashMap<String, String> {
    use std::env;
    let mut meta: HashMap<String, String> = HashMap::with_capacity(7);

    meta.insert("authors".into(), env!("CARGO_PKG_AUTHORS").into());
    meta.insert("description".into(), env!("CARGO_PKG_DESCRIPTION").into());
    meta.insert("homepage".into(), env!("CARGO_PKG_HOMEPAGE").into());
    meta.insert("name".into(), env!("CARGO_PKG_NAME").into());
    meta.insert("repository".into(), env!("CARGO_PKG_REPOSITORY").into());
    meta.insert("version".into(), env!("CARGO_PKG_VERSION").into());
    meta.insert(
        "allocator".into(),
        crate::alloc::get_allocator_name().into(),
    );

    meta
}
