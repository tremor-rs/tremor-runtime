use crate::report::StatusKind;

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

/// Test statistics
#[derive(Serialize, Debug, Clone)]
pub struct Stats {
    pub(crate) pass: u32,
    pub(crate) fail: u32,
    pub(crate) skip: u32,
    pub(crate) assert: u32,
}

impl Stats {
    pub(crate) fn report(&mut self, status: bool) -> StatusKind {
        if status {
            self.pass();
            StatusKind::Passed
        } else {
            self.fail();
            StatusKind::Failed
        }
    }
    pub(crate) fn new() -> Self {
        Stats {
            pass: 0,
            fail: 0,
            skip: 0,
            assert: 0,
        }
    }

    pub(crate) fn pass(&mut self) {
        self.pass += 1;
    }

    pub(crate) fn fail(&mut self) {
        self.fail += 1;
    }

    pub(crate) fn skip(&mut self) {
        self.skip += 1;
    }

    pub(crate) fn assert(&mut self) {
        self.assert += 1;
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.pass == 0 && self.fail == 0 && self.skip == 0 && self.assert == 0
    }

    pub(crate) fn is_pass(&self) -> bool {
        self.fail == 0
    }

    pub(crate) fn merge(&mut self, other: &Stats) {
        self.pass += other.pass;
        self.fail += other.fail;
        self.skip += other.skip;
        self.assert += other.assert;
    }
}
