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

#[derive(Serialize, Debug, Clone)]
pub(crate) struct Stats {
    pub(crate) pass: i32,
    pub(crate) fail: i32,
    pub(crate) skip: i32,
}

impl Stats {
    pub(crate) fn new() -> Self {
        Stats {
            pass: 0,
            fail: 0,
            skip: 0,
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

    pub(crate) fn merge(&mut self, other: &Stats) {
        self.pass += other.pass;
        self.fail += other.fail;
        self.skip += other.skip;
    }
}
