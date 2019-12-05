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

extern crate lalrpop;

#[cfg_attr(tarpaulin, skip)]
fn main() {
    lalrpop::process_root().expect("Unable to initialize LALRPOP");

    println!("cargo:rustc-cfg=can_join_spans");
    println!("cargo:rustc-cfg=can_show_location_of_runtime_parse_error");
}
