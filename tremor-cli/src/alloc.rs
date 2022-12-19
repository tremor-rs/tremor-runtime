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

#[cfg(feature = "stdalloc")]
#[global_allocator]
static ALLOC: std::alloc::System = std::alloc::System;
#[cfg(not(feature = "stdalloc"))]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[allow(clippy::same_functions_in_if_condition)]
pub(crate) fn get_allocator_name() -> &'static str {
    if cfg!(feature = "stdalloc") {
        "stdalloc"
    } else {
        // NOTE The default allocator SHOULD be set in the Cargo.toml default features
        "snmalloc"
    }
}
