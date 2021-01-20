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

// #[cfg(feature = "mimalloc")]
// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
#[cfg(feature = "stdalloc")]
#[global_allocator]
static ALLOC: std::alloc::System = std::alloc::System;
#[cfg(not(any(feature = "jemalloc", feature = "snmalloc", feature = "stdalloc")))]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[allow(clippy::same_functions_in_if_condition)]
pub(crate) fn get_allocator_name() -> &'static str {
    if cfg!(feature = "snmalloc") {
        "snmalloc"
    } else if cfg!(feature = "jemalloc") {
        "jemalloc"
    } else if cfg!(feature = "stdalloc") {
        "stdalloc"
    // } else if cfg!(feature = "mimalloc") {
    //     "mimalloc"
    } else {
        "snmalloc" // NOTE The default allocator SHOULD be set in the Cargo.toml default features
    }
}
