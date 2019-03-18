// Copyright 2018-2019, Wayfair GmbH
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

#![forbid(warnings)]
#![warn(unused_extern_crates)]
#![recursion_limit = "1024"]
#![cfg_attr(feature = "cargo-clippy", deny(clippy::all))]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[cfg(feature = "kafka")]
extern crate rdkafka_sys;
//#[cfg(feature = "mssql")]
//extern crate tiberius;
//#[cfg(feature = "kafka")]
//extern crate tokio_threadpool;
#[cfg(test)]
#[macro_use]
extern crate matches;
//extern crate reqwest;

#[macro_use]
pub mod macros;
pub mod async_sink;
pub mod config;
pub mod dflt;
pub mod dynamic;
pub mod errors;
#[macro_use]
pub mod functions;
pub mod lifecycle;
pub mod mapping;
pub mod metrics;
pub mod registry;
pub mod repository;
pub mod rest;
pub mod system;
pub mod url;
pub mod utils;
pub mod version;
