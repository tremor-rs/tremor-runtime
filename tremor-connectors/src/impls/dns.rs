// Copyright 2021, The Tremor Team
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

#![allow(clippy::doc_markdown)]
//! The dns connector integrates DNS client access into tremor allowing programmatic DNS queries
//! against the system-provided domain name system  resolver.
//!
//! :::note
//! No codecs, configuration, or processors are supported.
//! :::
//!
//!
//! ## Configuration
//!
//! ```tremor
//! define connector my_dns from dns;
//! ```
//!
//! Commands sent to a DNS connector instances `in` port will yield responses via the connector
//! instances corresponding `out` port.
//!
//! The response format is a set of record entries corresponding to the key provided as a value to
//! the `lookup` command
//!
//! ```tremor
//! [
//!   {"A": "1.2.3.4", "ttl": 60},
//!   {"CNAME": "www.tremor.rs", "ttl": 120}
//! ]
//! ```
//!
//! ## How do I lookup a domain?
//!
//! ```tremor
//! {
//!   "lookup": "tremor.rs"
//! }
//! ```
//!
//! ## How do i lookup a `CNAME`?
//!
//! ```tremor
//! {
//!   "lookup": {
//!     "name": "tremor.rs",
//!     "type": "CNAME"
//!   }
//! }
//! ```
//!
//! ## What record types are supported:
//!
//! * `A`
//! * `AAAA`
//! * `ANAME`
//! * `CNAME`
//! * `TXT`
//! * `PTR`
//! * `CAA`
//! * `HINFO`
//! * `HTTPS`
//! * `MX`
//! * `NAPTR`
//! * `NULL`
//! * `NS`
//! * `OPENPGPKEY`
//! * `SOA`
//! * `SRV`
//! * `SSHFP`
//! * `SVCB`
//! * `TLSA`
//!
//! :::note
//! If type is not specified `A` records will be looked up by default
//! :::

pub(crate) mod client;
