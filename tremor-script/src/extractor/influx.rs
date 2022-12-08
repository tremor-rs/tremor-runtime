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

//! Influx extrector matches data from the string that uses the [Influx Line Protocol](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/). It will fail if the input isn't a valid string.
//!
//! ## Predicate
//!
//! When used as a predicate with `~`, the predicate will pass if the target conforms to the influx line protocol.
//!
//! ## Extraction
//!
//! The extractor will return a record with the measurement, fields, tags and the timestamp extracted from the input.
//!
//! Example:
//!
//! ```tremor
//! match { "meta" :  "wea\\ ther,location=us-midwest temperature=82 1465839830100400200" } of
//!   case rp = %{ meta ~= influx||} => rp
//!   default => "no match"
//! end;
//! ```
//!
//! This will return:
//!
//! ```bash
//! "meta": {
//!           "measurement": "wea ther",
//!            "tags": {
//!                "location": "us-midwest"
//!              },
//!              "fields": {
//!               "temperature": 82.0
//!             },
//!             "timestamp": 1465839830100400200
//!         }
//! ```

use super::Result;
use tremor_value::Value;

pub(crate) fn execute(s: &str, result_needed: bool, ingest_ns: u64) -> Result<'static> {
    tremor_influx::decode::<Value>(s, ingest_ns)
        .ok()
        .flatten()
        .map_or(Result::NoMatch, |r| {
            if result_needed {
                Result::Match(r.into_static())
            } else {
                Result::MatchNull
            }
        })
}
