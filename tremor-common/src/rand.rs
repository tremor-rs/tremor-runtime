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

use rand::{rngs::SmallRng, SeedableRng};

/// Create a new random number generator - seed with `ingest_ns` from event context ( where appropriate )
#[must_use]
pub fn make_prng(ingest_ns_seed: u64) -> SmallRng {
    SmallRng::seed_from_u64(ingest_ns_seed)
}
