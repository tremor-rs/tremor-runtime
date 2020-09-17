// Copyright 2020, The Tremor Team
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
use std::time::Duration;

/// Fetches a hostname with `tremor-host.local` being the default
#[must_use]
pub fn hostname() -> String {
    hostname::get()
        .map_err(|_| ())
        .and_then(|s| s.into_string().map_err(|_| ()))
        .unwrap_or_else(|_| "tremor-host.local".to_string())
}

pub(crate) fn duration_to_millis(at: Duration) -> u64 {
    (at.as_secs() as u64 * 1_000) + (u64::from(at.subsec_nanos()) / 1_000_000)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::*;

    #[test]
    fn test_duration_to_millis() -> Result<()> {
        let d = duration_to_millis(Duration::from_secs(1));
        assert_eq!(d, 1000u64);

        let d = duration_to_millis(Duration::from_millis(1));
        assert_eq!(d, 1u64);

        Ok(())
    }
}
