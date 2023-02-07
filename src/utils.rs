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
use crate::errors::{Error, Kind as ErrorKind};

/// Fetches a hostname with `tremor-host.local` being the default
#[must_use]
pub fn hostname() -> String {
    hostname::get()
        .map_err(Error::from)
        .and_then(|hostname| {
            hostname.into_string().map_err(|os_string| {
                ErrorKind::Msg(format!("Invalid hostname: {}", os_string.to_string_lossy())).into()
            })
        })
        .unwrap_or_else(|_| "tremor_host.local".to_string())
}

#[must_use]
pub(crate) fn task_id() -> String {
    // tokio::task::try_id().map_or_else(|| String::from("<no-task>"), |i| i.to_string())
    String::from("<no-task>")
}
