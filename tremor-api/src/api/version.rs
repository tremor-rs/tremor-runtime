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

use crate::api::prelude::*;
use tremor_runtime::version::{DEBUG, VERSION};

#[derive(Serialize, Deserialize)]
pub struct Version {
    version: &'static str,
    debug: bool,
}

impl Default for Version {
    fn default() -> Self {
        Self {
            version: VERSION,
            debug: DEBUG,
        }
    }
}

// ALLOW: We allow this since it's required for generalizing accept fuinctions
#[allow(clippy::unused_async)]
pub(crate) async fn get(req: Request) -> Result<Response> {
    reply(&req, Version::default(), StatusCode::Ok)
}
