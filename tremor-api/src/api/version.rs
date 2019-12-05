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

use crate::api::{reply, State};
use actix_web::{web::Data, HttpRequest, Responder};
use tremor_runtime::version::VERSION;

#[derive(Serialize, Deserialize)]
pub struct Version {
    version: String,
}
impl Version {
    pub fn default() -> Self {
        Self {
            version: VERSION.to_string(),
        }
    }
}

pub fn get((req, data): (HttpRequest, Data<State>)) -> impl Responder {
    reply(&req, &data, Ok(Version::default()), false, 200)
}
