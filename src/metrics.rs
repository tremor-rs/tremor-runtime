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

//use bytes::Bytes;
//use prometheus::{self, Encoder, TextEncoder};

//use actix_web::HttpRequest;

pub static mut INSTANCE: &str = "tremor";
/*
pub fn get(_req: &HttpRequest) -> Bytes {
    let encoder = TextEncoder::new();
    let metric_familys = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_familys, &mut buffer).unwrap();
    Bytes::from(buffer)
}

*/
