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

use crate::registry::Registry;
use crate::tremor_fn;
use halfbrown::hashmap;
use simd_json::prelude::*;

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (origin::as_uri_string(context) {
            if let Some(uri) = context.origin_uri() {
                Ok(Value::String(uri.to_string().into()))
            } else {
                Ok(Value::null())
            }
        }))
        .insert(tremor_fn! (origin::as_uri_record(context) {
            if let Some(uri) = context.origin_uri() {
                Ok(Value::from(
                    hashmap! {
                        "scheme".into() => Value::from(uri.scheme().to_string()),
                        "host".into() => Value::from(uri.host().to_string()),
                        "port".into() => match uri.port() {
                            Some(n) => Value::from(*n),
                            // TODO would be nice to support this?
                            //None => Value::from(None),
                            None => Value::null(),
                        },
                        // TODO avoid uri path clone here?
                        "path".into() => Value::from(uri.path().clone()),
                    }
                ))
            } else {
                Ok(Value::null())
            }
        }))
        .insert(tremor_fn! (origin::scheme(context) {
            if let Some(uri) = context.origin_uri() {
                Ok(Value::String(uri.scheme().to_string().into()))
            } else {
                Ok(Value::null())
            }
        }))
        .insert(tremor_fn! (origin::host(context) {
            if let Some(uri) = context.origin_uri() {
                Ok(Value::String(uri.host().to_string().into()))
            } else {
                Ok(Value::null())
            }
        }))
        .insert(tremor_fn! (origin::port(context) {
            if let Some(uri) = context.origin_uri() {
                Ok(match uri.port() {
                    Some(n) => Value::from(*n),
                    None => Value::null(),
                })
            } else {
                Ok(Value::null())
            }
        }))
        .insert(tremor_fn! (origin::path(context) {
            if let Some(uri) = context.origin_uri() {
                // TODO make this work
                //Ok(Value::Array(uri.path().clone().into()))
                // TODO avoid uri path clone here?
                Ok(Value::from(uri.path().clone()))
            } else {
                Ok(Value::null())
            }
        }));
}
