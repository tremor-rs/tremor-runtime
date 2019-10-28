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

use crate::registry::Registry;
use crate::tremor_fn;
use halfbrown::hashmap;

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (origin::as_uri_string(context) {
            if let Some(ref uri) = context.origin_uri {
                Ok(Value::String(uri.to_string().into()))
            } else {
                Ok(Value::Null)
            }
        }))
        .insert(tremor_fn! (origin::as_uri_record(context) {
            if let Some(ref uri) = context.origin_uri {
                Ok(Value::Object(
                    hashmap! {
                        "scheme".into() => Value::from(uri.scheme().to_string()),
                        "host".into() => Value::from(uri.host().to_string()),
                        "port".into() => match uri.port() {
                            Some(n) => Value::from(*n),
                            None => Value::Null,
                        },
                        "path".into() => Value::from(uri.path().to_string()),
                    }
                ))
            } else {
                Ok(Value::Null)
            }
        }))
        .insert(tremor_fn! (origin::scheme(context) {
            if let Some(ref uri) = context.origin_uri {
                Ok(Value::String(uri.scheme().to_string().into()))
            } else {
                Ok(Value::Null)
            }
        }))
        .insert(tremor_fn! (origin::host(context) {
            if let Some(ref uri) = context.origin_uri {
                Ok(Value::String(uri.host().to_string().into()))
            } else {
                Ok(Value::Null)
            }
        }))
        .insert(tremor_fn! (origin::port(context) {
            if let Some(ref uri) = context.origin_uri {
                Ok(match uri.port() {
                    Some(n) => Value::I64(*n as i64),
                    // TODO would be nice to support this?
                    //None => Value::from(None),
                    None => Value::Null,
                })
            } else {
                Ok(Value::Null)
            }
        }))
        .insert(tremor_fn! (origin::path(context) {
            if let Some(ref uri) = context.origin_uri {
                Ok(Value::String(uri.path().to_string().into()))
            } else {
                Ok(Value::Null)
            }
        }));
}
