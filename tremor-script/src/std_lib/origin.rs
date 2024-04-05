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

use crate::prelude::*;
use crate::tremor_fn;

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (origin|as_uri_string(context) {
            Ok(context.origin_uri().map(ToString::to_string).map(Value::from).unwrap_or_default())
        }))
        .insert(tremor_fn! (origin|as_uri_record(context) {
            if let Some(uri) = context.origin_uri() {
                let port = match uri.port() {
                    Some(n) => Value::from(n),
                    // TODO would be nice to support this?
                    //None => Value::from(None),
                    None => Value::null(),
                };
                Ok(literal!({
                        "scheme": uri.scheme().to_string(),
                        "host": uri.host().to_string(),
                        "port": port,
                        "path": uri.path().to_vec()
                    }))
            } else {
                Ok(Value::null())
            }
        }))
        .insert(tremor_fn! (origin|scheme(context) {
            Ok(context.origin_uri().map(|uri| uri.scheme().to_string()).map(Value::from).unwrap_or_default())
        }))
        .insert(tremor_fn! (origin|host(context) {
            Ok(context.origin_uri().map(|uri| uri.host().to_string()).map(Value::from).unwrap_or_default())
        }))
        .insert(tremor_fn! (origin|port(context) {
            Ok(context.origin_uri().and_then(EventOriginUri::port).map(Value::from).unwrap_or_default())
        }))
        .insert(tremor_fn! (origin|path(context) {
            Ok(context.origin_uri().map_or_else(
                Value::null,
                |uri| {
                    // TODO avoid uri path clone here?
                    Value::from(uri.path().to_vec())
                }
            ))
        }));
}
