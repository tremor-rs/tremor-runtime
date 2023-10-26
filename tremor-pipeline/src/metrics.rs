// Copyright 2022, The Tremor Team
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

use beef::Cow;
use tremor_value::{literal, Object, Value};

const COUNT: Cow<'static, str> = Cow::const_str("count");
const MEASUREMENT: Cow<'static, str> = Cow::const_str("measurement");
const TAGS: Cow<'static, str> = Cow::const_str("tags");
const FIELDS: Cow<'static, str> = Cow::const_str("fields");
const TIMESTAMP: Cow<'static, str> = Cow::const_str("timestamp");

/// Generate an influx-compatible metrics value based on a count
#[must_use]
pub fn value_count(
    metric_name: Cow<'static, str>,
    tags: Object<'static>,
    count: u64,
    timestamp: u64,
) -> Value<'static> {
    literal!({
        MEASUREMENT: metric_name,
        TAGS: tags,
        FIELDS: {
            COUNT: count
        },
        TIMESTAMP: timestamp
    })
}

/// Generate an influx-compatible metrics value based on a named value, which will be encoded into a field
#[must_use]
pub fn value_named(
    metric_name: Cow<'static, str>,
    tags: Object<'static>,
    name: &'static str,
    value: u64,
    timestamp: u64,
) -> Value<'static> {
    literal!({
        MEASUREMENT: metric_name,
        TAGS: tags,
        FIELDS: {
            name: value
        },
        TIMESTAMP: timestamp
    })
}

/// Generate an influx-compatible metrics value based on a given set of fields
#[must_use]
pub fn value(
    metric_name: Cow<'static, str>,
    tags: Object<'static>,
    fields: Object<'static>,
    timestamp: u64,
) -> Value<'static> {
    literal!({
        MEASUREMENT: metric_name,
        TAGS: tags,
        FIELDS: fields,
        TIMESTAMP: timestamp
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json::ObjectHasher;
    use value_trait::prelude::*;

    #[test]
    fn value_test() {
        let mut t = Object::with_hasher(ObjectHasher::default());
        t.insert("tag".into(), "tag-value".into());

        let mut f = Object::with_hasher(ObjectHasher::default());
        f.insert("field".into(), "tag-value".into());
        let m = value("name".into(), t, f, 42);

        assert_eq!("name", m.get_str(&MEASUREMENT).expect("no value"));
        assert_eq!(42, m.get_u64(&TIMESTAMP).expect("no value"));
        let t = m.get(&TAGS).expect("no tags");
        assert_eq!("tag-value", t.get_str("tag").expect("no tag"));
        assert_eq!(None, t.get_str("no-tag"));
    }
}
