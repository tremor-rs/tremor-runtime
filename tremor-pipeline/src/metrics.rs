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
use halfbrown::HashMap;
use tremor_value::{literal, Value};

use abi_stable::{rstr, std_types::RCowStr};
use tremor_common::pdk::{beef_to_rcow_str, RHashMap};

const COUNT: RCowStr<'static> = RCowStr::Borrowed(rstr!("count"));
const MEASUREMENT: RCowStr<'static> = RCowStr::Borrowed(rstr!("measurement"));
const TAGS: RCowStr<'static> = RCowStr::Borrowed(rstr!("tags"));
const FIELDS: RCowStr<'static> = RCowStr::Borrowed(rstr!("fields"));
const TIMESTAMP: RCowStr<'static> = RCowStr::Borrowed(rstr!("timestamp"));

/// Generate an influx-compatible metrics value based on a count
#[must_use]
pub fn value_count(
    metric_name: Cow<'static, str>,
    tags: HashMap<Cow<'static, str>, Value<'static>>,
    count: u64,
    timestamp: u64,
) -> Value<'static> {
    // TODO: prettier conversions
    literal!({
        MEASUREMENT: beef_to_rcow_str(metric_name),
        TAGS: tags.into_iter().map(|(k, v)| (beef_to_rcow_str(k), v)).collect::<RHashMap<_, _>>(),
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
    tags: HashMap<Cow<'static, str>, Value<'static>>,
    name: &'static str,
    value: u64,
    timestamp: u64,
) -> Value<'static> {
    // TODO: prettier conversions
    literal!({
        MEASUREMENT: beef_to_rcow_str(metric_name),
        TAGS: tags.into_iter().map(|(k, v)| (beef_to_rcow_str(k), v)).collect::<RHashMap<_, _>>(),
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
    tags: HashMap<Cow<'static, str>, Value<'static>>,
    fields: HashMap<Cow<'static, str>, Value<'static>>,
    timestamp: u64,
) -> Value<'static> {
    // TODO: prettier conversions
    literal!({
        MEASUREMENT: beef_to_rcow_str(metric_name),
        TAGS: tags.into_iter().map(|(k, v)| (beef_to_rcow_str(k), v)).collect::<RHashMap<_, _>>(),
        FIELDS: fields.into_iter().map(|(k, v)| (beef_to_rcow_str(k), v)).collect::<RHashMap<_, _>>(),
        TIMESTAMP: timestamp
    })
}

#[cfg(test)]
mod test {
    use simd_json::ValueAccess;

    use super::*;
    #[test]
    fn value_test() {
        let mut t = HashMap::new();
        t.insert("tag".into(), "tag-value".into());

        let mut f = HashMap::new();
        f.insert("field".into(), "tag-value".into());
        let m = value("name".into(), t, f, 42);

        assert_eq!("name", m.get_str(&MEASUREMENT).expect("no value"));
        assert_eq!(42, m.get_u64(&TIMESTAMP).expect("no value"));
        let t = m.get(&TAGS).expect("no tags");
        assert_eq!("tag-value", t.get_str("tag").expect("no tag"));
        assert_eq!(None, t.get_str("no-tag"));
    }
}
