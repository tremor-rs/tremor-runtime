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

// DogStatsd Protocol v1.2 - https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/
//
// Examples
//
// Metric
// <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
//
// Event
// _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
//
// Service Check
// _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>

//! The `dogstatsd` codec supports the [Datadog `DogStatsD` v1.2 protocol](https://docs.datadoghq.com/developers/dogstatsd/datagram_shell).
//!
//! The format is similar to the `statsd` format, but also includes events and service checks.
//!
//! The codec translates a single `dogstatsd` measurement line into a structured event and vice versa.
//!
//! ## Examples
//!
//! The native format of a single dogstatsd event line is as follows:
//!
//! ### Metric
//!
//! ```text
//! datadog.metric:7|c|@0.1|#example_tag:example_value
//! ```
//!
//! The equivalent representation as a tremor value:
//!
//! ```json
//! {
//!   "metric": {
//!     "type": "c",
//!     "metric": "datadog.metric",
//!     "values": [7],
//!     "sample_rate": 0.1,
//!     "tags": ["example_tag:example_value"]
//!   }
//! }
//! ```
//!
//! ## Supported types
//!
//! - `c` for `counter`
//! - `ms` for `timing`
//! - `g` for `gauge`
//! - `h` for `histogram`
//! - `s` for `sets`
//! - `d` for `distribution`
//!
//!
//! ### Event
//!
//! ```text
//! _e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17
//! ```
//!
//! The equivalent representation as a tremor value:
//!
//! ```json
//! {
//!   "event": {
//!     "title": "An exception occurred",
//!     "text": "Cannot parse CSV file from 10.0.0.17",
//!   }
//! }
//! ```
//!
//!
//! ### Service Check
//!
//! ```text
//! _sc|Redis connection|2|#env:dev|m:Redis connection timed out after 10s
//! ```
//!
//! The equivalent representation as a tremor value:
//!
//! ```json
//! {
//!   "service_check": {
//!     "name": "Redis connection",
//!     "status": 2,
//!     "tags": ["env:dev"],
//!     "message": "Redis connection timed out after 10s",
//!   }
//! }
//! ```

use super::prelude::*;

#[derive(Clone)]
pub struct DogStatsD {}

impl Codec for DogStatsD {
    fn name(&self) -> &str {
        "dogstatsd"
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        decode(data, ingest_ns).map(Some)
    }

    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
        encode(data)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

fn encode(data: &Value) -> Result<Vec<u8>> {
    if let Some(metric) = data.get("metric") {
        encode_metric(metric)
    } else if let Some(event) = data.get("event") {
        encode_event(event)
    } else if let Some(service_check) = data.get("service_check") {
        encode_service_check(service_check)
    } else {
        Err(ErrorKind::InvalidDogStatsD.into())
    }
}

fn encode_metric(value: &Value) -> Result<Vec<u8>> {
    let mut itoa_buf = itoa::Buffer::new();
    let mut ryu_buf = ryu::Buffer::new();
    let mut r = Vec::with_capacity(512);
    r.extend_from_slice(
        value
            .get_str("metric")
            .ok_or(ErrorKind::InvalidDogStatsD)?
            .as_bytes(),
    );
    let t = value.get_str("type").ok_or(ErrorKind::InvalidDogStatsD)?;
    let values = value
        .get_array("values")
        .ok_or(ErrorKind::InvalidDogStatsD)?;
    let mut values = values.iter().filter_map(simd_json::ValueAccess::as_f64);

    r.push(b':');
    if let Some(x) = values.next() {
        if x.fract() == 0.0 {
            #[allow(clippy::cast_possible_truncation)]
            let n = x as i64;
            r.extend_from_slice(itoa_buf.format(n).as_bytes());
        } else {
            r.extend_from_slice(ryu_buf.format(x).as_bytes());
        }
    }
    for x in values {
        r.push(b':');
        if x.fract() == 0.0 {
            #[allow(clippy::cast_possible_truncation)]
            let n = x as i64;
            r.extend_from_slice(itoa_buf.format(n).as_bytes());
        } else {
            r.extend_from_slice(ryu_buf.format(x).as_bytes());
        }
    }
    r.push(b'|');
    r.extend_from_slice(t.as_bytes());

    if let Some(val) = value.get("sample_rate") {
        if val.is_number() {
            r.extend_from_slice(b"|@");
            r.extend_from_slice(val.encode().as_bytes());
        } else {
            return Err(ErrorKind::InvalidDogStatsD.into());
        }
    }

    write_tags(value, &mut r);

    if let Some(container_id) = value.get_str("container_id") {
        r.extend_from_slice(b"|c:");
        r.extend_from_slice(container_id.as_bytes());
    }

    Ok(r)
}

fn encode_event(value: &Value) -> Result<Vec<u8>> {
    let mut buf = itoa::Buffer::new();
    let mut r = Vec::with_capacity(512);
    let title = value.get_str("title").ok_or(ErrorKind::InvalidDogStatsD)?;
    let text = value.get_str("text").ok_or(ErrorKind::InvalidDogStatsD)?;

    r.extend_from_slice(b"_e{");
    r.extend_from_slice(buf.format(title.len()).as_bytes());
    r.push(b',');
    r.extend_from_slice(buf.format(text.len()).as_bytes());
    r.extend_from_slice(b"}:");
    r.extend_from_slice(title.as_bytes());
    r.push(b'|');
    r.extend_from_slice(text.as_bytes());

    if let Some(timestamp) = value.get_u32("timestamp") {
        r.extend_from_slice(b"|d:");
        r.extend_from_slice(buf.format(timestamp).as_bytes());
    }

    if let Some(hostname) = value.get_str("hostname") {
        r.extend_from_slice(b"|h:");
        r.extend_from_slice(hostname.as_bytes());
    }

    if let Some(aggregation_key) = value.get_str("aggregation_key") {
        r.extend_from_slice(b"|k:");
        r.extend_from_slice(aggregation_key.as_bytes());
    }

    if let Some(priority) = value.get_str("priority") {
        r.extend_from_slice(b"|p:");
        r.extend_from_slice(priority.as_bytes());
    }

    if let Some(source) = value.get_str("source") {
        r.extend_from_slice(b"|s:");
        r.extend_from_slice(source.as_bytes());
    }

    if let Some(dogstatsd_type) = value.get_str("type") {
        r.extend_from_slice(b"|t:");
        r.extend_from_slice(dogstatsd_type.as_bytes());
    }

    write_tags(value, &mut r);

    if let Some(container_id) = value.get_str("container_id") {
        r.extend_from_slice(b"|c:");
        r.extend_from_slice(container_id.as_bytes());
    }

    Ok(r)
}

fn encode_service_check(value: &Value) -> Result<Vec<u8>> {
    let mut buf = itoa::Buffer::new();
    let mut r = Vec::with_capacity(512);
    let name = value.get_str("name").ok_or(ErrorKind::InvalidDogStatsD)?;
    let status = value.get_i32("status").ok_or(ErrorKind::InvalidDogStatsD)?;

    r.extend_from_slice(b"_sc|");
    r.extend_from_slice(name.as_bytes());
    r.push(b'|');
    r.extend_from_slice(buf.format(status).as_bytes());

    if let Some(timestamp) = value.get_u32("timestamp") {
        r.extend_from_slice(b"|d:");
        r.extend_from_slice(buf.format(timestamp).as_bytes());
    }

    if let Some(hostname) = value.get_str("hostname") {
        r.extend_from_slice(b"|h:");
        r.extend_from_slice(hostname.as_bytes());
    }

    write_tags(value, &mut r);

    if let Some(message) = value.get_str("message") {
        r.extend_from_slice(b"|m:");
        r.extend_from_slice(message.as_bytes());
    }

    if let Some(container_id) = value.get_str("container_id") {
        r.extend_from_slice(b"|c:");
        r.extend_from_slice(container_id.as_bytes());
    }

    Ok(r)
}

#[inline]
fn write_tags(value: &Value, r: &mut Vec<u8>) {
    if let Some(tags) = value.get_array("tags") {
        r.extend_from_slice(b"|#");
        let mut tags = tags.iter().filter_map(simd_json::ValueAccess::as_str);
        if let Some(t) = tags.next() {
            r.extend_from_slice(t.as_bytes());
        }
        for t in tags {
            r.push(b',');
            r.extend_from_slice(t.as_bytes());
        }
    }
}

fn decode(data: &[u8], _ingest_ns: u64) -> Result<Value> {
    let data = simdutf8::basic::from_utf8(data)?;
    if let Some(data) = data.strip_prefix("_e{") {
        decode_event(data)
    } else if let Some(data) = data.strip_prefix("_sc|") {
        decode_service_check(data)
    } else {
        decode_metric(data)
    }
}

fn decode_metric(data: &str) -> Result<Value> {
    let mut map = Object::with_capacity(1);
    let mut m = Object::with_capacity(6);

    let (metric, data) = data.split_once(':').ok_or_else(invalid)?;
    m.insert_nocheck("metric".into(), Value::from(metric));

    // Value(s) - <VALUE1>:<VALUE2>

    let (vs, data) = data.split_once('|').ok_or_else(invalid)?;

    let values = vs
        .split(':')
        .map(|v| {
            lexical::parse::<f64, _>(v)
                .map(Value::from)
                .map_err(Error::from)
        })
        .collect::<Result<Vec<Value>>>()?;

    m.insert_nocheck("values".into(), Value::from(values));

    let data = if data.starts_with(|c| matches!(c, 'c' | 'd' | 'g' | 'h' | 's')) {
        let (t, data) = data.split_at(1);
        m.insert_nocheck("type".into(), t.into());
        data
    } else if data.starts_with("ms") {
        m.insert_nocheck("type".into(), "ms".into());
        data.get(2..).ok_or_else(invalid)?
    } else {
        data
    };

    // Optional Sections
    for section in data.split('|') {
        if let Some(sample_rate) = section.strip_prefix('@') {
            let sample_rate_float: f64 = lexical::parse(sample_rate)?;
            m.insert_nocheck("sample_rate".into(), Value::from(sample_rate_float));
        } else if let Some(tags) = section.strip_prefix('#') {
            let tags: Vec<&str> = tags.split(',').collect();
            m.insert_nocheck("tags".into(), Value::from(tags));
        } else if let Some(container_id) = section.strip_prefix("c:") {
            m.insert_nocheck("container_id".into(), Value::from(container_id));
        }
    }
    map.insert_nocheck("metric".into(), Value::from(m));

    Ok(Value::from(map))
}

// _e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17|t:warning|#err_type:bad_file
fn decode_event(data: &str) -> Result<Value> {
    let mut map = Object::with_capacity(1);
    let mut m = Object::with_capacity(10);

    let (titel_len, data) = data.split_once(',').ok_or_else(invalid)?;
    let (text_len, data) = data.split_once("}:").ok_or_else(invalid)?;
    let titel_len = lexical::parse::<usize, _>(titel_len)?;
    let text_len = lexical::parse::<usize, _>(text_len)?;
    if data.len() < titel_len + text_len + 1 {
        return Err(invalid());
    }
    let (title, data) = data.split_at(titel_len);
    let data = data.strip_prefix('|').ok_or_else(invalid)?;

    let (text, data) = data.split_at(text_len);
    m.insert_nocheck("title".into(), Value::from(title));
    m.insert_nocheck("text".into(), Value::from(text));

    // Optional Sections

    for section in data.split('|') {
        if let Some(s) = section.strip_prefix("d:") {
            let timestamp: u32 = lexical::parse(s)?;
            m.insert_nocheck("timestamp".into(), Value::from(timestamp));
        } else if let Some(s) = section.strip_prefix("h:") {
            m.insert_nocheck("hostname".into(), Value::from(s));
        } else if let Some(s) = section.strip_prefix("p:") {
            m.insert_nocheck("priority".into(), Value::from(s));
        } else if let Some(s) = section.strip_prefix("s:") {
            m.insert_nocheck("source".into(), Value::from(s));
        } else if let Some(s) = section.strip_prefix("t:") {
            m.insert_nocheck("type".into(), Value::from(s));
        } else if let Some(s) = section.strip_prefix("k:") {
            m.insert_nocheck("aggregation_key".into(), Value::from(s));
        } else if let Some(s) = section.strip_prefix('#') {
            let tags: Vec<&str> = s.split(',').collect();
            m.insert_nocheck("tags".into(), Value::from(tags));
        } else if let Some(s) = section.strip_prefix("c:") {
            m.insert_nocheck("container_id".into(), Value::from(s));
        }
    }

    map.insert_nocheck("event".into(), Value::from(m));
    Ok(Value::from(map))
}

//_sc|Redis connection|2|#env:dev|m:Redis connection timed out after 10s
fn decode_service_check(data: &str) -> Result<Value> {
    let mut map = Object::with_capacity(1);
    let mut m = Object::with_capacity(7);

    let (name, data) = data.split_once('|').ok_or_else(invalid)?;
    m.insert_nocheck("name".into(), Value::from(name));

    let (status_str, data) = data.split_once('|').unwrap_or((data, ""));
    let status: u8 = lexical::parse(status_str)?;
    if status > 3 {
        return Err(invalid());
    }
    m.insert_nocheck("status".into(), Value::from(status));
    for section in data.split('|') {
        if let Some(s) = section.strip_prefix("d:") {
            let timestamp: u32 = lexical::parse(s)?;
            m.insert_nocheck("timestamp".into(), Value::from(timestamp));
        } else if let Some(s) = section.strip_prefix("h:") {
            m.insert_nocheck("hostname".into(), Value::from(s));
        } else if let Some(s) = section.strip_prefix('#') {
            let tags: Vec<&str> = s.split(',').collect();
            m.insert_nocheck("tags".into(), Value::from(tags));
        } else if let Some(s) = section.strip_prefix("c:") {
            m.insert_nocheck("container_id".into(), Value::from(s));
        } else if let Some(s) = section.strip_prefix("m:") {
            m.insert_nocheck("message".into(), Value::from(s));
        }
    }

    map.insert_nocheck("service_check".into(), Value::from(m));
    Ok(Value::from(map))
}

fn invalid() -> Error {
    Error::from(ErrorKind::InvalidDogStatsD)
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_value::literal;

    #[test]
    fn dogstatsd_complete_payload() {
        let data = b"dog:111|g|@0.5|#foo:bar,fizz:buzz|c:123abc";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111.],
                "type": "g",
                "sample_rate": 0.5,
                "tags": ["foo:bar", "fizz:buzz"],
                "container_id": "123abc",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_bad_metric_payload() {
        let data = b"dog:111";
        assert!(decode(data, 0).is_err());
    }

    #[test]
    fn dogstatsd_complete_payload_multiple_values() {
        let data = b"dog:111:222:333:4.44|g|@0.5|#foo:bar,fizz:buzz|c:123abc";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111.,222.,333.,4.44],
                "type": "g",
                "sample_rate": 0.5,
                "tags": ["foo:bar", "fizz:buzz"],
                "container_id": "123abc",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_payload_with_sample_and_tags() {
        let data = b"dog:111|g|@0.5|#foo:bar,fizz:buzz";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111_f64],
                "type": "g",
                "sample_rate": 0.5,
                "tags": ["foo:bar", "fizz:buzz"],
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_payload_with_sample_and_container_id() {
        let data = b"dog:111|g|@0.5|c:123abc";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111.],
                "type": "g",
                "sample_rate": 0.5,
                "container_id": "123abc",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_payload_with_tags_and_container_id() {
        let data = b"dog:111|g|#foo:bar,fizz:buzz|c:123abc";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111.],
                "type": "g",
                "tags": ["foo:bar", "fizz:buzz"],
                "container_id": "123abc",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_payload_with_tags() {
        let data = b"dog:111|g|#foo:bar,fizz:buzz";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111.],
                "type": "g",
                "tags": ["foo:bar", "fizz:buzz"],
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_payload_with_tag() {
        let data = b"dog:111|g|#foo:bar";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111.],
                "type": "g",
                "tags": ["foo:bar"],
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_payload_with_container_id() {
        let data = b"dog:111|g|c:123abc";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "metric": "dog",
                "values": [111.],
                "type": "g",
                "container_id": "123abc",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_count() {
        let data = b"dog:1|c";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "type": "c",
                "metric": "dog",
                "values": [1.],
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded.as_slice(), data);
    }

    #[test]
    fn dogstatsd_time() {
        let data = b"dog:320|ms";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "metric": {
                "type": "ms",
                "metric": "dog",
                "values": [320.],
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_event() {
        let data = b"_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17|t:warning|#err_type:bad_file";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "event": {
                "title": "An exception occurred",
                "text": "Cannot parse CSV file from 10.0.0.17",
                "type": "warning",
                "tags": ["err_type:bad_file"],
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_basic_event() {
        let data = b"_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "event": {
                "title": "An exception occurred",
                "text": "Cannot parse CSV file from 10.0.0.17",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_complete_event() {
        let data = b"_e{4,6}:Test|A Test|d:1663016695|h:test.example.com|k:a1b2c3|p:normal|s:test|t:warning|#err_type:bad_file|c:123abc";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "event": {
                "title": "Test",
                "text": "A Test",
                "timestamp": 1_663_016_695_u32,
                "hostname": "test.example.com",
                "aggregation_key": "a1b2c3",
                "priority": "normal",
                "source": "test",
                "type": "warning",
                "tags": ["err_type:bad_file"],
                "container_id": "123abc",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_bad_event_payload() {
        let data = b"_e{4,6}:Test";
        assert!(decode(data, 0).is_err());
    }

    #[test]
    fn dogstatsd_service_check() {
        let data = b"_sc|Redis connection|2|#env:dev|m:Redis connection timed out after 10s";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "service_check": {
                "name": "Redis connection",
                "status": 2,
                "tags": ["env:dev"],
                "message": "Redis connection timed out after 10s",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_basic_service_check() {
        let data = b"_sc|Redis connection|2";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "service_check": {
                "name": "Redis connection",
                "status": 2,
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_complete_service_check() {
        let data = b"_sc|Redis connection|2|d:1663016695|h:test.example.com|#env:dev|m:Redis connection timed out after 10s|c:123abc";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "service_check": {
                "name": "Redis connection",
                "status": 2,
                "timestamp": 1_663_016_695_u32,
                "hostname":"test.example.com",
                "tags": ["env:dev"],
                "message": "Redis connection timed out after 10s",
                "container_id": "123abc",
            }
        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn dogstatsd_bad_service_check_payload() {
        let data = b"_sc|Redis connection";
        assert!(decode(data, 0).is_err());
    }

    #[test]
    fn bench() {
        let data = b"foo:1620649445.3351967|h";
        let m = decode(data, 0).expect("failed to decode");
        // ALLOW: Values are hardcoded
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"foo1:12345|c";
        let m = decode(data, 0).expect("failed to decode");
        // ALLOW: Values are hardcoded
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"foo2:1234567890|c";
        let m = decode(data, 0).expect("failed to decode");
        // ALLOW: Values are hardcoded
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"_sc|Redis connection|2|d:1663016695|h:test.example.com|#env:dev|m:Redis connection timed out after 10s|c:123abc";
        let m = decode(data, 0).expect("failed to decode");
        // ALLOW: Values are hardcoded
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"_sc|Redis connection|2";
        let m = decode(data, 0).expect("failed to decode");
        // ALLOW: Values are hardcoded
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17";
        let m = decode(data, 0).expect("failed to decode");
        // ALLOW: Values are hardcoded
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17|#env:dev,test:testing";
        let m = decode(data, 0).expect("failed to decode");
        // ALLOW: Values are hardcoded
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));
    }
}
