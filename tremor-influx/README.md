
**influx parser**

---

Influx wire protocol parsing

Parses a influx wire protocol into a map.

## Use as a library

The influx parser was designed so that influx wire protocol style parsing could be embedded into [tremor](https://www.tremor.rs)'s [scripting](https://docs.tremor.rs/tremor-script/) language for [extract](https://docs.tremor.rs/tremor-script/extractors/influx/) operations.

The parser can also be used standalone

```rust
  let s = "weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206";
  let r: Value = json!({
    "measurement": "weather",
    "tags": {
      "location": "us-midwest"
    },
    "fields": {
       "temperature_str": "too hot\\\\\\cold"
    },
    "timestamp": 1_465_839_830_100_400_206i64, }
  ).into();
  assert_eq!(Ok(Some(r)), decode(s, 0))
```
