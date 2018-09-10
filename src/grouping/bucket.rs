use classifier::Classification;
/// A This grouper is configred with a number of buckets and their alotted
/// throughput on a persecond basis.
///
/// Messages are not combined by key and the alottment is applied in a sliding
/// window fashion with a window granularity of 10ms.
///
/// There is no 'magical' default bucket but one can be configured if desired
/// along with a default rule.
///
/// Metrics are kept on a per rule basis per key for both drops and passes along with
/// a counter for messages dropped due to non matching rules.
///
///

/// buckets - buckets by classification
/// bucket.keys - keys for the dimensions
/// bucket.limit - limit for the bicket
/// bucket.windows - windows in the bucket
///
/// for m in messages {
///   if let bucket = buckets.get(m.classification) {
///      dimension = bucket.keys.map(|key| {m.data.get(key)});
///      if let window bucket.windows(dimension) {
///        if window.inc() { pass } else { drop }
///      } else {
///        bucket.windows[dimension] = Window::new(bucket.limit)
///        if bucket.windows[dimension].inc() { pass } else { drop }
///      }
///   } else {
///     return drop
///   }
/// }
use error::TSError;
use pipeline::{Event, Step};
use prometheus::{Counter, CounterVec};
use serde_json::{self, Value};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::Iterator;
use window::TimeWindow;
lazy_static! {
    /*
     * Number of messages marked to pass based on a given bucket.
     */
    static ref BKT_PASS: CounterVec =
        register_counter_vec!(opts!("ts_bucket_pass", "Passes based on a given bucket."), &["bucket"]).unwrap();
    /*
     * Number of messages marked to drop based on a given bucket.
     */
    static ref BKT_DROP: CounterVec =
        register_counter_vec!(opts!("ts_bucket_drop", "Drops based on a given bucket."), &["bucket"]).unwrap();
    /*
     * Number of messages marked to pass based on a given bucket.
     */
    static ref DIM_PASS: CounterVec =
        register_counter_vec!(opts!("ts_bucket_dim_pass", "Passes based on a given bucket and dimension."), &["bucket", "dimensions"]).unwrap();
    /*
     * Number of messages marked to drop based on a given bucket.
     */
    static ref DIM_DROP: CounterVec =
        register_counter_vec!(opts!("ts_bucket_dim_drop", "Drops based on a given bucket and dimension."), &["bucket", "dimensions"]).unwrap();
    /*
     * Number of messages that could not be matched to a bucket
     */
    static ref BKT_NOMATCH: Counter =
        register_counter!(opts!("ts_bucket_nomatchr", "Messages that could not be matched to any bucket.")).unwrap();
}

/// A grouper either drops or keeps all messages.
pub struct Grouper {
    buckets: HashMap<String, Bucket>,
}

struct Bucket {
    config: Classification,
    groups: HashMap<String, TimeWindow>,
}

impl Grouper {
    /// The grouper is configured with the following syntax:
    ///
    /// * rule: `<name>:<throughput per second>`
    /// * config: `<time range in ms>;<number of windows>;<rule>|<rule>|...`
    ///
    /// So the config `important:10000|unimportant:100|default:10`
    /// would create 3 buckets:
    /// * `important` that gets 10k msgs/s
    /// * `unimportant` that gets 100 msgs/s
    /// * `default` thet gets 10 msgs/s
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str::<Vec<Classification>>(opts) {
            Ok(configs) => {
                let mut bkt_map = HashMap::new();
                for config in configs {
                    let name = config.class.clone();
                    let bkt = Bucket {
                        config,
                        groups: HashMap::new(),
                    };
                    bkt_map.insert(name, bkt);
                }
                Grouper { buckets: bkt_map }
            }
            _ => panic!(
                "Bad configuration format `{}` use `[{{\"name\": \"<name>\", \"rate\": <rate>[, \"time_range\": <time range in ms>, \"windows\": <windows per range>, \"keys\": [\"<dimension1>\", ...]]}}]`.",
                opts
            ),
        }
    }
}

impl Step for Grouper {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let mut event = Event::from(event);
        match self.buckets.get_mut(&event.classification) {
            Some(Bucket { config, groups }) => {
                let dim_metric = event.dimensions.join(" / ");
                let dimensions: Vec<Value> = event
                    .dimensions
                    .iter()
                    .map(|d| Value::String(d.clone()))
                    .collect();
                // TODO: This is ugly! But it works. There sure is a better way of
                // serializing then creating a json ...
                let dimensions = serde_json::to_string(&Value::Array(dimensions))?;

                let window = match groups.entry(dimensions) {
                    Entry::Occupied(o) => o.into_mut(),
                    Entry::Vacant(v) => v.insert(TimeWindow::new(
                        config.windows,
                        config.time_range / (config.windows as u64),
                        config.rate,
                    )),
                };
                let drop = match window.inc() {
                    Ok(_) => {
                        BKT_PASS.with_label_values(&[config.class.as_str()]).inc();
                        DIM_PASS
                            .with_label_values(&[config.class.as_str(), dim_metric.as_str()])
                            .inc();
                        false
                    }
                    Err(_) => {
                        BKT_DROP.with_label_values(&[config.class.as_str()]).inc();
                        DIM_DROP
                            .with_label_values(&[config.class.as_str(), dim_metric.as_str()])
                            .inc();
                        true
                    }
                };
                event.drop = drop;
                Ok(event)
            }
            None => {
                BKT_NOMATCH.inc();
                event.drop = true;
                Ok(event)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use grouping;
    use parser;
    use pipeline::{Event, Step};
    use std::thread::sleep;
    use std::time::Duration;
    use utils;

    #[test]
    fn grouping_test_pass() {
        let s = Event::new("Example", false, utils::nanotime());
        let mut p = parser::new("json", "");
        let mut c = classifier::new("constant", "c");
        let config = json!([{"class": "c", "rate": 100}]).to_string();
        let mut g = grouping::new("bucket", &config);
        let r = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, false);
    }

    #[test]
    fn grouping_test_fail() {
        let s = Event::new("Example", false, utils::nanotime());
        let mut p = parser::new("json", "");
        let mut c = classifier::new("constant", "c");
        let config = json!([{"class": "a", "rate": 100}]).to_string();
        let mut g = grouping::new("bucket", &config);
        let r = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, true);
    }

    #[test]
    fn grouping_time_refresh() {
        let s = Event::new("Example", false, utils::nanotime());
        let mut p = parser::new("json", "");
        let mut c = classifier::new("constant", "c");
        let config = json!([{"class": "c", "rate": 1}]).to_string();
        let mut g = grouping::new("bucket", &config);
        let r1 = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");

        let s = Event::new("Example", false, utils::nanotime());
        let r2 = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        // we sleep for 1.1s as this should refresh our bucket
        sleep(Duration::new(1, 200_000_000));
        let s = Event::new("Example", false, utils::nanotime());
        let r3 = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        assert_eq!(r1.drop, false);
        assert_eq!(r2.drop, true);
        assert_eq!(r3.drop, false);
    }

    #[test]
    fn grouping_bucket_test() {
        let s1 = Event::new("{\"k\": \"12\"}", false, utils::nanotime());
        let s2 = Event::new("{\"k\": \"11\"}", false, utils::nanotime());
        let mut p = parser::new("json", "");
        let config =
            json!([{"class": "c", "rule": "k:\"1\"", "rate": 1, "dimensions": ["k"]}]).to_string();
        let mut c = classifier::new("mimir", &config);
        let mut g = grouping::new("bucket", &config);
        let r = p
            .apply(s1.clone())
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| {
                println!("{:?}", classified);
                g.apply(classified)
            })
            .expect("grouping failed");
        assert_eq!(r.drop, false);

        let r = p
            .apply(s1.clone())
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| {
                println!("{:?}", classified);
                g.apply(classified)
            })
            .expect("grouping failed");
        assert_eq!(r.drop, true);

        let r = p
            .apply(s2.clone())
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| {
                println!("{:?}", classified);
                g.apply(classified)
            })
            .expect("grouping failed");
        assert_eq!(r.drop, false);
    }
}
