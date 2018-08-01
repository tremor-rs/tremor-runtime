//! A This grouper is configred with a number of buckets and their alotted
//! throughput on a persecond basis.
//!
//! Messages are not combined by key and the alottment is applied in a sliding
//! window fashion with a window granularity of 10ms.
//!
//! There is no 'magical' default bucket but one can be configured if desired
//! along with a default rule.
//!
//! Metrics are kept on a per rule basis for both drops and passes along with
//! a counter for messages dropped due to non matching rules.
use error::TSError;
use pipeline::{Event, Step};
use prometheus::{Counter, CounterVec};
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
    name: String,
    window: TimeWindow,
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
        let opts: Vec<&str> = opts.split(';').collect();
        match opts.as_slice() {
            &[time_range, windows, buckets] => {
                let buckets: Vec<&str> = buckets.split('|').collect();
                let mut bkt_map = HashMap::new();
                let time_range = time_range.parse::<u64>().unwrap();
                let windows = windows.parse::<usize>().unwrap();
                for bucket in buckets {
                    let split: Vec<&str> = bucket.split(':').collect();
                    match split.as_slice() {
                        [name, rate] => {
                            let rate = rate.parse::<u64>().unwrap();
                            let name = String::from(*name);
                            let bkt = Bucket {
                                name: name.clone(),
                                window: TimeWindow::new(windows, time_range / (windows as u64), rate),
                            };
                            bkt_map.insert(name, bkt)
                        }
                        _ => panic!(
                            "Bad bucket format '{}', please use the syntax '<name>:<rate in msgs/s>'.",
                            bucket
                        ),
                    };
                }
                Grouper { buckets: bkt_map }
            }
            _ => panic!("Invalid options for bucketing, use <time range in ms>;<number of windows>;<rule 1>|<rule 2>|...|<rule n>"),

        }
    }
}
impl Step for Grouper {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let mut event = Event::from(event);
        match self.buckets.get_mut(&event.classification) {
            Some(Bucket { window, name, .. }) => {
                let drop = match window.inc() {
                    Ok(_) => {
                        BKT_PASS.with_label_values(&[name]).inc();
                        false
                    }
                    Err(_) => {
                        BKT_DROP.with_label_values(&["default"]).inc();
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

    #[test]
    fn grouping_test_pass() {
        let s = Event::new("Example");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", "c");
        let mut g = grouping::new("bucket", "1000;100;c:1000");
        let r = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, false);
    }

    #[test]
    fn grouping_test_fail() {
        let s = Event::new("Example");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", "c");
        let mut g = grouping::new("bucket", "1000;100;a:1000");
        let r = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, true);
    }

    #[test]
    fn grouping_time_refresh() {
        let s = Event::new("Example");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", "c");
        let mut g = grouping::new("bucket", "1000;100;c:1");
        let r1 = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");

        let s = Event::new("Example");
        let r2 = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        // we sleep for 1.1s as this should refresh our bucket
        sleep(Duration::new(1, 200_000_000));
        let s = Event::new("Example");
        let r3 = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .expect("grouping failed");
        assert_eq!(r1.drop, false);
        assert_eq!(r2.drop, true);
        assert_eq!(r3.drop, false);
    }
}
