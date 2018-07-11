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
use classifier::Classified;
use error::TSError;
use grouping::utils::{Grouper as GrouperT, MaybeMessage};
use std::collections::HashMap;
use std::iter::Iterator;
use window::TimeWindow;

static SUB_WINDOWS: usize = 100;

use prometheus::{Counter, CounterVec};

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
pub struct Grouper<'a> {
    buckets: HashMap<&'a str, Bucket<'a>>,
}

struct Bucket<'a> {
    name: &'a str,
    window: TimeWindow,
}

impl<'a> Grouper<'a> {
    /// The grouper is configured with the following syntax:
    ///
    /// * rule: `<name>:<throughput per second>``
    /// * config: `<rule>|<rule>|...`
    ///
    /// So the config `important:10000|unimportant:100|default:10`
    /// would create 3 buckets:
    /// * `important` that gets 10k msgs/s
    /// * `unimportant` that gets 100 msgs/s
    /// * `default` thet gets 10 msgs/s
    pub fn new(opts: &'a str) -> Self {
        let buckets: Vec<&str> = opts.split('|').collect();
        let mut bkt_map = HashMap::new();

        for bucket in buckets {
            let split: Vec<&str> = bucket.split(':').collect();
            match split.as_slice() {
                [name, rate] => {
                    let rate = rate.parse::<u64>().unwrap();
                    let bkt = Bucket {
                        name: name,
                        window: TimeWindow::new(SUB_WINDOWS, 1000 / (SUB_WINDOWS as u64), rate),
                    };
                    bkt_map.insert(*name, bkt)
                }
                _ => panic!(
                    "Bad bucket format '{}', please use the syntax '<name>:<rate in msgs/s>'.",
                    bucket
                ),
            };
        }
        Grouper { buckets: bkt_map }
    }
}
impl<'a> GrouperT for Grouper<'a> {
    fn group<'c, 'p>(&mut self, msg: Classified<'c, 'p>) -> Result<MaybeMessage<'p>, TSError> {
        match self.buckets.get_mut(msg.classification) {
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
                Ok(MaybeMessage {
                    drop: drop,
                    msg: msg.msg,
                })
            }
            None => {
                BKT_NOMATCH.inc();
                Ok(MaybeMessage {
                    drop: true,
                    msg: msg.msg,
                })
            }
        }
    }
}
