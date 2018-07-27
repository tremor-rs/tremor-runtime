use error::TSError;
use grouping::MaybeMessage;
use limiting::utils::Limiter as LimiterT;
use prometheus::IntGauge;
use std::cmp::max;
use std::f64;
use window::TimeWindow;

lazy_static! {
    static ref RATE_GAUGE: IntGauge =
        register_int_gauge!(opts!("ts_limiting_rate", "Current limiting rate.")).unwrap();
}

/// A Limitier algorith that just lets trough a percentage of messages
pub struct Limiter {
    window: TimeWindow,
    upper_limit: f64,
    lower_limit: f64,
    adjustment: u64,
}

impl Limiter {
    pub fn new(opts: &str) -> Self {
        let opts: Vec<&str> = opts.split(':').collect();
        match opts.as_slice() {
            &[time_range, windows, rate] => {
                let time_range = time_range.parse::<u64>().unwrap();
                let windows = windows.parse::<usize>().unwrap();
                let rate = rate.parse::<u64>().unwrap();

                Limiter {
                    window: TimeWindow::new(windows, time_range / (windows as u64), rate),
                    lower_limit: 0.0,
                    upper_limit: f64::INFINITY,
                    adjustment: 0,
                }
            }
            &[time_range, windows, rate, lower, upper, adjust] => {
                let time_range = time_range.parse::<u64>().unwrap();
                let windows = windows.parse::<usize>().unwrap();
                let rate = rate.parse::<u64>().unwrap();
                let lower_limit = lower.parse().unwrap();
                let upper_limit = upper.parse().unwrap();
                let adjustment = adjust.parse().unwrap();

                Limiter {
                    window: TimeWindow::new(windows, time_range / (windows as u64), rate),
                    lower_limit,
                    upper_limit,
                    adjustment,
                }
            }
            _ => {
                panic!("Invalid option for limiter use <time range>:<sub windows>:<initial limit>[:<lower latency limit>:<upper latency limit>:<adjustment>]")
            }
        }
    }
}

impl LimiterT for Limiter {
    fn apply<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<MaybeMessage<'a>, TSError> {
        if msg.drop {
            Ok(msg)
        } else {
            let drop = match self.window.inc() {
                Ok(_) => false,
                Err(_) => true,
            };
            Ok(MaybeMessage {
                key: None,
                classification: msg.classification,
                drop: drop,
                msg: msg.msg,
            })
        }
    }
    fn feedback(&mut self, feedback: f64) {
        match feedback {
            f if f > self.upper_limit => {
                // TODO: We should not set this to `adjustment` but perhaps
                // re-set to `adjustment` on ever X messages if it's 0 to test
                // if we have recovered.
                let m = self.window.max();
                self.window
                    .set_max(max(self.adjustment, m - self.adjustment));
                let m = self.window.max();
                RATE_GAUGE.set(m as i64);
                debug!("v {} ({})", m, f);
            }
            f if f < self.lower_limit => {
                let m = self.window.max();
                let c = self.window.count();
                if m < (c as f64 * 0.8) as u64 {
                    self.window.set_max(m + self.adjustment);
                    let m = self.window.max();
                    RATE_GAUGE.set(m as i64);
                }
                debug!("^ {} ({})", m, f);
            }
            f => debug!("= {} ({})", self.window.max(), f),
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use classifier::Classifier;
    use grouping;
    use grouping::Grouper;
    use limiting;
    use limiting::Limiter;
    use parser;
    use parser::Parser;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn no_capacity() {
        let s = "Example";
        let p = parser::new("raw", "");
        let c = classifier::new("constant", "c");
        let mut g = grouping::new("pass", "");
        let mut b = limiting::new("windowed", "1000:100:0");
        let msg = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g.group(classified))
            .and_then(|msg| b.apply(msg))
            .expect("handling failed!");
        assert_eq!(msg.drop, true);
    }

    #[test]
    fn grouping_test_fail() {
        let s = "Example";
        let p = parser::new("raw", "");
        let c = classifier::new("constant", "c");
        let mut g = grouping::new("pass", "");
        let mut b = limiting::new("windowed", "1000:100:1");
        let msg = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g.group(classified))
            .and_then(|msg| b.apply(msg))
            .expect("handling failed!");
        assert_eq!(msg.drop, false);
    }

    #[test]
    fn grouping_time_refresh() {
        let s = "Example";
        let p = parser::new("raw", "");
        let c = classifier::new("constant", "c");
        let mut g = grouping::new("pass", "");
        let mut b = limiting::new("windowed", "1000:100:1");
        let r1 = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g.group(classified))
            .and_then(|msg| b.apply(msg))
            .expect("grouping failed");
        let r2 = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g.group(classified))
            .and_then(|msg| b.apply(msg))
            .expect("grouping failed");
        // we sleep for 1.1s as this should refresh our bucket
        sleep(Duration::new(1, 200_000_000));
        let r3 = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g.group(classified))
            .and_then(|msg| b.apply(msg))
            .expect("grouping failed");
        assert_eq!(r1.drop, false);
        assert_eq!(r2.drop, true);
        assert_eq!(r3.drop, false);
    }
}
