use error::TSError;
use output::{OUTPUT_DELIVERED, OUTPUT_SKIPPED};
use pipeline::{Event, Step};
use std::collections::HashMap;
use std::time::{Duration, Instant};

struct DebugBucket {
    pass: u64,
    drop: u64,
}
pub struct Output {
    last: Instant,
    update_time: Duration,
    buckets: HashMap<String, DebugBucket>,
    drop: u64,
    pass: u64,
}

impl Output {
    pub fn new(_opts: &str) -> Self {
        Output {
            last: Instant::now(),
            update_time: Duration::from_secs(1),
            buckets: HashMap::new(),
            pass: 0,
            drop: 0,
        }
    }
}
impl Step for Output {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        if self.last.elapsed() > self.update_time {
            self.last = Instant::now();
            println!();
            println!(
                "|{:20}| {:7}| {:7}| {:7}|",
                "classification", "total", "pass", "drop"
            );
            println!(
                "|{:20}| {:7}| {:7}| {:7}|",
                "TOTAL",
                self.pass + self.drop,
                self.pass,
                self.drop
            );
            self.pass = 0;
            self.drop = 0;
            for (class, data) in &self.buckets {
                println!(
                    "|{:20}| {:7}| {:7}| {:7}|",
                    class,
                    data.pass + data.drop,
                    data.pass,
                    data.drop
                );
            }
            println!();
            self.buckets.clear();
        }
        let entry = self.buckets
            .entry(event.classification.clone())
            .or_insert(DebugBucket { pass: 0, drop: 0 });
        if event.drop {
            OUTPUT_SKIPPED.with_label_values(&["stdout"]).inc();
            entry.drop += 1;
            self.drop += 1;
        } else {
            OUTPUT_DELIVERED.with_label_values(&["stdout"]).inc();
            entry.pass += 1;
            self.pass += 1;
        };
        Ok(event)
    }
}
