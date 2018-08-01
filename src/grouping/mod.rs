//! This model handles grouping messages given on ther classification and
//! the first level of traffic shaping.

mod boolean;
mod bucket;

use error::TSError;
use pipeline::{Event, Step};
use prometheus::Counter;

lazy_static! {
    /*
     * Number of messages that pass the grouping stage.
     */
    static ref GROUPING_PASS: Counter =
        register_counter!(opts!("ts_grouping_pass", "Passes during the grouping stage.")).unwrap();
    /*
     * Number of messages marked to drop the grouping stage.
     */
    static ref GROUPING_DROP: Counter =
        register_counter!(opts!("ts_grouping_drop", "Drops during the grouping stage.")).unwrap();
    /*
     * Errors during the grouping stage
     */
    static ref GROUPING_ERR: Counter =
        register_counter!(opts!("ts_grouping_error", "Errors during the grouping stage.")).unwrap();
}

pub fn new(name: &str, opts: &str) -> Grouper {
    match name {
        "drop" => Grouper::Boolean(boolean::Grouper::new("true")),
        "pass" => Grouper::Boolean(boolean::Grouper::new("false")),
        "bucket" => Grouper::Bucket(bucket::Grouper::new(opts)),

        _ => panic!(
            "Unknown grouper: {} valid options are 'bucket', 'drop' and 'pass'",
            name
        ),
    }
}

pub enum Grouper {
    Boolean(boolean::Grouper),
    Bucket(bucket::Grouper),
}
impl Step for Grouper {
    fn apply(&mut self, msg: Event) -> Result<Event, TSError> {
        let r = match self {
            Grouper::Boolean(g) => g.apply(msg),
            Grouper::Bucket(g) => g.apply(msg),
        };
        match r {
            Err(_) => GROUPING_ERR.inc(),
            Ok(Event { drop: false, .. }) => GROUPING_PASS.inc(),
            Ok(Event { drop: true, .. }) => GROUPING_DROP.inc(),
        };
        r
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use grouping;
    use parser;
    use pipeline::{Event, Step};
    #[test]
    fn boolean_grouper() {
        let s = Event::new("Example");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", "Classification");
        let mut g_d = grouping::new("drop", "");
        let mut g_k = grouping::new("pass", "");

        let r = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g_d.apply(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, true);

        let s = Event::new("Example");
        let r = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g_k.apply(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, false);
    }

}
