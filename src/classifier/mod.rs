//! This module contains code used to classify messages based on
//! the rule based language.

mod constant;
mod mimir;

use error::TSError;
use pipeline::{Event, Step};

pub fn new(name: &str, opts: &str) -> Classifier {
    match name {
        "mimir" => Classifier::Mimir(mimir::Classifier::new(opts)),
        "constant" => Classifier::Constant(constant::Classifier::new(opts)),
        _ => panic!(
            "Unknown classifier: {} valid options are 'constant', and 'mimir'",
            name
        ),
    }
}

fn dflt_dimensions() -> Vec<String> {
    vec![]
}
fn dflt_time_range() -> u64 {
    1000
}

fn dflt_windows() -> usize {
    100
}

fn dflt_rate() -> u64 {
    0
}

#[derive(Deserialize, Debug, Clone)]
pub struct Classification {
    pub class: String,
    pub rule: Option<String>,
    #[serde(default = "dflt_dimensions")]
    pub dimensions: Vec<String>,
    #[serde(default = "dflt_time_range")]
    pub time_range: u64,
    #[serde(default = "dflt_rate")]
    pub rate: u64,
    #[serde(default = "dflt_windows")]
    pub windows: usize,
    pub index_key: Option<String>,
}

pub enum Classifier {
    Constant(constant::Classifier),
    Mimir(mimir::Classifier),
}
impl Step for Classifier {
    fn apply(&mut self, msg: Event) -> Result<Event, TSError> {
        match self {
            Classifier::Constant(c) => c.apply(msg),
            Classifier::Mimir(c) => c.apply(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use parser;
    use pipeline::{Event, Step};
    use utils::nanotime;

    #[test]
    fn constant_classifier() {
        let s = Event::new("Example", false, nanotime());
        let t = String::from("Classification");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", t.as_str());
        let classified = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .expect("classification failed!");
        assert_eq!(t, classified.classification);
    }

}
