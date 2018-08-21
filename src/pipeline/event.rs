use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use utils::duration_to_millis;

#[derive(Clone, Debug)]
pub enum OutputStep {
    Deliver,
    Drop,
}
#[derive(Clone, Debug)]
pub struct Event {
    pub key: Option<String>,
    pub drop: bool,
    pub raw: String,
    pub parsed: Value,
    pub classification: String,
    pub feedback: Option<f64>,
    pub ingest_time: u64,
    pub dimensions: Vec<String>,
    pub index: Option<String>,
    pub data_type: Option<String>,
    pub output_step: OutputStep,
}

impl Event {
    pub fn new(raw: &str) -> Self {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let ingest_time = duration_to_millis(since_the_epoch);
        Event {
            key: None,
            drop: false,
            raw: String::from(raw),
            parsed: Value::Null,
            classification: String::from(""),
            feedback: None,
            ingest_time,
            dimensions: Vec::new(),
            index: None,
            data_type: None,
            output_step: OutputStep::Deliver,
        }
    }
    pub fn from(original: Self) -> Self {
        Event {
            key: original.key,
            drop: original.drop,
            raw: original.raw,
            parsed: original.parsed,
            classification: original.classification,
            feedback: original.feedback,
            ingest_time: original.ingest_time,
            dimensions: original.dimensions,
            index: original.index,
            data_type: original.data_type,
            output_step: original.output_step,
        }
    }
}
