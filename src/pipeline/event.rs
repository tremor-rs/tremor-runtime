use serde_json::Value;

#[derive(Clone)]
pub struct Event {
    pub key: Option<String>,
    pub drop: bool,
    pub raw: String,
    pub parsed: Value,
    pub classification: String,
    pub feedback: Option<f64>,
}

impl Event {
    pub fn new(raw: &str) -> Self {
        Event {
            key: None,
            drop: false,
            raw: String::from(raw),
            parsed: Value::Null,
            classification: String::from(""),
            feedback: None,
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
        }
    }
}
