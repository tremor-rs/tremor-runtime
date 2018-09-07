use classifier::Classification;
use error::TSError;
use mimir;
use pipeline::{Event, Step};
use serde_json;

/// A constant classifier, it will classify all mesages the same way.
#[derive(Debug)]
pub struct Classifier {
    classifier: mimir::Classifier<Classification>,
}

impl Classifier {
    pub fn new(opts: &str) -> Self {
        let mut classifier = mimir::Classifier::new();
        let classifications: Vec<Classification> =
            serde_json::from_str(opts).expect("Failed to parse classifications");
        for c in &classifications {
            if let Some(ref rule) = c.rule {
                if let Err(err) = classifier.add(rule, c.clone()) {
                    panic!("Error parsing rule {:?} => {}", err, rule);
                };
            }
        }
        Classifier { classifier }
    }
}

impl Step for Classifier {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        // TODO: this clone is ugly

        let doc = if let Some(ref json) = event.json {
            self.classifier.document_from_json(json.as_str())?
        } else {
            return Err(TSError::new("Event doesn't have a JSON element"));
        };
        let mut event = Event::from(event);
        match self.classifier.classify(&doc) {
            None => {
                event.classification = String::from("default");
            }
            Some(class) => {
                event.classification = class.class.clone();
                event.dimensions = class
                    .clone()
                    .dimensions
                    .into_iter()
                    .map(|key| match doc.find_string(&key) {
                        Some(v) => v.clone(),
                        None => String::from(""),
                    })
                    .collect();
                if let Some(ref key) = class.index_key {
                    if let Some(v) = doc.find_string(key) {
                        event.index = Some(v.clone());
                    }
                }
                if let Some(v) = doc.find_string("type") {
                    event.data_type = Some(v.clone());
                }
            }
        };
        Ok(event)
    }
}

impl From<mimir::MimirError> for TSError {
    fn from(from: mimir::MimirError) -> TSError {
        TSError::new(format!("{:?}", from).as_str())
    }
}

#[cfg(test)]
mod tests1 {
    use classifier;
    use parser;
    use pipeline::{Event, Step};

    #[test]
    #[should_panic]
    fn load_no_config() {
        classifier::new("mimir", "");
    }

    #[test]
    #[should_panic]
    fn load_bad_raw_top() {
        classifier::new("mimir", "{}");
    }

    #[test]
    #[should_panic]
    fn load_bad_array_element() {
        classifier::new("mimir", "[{}]");
    }

    #[test]
    #[should_panic]
    fn load_bad_map_key() {
        classifier::new("mimir", "[{\"test\": 7}]");
    }

    #[test]
    #[should_panic]
    fn load_too_many_keys() {
        classifier::new("mimir", "[{\"test\": \"test\", \"test2\":\"test2\"}]");
    }

    #[test]
    fn load_empty_array() {
        classifier::new("mimir", "[]");
        assert!(true)
    }

    #[test]
    fn load_good_rule() {
        classifier::new(
            "mimir",
            "[{\"rule\":\"test:rule\", \"class\":\"test-class\"}]",
        );
        assert!(true)
    }

    // #[test]
    // fn test_basd_raw() {
    //     let s = Event::new("[]");
    //     let mut p = parser::new("raw", "");
    //     let mut c = classifier::new("mimir", "[{\"test:rule\": \"class-test\"}]");
    //     let r = p.apply(s).and_then(|parsed| c.apply(parsed));
    //     assert!(r.is_err())
    // }

    #[test]
    fn test_classification_default() {
        let s = Event::new("{}");
        let mut p = parser::new("json", "");
        let mut c = classifier::new("mimir", "[]");
        let r = p.apply(s).and_then(|parsed| c.apply(parsed));
        match r.clone() {
            Err(e) => println!("e: {:?}", e),
            _ => (),
        };
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }

    #[test]
    fn test_match() {
        let s = Event::new("{\"key\": \"value\"}");
        let mut p = parser::new("json", "");
        let mut c = classifier::new(
            "mimir",
            "[{\"rule\":\"key=value\", \"class\": \"test-class\"}]",
        );
        let r = p.apply(s).and_then(|parsed| c.apply(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
    #[test]
    fn test_no_match() {
        let s = Event::new("{\"key\": \"not the value\"}");
        let mut p = parser::new("json", "");
        let mut c = classifier::new(
            "mimir",
            "[{\"rule\":\"key=value\", \"class\": \"test-class\"}]",
        );
        let r = p.apply(s).and_then(|parsed| c.apply(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }

    #[test]
    fn test_partial_match() {
        let s = Event::new("{\"key\": \"contains the value\"}");
        let mut p = parser::new("json", "");
        let mut c = classifier::new(
            "mimir",
            "[{\"rule\": \"key:value\", \"class\": \"test-class\"}]",
        );
        let r = p.apply(s).and_then(|parsed| c.apply(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
}
