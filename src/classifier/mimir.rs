use error::TSError;
use mimir::{RuleBuilder, RuleSet};
use pipeline::{Event, Step};
use serde_json::{self, Value};
use std::collections::HashMap;

/// A constant classifier, it will classify all mesages the same way.
#[derive(Debug)]
pub struct Classifier {
    // by keeping the RuleBuilder in scope we prevent it and by that
    // the mimir reference to be freed.
    builder: RuleBuilder,
    rules: HashMap<usize, String>,
    mimir_rules: RuleSet,
}

impl Classifier {
    pub fn new(opts: &str) -> Self {
        let mut builder = RuleBuilder::new();
        match serde_json::from_str::<Value>(opts) {
            Ok(Value::Array(parsed)) => {
                let mut rules: HashMap<usize, String> = HashMap::new();
                let mut i = 0;
                for obj in &parsed {
                    match obj {
                        Value::Object(m) => {
                            if m.len() == 1 {

                                for (rule, name) in m.iter() {
                                    match (rule, name) {
                                        (_rule, Value::String(name)) => {
                                            rules.insert(i, name.clone());
                                            builder.add_rule(rule);
                                            i += 1;
                                        },
                                        _ => panic!("Bad format argument needs to be an array of objects with one key value pair.")
                                    };
                                }
                            } else {
                                panic!("Bad format argument needs to be an array of objects with one key value pair.")
                            }
                        },
                        _ => panic!("Bad format argument needs to be an array of objects with one key value pair.")

                    }
                }
                let mimir_rules = builder.done();
                Classifier {
                    builder,
                    rules,
                    mimir_rules,
                }
            }
            Ok(_) => panic!("Bad format argument needs to be an array of objects."),
            Err(e) => {
                warn!("Bad JSON: {}", e);
                panic!("Serade error while parsing rules for mimir classifier.")
            }
        }
    }
}

impl Step for Classifier {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        // TODO: this clone is ugly
        let mut doc = self.mimir_rules.document();
        doc.load_json(event.raw.as_str());
        let rs = doc.test_json();
        let mut event = Event::from(event);
        for x in 0usize..self.mimir_rules.num_rules() as usize {
            if rs.at(x) {
                return match self.rules.get(&x) {
                    Some(class) => {
                        event.classification = class.clone();
                        Ok(event)
                    }
                    None => {
                        event.classification = String::from("default");
                        Ok(event)
                    }
                };
            }
        }
        event.classification = String::from("default");
        Ok(event)
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
        classifier::new("mimir", "[{\"test:rule\": \"test-class\"}]");
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
        let mut p = parser::new("raw", "");
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
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("mimir", "[{\"key=value\": \"test-class\"}]");
        let r = p.apply(s).and_then(|parsed| c.apply(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
    #[test]
    fn test_no_match() {
        let s = Event::new("{\"key\": \"not the value\"}");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("mimir", "[{\"key=value\": \"test-class\"}]");
        let r = p.apply(s).and_then(|parsed| c.apply(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }

    #[test]
    fn test_partial_match() {
        let s = Event::new("{\"key\": \"contains the value\"}");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("mimir", "[{\"key:value\": \"test-class\"}]");
        let r = p.apply(s).and_then(|parsed| c.apply(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
}
