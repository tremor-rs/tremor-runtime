use error::TSError;
use parser::Parsed;
use regex::Regex;
use serde_json::{self, Map, Value};

use classifier::utils::{Classified, Classifier as ClassifierT};

lazy_static! {
    static ref EQ_RE: Regex = Regex::new(r"^(.*?)\s*=\s*(.*)$").unwrap();
    static ref SUB_RE: Regex = Regex::new(r"^(.*?)\s*:\s*(.*)$").unwrap();
}

#[derive(Debug)]
enum Cmp {
    Equal(String),
    Part(String),
}
#[derive(Debug)]
struct Match {
    key: String,
    cmp: Cmp,
    class: String,
}

impl Match {
    fn new(class: String, rule: String) -> Self {
        if let Some(cap) = EQ_RE.captures(rule.as_str()) {
            Match {
                key: cap[1].to_string(),
                cmp: Cmp::Equal(cap[2].to_string()),
                class: class,
            }
        } else if let Some(cap) = SUB_RE.captures(rule.as_str()) {
            Match {
                key: cap[1].to_string(),
                cmp: Cmp::Part(cap[2].to_string()),
                class: class,
            }
        } else {
            panic!("Bad rule: {}", rule)
        }
    }
    fn test(&self, obj: Map<String, Value>) -> bool {
        if let Some(Value::String(value)) = obj.get(&self.key) {
            match self.cmp {
                Cmp::Equal(ref m) => value.eq(m),
                Cmp::Part(ref m) => value.contains(m),
            }
        } else {
            false
        }
    }
}
/// A constant classifier, it will classify all mesages the same way.
#[derive(Debug)]
pub struct Classifier {
    rules: Vec<Match>,
}

impl Classifier {
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str::<Value>(opts) {
            Ok(Value::Array(parsed)) => {
                let mut rules: Vec<Match> = Vec::new();
                for obj in parsed.iter() {
                    match obj {
                        Value::Object(m) => {
                            if m.len() == 1 {

                                for (rule, name) in m.iter() {
                                    match (rule, name) {
                                        (rule, Value::String(class)) => {
                                            rules.push(Match::new(class.to_string(), rule.to_string()));
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
                Classifier { rules: rules }
            }
            Ok(_) => panic!("Bad format argument needs to be an array of objects."),
            Err(e) => {
                warn!("Bad JSON: {}", e);
                panic!("Serade error while parsing rules for matcher classifier.")
            }
        }
    }
}

impl<'p, 'c: 'p> ClassifierT<'p, 'c> for Classifier {
    fn classify(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError> {
        // TODO: this clone is ugly

        match msg.parsed.clone() {
            Value::Object(obj) => {
                for m in self.rules.iter() {
                    if m.test(obj.clone()) {
                        return Ok(Classified {
                            msg: msg,
                            classification: m.class.as_str(),
                        });
                    }
                }
                Ok(Classified {
                    msg: msg,
                    classification: "default",
                })
            }
            _ => Err(TSError::new(
                "Can't classifiy message, needs to be an object",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use classifier::Classifier;
    use parser;
    use parser::Parser;

    #[test]
    #[should_panic]
    fn load_no_config() {
        classifier::new("matcher", "");
    }

    #[test]
    #[should_panic]
    fn load_bad_json_top() {
        classifier::new("matcher", "{}");
    }

    #[test]
    #[should_panic]
    fn load_bad_array_element() {
        classifier::new("matcher", "[{}]");
    }

    #[test]
    #[should_panic]
    fn load_bad_map_key() {
        classifier::new("matcher", "[{\"test\": 7}]");
    }

    #[test]
    #[should_panic]
    fn load_too_many_keys() {
        classifier::new("matcher", "[{\"test\": \"test\", \"test2\":\"test2\"}]");
    }

    #[test]
    fn load_empty_array() {
        classifier::new("matcher", "[]");
        assert!(true)
    }

    #[test]
    fn load_good_rule() {
        classifier::new("matcher", "[{\"test:rule\": \"test-class\"}]");
        assert!(true)
    }

    #[test]
    fn test_basd_json() {
        let s = "[]";
        let p = parser::new("json", "");
        let c = classifier::new("matcher", "[{\"test:rule\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_err())
    }

    #[test]
    fn test_classification_default() {
        let s = "{}";
        let p = parser::new("json", "");
        let c = classifier::new("matcher", "[]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }

    #[test]
    fn test_match() {
        let s = "{\"key\": \"value\"}";
        let p = parser::new("json", "");
        let c = classifier::new("matcher", "[{\"key=value\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
    #[test]
    fn test_no_match() {
        let s = "{\"key\": \"not the value\"}";
        let p = parser::new("json", "");
        let c = classifier::new("matcher", "[{\"key=value\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }

    #[test]
    fn test_partial_match() {
        let s = "{\"key\": \"not the value\"}";
        let p = parser::new("json", "");
        let c = classifier::new("matcher", "[{\"key:value\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
}
