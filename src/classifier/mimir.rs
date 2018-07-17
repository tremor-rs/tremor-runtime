use error::TSError;
use mimir_rs::mimir::{RuleBuilder, RuleSet};
use parser::Parsed;
use serde_json::{self, Value};
use std::collections::HashMap;

use classifier::utils::{Classified, Classifier as ClassifierT};
/// A constant classifier, it will classify all mesages the same way.
#[derive(Debug)]
pub struct Classifier {
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
                for obj in parsed.iter() {
                    match obj {
                        Value::Object(m) => {
                            if m.len() == 1 {

                                for (rule, name) in m.iter() {
                                    match (rule, name) {
                                        (_rule, Value::String(name)) => {
                                            rules.insert(i, name.clone());
                                            builder.add_rule(name);
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
                Classifier {
                    rules: rules,
                    mimir_rules: builder.done(),
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

impl<'p, 'c: 'p> ClassifierT<'p, 'c> for Classifier {
    fn classify(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError> {
        // TODO: this clone is ugly
        let mut doc = self.mimir_rules.document();
        match msg.parsed.clone() {
            Value::Object(obj) => {
                for (key, value) in obj.iter() {
                    match value {
                        Value::String(s) => {
                            doc.add_string(key, s);
                        }
                        Value::Number(n) => {
                            if n.is_i64() {
                                match n.as_i64() {
                                    Some(num) => doc.add_int(key, num as i32),
                                    None => break,
                                };
                            } else if n.is_u64() {
                                match n.as_u64() {
                                    Some(num) => doc.add_int(key, num as i32),
                                    None => break,
                                };
                            } else {
                                //TODO: addfloat
                            }
                        }
                        _ => warn!("Unsupported value tyoe"),
                    }
                }

                let rs = doc.test();
                for x in 0usize..self.mimir_rules.num_rules() as usize {
                    if rs.at(x) {
                        return match self.rules.get(&x) {
                            Some(class) => Ok(Classified {
                                msg: msg,
                                classification: class.as_str(),
                            }),
                            None => Ok(Classified {
                                msg: msg,
                                classification: "default",
                            }),
                        };
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
        classifier::new("mimir", "");
    }

    #[test]
    #[should_panic]
    fn load_bad_json_top() {
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
        classifier::new("mimir", "[{\"test-rule\": \"test-class\"}]");
        assert!(true)
    }

    #[test]
    fn test_basd_json() {
        let s = "[]";
        let p = parser::new("json", "");
        let c = classifier::new("mimir", "[{\"test-rule\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_err())
    }

    #[test]
    fn test_classification_default() {
        let s = "{}";
        let p = parser::new("json", "");
        let c = classifier::new("mimir", "[]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }
}

#[cfg(test)]
mod tests2 {
    use classifier;
    use classifier::Classifier;
    use parser;
    use parser::Parser;

    #[test]
    #[should_panic]
    fn load_no_config() {
        classifier::new("mimir", "");
    }

    #[test]
    #[should_panic]
    fn load_bad_json_top() {
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

    #[test]
    fn test_basd_json() {
        let s = "[]";
        let p = parser::new("json", "");
        let c = classifier::new("mimir", "[{\"test:rule\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_err())
    }

    #[test]
    fn test_classification_default() {
        let s = "{}";
        let p = parser::new("json", "");
        let c = classifier::new("mimir", "[]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }

    #[test]
    fn test_match() {
        let s = "{\"key\": \"value\"}";
        let p = parser::new("json", "");
        let c = classifier::new("mimir", "[{\"key=value\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
    #[test]
    fn test_no_match() {
        let s = "{\"key\": \"not the value\"}";
        let p = parser::new("json", "");
        let c = classifier::new("mimir", "[{\"key=value\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "default")
    }

    #[test]
    fn test_partial_match() {
        let s = "{\"key\": \"contains the value\"}";
        let p = parser::new("json", "");
        let c = classifier::new("mimir", "[{\"key:value\": \"test-class\"}]");
        let r = p.parse(s).and_then(|parsed| c.classify(parsed));
        assert!(r.is_ok());
        assert_eq!(r.unwrap().classification, "test-class")
    }
}
