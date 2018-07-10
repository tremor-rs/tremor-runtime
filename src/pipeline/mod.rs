use classifier::{Classifier, Classifiers};
use error::TSError;
use grouping::{Grouper, Groupers};
use limiting::{Limiters, Limitier};
use output::{Output, Outputs};
use parser::{Parser, Parsers};

/// Pipeline struct, collecting all the steps of our internal pipeline
pub struct Pipeline {
    parser: Parsers,
    classifier: Classifiers,
    grouper: Groupers,
    limiting: Limiters,
    output: Outputs,
}

impl Pipeline {
    /// Creates a new pipeline
    pub fn new(
        parser: Parsers,
        classifier: Classifiers,
        grouper: Groupers,
        limiting: Limiters,
        output: Outputs,
    ) -> Self {
        Pipeline {
            parser,
            classifier,
            grouper,
            limiting,
            output,
        }
    }
    /// Runs each step of the pipeline and returns either a OK or a error result
    pub fn run(&self, msg: Msg) -> Result<(), TSError> {
        self.parser
            .parse(msg.payload())
            .and_then(|parsed| self.classifier.classify(parsed))
            .and_then(|classified| self.grouper.group(classified))
            .and_then(|grouped| self.limiting.apply(grouped))
            .and_then(|r| {
                if !r.drop {
                    if let Some(key) = msg.key() {
                        self.output.send(Some(key.as_str()), msg.payload().as_str())
                    } else {
                        self.output.send(None, msg.payload().as_str())
                    }
                } else {
                    Ok(())
                }
            })
    }
}

/// Generalized raw message struct
#[derive(Debug)]
pub struct Msg {
    payload: String,
    key: Option<String>,
}

impl Msg {
    pub fn new(key: Option<String>, payload: String) -> Self {
        Msg {
            key: key,
            payload: payload,
        }
    }
    pub fn payload(&self) -> String {
        self.payload.clone()
    }
    pub fn key(&self) -> Option<String> {
        if let Some(key) = self.key.clone() {
            Some(key.clone())
        } else {
            None
        }
    }
}
