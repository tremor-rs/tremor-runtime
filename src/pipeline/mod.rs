use classifier::{Classifier, Classifiers};
use error::TSError;
use grouping::MaybeMessage;
use grouping::{Grouper, Groupers};
use limiting::{Limiters, Limitier};
use output::{Output, Outputs};
use parser::{Parser, Parsers};

/// Pipeline struct, collecting all the steps of our internal pipeline
pub struct Pipeline<'p> {
    parser: Parsers,
    classifier: Classifiers<'p>,
    grouper: Groupers<'p>,
    limiting: Limiters,
    output: Outputs,
}

impl<'p> Pipeline<'p> {
    /// Creates a new pipeline
    pub fn new(
        parser: Parsers,
        classifier: Classifiers<'p>,
        grouper: Groupers<'p>,
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
    pub fn run(&mut self, msg: Msg) -> Result<(), TSError> {
        let parser = &self.parser;
        let classifier = &self.classifier;
        let grouper = &mut self.grouper;
        let limiting = &self.limiting;
        let output = &mut self.output;
        parser
            .parse(msg.payload)
            .and_then(|parsed| classifier.classify(parsed))
            .and_then(|classified| grouper.group(classified))
            .and_then(|grouped| limiting.apply(grouped))
            .and_then(|r| {
                output.send(MaybeMessage {
                    drop: r.drop,
                    key: msg.key,
                    msg: r.msg,
                    classification: r.classification,
                })
            })
    }
}

/// Generalized raw message struct
#[derive(Debug)]
pub struct Msg<'a> {
    payload: &'a str,
    key: Option<&'a str>,
}

impl<'a> Msg<'a> {
    pub fn new(key: Option<&'a str>, payload: &'a str) -> Self {
        Msg {
            key: key,
            payload: payload,
        }
    }
}
