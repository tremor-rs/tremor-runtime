use classifier::{Classifier, Classifiers};
use error::TSError;
use grouping::{Grouper, Groupers};
use limiting::{Limiters, Limitier};
use output::{Output, Outputs};
use parser::{Parser, Parsers};

use prometheus::Counter;

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    static ref OPTOUT_DROPED: Counter =
        register_counter!(opts!("ts_output_droped", "Messages dropped.")).unwrap();
    /*
     * Number of successes read received from the input
     */
    static ref OUTPUT_DELIVERED: Counter =
        register_counter!(opts!("ts_output_delivered", "Messages delivered.")).unwrap();
}

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
        let output = &self.output;
        parser
            .parse(msg.payload)
            .and_then(|parsed| classifier.classify(parsed))
            .and_then(|classified| grouper.group(classified))
            .and_then(|grouped| limiting.apply(grouped))
            .and_then(|r| {
                if !r.drop {
                    OUTPUT_DELIVERED.inc();
                    if let Some(key) = msg.key {
                        output.send(Some(key), msg.payload)
                    } else {
                        output.send(None, msg.payload)
                    }
                } else {
                    OPTOUT_DROPED.inc();
                    Ok(())
                }
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
