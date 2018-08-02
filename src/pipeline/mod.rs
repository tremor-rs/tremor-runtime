mod event;
mod step;

use classifier::Classifier;
use error::TSError;
use grouping::Grouper;
use limiting::{Feedback, Limiter};
use output::Output;
use parser::Parser;

pub use self::event::Event;
pub use self::step::Step;

/// Pipeline struct, collecting all the steps of our internal pipeline
pub struct Pipeline {
    parser: Parser,
    classifier: Classifier,
    grouper: Grouper,
    limiting: Limiter,
    output: Output,
}

impl Pipeline {
    /// Creates a new pipeline
    pub fn new(
        parser: Parser,
        classifier: Classifier,
        grouper: Grouper,
        limiting: Limiter,
        output: Output,
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
    pub fn run(&mut self, msg: &Msg) -> Result<(), TSError> {
        let parser = &mut self.parser;
        let classifier = &mut self.classifier;
        let grouper = &mut self.grouper;
        let limiting = &mut self.limiting;
        let output = &mut self.output;
        let event = parser
            .apply(Event::new(msg.payload))
            .and_then(|parsed| classifier.apply(parsed))
            .and_then(|classified| grouper.apply(classified))
            .and_then(|grouped| limiting.apply(grouped))
            .and_then(|r| output.apply(r));
        match event {
            Ok(Event {
                feedback: Some(feedback),
                ..
            }) => {
                limiting.feedback(feedback);
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
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
        Msg { key, payload }
    }
}
