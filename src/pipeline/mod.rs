mod event;
mod step;

use classifier::Classifier;
use error::TSError;
use grouping::Grouper;
use limiting::{Feedback, Limiter};
use output::{self, Output, OUTPUT_SKIPPED};
use parser::Parser;
use prometheus::Counter;

pub use self::event::Event;
pub use self::event::OutputStep;
pub use self::step::Step;

lazy_static! {
    /*
     * Number of errors in the pipeline
     */
    static ref PIPELINE_ERR: Counter =
        register_counter!(opts!("ts_pipeline_errors", "Errors in the pipeline.")).unwrap();
}

/// Pipeline struct, collecting all the steps of our internal pipeline
pub struct Pipeline {
    parser: Parser,
    classifier: Classifier,
    grouper: Grouper,
    limiting: Limiter,
    output: Output,
    drop_output: Output,
}

impl Pipeline {
    /// Creates a new pipeline
    pub fn new(
        parser: Parser,
        classifier: Classifier,
        grouper: Grouper,
        limiting: Limiter,
        output: Output,
        drop_output: Output,
    ) -> Self {
        Pipeline {
            parser,
            classifier,
            grouper,
            limiting,
            output,
            drop_output,
        }
    }
    /// Runs each step of the pipeline and returns either a OK or a error result
    pub fn run(&mut self, msg: &Msg) -> Result<(), TSError> {
        let parser = &mut self.parser;
        let classifier = &mut self.classifier;
        let grouper = &mut self.grouper;
        let limiting = &mut self.limiting;
        let output = &mut self.output;
        let drop_output = &mut self.drop_output;
        let event = parser
            .apply(Event::new(msg.payload.as_str()))
            .and_then(|parsed| classifier.apply(parsed))
            .and_then(|classified| grouper.apply(classified))
            .and_then(|grouped| limiting.apply(grouped))
            .and_then(|r| {
                if !r.drop {
                    output.apply(r)
                } else {
                    OUTPUT_SKIPPED
                        .with_label_values(&[output::step(&r), "pipeline"])
                        .inc();
                    Ok(r)
                }
            })
            .and_then(|mut r| {
                r.output_step = OutputStep::Drop;
                if r.drop {
                    r.drop = false;
                    drop_output.apply(r)
                } else {
                    OUTPUT_SKIPPED
                        .with_label_values(&[output::step(&r), "pipeline"])
                        .inc();
                    Ok(r)
                }
            });
        match event {
            Ok(Event {
                feedback: Some(feedback),
                ..
            }) => {
                limiting.feedback(feedback);
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(error) => {
                PIPELINE_ERR.inc();
                Err(error)
            }
        }
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
        Msg { key, payload }
    }
}
