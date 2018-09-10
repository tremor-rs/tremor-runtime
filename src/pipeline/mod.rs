mod event;
mod step;

use classifier::Classifier;
use error::TSError;
use grouping::Grouper;
use limiting::{Feedback, Limiter};
use output::{self, Output, OUTPUT_SKIPPED};
use parser::Parser;
use prometheus::{Counter, HistogramVec};
use std::f64;
use std::marker::Send;

pub use self::event::Event;
pub use self::event::OutputStep;
pub use self::step::Step;

lazy_static! {
    /*
     * Number of errors in the pipeline
     */
    static ref PIPELINE_ERR: Counter =
        register_counter!(opts!("ts_pipeline_errors", "Errors in the pipeline.")).unwrap();
    static ref PIPELINE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "ts_pipeline_latency",
        "Latency for event handing through the entire pipeline.",
        &[],
        vec![
            0.000_005, 0.000_01, 0.000_025,
            0.000_05, 0.000_1, 0.000_25,
            0.000_5, 0.001, 0.0025,
            0.005, 0.01, 0.025,
            0.05, 0.1, 0.25,
            f64::INFINITY]
    ).unwrap();

}

pub trait Pipelineable {
    fn run(&mut self, msg: &Msg) -> Result<(), TSError>;
    fn shutdown(&mut self) {
        println!("Hitting default pipelieable shutdown impl");
    }
}

/// Pipeline struct, collecting all the steps of our internal pipeline
pub struct Pipeline {
    parser: Parser,
    classifier: Classifier,
    grouper: Grouper,
    limiting: Limiter,
    output: Output,
    drop_output: Output,
    app_epoch: u64,
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
        app_epoch: u64,
    ) -> Box<Pipelineable + Send> {
        Box::new(Pipeline {
            parser,
            classifier,
            grouper,
            limiting,
            output,
            drop_output,
            app_epoch,
        })
    }
}

impl Pipelineable for Pipeline {
    /// Runs each step of the pipeline and returns either a OK or a error result
    fn run(&mut self, msg: &Msg) -> Result<(), TSError> {
        let parser = &mut self.parser;
        let classifier = &mut self.classifier;
        let grouper = &mut self.grouper;
        let limiting = &mut self.limiting;
        let output = &mut self.output;
        let drop_output = &mut self.drop_output;
        let timer = PIPELINE_HISTOGRAM.with_label_values(&[]).start_timer();
        let event = parser
            .apply(Event::new(
                msg.payload.as_str(),
                msg.ctx.unwrap().warmup,
                self.app_epoch,
            ))
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
        let r = match event {
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
        };
        timer.observe_duration();
        r
    }

    fn shutdown(&mut self) {
        println!("calling shutdown in pipeline");
        self.parser.shutdown();
        self.classifier.shutdown();
        self.grouper.shutdown();
        self.limiting.shutdown();
        self.output.shutdown();
        self.drop_output.shutdown();
    }
}

unsafe impl Send for Pipeline {}

#[derive(Clone, Copy, Debug)]
pub struct Context {
    pub warmup: bool,
}

impl Context {
    pub fn should_warmup(&self) -> bool {
        return self.warmup;
    }
}

/// Generalized raw message struct
#[derive(Clone, Debug)]
pub struct Msg {
    payload: String,
    key: Option<String>,
    pub ctx: Option<Context>,
}

impl Msg {
    pub fn new(key: Option<String>, payload: String) -> Self {
        Msg {
            key,
            payload,
            ctx: None,
        }
    }
    pub fn new_with_context(key: Option<String>, payload: String, ctx: Option<Context>) -> Self {
        Msg { key, payload, ctx }
    }
}

pub struct FakePipeline {}

impl Pipelineable for FakePipeline {
    fn run(&mut self, _msg: &Msg) -> Result<(), TSError> {
        Ok(())
    }

    fn shutdown(&mut self) {
        // Nothing to do
    }
}

unsafe impl Send for FakePipeline {}
