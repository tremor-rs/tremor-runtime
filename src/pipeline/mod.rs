mod event;
mod step;

use error::TSError;
use limiting::{Feedback, Limiter};
use output::{self, Output, OUTPUT_SKIPPED};
use prometheus::{Counter, HistogramVec};
use std::f64;

pub use self::event::Event;
pub use self::event::OutputStep;
pub use self::step::Step;

type PipeLineResult = Result<Option<f64>, TSError>;

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
    fn run(&mut self, event: Event) -> PipeLineResult;
    fn shutdown(&mut self) {
        //println!("Hitting default pipelieable shutdown impl");
    }
}

#[derive(Clone)]
pub struct SplitMultiLineBatch<T>
where
    T: Pipelineable,
{
    next: T,
}

impl<T> SplitMultiLineBatch<T>
where
    T: Pipelineable,
{
    pub fn new(next: T) -> Self {
        SplitMultiLineBatch { next }
    }
}

impl<T> Pipelineable for SplitMultiLineBatch<T>
where
    T: Pipelineable,
{
    fn run(&mut self, event: Event) -> PipeLineResult {
        let mut clone_event = Event::from(event.clone());
        clone_event.raw = String::new();
        let mut feedback: Option<f64> = None;
        for line in event.raw.as_str().lines() {
            let mut line_event = Event::from(clone_event.clone());
            line_event.raw = String::from(line);
            feedback = self.next.run(line_event)?;
        }
        Ok(feedback)
    }
}

#[derive(Clone)]
pub struct FilterEmpty<T>
where
    T: Pipelineable,
{
    next: T,
}
impl<T> FilterEmpty<T>
where
    T: Pipelineable,
{
    pub fn new(next: T) -> Self {
        FilterEmpty { next }
    }
}
impl<T> Pipelineable for FilterEmpty<T>
where
    T: Pipelineable,
{
    fn run(&mut self, event: Event) -> PipeLineResult {
        if event.raw == "" {
            Ok(None)
        } else {
            self.next.run(event)
        }
    }
}
pub struct Pipeline<T>
where
    T: Pipelineable,
{
    pipeline: T,
}
impl<T> Pipelineable for Pipeline<T>
where
    T: Pipelineable,
{
    fn run(&mut self, event: Event) -> PipeLineResult {
        self.pipeline.run(event)
    }
}

pub struct DivertablePipelineStep<S, N, D>
where
    S: Step,
    N: Pipelineable,
    D: Pipelineable,
{
    step: S,
    next: N,
    divert: D,
}

impl<S, N, D> DivertablePipelineStep<S, N, D>
where
    S: Step,
    N: Pipelineable,
    D: Pipelineable,
{
    pub fn new(step: S, next: N, divert: D) -> Self {
        Self { step, next, divert }
    }
}

impl<S, N, D> Pipelineable for DivertablePipelineStep<S, N, D>
where
    S: Step,
    N: Pipelineable,
    D: Pipelineable,
{
    fn run(&mut self, event: Event) -> PipeLineResult {
        let event = self.step.apply(event)?;
        if event.drop {
            self.divert.run(event)?;
            Ok(None)
        } else {
            self.next.run(event)
        }
    }
}

pub struct PipelineStep<S, N>
where
    S: Step,
    N: Pipelineable,
{
    step: S,
    next: N,
}

impl<S, N> PipelineStep<S, N>
where
    S: Step,
    N: Pipelineable,
{
    pub fn new(step: S, next: N) -> Self {
        Self { step, next }
    }
}

impl<S, N> Pipelineable for PipelineStep<S, N>
where
    S: Step,
    N: Pipelineable,
{
    fn run(&mut self, event: Event) -> PipeLineResult {
        let event = self.step.apply(event)?;
        self.next.run(event)
    }
}

pub struct TerminalPipelineStep<S>
where
    S: Step,
{
    step: S,
}

impl<S> TerminalPipelineStep<S>
where
    S: Step,
{
    pub fn new(step: S) -> Self {
        Self { step }
    }
}

impl<S> Pipelineable for TerminalPipelineStep<S>
where
    S: Step,
{
    fn run(&mut self, event: Event) -> PipeLineResult {
        let e = self.step.apply(event)?;
        Ok(e.feedback)
    }
}
impl<T> Pipeline<T>
where
    T: Pipelineable,
{
    pub fn new(pipeline: T) -> Self {
        Pipeline { pipeline }
    }
}
/// MainPipeline struct, collecting all the steps of our internal pipeline
pub struct MainPipeline {
    limiting: Limiter,
    output: Output,
    drop_output: Output,
}

impl MainPipeline {
    /// Creates a new pipeline
    pub fn new(limiting: Limiter, output: Output, drop_output: Output) -> Self {
        MainPipeline {
            limiting,
            output,
            drop_output,
        }
    }
}

impl Pipelineable for MainPipeline {
    /// Runs each step of the pipeline and returns either a OK or a error result
    fn run(&mut self, event: Event) -> PipeLineResult {
        let limiting = &mut self.limiting;
        let output = &mut self.output;
        let drop_output = &mut self.drop_output;
        let timer = PIPELINE_HISTOGRAM.with_label_values(&[]).start_timer();
        let event = limiting
            .apply(event)
            .and_then(|r| {
                if !r.drop {
                    output.apply(r)
                } else {
                    OUTPUT_SKIPPED
                        .with_label_values(&[output::step(&r), "pipeline"])
                        .inc();
                    Ok(r)
                }
            }).and_then(|mut r| {
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
                Ok(Some(feedback))
            }
            Ok(_) => Ok(None),
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
        self.limiting.shutdown();
        self.output.shutdown();
        self.drop_output.shutdown();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Context {
    pub warmup: bool,
}

impl Context {
    pub fn should_warmup(self) -> bool {
        self.warmup
    }
}

/// Generalized raw message struct
#[derive(Clone, Debug)]
pub struct Msg {
    pub payload: String,
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
    fn run(&mut self, _event: Event) -> PipeLineResult {
        Ok(None)
    }

    fn shutdown(&mut self) {
        // Nothing to do
    }
}
