#![allow(dead_code)]
// Copyright 2018, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod error;
mod graph;
mod messages;
mod onramp;
mod op;
pub mod prelude;
mod step;
pub mod types;

use self::graph::{Graph, GraphLink};
use self::onramp::OnRampActor;
use self::op::{OpSpec, StepConfig};
use self::types::ConfValue;
use crate::errors::*;
use actix;
use actix::prelude::*;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;
use uuid::Uuid;

type PipelineSteps = Vec<ConfValue>;
#[derive(Clone, Debug, Deserialize)]
pub struct SubPipeline {
    name: String,
    steps: PipelineSteps,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PipelineConfig {
    name: String,
    steps: PipelineSteps,
    subpipelines: Option<Vec<SubPipeline>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub onramps: Vec<ConfValue>,
    pub pipelines: Vec<PipelineConfig>,
}

type Pipelines = HashMap<String, GraphLink>;
type SubPipelines = HashMap<String, GraphLink>;

pub fn value_to_conf(step: ConfValue) -> Result<StepConfig> {
    let (name, config) = match step {
        ConfValue::String(s) => (s, ConfValue::Null),
        ConfValue::Mapping(m) => {
            if m.len() != 1 {
                return Err(ErrorKind::BadOpConfig("Need to have exactly 1 element".into()).into());
            } else {
                let mut name: String = "".to_string();
                let mut config: ConfValue = ConfValue::Null;
                for (k, v) in m {
                    match k {
                        ConfValue::String(k) => name = k,
                        _ => {
                            return Err(ErrorKind::BadOpConfig(
                                "Op name needs to be a string".into(),
                            )
                            .into())
                        }
                    };
                    config = v;
                }
                (name, config)
            }
        }
        _ => return Err(ErrorKind::BadOpConfig("Config needs to be a string or map".into()).into()),
    };
    let name_parts: Vec<&str> = name.split("::").collect();
    match *name_parts.as_slice() {
        [namespace, name] => Ok(StepConfig {
            namespace: namespace.to_string(),
            name: name.to_string(),
            config,
            uuid: Uuid::new_v4(),
        }),
        _ => Err(ErrorKind::BadOpConfig(format!("{} is not a namespaced step.", name)).into()),
    }
}

/// Turns a list of steps into a graph.
fn make_pipeline(
    step: ConfValue,
    mut rest: Vec<ConfValue>,
    end: GraphLink,
    pipelines: Pipelines,
    subs: SubPipelines,
) -> Result<GraphLink> {
    // Look at the step itself
    let step = value_to_conf(step)?;
    // If it's a sub pipeline we handle this first.
    if step.namespace == "sub" {
        // The clones here are ugly but since all elements are referece counted
        // and this is an one time startup cost it's going to be fine!
        if let Some(p) = subs.get(&step.name.clone()) {
            Ok(p.clone())
        } else {
            Err(ErrorKind::UnknownSubPipeline(step.name).into())
        }
    // Second look at other pipelines we reference to
    } else if step.namespace == "pipeline" {
        // The clones here are ugly but since all elements are reference counted
        // and this is an one time startup cost it's going to be fine!
        if let Some(p) = pipelines.get(&step.name.clone()) {
            Ok(p.clone())
        } else {
            Err(ErrorKind::UnknownSubPipeline(step.name).into())
        }
    // Otherwise we'll referencing a known step
    } else {
        // Treat on-error pipeline first, if it's set link it
        // otherwise just drop the error stuff
        let on_error = match step.config.get("on-error") {
            None => end.clone(),
            Some(ConfValue::Sequence(steps)) => {
                let mut steps = steps.clone();
                steps.reverse();
                // Build the error pipeline
                if let Some(first) = steps.pop() {
                    make_pipeline(first, steps, end.clone(), pipelines.clone(), subs.clone())?
                } else {
                    return Err(ErrorKind::BadOpConfig(format!(
                        "on-error for step '{}' contains no steps",
                        step.name
                    ))
                    .into());
                }
            }
            _ => {
                return Err(ErrorKind::BadOpConfig(format!(
                    "on-error for step '{}' contains no steps",
                    step.name
                ))
                .into());
            }
        };
        // Now connect the remaining outputs
        let mut outputs = Vec::new();
        if let Some(ConfValue::Sequence(outputs_s)) = step.config.get("outputs") {
            for s in outputs_s {
                match s {
                    s @ ConfValue::String(_) => outputs.push(make_pipeline(
                        s.clone(),
                        Vec::new(),
                        end.clone(),
                        pipelines.clone(),
                        subs.clone(),
                    )?),
                    ConfValue::Sequence(steps) => {
                        let mut steps = steps.clone();
                        steps.reverse();
                        if let Some(first) = steps.pop() {
                            outputs.push(make_pipeline(
                                first,
                                steps,
                                end.clone(),
                                pipelines.clone(),
                                subs.clone(),
                            )?)
                        } else {
                            return Err(ErrorKind::BadOpConfig(format!(
                                "additional outputs for step '{}' contains no steps",
                                step.name
                            ))
                            .into());
                        }
                    }
                    _ => {
                        return Err(ErrorKind::BadOpConfig(format!(
                            "Additional outputs for {} need to be strings or lists",
                            step.name
                        ))
                        .into())
                    }
                }
            }
        }

        // finally create the node itself
        let step = OpSpec::from_config(step)?;
        if let Some(next) = rest.pop() {
            Ok(Graph::node(
                step,
                make_pipeline(next, rest, end.clone(), pipelines, subs)?,
                on_error,
                outputs,
            ))
        } else {
            Ok(Graph::node(step, end, on_error, outputs))
        }
    }
}

/// Creates a sub pipeline (pipeline as a function) and registers it so the
/// main pipeline can reference it.
fn make_sub_pipelines(
    sub: SubPipeline,
    mut rest: Vec<SubPipeline>,
    end: GraphLink,
    pipelines: Pipelines,
    mut subs: SubPipelines,
) -> Result<SubPipelines> {
    let mut steps = sub.steps.clone();
    steps.reverse();
    if let Some(first) = steps.pop() {
        let p = make_pipeline(first, steps, end.clone(), pipelines.clone(), subs.clone())?;
        subs.insert(sub.name, p);
        if let Some(sub) = rest.pop() {
            make_sub_pipelines(sub, rest, end, pipelines, subs)
        } else {
            Ok(subs)
        }
    } else {
        Err(ErrorKind::MissingSteps(format!("Pipeline '{}' has zero steps", sub.name)).into())
    }
}

/// Walks the pipelines vector and starts the pipelines one by one, registering
/// them so later pipelines can reference earlier ones
pub fn start_pipelines(
    p: PipelineConfig,
    mut rest: Vec<PipelineConfig>,
    mut pipelines: Pipelines,
) -> Result<Pipelines> {
    let name = p.name.clone();
    let pipeline = start_pipeline(p, &pipelines)?;
    pipelines.insert(name, pipeline);
    match rest.pop() {
        None => Ok(pipelines),
        Some(p) => start_pipelines(p, rest, pipelines),
    }
}

/// Takes a pipeline configuration creates the graph structure and starts it in a own thread.
fn start_pipeline(p: PipelineConfig, pipelines: &Pipelines) -> Result<GraphLink> {
    // We use a channel to syncronize thread startup, this way we can run the pipeline
    // in the thread but still syncronously return the link to the pipeline.
    let (tx, rx) = channel();
    let pipelines = pipelines.clone();
    let name = p.name.clone();

    thread::spawn(move || {
        let leaf = Graph::leaf();

        // First set up the sub pipelines
        let mut subs = if let Some(subs) = p.subpipelines.clone() {
            subs
        } else {
            Vec::new()
        };
        subs.reverse();
        let subs = if let Some(sub) = subs.pop() {
            match make_sub_pipelines(
                sub.clone(),
                subs,
                leaf.clone(),
                pipelines.clone(),
                HashMap::new(),
            ) {
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
                Ok(subs) => subs,
            }
        } else {
            HashMap::new()
        };

        // Now generate the pipeline itself
        let mut steps = p.steps;
        steps.reverse();
        match make_pipeline(steps.pop().unwrap(), steps, leaf, pipelines, subs) {
            Err(e) => {
                let _ = tx.send(Err(e));
            }
            Ok(pipeline) => {
                actix::System::run(move || {
                    let (pipeline_actor, types) = match pipeline.lock().unwrap().make_actor() {
                        Ok(r) => r,
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            return;
                        }
                    };
                    let p1 = pipeline_actor.clone().unwrap();
                    let onramp = OnRampActor::create(|_ctx| OnRampActor {
                        pipeline: p1,
                        id: 0,
                        replies: HashMap::new(),
                    });

                    let _ = tx.send(Ok((onramp, pipeline, pipeline_actor, types)));
                });
            }
        };
    });
    match rx.recv() {
        Ok(Ok((onramp, head, addr, types))) => Ok(Arc::new(Mutex::new(Graph::Pipeline {
            name,
            uuid: Uuid::new_v4(),
            onramp,
            actor: addr.unwrap(),
            types,
            head,
        }))),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(ErrorKind::PipelineStartError(name).into()),
    }
}

//////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::prelude::*;
    use actix;
    use actix::prelude::*;
    use futures::{future, Future};
    use serde_yaml;
    use uuid::Uuid;
    #[test]
    fn simple_test() {
        let leaf = Graph::leaf();
        let stdout = Graph::node(
            OpSpec::new(
                OpType::Offramp,
                "stdout".to_string(),
                serde_yaml::from_str("prefix: ''").unwrap(),
                Uuid::new_v4(),
            ),
            leaf.clone(),
            leaf.clone(),
            Vec::new(),
        );

        let to_json = Graph::node(
            OpSpec::new(
                OpType::Render,
                "json".to_string(),
                ConfValue::Null,
                Uuid::new_v4(),
            ),
            stdout.clone(),
            leaf.clone(),
            Vec::new(),
        );
        let from_json = Graph::node(
            OpSpec::new(
                OpType::Parse,
                "json".to_string(),
                ConfValue::Null,
                Uuid::new_v4(),
            ),
            to_json.clone(),
            leaf.clone(),
            Vec::new(),
        );
        actix::System::run(move || {
            let (pipeline, _) = from_json.lock().unwrap().make_actor().unwrap();
            let pipeline = pipeline.unwrap();
            let res = pipeline.send(Event {
                data: EventData::new(0, 0, None, EventValue::Raw(vec![])),
            });
            Arbiter::spawn(res.then(|res| {
                match res {
                    Ok(result) => info!("ok: {:?}", result),
                    _ => error!("Something wrong"),
                };
                System::current().stop();
                future::result(Ok(()))
            }));
        });
    }

}
