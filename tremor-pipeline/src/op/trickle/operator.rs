// Copyright 2018-2020, Wayfair GmbH
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

use crate::common_cow;
use crate::errors::missing_config;
use crate::op::prelude::*;
use tremor_script::prelude::*;

use tremor_script::{self};

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub(crate) struct TrickleOperator {
    pub id: String,
    pub stmt: tremor_script::query::StmtRentalWrapper,
    pub op: Box<dyn Operator>,
}

impl TrickleOperator {
    #[allow(clippy::too_many_lines)]
    pub fn with_stmt(
        id: String,
        stmt_rentwrapped: tremor_script::query::StmtRentalWrapper,
    ) -> Result<Self> {
        use crate::op;
        let stmt = stmt_rentwrapped.suffix();
        let op: Box<dyn Operator> = match stmt {
            tremor_script::ast::Stmt::OperatorDecl(ref op) => {
                match (op.kind.module.as_str(), op.kind.operation.as_str()) {
                    ("debug", "history") => {
                        Box::new(op::debug::history::EventHistory {
                            config: op::debug::history::Config {
                                // FIXME hygienic error handling
                                op: op
                                    .params
                                    .as_ref()
                                    .and_then(|v| v.get("op"))
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| missing_config("op"))?
                                    .to_owned(),
                                name: op
                                    .params
                                    .as_ref()
                                    .and_then(|v| v.get("name"))
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| missing_config("name"))?
                                    .to_owned(),
                            },
                            id: op.id.clone().into(),
                        })
                    }
                    ("generic", "backpressure") => {
                        use op::generic::backpressure::{Backpressure, Config, Output};
                        let outputs = op
                            .params
                            .as_ref()
                            .and_then(|v| v.get("outputs"))
                            .and_then(Value::as_array)
                            .and_then(|os| {
                                os.iter()
                                    .map(|o| o.as_str().map(ToString::to_string))
                                    .collect::<Option<Vec<String>>>()
                            })
                            .unwrap_or_default();
                        let steps = op
                            .params
                            .as_ref()
                            .and_then(|v| v.get("steps"))
                            .and_then(Value::as_array)
                            .and_then(|a| {
                                a.iter().map(|x| x.as_u64()).collect::<Option<Vec<u64>>>()
                            })
                            .ok_or_else(|| missing_config("timeout"))?;
                        Box::new(Backpressure {
                            outputs: outputs.iter().cloned().map(Output::from).collect(),
                            config: Config {
                                // FIXME hygienic error handling
                                timeout: op
                                    .params
                                    .as_ref()
                                    .and_then(|v| v.get("timeout"))
                                    .and_then(Value::cast_f64)
                                    .ok_or_else(|| missing_config("timeout"))?,
                                steps: steps.clone(),
                                outputs: op
                                    .params
                                    .as_ref()
                                    .and_then(|v| v.get("outputs"))
                                    .and_then(Value::as_array)
                                    .and_then(|os| {
                                        os.iter()
                                            .map(|o| o.as_str().map(ToString::to_string))
                                            .collect::<Option<Vec<String>>>()
                                    })
                                    .unwrap_or_default(),
                            },
                            steps,
                            next: 0,
                        })
                    }
                    ("generic", "batch") => {
                        // FIXME hygienic error handling
                        let count = op
                            .params
                            .as_ref()
                            .and_then(|v| v.get("count"))
                            .and_then(Value::as_usize)
                            .ok_or_else(|| missing_config("count"))?;
                        let timeout = op
                            .params
                            .as_ref()
                            .and_then(|v| v.get("timeout"))
                            .and_then(Value::as_u64);
                        let max_delay_ns = if let Some(max_delay_ms) = timeout {
                            Some(max_delay_ms * 1_000_000)
                        } else {
                            None
                        };
                        Box::new(op::generic::batch::Batch {
                            config: op::generic::batch::Config { count, timeout },
                            event_id: 0,
                            len: 0,
                            data: op::generic::batch::empty(),
                            max_delay_ns,
                            first_ns: 0,
                            id: common_cow(&op.id),
                        })
                    }
                    ("generic", "wal") => {
                        op::generic::WalFactory::new_boxed().from_node(&NodeConfig {
                            id: "we need to clean up this entire creation code".into(),
                            kind: crate::NodeKind::Operator,
                            op_type: "generic::wal".into(),
                            config: op.params.clone().map(|v| {
                                serde_yaml::Value::from(
                                    v.iter()
                                        .filter_map(|(k, v)| {
                                            let mut v = v.encode();
                                            Some((
                                                serde_yaml::Value::from(k.as_str()),
                                                simd_json::serde::from_str::<serde_yaml::Value>(
                                                    &mut v,
                                                )
                                                .ok()?,
                                            ))
                                        })
                                        .collect::<serde_yaml::Mapping>(),
                                )
                            }),
                            defn: None,
                            node: None,
                        })?
                    }

                    ("generic", "counter") => Box::new(op::generic::counter::Counter {}),
                    ("grouper", "bucket") => Box::new(op::grouper::bucket::Grouper {
                        buckets: HashMap::new(),
                        _id: common_cow(&op.id),
                    }),
                    (s, o) => return Err(ErrorKind::UnknownOp(s.into(), o.into()).into()),
                }
            }
            _ => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non operator into a operator".into(),
                )
                .into())
            }
        };

        Ok(Self {
            id,
            stmt: stmt_rentwrapped,
            op,
        })
    }
}

impl Operator for TrickleOperator {
    fn on_event(
        &mut self,
        port: &str,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<Vec<(Cow<'static, str>, Event)>> {
        self.op.on_event(port, state, event)
    }
}
