// Copyright 2018-2019, Wayfair GmbH
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

use crate::errors::*;
use crate::{Event, Operator};
use tremor_script::prelude::*;

use tremor_script::{self};

#[derive(Debug)]
pub struct TrickleOperator {
    pub id: String,
    pub stmt: tremor_script::query::StmtRentalWrapper,
    pub op: Box<dyn Operator>,
}

impl TrickleOperator {
    pub fn with_stmt(
        id: String,
        stmt_rentwrapped: tremor_script::query::StmtRentalWrapper,
    ) -> Result<TrickleOperator> {
        use crate::op;
        let stmt = stmt_rentwrapped.stmt.suffix();
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
                                    .ok_or_else(|| error_missing_config("op"))?
                                    .to_owned(),
                                name: op
                                    .params
                                    .as_ref()
                                    .and_then(|v| v.get("name"))
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| error_missing_config("name"))?
                                    .to_owned(),
                            },
                            id: op.id.to_string(),
                        })
                    }
                    ("generic", "backpressure") => {
                        Box::new(op::generic::backpressure::Backpressure {
                            config: op::generic::backpressure::Config {
                                // FIXME hygienic error handling
                                timeout: op
                                    .params
                                    .as_ref()
                                    .and_then(|v| v.get("timeout"))
                                    .and_then(Value::cast_f64)
                                    .ok_or_else(|| error_missing_config("timeout"))?,
                                steps: op
                                    .params
                                    .as_ref()
                                    .and_then(|v| v.get("steps"))
                                    .and_then(Value::as_array)
                                    .and_then(|a| {
                                        a.iter().map(|x| x.as_u64()).collect::<Option<Vec<u64>>>()
                                    })
                                    .ok_or_else(|| error_missing_config("timeout"))?,
                            },
                            backoff: 0,
                            last_pass: 0,
                        })
                    }
                    ("generic", "batch") => {
                        // FIXME hygienic error handling
                        let count = op
                            .params
                            .as_ref()
                            .and_then(|v| v.get("count"))
                            .and_then(Value::as_usize)
                            .ok_or_else(|| error_missing_config("count"))?;
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
                            id: op.id.clone(),
                        })
                    }
                    ("grouper", "bucket") => {
                        use halfbrown::HashMap;
                        Box::new(op::grouper::bucket::BucketGrouper {
                            buckets: HashMap::new(),
                            _id: op.id.clone(),
                        })
                    }
                    (s, o) => return Err(ErrorKind::UnknownOp(s.into(), o.into()).into()),
                }
            }
            _ => {
                unreachable!("bad operator");
            }
        };

        Ok(TrickleOperator {
            id,
            stmt: stmt_rentwrapped,
            op,
        })
    }
}

impl Operator for TrickleOperator {
    #[allow(clippy::transmute_ptr_to_ptr)]
    #[allow(mutable_transmutes)]
    fn on_event(&mut self, port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        self.op.on_event(port, event)
    }
}
