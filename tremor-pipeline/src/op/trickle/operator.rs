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

use tremor_script::{self};

#[derive(Debug)]
pub struct TrickleOperator {
    pub id: String,
    pub stmt: tremor_script::StmtRentalWrapper,
    pub op: Box<dyn Operator>,
}

impl TrickleOperator {
    pub fn with_stmt(
        id: String,
        stmt_rentwrapped: tremor_script::StmtRentalWrapper,
    ) -> TrickleOperator {
        use crate::op;
        use simd_json::value::ValueTrait;
        let stmt = stmt_rentwrapped.stmt.suffix();
        let op: Box<dyn Operator> = match stmt {
            tremor_script::ast::Stmt::OperatorDecl(ref op) => {
                match (op.kind.module.as_str(), op.kind.operation.as_str()) {
                    ("debug", "history") => {
                        Box::new(op::debug::history::EventHistory {
                            config: op::debug::history::Config {
                                // FIXME hygienic error handling
                                op: op.params.as_ref().expect("expected op to be configured")["op"]
                                    .to_string(),
                                name: op.params.as_ref().expect("expected name to be configured")
                                    ["name"]
                                    .to_string(),
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
                                    .expect("expected timeout to be configured")["timeout"]
                                    .cast_f64()
                                    .expect("snot badger"),
                                steps: op.params.as_ref().expect("expected steps to be configured")
                                    ["steps"]
                                    .as_array()
                                    .expect("expected to be an array type")
                                    .iter()
                                    .map(|x| x.as_u64().expect("snot biscuit"))
                                    .collect(),
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
                            .expect("expected timeout to be configured")["count"]
                            .as_u64()
                            .expect("snot badger") as usize;
                        let timeout = match op.params.as_ref().expect("...").get("timeout") {
                            Some(v) => Some(v.as_u64()),
                            None => None,
                        };
                        let max_delay_ns = if let Some(max_delay_ms) = timeout {
                            Some(max_delay_ms.expect("") * 1_000_000)
                        } else {
                            None
                        };
                        Box::new(op::generic::batch::Batch {
                            config: op::generic::batch::Config {
                                count,
                                timeout: timeout.expect(""),
                            },
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
                    _ => {
                        unreachable!("invalid");
                    }
                }
            }
            _ => {
                unreachable!("bad operator");
            }
        };

        TrickleOperator {
            id,
            stmt: stmt_rentwrapped,
            op,
        }
    }
}

impl Operator for TrickleOperator {
    #[allow(clippy::transmute_ptr_to_ptr)]
    #[allow(mutable_transmutes)]
    fn on_event(&mut self, port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        self.op.on_event(port, event)
    }
}
