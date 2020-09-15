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

use crate::op::prelude::*;
use tremor_script;
use tremor_script::prelude::*;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub(crate) struct TrickleOperator {
    pub id: String,
    pub stmt: tremor_script::query::StmtRentalWrapper,
    pub op: Box<dyn Operator>,
}

fn mk_node_confiug<'script>(
    id: Cow<'static, str>,
    op_type: String,
    config: Option<HashMap<String, Value<'script>>>,
) -> NodeConfig {
    NodeConfig {
        id,
        kind: crate::NodeKind::Operator,
        op_type,
        config: config.map(|v| {
            serde_yaml::Value::from(
                v.iter()
                    .filter_map(|(k, v)| {
                        let mut v = v.encode();
                        Some((
                            serde_yaml::Value::from(k.as_str()),
                            simd_json::serde::from_str::<serde_yaml::Value>(&mut v).ok()?,
                        ))
                    })
                    .collect::<serde_yaml::Mapping>(),
            )
        }),
        defn: None,
        node: None,
    }
}

impl TrickleOperator {
    #[allow(clippy::too_many_lines)]
    pub fn with_stmt(
        id: String,
        stmt_rentwrapped: tremor_script::query::StmtRentalWrapper,
    ) -> Result<Self> {
        use crate::operator;
        let stmt = stmt_rentwrapped.suffix();
        let op: Box<dyn Operator> = match stmt {
            tremor_script::ast::Stmt::OperatorDecl(ref op) => {
                let config = mk_node_confiug(
                    op.id.clone().into(),
                    format!("{}::{}", op.kind.module, op.kind.operation),
                    op.params.clone(),
                );
                operator(&config)?
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
    ) -> Result<EventAndInsights> {
        self.op.on_event(port, state, event)
    }

    fn handles_signal(&self) -> bool {
        self.op.handles_signal()
    }
    fn on_signal(&mut self, signal: &mut Event) -> Result<EventAndInsights> {
        self.op.on_signal(signal)
    }

    fn handles_contraflow(&self) -> bool {
        self.op.handles_contraflow()
    }
    fn on_contraflow(&mut self, contraevent: &mut Event) {
        self.op.on_contraflow(contraevent)
    }

    fn metrics(
        &self,
        tags: HashMap<Cow<'static, str>, Value<'static>>,
        timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        self.op.metrics(tags, timestamp)
    }

    fn skippable(&self) -> bool {
        self.op.skippable()
    }
}
