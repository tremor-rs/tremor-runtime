// Copyright 2020-2021, The Tremor Team
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
use tremor_script::{
    ast::{self, Helper},
    prelude::*,
};
#[derive(Debug)]
pub(crate) struct TrickleOperator {
    op: Box<dyn Operator>,
}

fn mk_node_config(id: String, op_type: String, config: Value) -> NodeConfig {
    NodeConfig {
        id,
        kind: crate::NodeKind::Operator,
        op_type,
        config: if config
            .as_object()
            .map(HashMap::is_empty)
            .unwrap_or_default()
        {
            None
        } else {
            Some(config.into_static())
        },
        ..NodeConfig::default()
    }
}

impl TrickleOperator {
    pub fn with_stmt(
        operator_uid: OperatorUId,
        defn: &ast::OperatorDefinition<'static>,
        helper: &mut Helper,
    ) -> Result<Self> {
        use crate::operator;

        let op = defn.clone();
        let config = mk_node_config(
            op.id.clone(),
            format!("{}::{}", op.kind.module, op.kind.operation),
            op.params.generate_config(helper)?,
        );

        Ok(Self {
            op: operator(operator_uid, &config)?,
        })
    }
}

impl Operator for TrickleOperator {
    fn on_event(
        &mut self,
        node_id: u64,
        uid: OperatorUId,
        port: &Port<'static>,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        self.op.on_event(node_id, uid, port, state, event)
    }

    fn handles_signal(&self) -> bool {
        self.op.handles_signal()
    }
    fn on_signal(
        &mut self,
        node_id: u64,
        uid: OperatorUId,
        state: &mut Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        self.op.on_signal(node_id, uid, state, signal)
    }

    fn handles_contraflow(&self) -> bool {
        self.op.handles_contraflow()
    }
    fn on_contraflow(&mut self, uid: OperatorUId, contraevent: &mut Event) {
        self.op.on_contraflow(uid, contraevent);
    }

    fn metrics(&self, tags: &Object<'static>, timestamp: u64) -> Result<Vec<Value<'static>>> {
        self.op.metrics(tags, timestamp)
    }

    fn skippable(&self) -> bool {
        self.op.skippable()
    }
}
