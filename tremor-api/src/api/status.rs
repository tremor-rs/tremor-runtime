// Copyright 2022, The Tremor Team
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

//! Runtime status API

use halfbrown::HashMap;
use tremor_system::instance::State;

use crate::api::prelude::*;

#[derive(Serialize, Debug)]
struct RuntimeStatus {
    all_running: bool,
    num_flows: usize,
    flows: HashMap<State, usize>,
}

impl RuntimeStatus {
    fn new(num_flows: usize) -> Self {
        Self {
            num_flows,
            ..Self::default()
        }
    }
    fn add_flow(&mut self, status: State) {
        *self.flows.entry(status).or_insert(0) += 1;
        self.all_running = self.all_running && status == State::Running;
    }
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            all_running: true,
            num_flows: 0,
            flows: HashMap::new(),
        }
    }
}

pub(crate) async fn get_runtime_status(req: Request) -> Result<Response> {
    let runtime = &req.state().runtime;

    let flows = runtime.get_flows().await?;
    let mut runtime_status = RuntimeStatus::new(flows.len());
    let mut all_in_good_state: bool = true;

    for flow in &flows {
        let status = flow.report_status().await?;
        runtime_status.add_flow(status.status);

        // report OK if all flows are in a good/intended state (Running)
        all_in_good_state = all_in_good_state
            && matches!(
                status.status,
                State::Running | State::Paused | State::Initializing
            );
    }
    let code = if all_in_good_state {
        StatusCode::Ok
    } else {
        StatusCode::ServiceUnavailable
    };
    reply(&req, runtime_status, code)
}
