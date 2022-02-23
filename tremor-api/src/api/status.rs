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


use tremor_runtime::instance::InstanceState;
use halfbrown::HashMap;

use crate::api::prelude::*;

#[derive(Serialize, Debug)]
struct RuntimeStatus {
    all_running: bool,
    num_flows: usize,
    flows: HashMap<InstanceState, usize>
}

impl Default for RuntimeStatus {
    fn default() -> Self {
        Self {
            all_running: true,
            num_flows: 0,
            flows: HashMap::new()
        }
    }
}


pub(crate) async fn get_runtime_status(req: Request) -> Result<Response> {
    let world = &req.state().world;
    
    let flows = world.get_flows().await?;
    let mut runtime_status = RuntimeStatus::default();
    runtime_status.num_flows = flows.len();
    let mut all_in_good_state: bool = true;
    
    for flow in &flows {
        let status = flow.report_status().await?;
        *runtime_status.flows.entry(status.status).or_insert(0) += 1;

        // report OK if all flows are in a good/intended state (Running)
        runtime_status.all_running = runtime_status.all_running && status.status == InstanceState::Running;
        all_in_good_state = all_in_good_state && matches!(status.status, InstanceState::Running | InstanceState::Paused | InstanceState::Initializing);
    }
    let code = if all_in_good_state {
        StatusCode::Ok
    } else {
        StatusCode::ServiceUnavailable
    };
    reply(&req, runtime_status, code)
}