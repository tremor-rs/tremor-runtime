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

use crate::config;
use crate::errors::*;
use tremor_pipeline;

pub mod codec;
pub mod offramp;
pub mod onramp;

pub type OnRampVec = Vec<OnRamp>;
pub type OffRampVec = Vec<OffRamp>;
pub type BindingVec = config::BindingVec;

pub type PipelineVec = Vec<tremor_pipeline::Pipeline>;

pub use crate::config::Binding;
pub use crate::config::OffRamp;
pub use crate::config::OnRamp;
pub use serde_yaml::Value as OpConfig;
pub use tremor_pipeline::Event;
pub use tremor_pipeline::EventValue;

#[derive(Debug)]
pub struct Todo {
    pub onramps: OnRampVec,
    pub offramps: OffRampVec,
    pub bindings: BindingVec,
    pub pipes: PipelineVec,
}

pub fn incarnate(config: config::Config) -> Result<Todo> {
    let onramps = incarnate_onramps(config.onramp.clone());
    let offramps = incarnate_offramps(config.offramp.clone());
    let bindings = incarnate_links(config.binding);
    let pipes = incarnate_pipes(config.pipeline)?;
    // validate links ...
    // ... registry
    // check conflicts ( deploys, pipes )
    // check deps ( links )
    // push deploys, pipes, .... links ( always last )
    Ok(Todo {
        onramps,
        offramps,
        bindings,
        pipes,
    })
}

fn incarnate_onramps(config: config::OnRampVec) -> OnRampVec {
    config.into_iter().map(|d| d).collect()
}

fn incarnate_offramps(config: config::OffRampVec) -> OffRampVec {
    config.into_iter().map(|d| d).collect()
}

fn incarnate_links(config: config::BindingVec) -> BindingVec {
    config.clone()
}

pub fn incarnate_pipes(config: config::PipelineVec) -> Result<PipelineVec> {
    config
        .into_iter()
        .map(|d| Ok(tremor_pipeline::build_pipeline(d)?))
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use crate::url::TremorURL;
    use serde_yaml;
    use std::fs::File;
    use std::io::BufReader;

    fn slurp(file: &str) -> config::Config {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).unwrap()
    }

    #[test]
    fn load_simple_deploys() {
        let config = slurp("tests/configs/deploy.simple.yaml");
        println!("{:?}", config);
        let runtime = incarnate(config).unwrap();
        assert_eq!(1, runtime.onramps.len());
        assert_eq!(1, runtime.offramps.len());
        assert_eq!(0, runtime.bindings.len());
        assert_eq!(0, runtime.pipes.len());
    }

    #[test]
    #[ignore] // The order of links isn't specified, this text needs to be fixed
    fn load_simple_links() {
        let config = slurp("tests/configs/link.simple.yaml");
        let runtime = incarnate(config).unwrap();
        assert_eq!(0, runtime.onramps.len());
        assert_eq!(0, runtime.offramps.len());
        assert_eq!(2, runtime.bindings[0].links.len());
        assert_eq!(
            TremorURL::parse("/onramp/bench-001/blaster").unwrap(),
            *runtime.bindings[0].links.iter().next().unwrap().0
        );
        assert_eq!(
            TremorURL::parse("/pipeline/main/in").unwrap(),
            runtime.bindings[0].links.iter().next().unwrap().1[0]
        );
        assert_eq!(0, runtime.pipes.len());
    }

    #[test]
    fn load_simple_pipes() {
        let config = slurp("tests/configs/pipe.simple.yaml");
        println!("{:?}", &config);
        let runtime = incarnate(config).unwrap();
        assert_eq!(0, runtime.onramps.len());
        assert_eq!(0, runtime.offramps.len());
        assert_eq!(0, runtime.bindings.len());
        assert_eq!(1, runtime.pipes.len());
    }

    #[test]
    fn load_passthrough_stream() {
        let config = slurp("tests/configs/ut.passthrough.yaml");
        println!("{:?}", &config);
        let runtime = incarnate(config).unwrap();
        assert_eq!(1, runtime.onramps.len());
        assert_eq!(1, runtime.offramps.len());
        assert_eq!(2, runtime.bindings[0].links.len());
        assert_eq!(1, runtime.pipes.len());
    }

    #[test]
    fn load_passthrough_op() {
        let config = slurp("tests/configs/ut.single-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_branch_op() {
        let config = slurp("tests/configs/ut.branch-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine_op() {
        let config = slurp("tests/configs/ut.combine-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine2_op() {
        let config = slurp("tests/configs/ut.combine2-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine3_op() {
        let config = slurp("tests/configs/ut.combine3-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_ok());
    }

    #[test]
    fn load_combine4_op_cycle_error() {
        let config = slurp("tests/configs/ut.combine4-op.yaml");
        println!("{:?}", &config);
        assert!(incarnate(config).is_err());
    }

    #[test]
    fn pipeline_to_runner() {}
}
