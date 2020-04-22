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

use crate::errors::Result;
use crate::registry::ServantId;
use crate::repository::Artefact;
use crate::system::World;
use std::fmt;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ActivationState {
    Deactivated,
    Activated,
    Zombie,
}

#[derive(Clone)]
pub struct ActivatorLifecycleFsm<A: Artefact> {
    pub artefact: A,
    world: World,
    pub state: ActivationState,
    pub resolution: Option<A::SpawnResult>,
    id: ServantId,
}

impl<A: Artefact> fmt::Debug for ActivatorLifecycleFsm<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LifecycleFsm {{ id: {}, state: {:?} }}",
            self.id, self.state
        )
    }
}

impl<A: Artefact> ActivatorLifecycleFsm<A> {
    pub async fn new(world: World, artefact: A, id: ServantId) -> Result<Self> {
        let mut fresh = Self {
            artefact,
            world,
            state: ActivationState::Deactivated,
            resolution: None,
            id,
        };
        let resoluion = fresh.on_spawn().await?; // Initial transition
        fresh.resolution = Some(resoluion);
        Ok(fresh)
    }

    async fn on_spawn(&self) -> Result<A::SpawnResult> {
        self.artefact.spawn(&self.world, self.id.clone()).await
    }

    fn on_activate(&self) {
        // ALLOW: this is for clippy
        let _clippy = self;
        debug!(
            "Lifecycle on_activate not implemented state is now: {:?}",
            ActivationState::Activated
        );
    }

    fn on_passivate(&self) {
        // ALLOW: this is for clippy
        let _clippy = self;
        debug!("Lifecycle on_passivate not implemented");
    }

    fn on_destroy(&self) {
        // ALLOW: this is for clippy
        let _clippy = self;
        debug!(
            "Lifecycle on_destroy not implemented state is now:: {:?}",
            ActivationState::Zombie
        );
    }
}

impl<A: Artefact> ActivatorLifecycleFsm<A> {
    pub fn transition(&mut self, to: ActivationState) -> Result<&mut Self> {
        loop {
            match (&self.state, &to) {
                (ActivationState::Deactivated, ActivationState::Activated) => {
                    self.state = ActivationState::Activated;
                    self.on_activate();
                    break;
                }
                (ActivationState::Deactivated, ActivationState::Zombie) => {
                    self.state = ActivationState::Zombie;
                    self.on_destroy();
                    break;
                }
                (ActivationState::Activated, ActivationState::Zombie) => {
                    self.state = ActivationState::Deactivated;
                    self.on_passivate();
                    continue; // NOTE by composition Active -> Deactive, Deactive -> Zombie
                }
                (ActivationState::Zombie, _) => break,
                _ => {
                    // NOTE Default transition is 'do nothing'
                    //    TODO dev mode -> defaults to panic ( not yet implemented )
                    //    TODO prod mode -> defaults to loopback / noop / do nothing
                    //    TODO convenience macro loopback!
                    return Err("Illegel State Transition".into());
                }
            };
        }

        Ok(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use crate::incarnate;
    use crate::repository::BindingArtefact;
    use crate::system::World;
    use crate::url::TremorURL;
    use std::fs::File;
    use std::io::BufReader;

    macro_rules! b {
        ($f:expr) => {
            async_std::task::block_on($f)
        };
    }

    fn slurp(file: &str) -> config::Config {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).expect("failed to parse file")
    }

    #[test]
    fn pipeline_activation_lifecycle() {
        let (world, _) = b!(World::start(10, None)).expect("failed to start world");

        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).expect("failed to incarnate runtime");
        let pipeline = runtime.pipes.pop().expect("artefact not found");
        let id = TremorURL::parse("/pipeline/test/snot").expect("failed to parse id");

        assert!(b!(world.repo.find_pipeline(&id))
            .expect("failed to communicate to repository")
            .is_none());

        // Legal <initial> -> Deactivated
        assert_eq!(
            Ok(ActivationState::Deactivated),
            b!(world.bind_pipeline_from_artefact(&id, pipeline.into()))
        );

        // Legal Deactivated -> Activated
        assert_eq!(
            Ok(ActivationState::Activated),
            b!(world
                .reg
                .transition_pipeline(&id, ActivationState::Activated))
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world.reg.transition_pipeline(&id, ActivationState::Zombie))
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world
                .reg
                .transition_pipeline(&id, ActivationState::Deactivated))
        );

        // Zombies don't return from the deady
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world
                .reg
                .transition_pipeline(&id, ActivationState::Activated))
        );
    }

    #[test]
    fn onramp_activation_lifecycle() {
        let (world, _) = b!(World::start(10, None)).expect("failed to start world");

        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).expect("failed to incarnate runtime");
        let artefact = runtime.onramps.pop().expect("artefact not found");
        let id = TremorURL::parse("/onramp/test/00").expect("artefact not found");
        assert!(b!(world.repo.find_onramp(&id))
            .expect("failed to communicate to repository")
            .is_none());

        assert!(b!(world.repo.publish_onramp(&id, false, artefact)).is_ok());

        // Legal <initial> -> Deactivated
        assert_eq!(Ok(ActivationState::Deactivated), b!(world.bind_onramp(&id)),);

        // Legal Deactivated -> Activated
        assert_eq!(
            Ok(ActivationState::Activated),
            b!(world.reg.transition_onramp(&id, ActivationState::Activated))
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world.reg.transition_onramp(&id, ActivationState::Zombie))
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world
                .reg
                .transition_onramp(&id, ActivationState::Deactivated))
        );

        // Zombies don't return from the deady
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world.reg.transition_onramp(&id, ActivationState::Activated))
        );
    }

    #[test]
    fn offramp_activation_lifecycle() {
        let (world, _) = b!(World::start(10, None)).expect("failed to start world");

        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).expect("failed to incarnate runtime");
        let artefact = runtime.offramps.pop().expect("artefact not found");
        let id = TremorURL::parse("/offramp/test/00").expect("artefact not found");
        assert!(b!(world.repo.find_offramp(&id))
            .expect("failed to communicate to repository")
            .is_none());

        assert!(b!(world.repo.publish_offramp(&id, false, artefact)).is_ok());

        // Legal <initial> -> Deactivated
        assert_eq!(
            Ok(ActivationState::Deactivated),
            b!(world.bind_offramp(&id))
        );

        // Legal Deactivated -> Activated
        assert_eq!(
            Ok(ActivationState::Activated),
            b!(world
                .reg
                .transition_offramp(&id, ActivationState::Activated))
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world.reg.transition_offramp(&id, ActivationState::Zombie))
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world
                .reg
                .transition_offramp(&id, ActivationState::Deactivated))
        );

        // Zombies don't return from the deady
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world
                .reg
                .transition_offramp(&id, ActivationState::Activated))
        );
    }

    #[test]
    fn binding_activation_lifecycle() {
        let (world, _) = b!(World::start(10, None)).expect("failed to start world");

        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).expect("failed to incarnate runtime");
        let artefact = BindingArtefact {
            binding: runtime.bindings.pop().expect("artefact not found"),
            mapping: None,
        };
        let id = TremorURL::parse("/binding/test/snot").expect("artefact not found");

        assert!(b!(world.repo.find_binding(&id))
            .expect("failed to communicate to repository")
            .is_none());

        assert!(b!(world.repo.publish_binding(&id, false, artefact.clone())).is_ok());

        assert!(b!(world.reg.find_binding(&id))
            .expect("failed to communicate to registry")
            .is_none());

        // Legal <initial> -> Deactivated
        assert_eq!(
            Ok(ActivationState::Deactivated),
            b!(world.bind_binding_a(&id, &artefact)),
        );

        assert!(b!(world.reg.find_binding(&id))
            .expect("failed to communicate to registry")
            .is_some());

        // Legal Deactivated -> Activated
        assert_eq!(
            Ok(ActivationState::Activated),
            b!(world
                .reg
                .transition_binding(&id, ActivationState::Activated))
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world.reg.transition_binding(&id, ActivationState::Zombie))
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world
                .reg
                .transition_binding(&id, ActivationState::Deactivated))
        );

        // Zombies don't return from the deady
        assert_eq!(
            Ok(ActivationState::Zombie),
            b!(world
                .reg
                .transition_binding(&id, ActivationState::Activated))
        );

        // TODO - full undeployment 'white-box' acceptance tests
        //        println!("TODO {:?}", world.repo.unpublish_binding(&id));
        let _r = b!(world.unbind_binding_a(&id, &artefact));
        //        assert!(world.repo.unpublish_binding(&id).is_ok());
        println!(
            "TODO {:?}",
            b!(world.reg.find_binding(&id)).expect("failed to communicate to registry")
        );
        assert!(b!(world.reg.find_binding(&id))
            .expect("failed to communicate to registry")
            .is_none());
    }
}
