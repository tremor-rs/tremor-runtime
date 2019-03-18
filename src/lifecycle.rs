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
use crate::registry::ServantId;
use crate::repository::Artefact;
use crate::system::World;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ActivationState {
    Deactivated,
    Activated,
    Zombie,
}

pub trait Transition<S>: Sized {
    fn transition(&mut self, to: S) -> Result<&mut Self>;
}

#[derive(Clone)]
pub struct ActivatorLifecycleFsm<A: Artefact> {
    artefact: A,
    world: World,
    pub state: ActivationState,
    pub resolution: Option<A::SpawnResult>,
    id: ServantId,
}

impl<A: Artefact> ActivatorLifecycleFsm<A> {
    pub fn new(world: World, artefact: A, id: ServantId) -> Result<Self> {
        let mut fresh = ActivatorLifecycleFsm {
            artefact,
            world,
            state: ActivationState::Deactivated,
            resolution: None,
            id,
        };
        let resoluion = fresh.on_spawn()?; // Initial transition
        fresh.resolution = Some(resoluion);
        Ok(fresh)
    }

    fn on_spawn(&self) -> Result<A::SpawnResult> {
        self.artefact.spawn(&self.world, self.id.clone())
    }

    fn on_activate(&self) {
        debug!(
            "Lifecycle on_activate not implemented state is now: {:?}",
            ActivationState::Activated
        );
    }

    fn on_passivate(&self) {
        debug!("Lifecycle on_passivate not implemented");
    }

    fn on_destroy(&self) {
        debug!(
            "Lifecycle on_destroy not implemented state is now:: {:?}",
            ActivationState::Zombie
        );
    }
}

impl<A: Artefact> Transition<ActivationState> for ActivatorLifecycleFsm<A> {
    fn transition(&mut self, to: ActivationState) -> Result<&mut Self> {
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
                    return Err("eIllegel State Transition".into());
                }
            };
        }

        Ok(self)
    }
}

#[cfg(test)]
mod test {
    //use super::Transition;
    use super::*;
    use crate::config;
    use crate::dynamic::incarnate;
    use crate::system::World;
    use crate::url::TremorURL;
    use std::fs::File;
    use std::io::BufReader;

    fn slurp(file: &str) -> config::Config {
        let file = File::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).unwrap()
    }

    #[test]
    fn pipeline_activation_lifecycle() {
        let (world, _) = World::start(10).unwrap();

        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).unwrap();
        let pipeline = runtime.pipes.pop().unwrap();
        let id = TremorURL::parse("/pipeline/test/snot").unwrap();

        assert!(world.repo.find_pipeline(&id).unwrap().is_none());

        // Legal <initial> -> Deactivated
        assert_eq!(
            world.bind_pipeline_from_artefact(&id, pipeline).unwrap(),
            ActivationState::Deactivated
        );

        // Legal Deactivated -> Activated
        assert_eq!(
            world
                .reg
                .transition_pipeline(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Activated
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            world
                .reg
                .transition_pipeline(&id, ActivationState::Zombie)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the dead
        assert_eq!(
            world
                .reg
                .transition_pipeline(&id, ActivationState::Deactivated)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the deady
        assert_eq!(
            world
                .reg
                .transition_pipeline(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Zombie
        );
    }

    #[test]
    fn onramp_activation_lifecycle() {
        let (world, _) = World::start(10).unwrap();

        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).unwrap();
        let artefact = runtime.onramps.pop().unwrap();
        let id = TremorURL::parse("/onramp/test/00").unwrap();
        assert!(world.repo.find_onramp(&id).unwrap().is_none());

        assert!(world.repo.publish_onramp(&id, artefact).is_ok());

        // Legal <initial> -> Deactivated
        assert_eq!(
            world.bind_onramp(&id).unwrap(),
            ActivationState::Deactivated
        );

        // Legal Deactivated -> Activated
        assert_eq!(
            world
                .reg
                .transition_onramp(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Activated
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            world
                .reg
                .transition_onramp(&id, ActivationState::Zombie)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the dead
        assert_eq!(
            world
                .reg
                .transition_onramp(&id, ActivationState::Deactivated)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the deady
        assert_eq!(
            world
                .reg
                .transition_onramp(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Zombie
        );
    }

    #[test]
    fn offramp_activation_lifecycle() {
        let (world, _) = World::start(10).unwrap();

        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).unwrap();
        let artefact = runtime.offramps.pop().unwrap();
        let id = TremorURL::parse("/offramp/test/00").unwrap();
        assert!(world.repo.find_offramp(&id).unwrap().is_none());

        assert!(world.repo.publish_offramp(&id, artefact).is_ok());

        // Legal <initial> -> Deactivated
        assert_eq!(
            world.bind_offramp(&id).unwrap(),
            ActivationState::Deactivated
        );

        // Legal Deactivated -> Activated
        assert_eq!(
            world
                .reg
                .transition_offramp(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Activated
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            world
                .reg
                .transition_offramp(&id, ActivationState::Zombie)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the dead
        assert_eq!(
            world
                .reg
                .transition_offramp(&id, ActivationState::Deactivated)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the deady
        assert_eq!(
            world
                .reg
                .transition_offramp(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Zombie
        );
    }

    #[test]
    fn binding_activation_lifecycle() {
        let (world, _) = World::start(10).unwrap();
        let config = slurp("tests/configs/ut.passthrough.yaml");
        let mut runtime = incarnate(config).unwrap();
        let artefact = runtime.bindings.pop().unwrap();
        let id = TremorURL::parse("/binding/test/snot").unwrap();

        assert!(world.repo.find_binding(&id).unwrap().is_none());

        assert!(world.repo.publish_binding(&id, artefact.clone()).is_ok());
        assert!(world.reg.find_binding(&id).unwrap().is_none());

        // Legal <initial> -> Deactivated
        assert_eq!(
            world.bind_binding_a(&id, artefact.clone()).unwrap(),
            ActivationState::Deactivated
        );

        assert!(world.reg.find_binding(&id).unwrap().is_some());

        // Legal Deactivated -> Activated
        assert_eq!(
            world
                .reg
                .transition_binding(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Activated
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            world
                .reg
                .transition_binding(&id, ActivationState::Zombie)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the dead
        assert_eq!(
            world
                .reg
                .transition_binding(&id, ActivationState::Deactivated)
                .unwrap(),
            ActivationState::Zombie
        );

        // Zombies don't return from the deady
        assert_eq!(
            world
                .reg
                .transition_binding(&id, ActivationState::Activated)
                .unwrap(),
            ActivationState::Zombie
        );

        // TODO - full undeployment 'white-box' acceptance tests
        //        println!("TODO {:?}", world.repo.unpublish_binding(&id));
        let _r = world.unbind_binding_a(&id, artefact.clone());
        //        assert!(world.repo.unpublish_binding(&id).is_ok());
        println!("TODO {:?}", world.reg.find_binding(&id).unwrap());
        assert!(world.reg.find_binding(&id).unwrap().is_none());
    }
}
