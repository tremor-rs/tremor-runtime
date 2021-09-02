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

use crate::errors::Result;
use crate::registry::{Instance, ServantId};
use crate::repository::Artefact;
use crate::system::World;
use std::fmt;

/// Possible lifecycle states of an instance
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum InstanceState {
    Initialized,
    Running,
    Paused,
    Stopped,
}

//
//         Start
//       ┌────────────────────┐
//       │                    │
// ┌─────┤ Initialized        │
// │     │                    │
// │     └─────────┬──────────┘
// │               │
// │ stop          │ start
// │               │
// │               ▼
// │     ┌────────────────────┐  pause    ┌────────────────────┐
// │     │                    ├──────────►│                    │
// │     │ Running            │  resume   │ Paused             │
// │     │                    │◄──────────┤                    │
// │     └─────────┬──────────┘           └───┬────────────────┘
// │               │                          │
// │               │ stop                     │ stop
// │               │                          │
// │               │                          │
// │               ▼                          │
// │     ┌───────────────────┐                │
// │     │                   │                │
// └────►│ Stopped           │◄───────────────┘
//       │                   │
//       └───────────────────┘
//         End
/// Instance lifecycle FSM
#[derive(Clone)]
pub struct InstanceLifecycleFsm<A: Artefact> {
    /// The artefact this instance is derived from
    pub artefact: A,
    world: World,
    /// The current instance state
    pub state: InstanceState,
    /// the specialized spawn result - representing the living instance
    pub instance: A::SpawnResult,
    id: ServantId,
}

impl<A: Artefact> fmt::Debug for InstanceLifecycleFsm<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LifecycleFsm {{ id: {}, state: {:?} }}",
            self.id, self.state
        )
    }
}

impl<A: Artefact> InstanceLifecycleFsm<A> {
    /// -> Initialized
    pub async fn new(world: World, artefact: A, id: ServantId) -> Result<Self> {
        // delegating actual spawning to the artefact
        let instance = artefact.spawn(&world, id.clone()).await?;
        let fresh = Self {
            artefact,
            world,
            state: InstanceState::Initialized,
            instance,
            id,
        };
        Ok(fresh)
    }

    /// Initialized -> Running
    async fn on_start(&mut self) -> Result<()> {
        self.instance.start(&self.world, &self.id).await
    }

    /// Running -> Paused
    async fn on_pause(&mut self) -> Result<()> {
        self.instance.pause(&self.world, &self.id).await
    }

    /// Paused -> Running
    async fn on_resume(&mut self) -> Result<()> {
        self.instance.resume(&self.world, &self.id).await
    }

    /// _ -> Stopped
    async fn on_stop(&mut self) -> Result<()> {
        self.instance.stop(&self.world, &self.id).await
    }
}

impl<A: Artefact> InstanceLifecycleFsm<A> {
    pub async fn start(&mut self) -> Result<&mut Self> {
        self.transition(InstanceState::Running).await
    }

    pub async fn stop(&mut self) -> Result<&mut Self> {
        self.transition(InstanceState::Stopped).await
    }

    pub async fn pause(&mut self) -> Result<&mut Self> {
        self.transition(InstanceState::Paused).await
    }

    pub async fn resume(&mut self) -> Result<&mut Self> {
        self.transition(InstanceState::Running).await
    }

    pub async fn transition(&mut self, to: InstanceState) -> Result<&mut Self> {
        use InstanceState::{Initialized, Paused, Running, Stopped};
        match (&self.state, &to) {
            (Initialized, Running) => {
                self.state = Running;
                self.on_start().await?;
            }
            (_, Stopped) => {
                self.state = Stopped;
                self.on_stop().await?;
            }
            (Stopped, _) => {
                // do nothing
            }
            (Running, Paused) => {
                self.state = Paused;
                self.on_pause().await?;
            }
            (Paused, Running) => {
                self.state = Running;
                self.on_resume().await?;
            }
            (current, intended) if current == intended => {
                // do
            }
            _ => {
                return Err(format!(
                    "Illegal State Transition from {:?} to {:?}",
                    &self.state, &to
                )
                .into());
            }
        };

        Ok(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use crate::system::World;
    use crate::url::TremorUrl;
    use std::io::BufReader;
    use tremor_common::file as cfile;

    fn slurp(file: &str) -> config::Config {
        let file = cfile::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).expect("failed to parse file")
    }

    #[async_std::test]
    async fn onramp_activation_lifecycle() -> Result<()> {
        let (world, _) = World::start(10, None).await?;

        let mut config = slurp("tests/configs/ut.passthrough.yaml");
        let artefact = config.onramp.pop().expect("artefact not found");
        let id = TremorUrl::from_onramp_instance(&artefact.id, "snot")?;
        assert!(world
            .repo
            .find_onramp(&id)
            .await
            .expect("failed to communicate to repository")
            .is_none());

        assert!(world
            .repo
            .publish_onramp(&id, false, artefact)
            .await
            .is_ok());

        // Legal <initial> -> Running
        assert_eq!(Ok(InstanceState::Initialized), world.bind_onramp(&id).await,);

        // Legal Initialized -> Running
        assert_eq!(
            Ok(InstanceState::Running),
            world
                .reg
                .transition_onramp(&id, InstanceState::Running)
                .await
        );
        // Legal Running -> Paused
        assert_eq!(
            Ok(InstanceState::Paused),
            world
                .reg
                .transition_onramp(&id, InstanceState::Paused)
                .await
        );

        // Legal Paused -> Stopped
        assert_eq!(
            Ok(InstanceState::Stopped),
            world
                .reg
                .transition_onramp(&id, InstanceState::Stopped)
                .await
        );

        // stopped onramps cannot be transitioned back
        assert_eq!(
            Ok(InstanceState::Stopped),
            world
                .reg
                .transition_onramp(&id, InstanceState::Running)
                .await
        );
        Ok(())
    }

    #[async_std::test]
    async fn offramp_activation_lifecycle() {
        let (world, _) = World::start(10, None).await.expect("failed to start world");

        let mut config = slurp("tests/configs/ut.passthrough.yaml");
        let artefact = config.offramp.pop().expect("artefact not found");
        let id =
            TremorUrl::from_offramp_instance(&artefact.id, "snot").expect("artefact not found");
        assert!(world
            .repo
            .find_offramp(&id)
            .await
            .expect("failed to communicate to repository")
            .is_none());

        assert!(world
            .repo
            .publish_offramp(&id, false, artefact)
            .await
            .is_ok());

        // Legal <initial> -> Running
        assert_eq!(
            Ok(InstanceState::Initialized),
            world.bind_offramp(&id).await
        );

        // Legal Initialized -> Running
        assert_eq!(
            Ok(InstanceState::Running),
            world
                .reg
                .transition_offramp(&id, InstanceState::Running)
                .await
        );
        // Legal Running -> Paused
        assert_eq!(
            Ok(InstanceState::Paused),
            world
                .reg
                .transition_offramp(&id, InstanceState::Paused)
                .await
        );

        // Legal Paused -> Stopped
        assert_eq!(
            Ok(InstanceState::Stopped),
            world
                .reg
                .transition_offramp(&id, InstanceState::Stopped)
                .await
        );

        // Stopped offramps can't return from the dead
        assert_eq!(
            Ok(InstanceState::Stopped),
            world
                .reg
                .transition_offramp(&id, InstanceState::Running)
                .await
        );
    }

    #[async_std::test]
    async fn binding_activation_lifecycle() {
        let (world, _) = World::start(10, None).await.expect("failed to start world");

        // -> Initialized -> Running
        crate::load_cfg_file(&world, "tests/configs/ut.passthrough.yaml")
            .await
            .unwrap();
        let id = TremorUrl::from_binding_instance("test", "snot").expect("invalid binding url");

        assert!(world
            .reg
            .find_binding(&id)
            .await
            .expect("failed to communicate to registry")
            .is_some());

        // Legal Running -> Paused
        assert_eq!(
            Ok(InstanceState::Paused),
            world
                .reg
                .transition_binding(&id, InstanceState::Paused)
                .await
        );

        // Legal Paused -> Running
        assert_eq!(
            Ok(InstanceState::Running),
            world
                .reg
                .transition_binding(&id, InstanceState::Running)
                .await
        );
        // Legal Running -> Stopped
        assert_eq!(
            Ok(InstanceState::Stopped),
            world
                .reg
                .transition_binding(&id, InstanceState::Stopped)
                .await
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(InstanceState::Stopped),
            world
                .reg
                .transition_binding(&id, InstanceState::Running)
                .await
        );

        // TODO - full undeployment 'white-box' acceptance tests
        //        println!("TODO {:?}", world.repo.unpublish_binding(&id));
        let _r = world.unbind_binding(&id).await;
        //        assert!(world.repo.unpublish_binding(&id).is_ok());
        println!(
            "TODO {:?}",
            world
                .reg
                .find_binding(&id)
                .await
                .expect("failed to communicate to registry")
        );
        assert!(world
            .reg
            .find_binding(&id)
            .await
            .expect("failed to communicate to registry")
            .is_none());
    }
}
