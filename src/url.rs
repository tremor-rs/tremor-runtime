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

/// FIXME - remove and complete refactoring after connector and troy branches are merged

/// Deprecated - Alias for `tremor_common::url::ResourceType`
pub type ResourceType = tremor_common::url::ResourceType;

/// Deprecated - Alias for `tremor_common::url::Scope`
pub type Scope = tremor_common::url::Scope;

/// Expose common tremor url ports
pub use tremor_common::url::ports;

/// A tremor URL identifying an entity in tremor
#[allow(clippy::module_name_repetitions)]
pub type TremorUrl = tremor_common::url::TremorUrl;

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::Result;

    // These tests are a duplicate of the tests in `tremor_common::url`
    // These are kept to assert that the lifted and shifted code hasn't
    // changed materially having been moved from the top-level to the
    // `tremor-common` repo
    //

    #[test]
    fn bad_url() {
        assert!(TremorUrl::parse("snot://").is_err())
    }

    #[test]
    fn bad_url2() {
        assert!(TremorUrl::parse("foo/bar/baz/bogo/mips/snot").is_err())
    }

    #[test]
    fn url() -> Result<()> {
        let url = TremorUrl::parse("tremor://127.0.0.1:1234/pipeline/main/01/in?format=json")?;

        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());
        Ok(())
    }

    #[test]
    fn short_url() -> Result<()> {
        let url = TremorUrl::parse("/pipeline/main/01/in")?;

        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());
        Ok(())
    }

    #[test]
    fn from_onramp_id() -> Result<()> {
        let url = TremorUrl::from_onramp_id("test")?;
        assert_eq!(Some(ResourceType::Onramp), url.resource_type());
        assert_eq!(Some("test"), url.artefact());
        Ok(())
    }

    #[test]
    fn from_offramp_id() -> Result<()> {
        let url = TremorUrl::from_offramp_id("test")?;
        assert_eq!(Some(ResourceType::Offramp), url.resource_type());
        assert_eq!(Some("test"), url.artefact());
        Ok(())
    }

    #[test]
    fn test_servant_scope() -> Result<()> {
        let url = TremorUrl::parse("in")?;
        assert_eq!(Scope::Servant, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(None, url.artefact());
        Ok(())
    }

    #[test]
    fn test_type_scope() -> Result<()> {
        let url = TremorUrl::parse("01/in")?;
        assert_eq!(Scope::Type, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(None, url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());
        Ok(())
    }

    #[test]
    fn test_artefact_scope() -> Result<()> {
        let url = TremorUrl::parse("pipe/01/in")?;
        assert_eq!(Scope::Artefact, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(Some("pipe"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());
        Ok(())
    }

    #[test]
    fn test_port_scope() -> Result<()> {
        let url = TremorUrl::parse("binding/pipe/01/in")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Binding), url.resource_type());
        assert_eq!(Some("pipe"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());

        let url = TremorUrl::parse("onramp/id/01/out")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Onramp), url.resource_type());
        assert_eq!(Some("id"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::OUT.as_ref()), url.instance_port());

        let url = TremorUrl::parse("offramp/id/01/in")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Offramp), url.resource_type());
        assert_eq!(Some("id"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());

        Ok(())
    }

    #[test]
    fn test_set_instance() -> Result<()> {
        let mut url = TremorUrl::parse("tremor://127.0.0.1:1234/pipeline/main")?;
        assert_eq!(Scope::Artefact, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(None, url.instance());
        url.set_instance("inst");
        assert_eq!(Scope::Servant, url.scope());
        assert_eq!(Some("inst"), url.instance());

        Ok(())
    }
}
