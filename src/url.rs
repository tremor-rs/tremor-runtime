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
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// The type or resource the url references
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum ResourceType {
    /// This is a pipeline
    Pipeline,
    /// This is a onramp
    Onramp,
    /// This is a offramop
    Offramp,
    /// This is a binding
    Binding,
}

/// The scrope of the URL
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Scope {
    /// This URL identifies a specific port
    Port,
    /// This URL identifies a servant (a running instance)
    Servant,
    /// This URL identifes an artefact (a non running configuration)
    Artefact,
    /// This URL identifies a type of artefact
    Type,
}

/// A tremor URL identifying an entity in tremor
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct TremorURL {
    scope: Scope,
    host: String,
    resource_type: Option<ResourceType>,
    artefact: Option<String>,
    instance: Option<String>,
    instance_port: Option<String>,
}

fn decode_type(t: &str) -> Result<ResourceType> {
    match t {
        "pipeline" => Ok(ResourceType::Pipeline),
        "onramp" => Ok(ResourceType::Onramp),
        "offramp" => Ok(ResourceType::Offramp),
        "binding" => Ok(ResourceType::Binding),
        other => Err(format!("Bad Resource type: {}", other).into()),
    }
}
impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Pipeline => write!(f, "pipeline"),
            Self::Onramp => write!(f, "onramp"),
            Self::Offramp => write!(f, "offramp"),
            Self::Binding => write!(f, "binding"),
        }
    }
}

impl fmt::Display for TremorURL {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let r = write!(f, "tremor://{}", self.host);
        if let Some(resource_type) = &self.resource_type {
            write!(f, "/{}", resource_type)?;
            if let Some(artefact) = &self.artefact {
                write!(f, "/{}", artefact)?;
                if let Some(instance) = &self.instance {
                    write!(f, "/{}", instance)?;
                    if let Some(instance_port) = &self.instance_port {
                        write!(f, "/{}", instance_port)?;
                    };
                };
            };
        };
        r
    }
}

impl TremorURL {
    /// Creates an URL from a given onramp id
    pub fn from_onramp_id(id: &str) -> Result<Self> {
        Self::parse(&format!("/onramp/{}", id))
    }

    /// Creates an URL from a given offramp id
    pub fn from_offramp_id(id: &str) -> Result<Self> {
        Self::parse(&format!("/offramp/{}", id))
    }

    /// Parses a string into a Trmeor URL
    pub fn parse(url: &str) -> Result<Self> {
        let (r, relative) = Self::parse_url(url, false)?;

        if let Some(parts) = r
            .path_segments()
            .map(std::iter::Iterator::collect::<Vec<_>>)
        {
            let (scope, resource_type, artefact, instance, instance_port) = if relative {
                // TODO: This is not correct!
                match parts.as_slice() {
                    [port] => (Scope::Servant, None, None, None, Some((*port).to_string())),
                    [instance, port] => (
                        Scope::Type,
                        None,
                        None,
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),
                    [artefact, instance, port] => (
                        Scope::Artefact,
                        None,
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),
                    [resource_type, artefact, instance, port] => (
                        Scope::Port,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),

                    _ => return Err(format!("Bad URL: {}", url).into()),
                }
            } else {
                match parts.as_slice() {
                    [resource_type, artefact, instance, port] => (
                        Scope::Port,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),
                    [resource_type, artefact, instance] => (
                        Scope::Servant,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        None,
                    ),
                    [resource_type, artefact] => (
                        Scope::Artefact,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        None,
                        None,
                    ),
                    [resource_type] => (
                        Scope::Type,
                        Some(decode_type(resource_type)?),
                        None,
                        None,
                        None,
                    ),

                    _ => return Err(format!("Bad URL: {}", url).into()),
                }
            };

            let host = if let Some(host) = r.host_str() {
                host.to_owned()
            } else {
                "localhost".to_owned()
            };

            Ok(Self {
                host,
                scope,
                resource_type,
                artefact,
                instance,
                instance_port,
            })
        } else {
            Err(format!("Bad URL: {}", url).into())
        }
    }

    fn parse_url(url: &str, relative: bool) -> Result<(url::Url, bool)> {
        match url::Url::parse(url) {
            Ok(r) => Ok((r, relative)),
            Err(url::ParseError::RelativeUrlWithoutBase) => match url.get(0..1) {
                Some("/") => Self::parse_url(&format!("tremor://{}", url), false),
                _ => Self::parse_url(&format!("tremor:///{}", url), true),
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Trims the url to the instance
    pub fn trim_to_instance(&mut self) {
        self.instance_port = None;
        self.scope = Scope::Servant;
    }

    /// Trims the url to the artefact
    pub fn trim_to_artefact(&mut self) {
        self.instance_port = None;
        self.instance = None;
        self.scope = Scope::Artefact;
    }

    /// Sets the instance of the URL, will extend
    /// the scope to `Scope::Servant` if it was
    /// `Artefact` before.
    pub fn set_instance(&mut self, i: String) {
        self.instance = Some(i);
        if self.scope == Scope::Artefact {
            self.scope = Scope::Servant;
        }
    }

    /// Sets the port of the URL, will extend
    /// the scope to `Scope::Port` if it was
    /// `Servant` before.
    pub fn set_port(&mut self, i: String) {
        self.instance_port = Some(i);
        if self.scope == Scope::Servant {
            self.scope = Scope::Port;
        }
    }

    /// Retrives the instance
    pub fn instance(&self) -> Option<&str> {
        self.instance.as_ref().map(String::as_str)
    }

    /// Retrives the artefact
    pub fn artefact(&self) -> Option<&str> {
        self.artefact.as_ref().map(String::as_str)
    }
    /// Retrives the port
    pub fn instance_port(&self) -> Option<&str> {
        self.instance_port.as_ref().map(String::as_str)
    }
    /// Retrives the type
    pub fn resource_type(&self) -> Option<ResourceType> {
        self.resource_type
    }
    /// Retrives the scope
    pub fn scope(&self) -> Scope {
        self.scope
    }
}

impl<'de> Deserialize<'de> for TremorURL {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(|err| Error::custom(err.to_string()))
    }
}

impl Serialize for TremorURL {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bad_url() -> Result<()> {
        match TremorURL::parse("snot://") {
            Err(_) => Ok(()),
            Ok(_) => Err("expected a bad url".into()),
        }
    }

    #[test]
    fn bad_url2() -> Result<()> {
        match TremorURL::parse("foo/bar/baz/bogo/mips/snot") {
            Err(_) => Ok(()),
            Ok(_) => Err("expected a bad url".into()),
        }
    }

    #[test]
    fn url() -> Result<()> {
        let url = TremorURL::parse("tremor://127.0.0.1:1234/pipeline/main/01/in?format=json")
            .expect("failed to parse url");

        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());
        Ok(())
    }

    #[test]
    fn short_url() -> Result<()> {
        let url = TremorURL::parse("/pipeline/main/01/in").expect("failed to parse url");

        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());
        Ok(())
    }

    #[test]
    fn from_onramp_id() -> Result<()> {
        let url = TremorURL::from_onramp_id("test")?;
        assert_eq!(Some(ResourceType::Onramp), url.resource_type());
        assert_eq!(Some("test"), url.artefact());
        Ok(())
    }

    #[test]
    fn from_offramp_id() -> Result<()> {
        let url = TremorURL::from_offramp_id("test")?;
        assert_eq!(Some(ResourceType::Offramp), url.resource_type());
        assert_eq!(Some("test"), url.artefact());
        Ok(())
    }

    #[test]
    fn test_servant_scope() -> Result<()> {
        let url = TremorURL::parse("in")?;
        assert_eq!(Scope::Servant, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(None, url.artefact());
        Ok(())
    }

    #[test]
    fn test_type_scope() -> Result<()> {
        let url = TremorURL::parse("01/in")?;
        assert_eq!(Scope::Type, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(None, url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());
        Ok(())
    }

    #[test]
    fn test_artefact_scope() -> Result<()> {
        let url = TremorURL::parse("pipe/01/in")?;
        assert_eq!(Scope::Artefact, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(Some("pipe"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());
        Ok(())
    }

    #[test]
    fn test_port_scope() -> Result<()> {
        let url = TremorURL::parse("binding/pipe/01/in")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Binding), url.resource_type());
        assert_eq!(Some("pipe"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());

        let url = TremorURL::parse("onramp/id/01/out")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Onramp), url.resource_type());
        assert_eq!(Some("id"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("out"), url.instance_port());

        let url = TremorURL::parse("offramp/id/01/in")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Offramp), url.resource_type());
        assert_eq!(Some("id"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());

        Ok(())
    }
}
