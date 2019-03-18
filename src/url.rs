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
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use url;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum ResourceType {
    Pipeline,
    Onramp,
    Offramp,
    Binding,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Scope {
    Port,
    Type,
    Artefact,
    Servant,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct TremorURL {
    pub scope: Scope,
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
            ResourceType::Pipeline => write!(f, "pipeline"),
            ResourceType::Onramp => write!(f, "onramp"),
            ResourceType::Offramp => write!(f, "offramp"),
            ResourceType::Binding => write!(f, "binding"),
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
    pub fn from_onramp_id(id: &str) -> Result<Self> {
        TremorURL::parse(&format!("/onramp/{}", id))
    }

    pub fn parse(url: &str) -> Result<Self> {
        let (r, relative) = Self::parse_url(url, false)?;

        if let Some(parts) = r.path_segments().map(|c| c.collect::<Vec<_>>()) {
            let (scope, resource_type, artefact, instance, instance_port) = if relative {
                // TODO: This is not correct!
                match parts.as_slice() {
                    [port] => (Scope::Servant, None, None, None, Some(port.to_string())),
                    [instance, port] => (
                        Scope::Type,
                        None,
                        None,
                        Some(instance.to_string()),
                        Some(port.to_string()),
                    ),
                    [artefact, instance, port] => (
                        Scope::Artefact,
                        None,
                        Some(artefact.to_string()),
                        Some(instance.to_string()),
                        Some(port.to_string()),
                    ),
                    [resource_type, artefact, instance, port] => (
                        Scope::Port,
                        Some(decode_type(resource_type)?),
                        Some(artefact.to_string()),
                        Some(instance.to_string()),
                        Some(port.to_string()),
                    ),

                    _ => return Err(format!("Bad URL: {}", url).into()),
                }
            } else {
                match parts.as_slice() {
                    [resource_type, artefact, instance, port] => (
                        Scope::Port,
                        Some(decode_type(resource_type)?),
                        Some(artefact.to_string()),
                        Some(instance.to_string()),
                        Some(port.to_string()),
                    ),
                    [resource_type, artefact, instance] => (
                        Scope::Servant,
                        Some(decode_type(resource_type)?),
                        Some(artefact.to_string()),
                        Some(instance.to_string()),
                        None,
                    ),
                    [resource_type, artefact] => (
                        Scope::Artefact,
                        Some(decode_type(resource_type)?),
                        Some(artefact.to_string()),
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

            Ok(TremorURL {
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
    pub fn set_instance(&mut self, i: String) {
        self.instance = Some(i);
    }

    pub fn instance(&self) -> Option<String> {
        self.instance.clone()
    }
    pub fn artefact(&self) -> Option<String> {
        self.artefact.clone()
    }
    pub fn instance_port(&self) -> Option<String> {
        self.instance_port.clone()
    }
    pub fn resource_type(&self) -> Option<ResourceType> {
        self.resource_type
    }
}

impl<'de> Deserialize<'de> for TremorURL {
    fn deserialize<D>(deserializer: D) -> std::result::Result<TremorURL, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        TremorURL::parse(&s).map_err(|err| Error::custom(err.to_string()))
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
    /*
    use super::*;
    #[test]
    fn url() {
        let tremor_url =
            TremorURL::parse("tremor://127.0.0.1:1234/pipeline/main/in?format=json").unwrap();

        assert_eq!(tremor_url.relative(), false);
        assert_eq!(tremor_url.scheme(), "tremor");
        assert_eq!(tremor_url.username(), "");
        assert_eq!(tremor_url.password(), None);
        assert_eq!(tremor_url.host_str(), "127.0.0.1");
        assert_eq!(tremor_url.port(), Some(1234));
        assert_eq!(tremor_url.path(), "/pipeline/main/in");
    }

    #[test]
    fn short_url() {
        let tremor_url = TremorURL::parse("/pipeline/main/in").unwrap();

        assert_eq!(tremor_url.relative(), false);
        assert_eq!(tremor_url.scheme(), "tremor");
        assert_eq!(tremor_url.username(), "");
        assert_eq!(tremor_url.password(), None);
        assert_eq!(tremor_url.host_str(), "");
        assert_eq!(tremor_url.port(), None);
        assert_eq!(tremor_url.path(), "/pipeline/main/in");
    }

    #[test]
    fn relative_short_url() {
        let tremor_url = TremorURL::parse("pipeline/main/in").unwrap();

        assert_eq!(tremor_url.relative(), true);
        assert_eq!(tremor_url.scheme(), "tremor");
        assert_eq!(tremor_url.username(), "");
        assert_eq!(tremor_url.password(), None);
        assert_eq!(tremor_url.host_str(), "");
        assert_eq!(tremor_url.port(), None);
        assert_eq!(tremor_url.path(), "/pipeline/main/in");
    }
    */
}
