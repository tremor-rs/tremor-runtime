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

use crate::errors::Result;
use tremor_value::Value;
use value_trait::ValueAccess;

/// Authorization methods
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Auth {
    #[serde(alias = "basic")]
    Basic { username: String, password: String },
    #[serde(alias = "gcp")]
    Gcp,
    #[serde(alias = "none")]
    None,
}

impl From<Option<&Value<'_>>> for Auth {
    fn from(meta: Option<&Value<'_>>) -> Self {
        match meta {
            None => Auth::None,
            Some(Value::Object(basic)) => {
                if let Some(basic) = basic.get("basic") {
                    let username = basic.get_str("username");
                    let password = basic.get_str("password");
                    match (username, password) {
                        (Some(u), Some(p)) => Auth::Basic {
                            username: u.to_string(),
                            password: p.to_string(),
                        },
                        _otherwise => {
                            error!("Unable to parse basic authentication configuration");
                            Auth::None
                        }
                    }
                } else {
                    warn!("Unable to process HTTP basic authentication configuration");
                    Auth::None
                }
            }
            Some(Value::String(s)) => {
                if "gcp" == &s.to_string() {
                    Auth::Gcp
                } else {
                    error!("Invalid authentication type `{}`", &s.to_string());
                    Auth::None
                }
            }
            Some(_other) => {
                error!("Invalid authentication type");
                Auth::None
            }
        }
    }
}

impl Auth {
    /// Prepare a HTTP authentication header value given the authentication strategy
    pub fn header_value(&self) -> Result<Option<String>> {
        match *self {
            Auth::Gcp => {
                let t = gouth::Token::new()?;
                Ok(Some(t.header_value()?.to_string()))
            }
            Auth::Basic {
                ref username,
                ref password,
            } => {
                let encoded = base64::encode(&format!("{}:{}", username, password));
                Ok(Some(format!("Basic {}", &encoded)))
            }
            Auth::None => Ok(None),
        }
    }
}
