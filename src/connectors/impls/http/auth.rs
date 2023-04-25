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
use std::io::Write;
use tremor_common::base64::{self, BASE64};

/// Authorization methods
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Auth {
    #[serde(alias = "basic")]
    Basic { username: String, password: String },
    #[serde(alias = "bearer")]
    Bearer(String),
    #[serde(alias = "elastic_api_key")]
    ElasticsearchApiKey { id: String, api_key: String },
    #[serde(alias = "gcp")]
    Gcp,
    #[serde(alias = "none")]
    None,
}

impl Auth {
    /// Prepare a HTTP autheorization header value given the auth strategy
    pub fn as_header_value(&self) -> Result<Option<String>> {
        match self {
            Auth::Gcp => {
                let t = gouth::Token::new()?;
                Ok(Some(t.header_value()?.to_string()))
            }
            Auth::Basic {
                ref username,
                ref password,
            } => {
                let encoded = base64::encode(format!("{username}:{password}"));
                Ok(Some(format!("Basic {}", &encoded)))
            }
            Auth::Bearer(token) => Ok(Some(format!("Bearer {}", &token))),
            Auth::ElasticsearchApiKey { id, api_key } => {
                let mut header_value = "ApiKey ".to_string();
                let mut writer =
                    ::base64::write::EncoderStringWriter::from_consumer(&mut header_value, &BASE64);
                write!(writer, "{id}:")?;
                write!(writer, "{api_key}")?;
                writer.into_inner(); // release the reference, so header-value is accessible again
                Ok(Some(header_value))
            }
            Auth::None => Ok(None),
        }
    }
}

impl Default for Auth {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_value_basic() {
        let auth = Auth::Basic {
            username: "badger".to_string(),
            password: "snot".to_string(),
        };
        assert_eq!(
            Ok(Some("Basic YmFkZ2VyOnNub3Q=".to_string())),
            auth.as_header_value()
        );
    }

    #[test]
    fn header_value_bearer() {
        let auth = Auth::Bearer("token".to_string());
        assert_eq!(Ok(Some("Bearer token".to_string())), auth.as_header_value());
    }

    #[test]
    fn header_value_elastic_api_key() {
        let auth = Auth::ElasticsearchApiKey {
            id: "badger".to_string(),
            api_key: "snot".to_string(),
        };
        assert_eq!(
            Ok(Some("ApiKey YmFkZ2VyOnNub3Q=".to_string())),
            auth.as_header_value()
        );
    }
}
