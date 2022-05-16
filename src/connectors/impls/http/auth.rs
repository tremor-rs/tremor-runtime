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

use elasticsearch::auth::{ClientCertificate, Credentials};

use crate::{
    connectors::utils::tls::{load_certs, load_keys},
    errors::Result,
};

/// Authorization methods
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Auth {
    #[serde(alias = "basic")]
    Basic { username: String, password: String },
    #[serde(alias = "bearer")]
    Bearer(String),
    #[serde(alias = "client_certificate")]
    ClientCertificate {
        /// path to a file containing one or more certificates in DER encoding that form a chain, ordered from leaf to root cert
        certificate_chain: String,
        /// path to a file containing the private key in either RSA or PKCS8 encoded form
        private_key: String,
    },
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
                let encoded = base64::encode(&format!("{}:{}", username, password));
                Ok(Some(format!("Basic {}", &encoded)))
            }
            Auth::Bearer(token) => Ok(Some(format!("Bearer {}", &token))),
            Auth::ElasticsearchApiKey { id, api_key } => {
                let mut header_value = "ApiKey ".to_string();
                base64::encode_config_buf(id, base64::STANDARD, &mut header_value);
                base64::encode_config_buf(":", base64::STANDARD, &mut header_value);
                base64::encode_config_buf(api_key, base64::STANDARD, &mut header_value);
                Ok(Some(header_value))
            }
            Auth::None | Auth::ClientCertificate { .. } => Ok(None),
        }
    }

    pub fn as_elastic_credentials(&self) -> Result<Option<Credentials>> {
        match self {
            Auth::Basic { username, password } => {
                Ok(Some(Credentials::Basic(username.clone(), password.clone())))
            }
            Auth::Bearer(token) => Ok(Some(Credentials::Bearer(token.clone()))),
            Auth::ElasticsearchApiKey { id, api_key } => Ok(Some(Credentials::ApiKey(
                id.to_string(),
                api_key.to_string(),
            ))),
            Auth::ClientCertificate {
                certificate_chain,
                private_key,
            } => {
                // stomp all the cert and private key bytes into 1 byte array
                let certs = load_certs(&std::path::Path::new(certificate_chain))?;
                let mut key = load_keys(&std::path::Path::new(private_key))?;
                let mut data = Vec::with_capacity(
                    key.0.len() + certs.iter().map(|cert| cert.0.len()).sum::<usize>(),
                );
                for mut cert in certs {
                    data.append(&mut cert.0)
                }
                data.append(&mut key.0);
                let client_certificate = ClientCertificate::Pem(data);
                Ok(Some(Credentials::Certificate(client_certificate)))
            }
            Auth::None | Auth::Gcp => Ok(None),
        }
    }
}
