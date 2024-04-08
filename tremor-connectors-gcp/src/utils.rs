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

use gouth::Token;
use log::error;
use simd_json::OwnedValue;
use simd_json_derive::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

#[async_trait::async_trait]
pub(crate) trait ChannelFactory<
    TChannel: tonic::codegen::Service<
            http::Request<tonic::body::BoxBody>,
            Response = http::Response<tonic::transport::Body>,
        > + Clone,
>
{
    async fn make_channel(&self, connect_timeout: Duration) -> anyhow::Result<TChannel>;
}

/// Token Source
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenSrc {
    /// Enmbedded JSON
    Json(OwnedValue),
    /// File for json
    File(String),
    /// Environment variable driven`t`
    Env,
}

impl TokenSrc {
    pub(crate) fn to_token(&self) -> anyhow::Result<Token> {
        Ok(match self {
            TokenSrc::Json(json) => {
                let json = json.json_string()?;
                gouth::Builder::new().json(json).build()?
            }
            TokenSrc::File(file) => gouth::Builder::new().file(file).build()?,
            TokenSrc::Env => gouth::Token::new()?,
        })
    }
}

impl std::fmt::Debug for AuthInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GouthTokenProvider")
            .field("gouth_token", &self.gouth_token.is_some())
            .field("src", &self.src)
            .finish()
    }
}

impl Clone for AuthInterceptor {
    fn clone(&self) -> Self {
        Self {
            gouth_token: None,
            src: self.src.clone(),
        }
    }
}

impl From<TokenSrc> for AuthInterceptor {
    fn from(src: TokenSrc) -> Self {
        Self::new(src)
    }
}

pub(crate) struct AuthInterceptor {
    pub(crate) gouth_token: Option<Token>,
    pub(crate) src: TokenSrc,
}

impl AuthInterceptor {
    pub fn new(src: TokenSrc) -> Self {
        Self {
            gouth_token: None,
            src,
        }
    }
    fn get_token(&mut self) -> ::std::result::Result<Arc<String>, Status> {
        let token = if let Some(ref token) = self.gouth_token {
            token
        } else {
            let new_token = self
                .src
                .to_token()
                .map_err(|e| Status::unavailable(format!("Failed to read Google Token: {e}")))?;

            self.gouth_token.get_or_insert(new_token)
        };

        token.header_value().map_err(|e| {
            Status::unavailable(format!("Failed to read the Google Token header value: {e}"))
        })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> ::std::result::Result<Request<()>, Status> {
        let header_value = self.get_token()?;
        let metadata_value = match MetadataValue::from_str(header_value.as_str()) {
            Ok(val) => val,
            Err(e) => {
                error!("Failed to get token: {}", e);

                return Err(Status::unavailable(
                    "Failed to retrieve authentication token.",
                ));
            }
        };
        request
            .metadata_mut()
            .insert("authorization", metadata_value);

        Ok(request)
    }
}

#[cfg(test)]
pub(crate) mod tests;
