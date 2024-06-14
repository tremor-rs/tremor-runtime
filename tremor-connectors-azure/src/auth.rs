// Copyright 2024, The Tremor Team
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

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

// Azure client credentials token response. Provided for any successful token request.
#[derive(Deserialize, Serialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

// A token once resolved from the Azure auth service.
pub(crate) struct Token {
    access_token: String,
    expires_in: Instant,
}

// Encapsulation of an Azure authentication endpoint for client credentials flow types.
#[derive(Serialize, Deserialize)]
pub struct Config {
    #[serde(skip)]
    #[serde(default = "default_base_url")]
    pub(crate) base_url: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) tenant_id: String,
    #[serde(default = "default_scope")]
    pub(crate) scope: String, // NOTE Injected by specific sources/sinks on construction
    #[serde(skip)]
    token: Option<Token>,
}

// Default scope for authentication - we default this so that deserialization
// can work without a user defined scope. However by *design* consumers of the
// auth mechanism *MUST* always provide a scope.
//
// As connectors are generally tied to a specific API, the scope should never be
// user provided, but may be useful to provide for testing purposes.
fn default_scope() -> String {
    "https://login.microsoftonline.com".to_string()
}

// Default base URL for Azure authentication against microsoft online.
fn default_base_url() -> String {
    "https://login.microsoftonline.com".to_string()
}

impl Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("base_url", &self.base_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &self.client_secret)
            .field("tenant_id", &self.tenant_id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl Clone for Config {
    fn clone(&self) -> Self {
        Self {
            base_url: self.base_url.clone(),
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            tenant_id: self.tenant_id.clone(),
            scope: self.scope.clone(),
            token: None,
        }
    }
}

impl Config {
    // Mock instance for testing purposes. See tests for usage.
    #[cfg(test)]
    pub fn new_mock(mock_server: &mockito::ServerGuard) -> Self {
        Self {
            base_url: mock_server.url(),
            client_id: crate::auth::test::MOCK_CLIENT_ID.to_string(),
            client_secret: crate::auth::test::MOCK_CLIENT_SECRET.to_string(),
            tenant_id: crate::auth::test::MOCK_TENANT_ID.to_string(),
            scope: crate::auth::test::MOCK_SCOPE.to_string(),
            token: None,
        }
    }

    // Resolves or refreshes a token from the Azure auth service.
    pub(crate) async fn get_token(&mut self) -> anyhow::Result<String> {
        if self.token.is_none() || self.is_token_expired() {
            let token = self.refresh_token().await?;
            self.token = Some(token);
        }

        match self.token {
            Some(ref token) => Ok(token.access_token.clone()),
            None => Err(anyhow::anyhow!("Failed to get token")),
        }
    }

    // Refreshes a token from the Azure auth service. Used by get_token.
    async fn refresh_token(&self) -> anyhow::Result<Token> {
        let url = format!("{}/{}/oauth2/v2.0/token", self.base_url, self.tenant_id);
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("scope", &self.scope),
        ];

        let client = Client::new();
        let res = client.post(&url).form(&params).send().await;

        match res {
            Ok(res) => {
                let body_str = res.text().await?;

                let res = serde_json::from_str::<TokenResponse>(&body_str)?;

                Ok(Token {
                    access_token: res.access_token,
                    expires_in: Instant::now() + Duration::from_secs(res.expires_in),
                })
            }
            Err(e) => Err(anyhow::anyhow!("Failed to read response body: {}", e)),
        }
    }

    // Checks if the token is expired. Used by get_token.
    fn is_token_expired(&mut self) -> bool {
        if let Some(token) = &mut self.token {
            token.expires_in <= Instant::now()
        } else {
            true
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    pub(crate) const MOCK_CLIENT_ID: &str = "client_id";

    pub(crate) const MOCK_CLIENT_SECRET: &str = "client_secret";

    pub(crate) const MOCK_TENANT_ID: &str = "test_tenant_id";

    pub(crate) const MOCK_SCOPE: &str = "https://graph.microsoft.com/.default";

    pub(crate) async fn mock_auth_server() -> mockito::ServerGuard {
        let mut server = mockito::Server::new_async().await;

        server
            .mock(
                "POST",
                format!("/{MOCK_TENANT_ID}/oauth2/v2.0/token").as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"access_token": "test_access_token", "expires_in": 3600 }"#)
            .create();

        server
    }

    pub(crate) async fn mock_bad_auth_server() -> mockito::ServerGuard {
        let mut server = mockito::Server::new_async().await;

        server
            .mock(
                "POST",
                format!("/{MOCK_TENANT_ID}/oauth2/v2.0/token").as_str(),
            )
            .with_status(503)
            .with_header("content-type", "application/text")
            .with_body("I hate you, pesky human!")
            .create();

        server
    }

    #[test]
    fn clone_debug_for_coverage() {
        let config = Config {
            base_url: "base_url".to_string(),
            client_id: "client_id".to_string(),
            client_secret: "client_secret".to_string(),
            tenant_id: "tenant_id".to_string(),
            scope: "scope".to_string(),
            token: None,
        };

        let clone = config.clone();
        let as_str = format!("{clone:?}");

        assert_eq!(clone.base_url, "base_url");
        assert_eq!(clone.client_id, "client_id");
        assert_eq!(clone.client_secret, "client_secret");
        assert_eq!(clone.tenant_id, "tenant_id");
        assert_eq!(clone.scope, "scope");
        assert_eq!(as_str, "AuthConfig { base_url: \"base_url\", client_id: \"client_id\", client_secret: \"client_secret\", tenant_id: \"tenant_id\", scope: \"scope\", .. }");
    }

    #[test]
    fn deserialization_has_right_default_url() -> anyhow::Result<()> {
        let config: Config = serde_json::from_str(
            r#"{"client_id":"client_id","client_secret":"client_secret","tenant_id":"tenant_id","scope":"https://graph.microsoft.com/.default"}"#,
        )?;
        assert_eq!(config.base_url, default_base_url());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partial_failure_of_auth_endpoint() -> anyhow::Result<()> {
        let server = mock_bad_auth_server().await;
        let mut azure_oauth = Config::new_mock(&server);
        let token = azure_oauth.get_token().await;
        assert!(token.is_err());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_token_success() -> anyhow::Result<()> {
        let server = mock_auth_server().await;
        let mut azure_oauth = Config::new_mock(&server);
        let token = azure_oauth.get_token().await?;
        assert_eq!(token, "test_access_token");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_token_expired_refresh() -> anyhow::Result<()> {
        let server = mock_auth_server().await;
        let mut azure_oauth = Config::new_mock(&server);

        let ten_hours = Duration::from_secs(36_000);
        match Instant::now().checked_sub(ten_hours) {
            Some(ten_hours_ago) => {
                azure_oauth.token = Some(Token {
                    access_token: "expired_token".to_string(),
                    expires_in: ten_hours_ago,
                });
            }
            None => {
                return Err(anyhow::anyhow!("Rust hates you, pesky human"));
            }
        };

        assert!(azure_oauth.is_token_expired());

        Ok(())
    }
}
